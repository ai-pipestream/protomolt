package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.blob.s3.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static org.assertj.core.api.Assertions.*;

/** Admin/opaque publication fixture composing a delayed HTTP effect with graceful handoff and SQL tombstones. */
@Testcontainers
class DocumentSuccessorLatePutIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    static final String GENERATION = "late-successor", BUCKET = "late-successor";
    private static final Duration LEASE = Duration.ofMinutes(5);

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void tombstoneReclaimsRemotePredecessorPutWithoutChangingPublishedSuccessor(boolean graceful) throws Exception {
        try (var c = context(POSTGRES); var direct = opened(null, Duration.ofSeconds(10));
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = c.tx();
            direct.ensureNamespace(BUCKET);
            try (var admin = client(null, Duration.ofSeconds(10))) {
                admin.putBucketVersioning(b -> b.bucket(BUCKET).versioningConfiguration(v -> v.status("Enabled")));
            }
            var profile = new ManagedBackendLedger.Profile(S3BackendIdentity.of(S3.getEndpoint().toString(), S3.getRegion(), true), "late-realm");
            new ManagedBackendLedger(tx).bind(GENERATION, profile);
            var input = input(tx, profile);
            var selectedBackend = new AtomicReference<>(direct);
            var bootstrap = new PayloadBudget(64_000_000);
            try (var first = new Host(tx, profile, selectedBackend, Duration.ofSeconds(10))) {
                first.attestOnClose = graceful;
                var cancelledAfterOwner = new RepositoryReadControl() {
                    public boolean isCancelled() {
                        return tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM repository_operation_owners WHERE operation_id=:id")
                                .setParameter("id", input.command().operationId()).getSingleResult()).intValue()) != 0;
                    }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                assertThatThrownBy(() -> execute(first, input, cancelledAfterOwner)).isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                var key = new RepositoryOperationLedger.Key("account", "principal", input.command().operationId());
                var claimRow = tx.readOnly(em -> (Object[]) em.createNativeQuery(
                        "SELECT claim_token,lease_until FROM repository_execution_claims WHERE operation_id=:id")
                        .setParameter("id", key.operationId()).getSingleResult());
                var claim = new RepositoryExecutionClaimLedger.Claim(key, input.command().sha256(), 1, (UUID) claimRow[0], (Instant) claimRow[1]);
                var identity = new RepositoryCoordinatorDrain.Identity(key, input.command().sha256(), 1, claim.token(), first.sessions.coordinatorIdentity());
                try (var loaded = new DocumentPublicationPreparationJournal(tx, bootstrap).load(ADMIN, claim, 0, NONE).orElseThrow()) {
                    var previous = loaded.record();
                    var oldAttempt = previous.seeds().attempts().get("a");
                    var oldObject = previous.prepare().plan().members().getFirst().attempt().orElseThrow().uploads().getFirst().object();
                    try (var gateway = new DelayedS3PutGateway(S3.getEndpoint(), "/" + BUCKET + "/" + oldObject.objectKey());
                         var delayed = opened(gateway.endpoint(), Duration.ofSeconds(1))) {
                        selectedBackend.set(delayed);
                        var call = workers.submit(() -> execute(first, input, NONE));
                        gateway.awaitCaptured();
                        assertThatThrownBy(() -> call.get(5, TimeUnit.SECONDS))
                                .isInstanceOfSatisfying(ExecutionException.class, failure -> {
                                    var causes = new ArrayList<Throwable>();
                                    for (Throwable cause = failure; cause != null; cause = cause.getCause()) causes.add(cause);
                                    assertThat(causes).anyMatch(ApiCallTimeoutException.class::isInstance);
                                });
                        assertThat(call.isDone()).isTrue();
                        gateway.disconnectClient();
                        if (graceful) first.drain();
                        int drains = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                                SELECT count(*) FROM repository_coordinator_drains d
                                JOIN repository_coordinator_local_drains l USING(account_id,principal,operation_id,claim_epoch,claim_token,incarnation)
                                WHERE d.operation_id=:id AND d.claim_epoch=1 AND d.claim_token=:token AND d.incarnation=:host
                                """).setParameter("id", key.operationId()).setParameter("token", claim.token())
                                .setParameter("host", first.sessions.coordinatorIdentity()).getSingleResult()).intValue());
                        assertThat(drains).isEqualTo(graceful ? 1 : 0);
                        if (!graceful) assertNoCoordinatorDrain(tx, key.operationId());
                        assertThat(first.uploads.providerActivity().active()).isZero();
                        delayed.close(); // The repository's old SDK is gone; the intermediary still owns the original request.
                        assertThat(first.uploads.providerActivity().active()).isZero();
                        assertThat(gateway.requests()).isEqualTo(1);
                        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN, input.command()).state())
                                .isEqualTo(DocumentPublicationReplay.State.PENDING);
                        assertUnverified(tx, oldAttempt);
                        assertThatThrownBy(() -> direct.store().get(BUCKET, oldObject.objectKey()))
                                .isInstanceOf(BlobStore.BlobNotFoundException.class);
                        // Await actual lease expiry, including the selected attempt's last heartbeat.
                        tx.readOnly(em -> em.createNativeQuery("""
                                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                                  (GREATEST(c.lease_until,o.lease_until,a.lease_until)-clock_timestamp())))+0.1)
                                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                                JOIN document_part_attempts a ON a.attempt_id=:attempt WHERE c.operation_id=:id
                                """).setParameter("id", key.operationId()).setParameter("attempt", oldAttempt).getSingleResult());
                        try (var second = new Host(tx, profile, new AtomicReference<>(direct), LEASE)) {
                            RepositoryCoordinatorReservation.Proposal reservation;
                            if (graceful) {
                                var handoff = new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(), second.sessions.coordinatorIdentity(), LEASE);
                                RepositoryCoordinatorHandoff.reserve(tx, ADMIN, handoff, NONE);
                                reservation = new RepositoryCoordinatorReservation.Graceful(handoff);
                            } else {
                                var owner = tx.readOnly(em -> (Object[]) em.createNativeQuery(
                                        "SELECT owner_generation,owner_token FROM repository_operation_owners WHERE operation_id=:id")
                                        .setParameter("id", key.operationId()).getSingleResult());
                                var expired = new RepositoryCoordinatorReservation.ExpiredUnquiesced(identity, UUID.randomUUID(),
                                        second.sessions.coordinatorIdentity(), LEASE, new RepositoryCoordinatorReservation.OwnerIdentity(
                                        ((Number) owner[0]).longValue(), (UUID) owner[1]));
                                RepositoryCoordinatorExpiration.reserve(tx, ADMIN, expired, NONE);
                                reservation = expired;
                                // The old manager is still open; retry must fail at the stale claim, before any provider call.
                                assertThatThrownBy(() -> execute(first, input, NONE))
                                        .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
                                assertThat(gateway.requests()).isEqualTo(1);
                            }
                            // Both paths reload under reserved read authority without stamping an execution fence.
                            try (var recovered = new RepositoryReservedPreparation(tx,bootstrap,
                                    new SqlTimeouts(Duration.ofSeconds(1),Duration.ofSeconds(5))).load(ADMIN,ADMIN,reservation,
                                    new RepositoryCoordinatorReservation.OwnerIdentity(previous.predecessorGeneration()+1,previous.seeds().ownerNonce()),NONE)) {
                                var plan = RepositorySuccessorInstall.prepare(reservation, recovered.record(), LEASE,
                                        Map.of("a", DocumentPublicationCandidate.Mode.OPAQUE));
                                RepositorySuccessorInstall.install(tx, second.budget, ADMIN, plan, NONE);
                                second.sessions.activateSuccessor(ADMIN, ADMIN, plan, NONE);
                                var result = execute(second, input, NONE);
                                var newAttempt = plan.next().seeds().attempts().get("a");
                                var newObject = plan.next().prepare().plan().members().getFirst().attempt().orElseThrow().uploads().getFirst().object();
                                assertThat(newAttempt).isNotEqualTo(oldAttempt);
                                assertThat(newObject.objectKey()).isNotEqualTo(oldObject.objectKey());
                                var version = tx.readOnly(em -> (String) em.createNativeQuery(
                                        "SELECT provider_version FROM document_part_attempt_objects WHERE attempt_id=:id AND verified")
                                        .setParameter("id", newAttempt).getSingleResult());
                                assertThat(direct.store().getBounded(BUCKET, newObject.objectKey(), version, input.body().bytes().length).data())
                                        .containsExactly(input.body().bytes());
                                int referenced = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                                        SELECT count(*) FROM document_revision_parts p JOIN document_part_attempt_objects o ON p.object_id=o.physical_object_id
                                        WHERE p.revision_id=:revision AND o.attempt_id=:attempt
                                        """).setParameter("revision", UUID.fromString(result.getMembers(0).getRevisionId()))
                                        .setParameter("attempt", newAttempt).getSingleResult()).intValue());
                                assertThat(referenced).isEqualTo(1);
                                var recovery = new DocumentAttemptRecovery(new DocumentAttemptCleanupLedger(tx), (generation, retained) -> {
                                    assertThat(generation).isEqualTo(GENERATION);
                                    assertThat(retained).isEqualTo(profile);
                                    return direct.reclaimer();
                                });
                                recover(recovery, oldAttempt);
                                assertAbsentTombstone(tx, oldAttempt);
                                var delivered = gateway.forwardTo(S3.getEndpoint());
                                assertThat(delivered.status()).isEqualTo(200);
                                assertThat(delivered.version()).isNotBlank();
                                assertThat(direct.store().getBounded(BUCKET, oldObject.objectKey(), delivered.version(), input.body().bytes().length).data())
                                        .containsExactly(input.body().bytes());
                                assertUnverified(tx, oldAttempt);
                                assertThat(new DocumentAttemptCleanupLedger(tx).candidates(Duration.ZERO, 100, GENERATION)).contains(oldAttempt);
                                recover(recovery, oldAttempt);
                                assertAbsentTombstone(tx, oldAttempt);
                                assertUnverified(tx, oldAttempt);
                                assertThatThrownBy(() -> direct.store().get(BUCKET, oldObject.objectKey(), delivered.version()))
                                        .isInstanceOf(BlobStore.BlobNotFoundException.class);
                                assertThat(direct.store().getBounded(BUCKET, newObject.objectKey(), version, input.body().bytes().length).data())
                                        .containsExactly(input.body().bytes());
                                assertThat(second.sessions.execute(ADMIN, input.command(), Map.of(), Map.of(), Map.of(), Map.of(), Optional.empty(),
                                        (member, occurrence) -> { throw new AssertionError("Receipt replay must not resolve schemas"); }, NONE)).isEqualTo(result);
                                assertThat(gateway.requests()).isEqualTo(1);
                                if (!graceful) {
                                    assertNoCoordinatorDrain(tx, key.operationId());
                                }
                            }
                        }
                    }
                }
            }
            assertThat(bootstrap.reservedBytes()).isZero();
        }
    }

    private static void recover(DocumentAttemptRecovery recovery, UUID attempt) {
        var result = recovery.recover(attempt, Duration.ofSeconds(10));
        assertThat(result.failure()).isNull();
        assertThat(result.outcome()).isEqualTo(DocumentAttemptRecovery.Outcome.ABSENT);
    }
    private static void assertNoCoordinatorDrain(Tx tx, UUID operation) {
        for (var table : java.util.List.of("repository_coordinator_drains", "repository_coordinator_local_drains")) {
            int count = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                    + " WHERE operation_id=:id AND claim_epoch=1").setParameter("id", operation).getSingleResult()).intValue());
            assertThat(count).as(table).isZero();
        }
    }
    private static void assertUnverified(Tx tx, UUID attempt) {
        var counts = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT count(*), count(*) FILTER (WHERE verified), count(provider_version)
                FROM document_part_attempt_objects WHERE attempt_id=:id
                """).setParameter("id", attempt).getSingleResult());
        assertThat(((Number) counts[0]).intValue()).isEqualTo(1);
        assertThat(((Number) counts[1]).intValue()).isZero();
        assertThat(((Number) counts[2]).intValue()).isZero();
        int references = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT count(*) FROM document_revision_parts p JOIN document_part_attempt_objects o ON p.object_id=o.physical_object_id
                WHERE o.attempt_id=:id
                """).setParameter("id", attempt).getSingleResult()).intValue());
        assertThat(references).isZero();
    }
    private static void assertAbsentTombstone(Tx tx, UUID attempt) {
        var state = tx.readOnly(em -> (String) em.createNativeQuery("SELECT state FROM document_part_attempt_cleanup WHERE attempt_id=:id")
                .setParameter("id", attempt).getSingleResult());
        assertThat(state).isEqualTo("ABSENT");
    }
    record Input(DocumentPublicationCommand command, Map<UUID, DocumentUploadPlan.Placement> placements, PartObject body) {}
    static Input input(Tx tx, ManagedBackendLedger.Profile profile) {
        return input(tx, profile, DocumentSecurity.getDefaultInstance());
    }
    static Input input(Tx tx, ManagedBackendLedger.Profile profile, DocumentSecurity security) {
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account";
        drive.name = "late"; drive.bucket = BUCKET; drive.prefix = "root"; drive.provider = "s3"; drive.driveType = "CUSTOM"; drive.status = "ACTIVE";
        new DriveLedger(tx).insert(drive);
        var ownership = OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source").setSecurity(security).build();
        var parts = DocumentPartCodec.split(Document.newBuilder().setDocId("late-document").setOwnership(ownership).build(), PartLayouts.document());
        assertThat(parts).hasSize(1);
        var body = parts.getFirst();
        var member = DocumentPublicationMember.newBuilder().setMemberId("a").setDriveId(drive.driveId.toString()).setOwnership(ownership)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId("account").setDocId("late-document").setGraphId("graph").setGraphAddressId("node")))
                .addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(body.part()).setSubKey(body.subKey()))
                        .setUpload(PublicationUpload.newBuilder().setSizeBytes(body.bytes().length).setSha256(body.sha256()).setContentType("application/protobuf")));
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED).setAnyResolvedSchema(true)
                .setValidationProfile("protomolt-retained-schema-admission/v1")
                .setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(100).setMaxFragmentBytes(4_000_000)
                        .setMaxRoots(100).setMaxEvidenceBytes(4_000_000).setMaxBindings(20).setMaxRetainedBytes(16_000_000).setMaxDecodedBytes(1_000_000))
                .build(), () -> {});
        new DocumentSchemaPolicies(tx).activate(policy, 0, () -> {});
        return new Input(command, Map.of(drive.driveId, DocumentUploadPlan.Placement.sample(drive, GENERATION, profile)), body);
    }
    static DocumentPublicationResult execute(Host host, Input input, RepositoryReadControl control) throws Exception {
        return host.sessions.execute(ADMIN, input.command(), input.placements(), Map.of(new DocumentUploadPayloads.Key("a", 0), input.body()),
                Map.of(), Map.of("a", DocumentPublicationCandidate.Mode.OPAQUE), Optional.empty(),
                (member, occurrence) -> { throw new AssertionError("Opaque fixture must not resolve schemas"); }, control);
    }
    private static S3Client client(URI proxy, Duration timeout) {
        var http = UrlConnectionHttpClient.builder().connectionTimeout(Duration.ofSeconds(1)).socketTimeout(Duration.ofSeconds(5));
        if (proxy != null) http.proxyConfiguration(c -> c.endpoint(proxy).nonProxyHosts(Set.of())
                .useSystemPropertyValues(false).useEnvironmentVariablesValues(false));
        return S3Client.builder().endpointOverride(S3.getEndpoint()).region(Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .serviceConfiguration(c -> c.chunkedEncodingEnabled(false).expectContinueEnabled(false))
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED).httpClientBuilder(http)
                .overrideConfiguration(c -> c.apiCallTimeout(timeout).retryStrategy(r -> r.maxAttempts(1))).build();
    }
    private static OpenedBlobStore opened(URI proxy, Duration timeout) {
        var sdk = client(proxy, timeout);
        return new OpenedBlobStore(new S3BlobStore(sdk), sdk::close,
                Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.BOUNDED_READ, BlobCapability.PHYSICAL_RECLAMATION),
                new S3NamespaceProvisioner(sdk), new S3ObjectReclaimer(sdk));
    }
    static final class Host implements AutoCloseable {
        final PayloadBudget budget = new PayloadBudget(128_000_000);
        final DocumentReadLedger reads;
        final DocumentPartReader reader;
        final DocumentUploadCoordinator uploads;
        final DocumentPublicationSessions sessions;
        boolean drained;
        boolean attestOnClose = true;
        Host(Tx tx, ManagedBackendLedger.Profile profile, AtomicReference<OpenedBlobStore> backend, Duration lease) {
            var drives = new DriveLedger(tx);
            reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
            reader = new DocumentPartReader((generation, selected) -> {
                assertThat(generation).isEqualTo(GENERATION); assertThat(selected).isEqualTo(profile); return backend.get().store();
            }, 1, 4_000_000, budget);
            uploads = new DocumentUploadCoordinator(tx, drives, budget, (generation, selected) -> {
                assertThat(generation).isEqualTo(GENERATION); assertThat(selected).isEqualTo(profile);
                return new DocumentUploadCoordinator.Backend(profile.identity(), backend.get());
            }, 1, Duration.ofMillis(25), new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)));
            var execution = new DocumentPublicationExecution(tx, drives, reads, uploads, reader, budget,
                    new DocumentRevisionAssembly.Limits(4_000_000, 100, 100, 100, 1_000_000), false);
            sessions = DocumentPublicationSessions.journaled(tx, execution, lease, 2, 4_000_000, budget);
        }
        void drain() throws Exception {
            if (drained) return;
            var progress = sessions.drainRegistrations(Duration.ofSeconds(5), ignored -> ADMIN, NONE);
            assertThat(progress.registrationsIdle()).isTrue(); assertThat(progress.unresolved()).isZero();
            assertThat(sessions.awaitIdle(Duration.ZERO)).isTrue();
            uploads.close(); assertThat(uploads.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(uploads.awaitProviderIdle(Duration.ofSeconds(5))).isTrue();
            reader.close(); reads.closeForShutdown(); assertThat(reads.awaitLocalDrain(Duration.ofSeconds(5))).isTrue();
            reads.releaseDrained(1); assertThat(reads.outstandingReads()).isZero(); reads.attestLocalQuiescence();
            sessions.attestLocalDrain(ignored -> ADMIN, NONE);
            assertThat(budget.reservedBytes()).isZero(); drained = true;
        }
        public void close() throws Exception {
            if (attestOnClose) { drain(); return; }
            // Test resource cleanup only: no V90/V91 marker and no reader-quiescence claim.
            sessions.close(); assertThat(sessions.awaitIdle(Duration.ofSeconds(5))).isTrue();
            uploads.close(); assertThat(uploads.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(uploads.awaitProviderIdle(Duration.ofSeconds(5))).isTrue();
            reader.close(); reads.closeForShutdown(); assertThat(reads.awaitLocalDrain(Duration.ofSeconds(5))).isTrue();
            reads.releaseDrained(1);
            assertThat(reads.outstandingReads()).isZero(); assertThat(budget.reservedBytes()).isZero();
        }
    }
}
