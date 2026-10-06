package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.StringValue;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Graceful successor publication using real SQL, versioned provider bytes and runtime validation. */
public final class JournaledSuccessorPublicationProbe {
    private static final RepositoryCaller ADMIN = new RepositoryCaller("principal", true);
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final Map<String, DocumentPublicationCandidate.Mode> MODES = Map.of("a", DocumentPublicationCandidate.Mode.TYPED);

    static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        var destination = source.candidate().getPartsList().stream().filter(DocumentPublicationPart::hasReuse)
                .findFirst().orElseThrow().getReuse().getSource();
        var command = AssessmentMixedReuseProbe.command(source.candidate().toBuilder().setDestination(destination).build());
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var placements = Map.of(source.placement().drive().id(), source.placement());
        var bodies = new HashMap<DocumentUploadPayloads.Key, PartObject>();
        for (int ordinal = 0; ordinal < source.candidate().getPartsCount(); ordinal++) {
            var part = source.candidate().getParts(ordinal);
            if (part.hasUpload()) bodies.put(new DocumentUploadPayloads.Key("a", ordinal),
                    new PartObject(part.getSlot().getPart(), part.getSlot().getSubKey(),
                            source.fragments().get(ordinal).toByteArray(), part.getUpload().getSha256()));
        }
        var bootstrapBudget = new PayloadBudget(64_000_000);
        try (var first = new Host(tx, provider, observation, Duration.ofSeconds(10))) {
            var interruption = new IllegalStateException("Injected schema lookup interruption after upload");
            var resolverCalls = new AtomicInteger();
            try {
                first.sessions.execute(CALLER, command, placements, bodies, Map.of(), MODES,
                        Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())), (member, occurrence) -> {
                            resolverCalls.incrementAndGet(); throw interruption;
                        }, NONE);
                throw new AssertionError("Interrupted predecessor published");
            } catch (IllegalStateException failure) {
                require(failure == interruption, "original schema interruption propagates");
            }
            require(resolverCalls.get() == 1 && first.uploadCalls.get() > 0 && first.readCalls.get() > 0,
                    "predecessor performed real upload and retained read before resolver interruption");
            require(new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow().mutationRevision
                    == destination.getExpectedMutationRevision(), "interrupted predecessor did not publish a revision");
            var row = tx.readOnly(em -> (Object[]) em.createNativeQuery(
                    "SELECT claim_epoch,claim_token,lease_until FROM repository_execution_claims WHERE operation_id=:id")
                    .setParameter("id", command.operationId()).getSingleResult());
            var claim = new RepositoryExecutionClaimLedger.Claim(key, command.sha256(), ((Number) row[0]).longValue(),
                    (UUID) row[1], (Instant) row[2]);
            var identity = new RepositoryCoordinatorDrain.Identity(key, command.sha256(), claim.epoch(), claim.token(),
                    first.sessions.coordinatorIdentity());
            try (var loaded = new DocumentPublicationPreparationJournal(tx, bootstrapBudget).load(ADMIN, claim, 0, NONE).orElseThrow()) {
                var previous = loaded.record();
                var oldAttempt = previous.seeds().attempts().get("a");
                var oldObjects = objects(tx, oldAttempt);
                require(!oldObjects.isEmpty(), "predecessor has real uploaded objects");
                verifyBytes(provider, oldObjects);
                first.drain();
                var oldState = snapshot(tx, oldAttempt);
                // The real leases expire naturally. Never update timestamps to force recovery.
                tx.readOnly(em -> em.createNativeQuery("""
                        SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.1)
                        FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                        WHERE operation_id=:id
                        """).setParameter("id", command.operationId()).getSingleResult());
                try (var second = new Host(tx, provider, observation, LEASE)) {
                    var handoff = new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(),
                            second.sessions.coordinatorIdentity(), LEASE);
                    RepositoryCoordinatorHandoff.reserve(tx, ADMIN, handoff, NONE);
                    try (var recovered = new RepositoryReservedPreparation(tx,bootstrapBudget,
                            new SqlTimeouts(Duration.ofSeconds(1),Duration.ofSeconds(5))).load(ADMIN,CALLER,
                            new RepositoryCoordinatorReservation.Graceful(handoff),
                            new RepositoryCoordinatorReservation.OwnerIdentity(previous.predecessorGeneration()+1,previous.seeds().ownerNonce()),NONE)) {
                        var plan = RepositorySuccessorInstall.prepare(handoff, recovered.record(), LEASE, MODES);
                        RepositorySuccessorInstall.install(tx, second.budget, ADMIN, plan, NONE);
                        second.sessions.activateSuccessor(ADMIN, CALLER, plan, NONE);
                        var definition = ObservedAssessmentProbe.asset(StringValue.getDescriptor());
                        var result = second.sessions.execute(CALLER, command, Map.of(), bodies, Map.of(), MODES,
                                Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())),
                                (member, occurrence) -> definition, NONE);
                        require(result.getMembersCount() == 1 && second.uploadCalls.get() > 0 && second.readCalls.get() > 0,
                                "successor published through real upload/read and runtime validation");
                        int uploadCalls = second.uploadCalls.get(), readCalls = second.readCalls.get();
                        require(second.sessions.execute(CALLER, command, Map.of(), Map.of(), Map.of(), Map.of(), Optional.empty(),
                                (member, occurrence) -> { throw new AssertionError("Receipt replay resolved schema"); }, NONE).equals(result),
                                "exact successor receipt replay");
                        require(second.uploadCalls.get() == uploadCalls && second.readCalls.get() == readCalls,
                                "receipt replay did not access provider");
                        var nextAttempt = plan.next().seeds().attempts().get("a");
                        require(!nextAttempt.equals(oldAttempt), "successor has distinct attempt identity");
                        var newObjects = objects(tx, nextAttempt);
                        verifyBytes(provider, newObjects);
                        require(newObjects.size() == oldObjects.size(), "same declared upload count");
                        for (var a : oldObjects) for (var b : newObjects)
                            require(!a[0].equals(b[0]) && !a[2].equals(b[2]), "successor has distinct physical IDs and keys");
                        require(snapshot(tx, oldAttempt).equals(oldState), "successor leaves predecessor attempts and cleanup unchanged");
                        verifyBytes(provider, oldObjects);
                        var selected = tx.readOnly(em -> (UUID) em.createNativeQuery("""
                                SELECT attempt_id FROM document_operation_selections
                                WHERE operation_id=:id AND owner_generation=2 AND member_id='a'
                                """).setParameter("id", command.operationId()).getSingleResult());
                        require(selected.equals(nextAttempt), "generation two selects its own attempt");
                        var current = new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow();
                        // Mutation revisions use a shared sequence, not a per-document counter.
                        require(current.mutationRevision > destination.getExpectedMutationRevision()
                                && current.mutationRevision == result.getMembers(0).getMutationRevision(),
                                "successor receipt identifies the current mutation revision");
                        int commits = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM document_revision_commits WHERE operation_id=:id")
                                .setParameter("id", command.operationId()).getSingleResult()).intValue());
                        require(commits == 1, "one committed revision for this operation");
                        var revision = UUID.fromString(result.getMembers(0).getRevisionId());
                        var descriptor = tx.readOnly(em -> (String) em.createNativeQuery("""
                                SELECT encode(descriptor_sha256,'hex') FROM document_revision_schema_assets
                                WHERE revision_id=:id AND type_url=:url
                                """).setParameter("id", revision).setParameter("url", definition.metadata().getTypeUrl()).getSingleResult());
                        require(descriptor.equals(definition.metadata().getArtifactSha256()), "successor retains exact Any schema definition");
                        int bound = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                                SELECT count(*) FROM document_revision_parts p JOIN document_revision_current c USING(revision_id)
                                JOIN document_part_attempt_objects o ON p.object_id=o.physical_object_id
                                WHERE c.node_id=:node AND o.attempt_id=:attempt
                                """).setParameter("node", source.node()).setParameter("attempt", nextAttempt).getSingleResult()).intValue());
                        require(bound == newObjects.size(), "published revision references successor physical objects");
                        require(second.sessions.retainedSessions() == 0, "durable terminal replay releases successor session");
                    }
                }
            }
        }
        require(bootstrapBudget.reservedBytes() == 0, "recovery preparation reservation released");
        System.out.println("JOURNALED_SUCCESSOR_PUBLICATION_OK");
    }

    @SuppressWarnings("unchecked")
    private static List<Object[]> objects(Tx tx, UUID attempt) {
        return tx.readOnly(em -> em.createNativeQuery("""
                SELECT physical_object_id,storage_namespace,object_key,provider_version,expected_size,expected_sha256,verified
                FROM document_part_attempt_objects WHERE attempt_id=:id ORDER BY ordinal
                """).setParameter("id", attempt).getResultList());
    }
    private static void verifyBytes(AssessmentProviderProbe provider, List<Object[]> objects) {
        for (var object : objects) {
            require(Boolean.TRUE.equals(object[6]) && object[3] instanceof String version && !version.isBlank(),
                    "real verified versioned provider object");
            var bytes = provider.store().getBounded((String) object[1], (String) object[2], (String) object[3],
                    Math.toIntExact(((Number) object[4]).longValue()));
            require(DocumentPartCodec.sha256Hex(bytes.data()).equals(object[5]) && bytes.versionId().equals(object[3]),
                    "exact retained provider version and digest");
        }
    }
    private static List<String> snapshot(Tx tx, UUID attempt) {
        return tx.readOnly(em -> {
            var values = new ArrayList<String>();
            for (var table : List.of("document_part_attempts", "document_part_attempt_objects", "document_part_attempt_cleanup"))
                values.add((String) em.createNativeQuery("SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM "
                        + table + " t WHERE attempt_id=:id").setParameter("id", attempt).getSingleResult());
            return values;
        });
    }
    private static final class Host implements AutoCloseable {
        final PayloadBudget budget = new PayloadBudget(128_000_000);
        final PayloadBudget payload = new PayloadBudget(16_000_000);
        final AtomicInteger uploadCalls = new AtomicInteger(), readCalls = new AtomicInteger();
        final OpenedBlobStore opened;
        final DocumentPartReader reader;
        final DocumentReadLedger reads;
        final DocumentUploadCoordinator uploads;
        final DocumentPublicationSessions sessions;
        final Tx tx;
        final UUID readerIdentity = UUID.randomUUID();
        boolean drained;

        Host(Tx tx, AssessmentProviderProbe provider, DocumentAssessmentRuntimeObserver.Observation observation, Duration lease) {
            this.tx = tx;
            opened = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(Map.of(
                    "endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), "region", System.getenv("PROTOMOLT_TEST_S3_REGION"),
                    "path-style", "true", "conditional-writes", "true", "access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS"),
                    "secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")));
            var drives = new DriveLedger(tx);
            reads = new DocumentReadLedger(tx, readerIdentity, 1);
            reader = new DocumentPartReader((generation, profile) -> {
                require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact read backend");
                readCalls.incrementAndGet(); return opened.store();
            }, 2, 16_000_000, payload);
            uploads = new DocumentUploadCoordinator(tx, drives, budget, (generation, profile) -> {
                require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact upload backend");
                uploadCalls.incrementAndGet(); return new DocumentUploadCoordinator.Backend(provider.profile().identity(), opened);
            }, 2, Duration.ofMillis(25), new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)));
            var limits = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);
            var assessments = new DocumentPublicationAssessmentExecution(tx, drives, reads, reader, budget, limits,
                    observation, Duration.ofMinutes(2), Duration.ofSeconds(1));
            var execution = new DocumentPublicationExecution(tx, drives, reads, uploads, reader, budget, limits, false, assessments);
            sessions = DocumentPublicationSessions.journaled(tx, execution, lease, 2, 4_000_000, budget);
        }
        void drain() throws Exception {
            if (drained) return;
            var progress = sessions.drainRegistrations(Duration.ofSeconds(5), ignored -> ADMIN, NONE);
            require(progress.registrationsIdle() && progress.unresolved() == 0, "all retained registrations marked draining");
            require(sessions.awaitIdle(Duration.ZERO), "session calls drained");
            uploads.close();
            require(uploads.awaitIdle(Duration.ofSeconds(5)) && uploads.awaitProviderIdle(Duration.ofSeconds(5)), "provider calls drained");
            reader.close(); reads.closeForShutdown();
            require(reads.awaitLocalDrain(Duration.ofSeconds(5)), "read calls drained");
            reads.releaseDrained(1);
            require(reads.outstandingReads() == 0, "all locally retained read handles released");
            int pins = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_read_pins WHERE reader_incarnation=:id")
                    .setParameter("id", readerIdentity).getSingleResult()).intValue());
            require(pins == 0, "reader incarnation has no retained SQL pins");
            reads.attestLocalQuiescence();
            sessions.attestLocalDrain(ignored -> ADMIN, NONE);
            require(budget.reservedBytes() == 0 && payload.reservedBytes() == 0, "host byte reservations released");
            drained = true;
        }
        public void close() throws Exception {
            drain();
            opened.close();
        }
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
}
