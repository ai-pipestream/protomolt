package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.AuthenticatedCallerResolver;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.CredentialBinding;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.service.DocumentPublicationGrpcService;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.DocumentPublicationInput;
import ai.protomolt.proto.repo.spi.DocumentPublicationRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Scoped publication authorization parity driver on the production JAR host:
 * one scenario driver exercised through the library facade and an authenticated
 * in-process gRPC channel in front of the same production publisher, against
 * real PostgreSQL and versioned S3. Provisioning uses the internal process-only
 * credential and grant ports; scoped calls never use the operator token or
 * process authority. Every scoped invocation is checked at the repository boundary
 * against the exact provisioned identity (principal, issuer, credential ID and
 * generation) expected for that invocation. Assertions cover receipts, durable SQL
 * state, recorded provider versions and observed adapter write calls. Runs without
 * JUnit; prints one marker per qualified scenario.
 */
public final class ScopedPublicationProbe {
    private static final String GENERATION = "scoped-parity";
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final long GRANT_LIFETIME_MICROS = TimeUnit.MINUTES.toMicros(5);
    private static final RepositoryCaller ADMIN = new RepositoryCaller("operator", true);
    private static final DocumentSecurity PUBLIC_READ_WRITE = DocumentSecurity.newBuilder()
            .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))
            .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_WRITE)).build();
    private static final DocumentSecurity PUBLIC_READ = DocumentSecurity.newBuilder()
            .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ)).build();
    private static final DocumentRevisionAssembly.Limits LIMITS =
            new DocumentRevisionAssembly.Limits(1_000_000, 100, 100, 100, 100_000);

    private ScopedPublicationProbe() {}

    public static void main(String[] args) throws Exception {
        require(args.length == 1, "usage: ScopedPublicationProbe <admission-runtime-bundle>");
        var bundle = Path.of(args[0]);
        String run = System.getenv("PROTOMOLT_TEST_PROBE_RUN");
        require(run != null && !run.isBlank(), "the driver supplies a fresh run identifier");
        System.out.println("SCOPED_PUBLICATION_RUN " + run);
        try (var env = Environment.open()) {
            for (var via : Invocation.values()) {
                validTypedPublication(env, bundle, via);
                validOpaquePublication(env, bundle, via);
                samePrincipalDifferentKey(env, bundle, via);
                rotatedCredentialRefusals(env, bundle, via);
                unboundPrincipalRefused(env, bundle, via);
                wrongAccountRefused(env, bundle, via);
                changedCommandRefused(env, bundle, via);
                mixedBatchNeverPartiallyCommits(env, bundle, via);
                grantRevocationSemantics(env, bundle, via);
                grantExpiryBlocksCreation(env, bundle, via);
                rotationRefusesCommittedReplay(env, bundle, via);
                readRevocationBlocksDelivery(env, bundle, via);
                revocationWhileUploadHeld(env, bundle, via);
                finalCommitWinsAndRevocationWaits(env, bundle, via);
                twoOperationsShareOneKey(env, bundle, via);
                exactRetryIdentity(env, bundle, via);
                contractViolationIsNotAuthorization(env, bundle, via);
                substitutedBindingIsDetected(env, bundle, via);
            }
            transportFailuresFailClosed(env, bundle);
            for (var via : Invocation.values()) {
                for (String ordering : new String[]{"revoked", "held-live", "expired"}) {
                    finalGrantCheckWaitsForRevokerOutcome(env, bundle, via, ordering);
                }
            }
        }
        System.out.println("SCOPED_PUBLICATION_PARITY_OK");
    }

    enum Invocation { LIBRARY, GRPC }

    @FunctionalInterface private interface Call { Object run() throws Exception; }

    private static Object outcome(Call call) {
        try { return call.run(); }
        catch (Throwable failure) { return failure; }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void requireEquals(Object expected, Object actual, String message) {
        if (!java.util.Objects.equals(expected, actual))
            throw new AssertionError(message + ": expected <" + expected + "> but was <" + actual + ">");
    }

    private static RepositoryException.Code codeOf(Invocation via, Throwable failure) {
        if (via == Invocation.LIBRARY) {
            if (failure instanceof RepositoryException repository) return repository.code();
            if (failure instanceof IllegalArgumentException) return RepositoryException.Code.INVALID_ARGUMENT;
            return RepositoryException.Code.INTERNAL;
        }
        if (!(failure instanceof StatusRuntimeException status)) return RepositoryException.Code.INTERNAL;
        return switch (status.getStatus().getCode()) {
            case NOT_FOUND -> RepositoryException.Code.NOT_FOUND;
            case PERMISSION_DENIED -> RepositoryException.Code.PERMISSION_DENIED;
            case UNAUTHENTICATED -> RepositoryException.Code.UNAUTHENTICATED;
            case INVALID_ARGUMENT -> RepositoryException.Code.INVALID_ARGUMENT;
            case FAILED_PRECONDITION -> RepositoryException.Code.FAILED_PRECONDITION;
            case ABORTED -> RepositoryException.Code.CONFLICT;
            case UNAVAILABLE -> RepositoryException.Code.UNAVAILABLE;
            case RESOURCE_EXHAUSTED -> RepositoryException.Code.RESOURCE_EXHAUSTED;
            case DATA_LOSS -> RepositoryException.Code.DATA_LOSS;
            case CANCELLED -> RepositoryException.Code.CANCELLED;
            case DEADLINE_EXCEEDED -> RepositoryException.Code.DEADLINE_EXCEEDED;
            case UNIMPLEMENTED -> RepositoryException.Code.UNSUPPORTED;
            default -> RepositoryException.Code.INTERNAL;
        };
    }

    private static void requireRefusal(Host host, Invocation via, RepositoryException.Code code, Call call, String message) throws Exception {
        var writes = host.writes.snapshot();
        var result = outcome(call);
        requireEquals(writes, host.writes.snapshot(), message + ": no new provider write calls or returns");
        if (!(result instanceof RuntimeException failure))
            throw new AssertionError(message + ": expected " + code + " refusal but completed with " + result);
        var actual = codeOf(via, failure);
        if (actual != code) throw new AssertionError(message + ": expected " + code + " but was " + actual, failure);
    }

    private static NodeAddress destination(DocumentPublicationCommand command) {
        return command.intent().getMembers(0).getDestination().getAddress();
    }

    // ---------- scenarios (matrix rows; each runs on both invocation paths) ----------

    /** Valid scoped initial publication, exact grant, valid typed content: committed revision, matching receipt, retained schema, exact replay. */
    private static void validTypedPublication(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, true)) {
            host.openRuntime();
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            var response = host.publish(via, key, "principal", host.request());
            host.requireCommitted(response, host.command(), "principal");
            requireEquals(1L, host.successRows(host.command()), "typed success rows");
            requireEquals((long) host.bodies().size(), host.recordedVersions(host.command()), "typed recorded versions");
            require(host.documentExists(destination(host.command())), "typed destination committed");
            requireEquals(1, host.schemaResolutions.get(), "typed schema resolutions");
            host.requireExactReadback(host.libraryCaller(key, "principal"), response.getCommitted().getMembers(0), host.bodies(), true);
            requireEquals(response, host.publish(via, key, "principal", host.request()), "same-path typed replay");
            var other = via == Invocation.LIBRARY ? Invocation.GRPC : Invocation.LIBRARY;
            requireEquals(response, host.publish(other, key, "principal", host.request()), "cross-path typed replay");
            requireEquals(1L, host.successRows(host.command()), "typed replay added no success row");
            requireEquals((long) host.bodies().size(), host.recordedVersions(host.command()), "typed replay added no recorded versions");
            requireEquals(1, host.schemaResolutions.get(), "typed replay resolved no schemas");
            host.requireScopedCallersOnly("principal");
        }
        System.out.println("SCOPED_PUBLICATION_TYPED_" + via + "_OK");
    }

    /** The opaque leg of the positive case with real provider writes and exact replay. */
    private static void validOpaquePublication(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            var response = host.publish(via, key, "principal", host.request());
            host.requireCommitted(response, host.command(), "principal");
            requireEquals(0, host.schemaResolutions.get(), "opaque schema resolutions");
            host.requireExactReadback(host.libraryCaller(key, "principal"), response.getCommitted().getMembers(0), host.bodies(), false);
            var published = response.getCommitted().getMembers(0);
            var invalid = outcome(() -> {
                host.history().readValidated(host.libraryCaller(key, "principal"), published.getAddress(),
                        UUID.fromString(published.getRevisionId()), RepositoryReadControl.NONE);
                return null;
            });
            require(invalid instanceof RepositoryException failure
                            && failure.code() == RepositoryException.Code.FAILED_PRECONDITION,
                    "opaque member has no retained typed admission: " + invalid);
            var other = via == Invocation.LIBRARY ? Invocation.GRPC : Invocation.LIBRARY;
            requireEquals(response, host.publish(other, key, "principal", host.request()), "cross-path opaque replay");
            requireEquals(1L, host.successRows(host.command()), "opaque replay added no success row");
            host.requireScopedCallersOnly("principal");
        }
        System.out.println("SCOPED_PUBLICATION_OPAQUE_" + via + "_OK");
    }

    /** Two provisioned keys for one principal carry distinct identities; the second key cannot use the first key's grant or replay its operation. */
    private static void samePrincipalDifferentKey(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var keyA = host.registerKey("principal");
            var keyB = host.registerKey("principal");
            require(!keyB.binding().credentialId().equals(keyA.binding().credentialId()),
                    "two keys for one principal must provision distinct identities");
            host.grantAccounts("principal");
            host.installGrant(keyA, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, keyB, "principal", host.request()), "second key cannot use first key's grant");
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, keyB, "principal", host.request()), "exact refusal retry stays refused");
            requireEquals(0L, host.successRows(host.command()), "refusal left no success row");
            requireEquals(0L, host.recordedVersions(host.command()), "refusal left no recorded versions");
            requireEquals(0L, host.selectedAttempts(host.command()), "refusal left no selected attempts");
            require(!host.documentExists(destination(host.command())), "refusal created no document");
            var response = host.publish(via, keyA, "principal", host.request());
            host.requireCommitted(response, host.command(), "principal");
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, keyB, "principal", host.request()), "second key cannot replay first key's operation");
            requireEquals(response, host.publish(via, keyA, "principal", host.request()), "first key replays its receipt");
            requireEquals(1L, host.successRows(host.command()), "still one success row");
            host.requireScopedCallersOnly("principal");
        }
        System.out.println("SCOPED_PUBLICATION_KEY_SEPARATION_" + via + "_OK");
    }

    /** Rotation fences the old generation at the durable check and never transfers an unfinished grant. */
    private static void rotatedCredentialRefusals(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var keyV1 = host.registerKey("principal");
            host.grantAccounts("principal");
            host.installGrant(keyV1, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            var keyV2 = host.retoken("principal", host.rotate(keyV1, "principal"));
            requireRefusal(host, via, RepositoryException.Code.UNAUTHENTICATED,
                    () -> host.publish(via, keyV1, "principal", host.request()), "rotated old generation is not live");
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, keyV2, "principal", host.request()), "rotation did not transfer the grant");
            requireEquals(0L, host.successRows(host.command()), "rotation refusals left no success row");
            requireEquals(0L, host.recordedVersions(host.command()), "rotation refusals left no recorded versions");
            require(!host.documentExists(destination(host.command())), "rotation refusals created no document");
        }
        System.out.println("SCOPED_PUBLICATION_ROTATION_" + via + "_OK");
    }

    /** A principal-only caller (no provisioned key identity) cannot use the grant even with account membership. */
    private static void unboundPrincipalRefused(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            var unbound = host.mintUnbound("principal");
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, unbound, "principal", host.request()), "unbound principal cannot use the grant");
            requireEquals(0L, host.successRows(host.command()), "unbound refusal left no success row");
            requireEquals(0L, host.recordedVersions(host.command()), "unbound refusal left no recorded versions");
            require(!host.documentExists(destination(host.command())), "unbound refusal created no document");
        }
        System.out.println("SCOPED_PUBLICATION_UNBOUND_" + via + "_OK");
    }

    /** A supplied account name in the request is not membership; the host-narrowed caller loses the grant check. */
    private static void wrongAccountRefused(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            host.grantAccounts("principal", Set.of("elsewhere-" + UUID.randomUUID()));
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, key, "principal", host.request()), "a supplied account name is not membership");
            requireEquals(0L, host.successRows(host.command()), "account refusal left no success row");
            requireEquals(0L, host.recordedVersions(host.command()), "account refusal left no recorded versions");
            require(!host.documentExists(destination(host.command())), "account refusal created no document");
        }
        System.out.println("SCOPED_PUBLICATION_ACCOUNT_" + via + "_OK");
    }

    /** The grant binds the exact canonical command; a changed member digest refuses the same operation id. */
    private static void changedCommandRefused(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            var changed = new DocumentPublicationCommand(host.command().intent().toBuilder().setMembers(0,
                    host.command().intent().getMembers(0).toBuilder().setDestination(
                            host.command().intent().getMembers(0).getDestination().toBuilder().setAddress(
                                    host.command().intent().getMembers(0).getDestination().getAddress().toBuilder()
                                            .setDocId("changed-" + UUID.randomUUID())))).build());
            var changedRequest = request(changed, host.bodies(), Map.of("member-0", false));
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, key, "principal", changedRequest), "changed command digest refused");
            requireEquals(0L, host.successRows(changed), "changed command left no success row");
            requireEquals(0L, host.recordedVersions(changed), "changed command left no recorded versions");
            require(!host.documentExists(destination(changed)), "changed command created no document");
            host.requireCommitted(host.publish(via, key, "principal", host.request()), host.command(), "principal");
        }
        System.out.println("SCOPED_PUBLICATION_COMMAND_" + via + "_OK");
    }

    /** A grant waives neither source READ nor existing-target WRITE; a mixed batch never partially commits. */
    private static void mixedBatchNeverPartiallyCommits(Environment env, Path bundle, Invocation via) throws Exception {
        for (String denial : new String[]{"source-read", "target-write"}) {
            boolean targetWrite = denial.equals("target-write");
            var seedPolicy = targetWrite ? PUBLIC_READ : DocumentSecurity.getDefaultInstance();
            try (var host = Host.prepare(env, bundle, false)) {
                host.openRuntime();
                host.openTransport(4);
                // Seed the denied source/existing target through the real publisher with process authority.
                var seeded = uploadCommand(host.account, host.drive, false, "seed-" + UUID.randomUUID(), seedPolicy);
                var seedResponse = host.publishAsOperator(seeded.request());
                host.requireCommitted(seedResponse, seeded.command(), "operator");
                var seedAddress = destination(seeded.command());
                var seedRow = new DocumentLedger(env.tx).findByNodeId(DocumentIds.nodeId(seedAddress)).orElseThrow();
                var seedRevision = UUID.fromString(seedResponse.getCommitted().getMembers(0).getRevisionId());
                var seedParts = seededParts(env.tx, seeded, seedRevision, env);
                var sourceCondition = DocumentRevisionCondition.newBuilder().setAddress(seedAddress)
                        .setExpectedMutationRevision(seedRow.mutationRevision).build();
                var member1 = DocumentPublicationMember.newBuilder().setMemberId("member-1").setDriveId(host.drive.driveId.toString())
                        .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                        .setOwnership(OwnershipContext.newBuilder().setAccountId(host.account).setDatasourceId("source").setSecurity(seedPolicy));
                if (targetWrite) {
                    member1.setDestination(DocumentRevisionCondition.newBuilder().setAddress(seedAddress)
                            .setExpectedMutationRevision(seedRow.mutationRevision));
                } else {
                    member1.setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                            .setAccountId(host.account).setGraphId("graph").setGraphAddressId("node").setDocId("derived-" + UUID.randomUUID())));
                }
                for (var part : seedParts) {
                    member1.addParts(DocumentPublicationPart.newBuilder().setSlot(part.slot())
                            .setReuse(PublicationReuse.newBuilder().setSource(sourceCondition)
                                    .setSourceSlot(part.slot()).setObject(part.identity())));
                }
                var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                        .setAccountId(host.account).setOperationId(UUID.randomUUID().toString())
                        .addMembers(host.command().intent().getMembers(0)).addMembers(member1).build());
                var request = request(command, host.bodies(), Map.of("member-0", false, "member-1", false));
                var key = host.registerKey("principal");
                host.grantAccounts("principal");
                host.installGrant(key, "principal", command, env.dbNowMicros() + GRANT_LIFETIME_MICROS);
                requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                        () -> host.publish(via, key, "principal", request), "grant does not waive " + denial);
                require(!host.documentExists(destination(command)), denial + ": grant-covered member never committed");
                if (targetWrite) {
                    var unchanged = new DocumentLedger(env.tx).findByNodeId(DocumentIds.nodeId(seedAddress)).orElseThrow();
                    requireEquals(seedRow.mutationRevision, unchanged.mutationRevision,
                            denial + ": existing target revision is unchanged");
                } else {
                    require(!host.documentExists(member1.getDestination().getAddress()), denial + ": second member never committed");
                }
                requireEquals(0L, host.successRows(command), denial + ": no success row");
                requireEquals(0L, host.recordedVersions(command), denial + ": no recorded versions");
                requireEquals(0L, host.selectedAttempts(command), denial + ": no selected attempts");
                host.requireScopedCallersOnly("principal");
            }
        }
        System.out.println("SCOPED_PUBLICATION_MIXED_ATOMICITY_" + via + "_OK");
    }

    private record SeededPart(DocumentPublicationSlot slot, PublicationObjectIdentity identity) {}

    /** Exact provider identities of a committed revision's uploaded parts, read back from the durable rows. */
    private static List<SeededPart> seededParts(Tx tx, CommandSpec seed, UUID revision, Environment env) {
        var rows = tx.readOnly(em -> em.createNativeQuery("""
                SELECT rp.revision_ordinal, rp.object_id, o.object_key, o.provider_version,
                       o.expected_size, o.expected_sha256, o.content_type
                FROM document_revision_parts rp
                JOIN document_part_attempt_objects o ON o.physical_object_id = rp.object_id
                WHERE rp.revision_id = :revision ORDER BY rp.revision_ordinal
                """).setParameter("revision", revision).getResultList());
        var parts = new ArrayList<SeededPart>();
        for (var row : rows) {
            var columns = (Object[]) row;
            int ordinal = ((Number) columns[0]).intValue();
            var body = seed.bodies().get(new DocumentUploadPayloads.Key("member-0", ordinal));
            require(body != null, "seeded revision ordinal has a fixture body");
            var slot = DocumentPublicationSlot.newBuilder().setPart(body.part()).setSubKey(body.subKey()).build();
            var identity = PublicationObjectIdentity.newBuilder().setObjectId(columns[1].toString())
                    .setBackendGeneration(GENERATION).setStorageRealm(env.profile.storageRealm())
                    .setNamespace(env.namespace).setObjectKey((String) columns[2])
                    .setProviderVersion((String) columns[3])
                    .setSizeBytes(((Number) columns[4]).longValue()).setSha256((String) columns[5])
                    .setContentType((String) columns[6]).build();
            parts.add(new SeededPart(slot, identity));
        }
        require(!parts.isEmpty(), "seeded revision has uploaded parts");
        return List.copyOf(parts);
    }

    /** Missing/wrong tokens, resolver outage without fallthrough, and identity-dropping host mappers all fail closed. */
    private static void transportFailuresFailClosed(Environment env, Path bundle) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            int reached = host.received.size();
            var writes = host.writes.snapshot();
            var missing = outcome(() -> host.plainStub().publishDocument(host.request()));
            require(missing instanceof StatusRuntimeException failure
                            && failure.getStatus().getCode() == Status.Code.UNAUTHENTICATED,
                    "missing token must be UNAUTHENTICATED: " + missing);
            var forged = outcome(() -> host.stubFor("forged-" + UUID.randomUUID()).publishDocument(host.request()));
            require(forged instanceof StatusRuntimeException failure
                            && failure.getStatus().getCode() == Status.Code.UNAUTHENTICATED,
                    "wrong token must be UNAUTHENTICATED: " + forged);
            requireEquals(reached, host.received.size(), "token refusals never reached the repository");
            host.replaceResolver(token -> { throw new IllegalStateException("credential store unreachable"); });
            var outage = outcome(() -> host.stubFor(key.token()).publishDocument(host.request()));
            require(outage instanceof StatusRuntimeException failure
                            && failure.getStatus().getCode() != Status.Code.OK
                            && failure.getStatus().getCode() != Status.Code.UNAUTHENTICATED,
                    "resolver outage is not a bad-credential verdict: " + outage);
            var chain = (AuthenticatedCallerResolver) CallerResolver.chain(List.of(
                    (AuthenticatedCallerResolver) token -> { throw new IllegalStateException("first store down"); },
                    (AuthenticatedCallerResolver) token -> host.authenticationFor(key.token())));
            host.replaceResolver(chain);
            var fallthrough = outcome(() -> host.stubFor(key.token()).publishDocument(host.request()));
            require(fallthrough instanceof StatusRuntimeException failure
                            && failure.getStatus().getCode() != Status.Code.OK
                            && failure.getStatus().getCode() != Status.Code.UNAUTHENTICATED,
                    "resolver failure must not fall through to another authority: " + fallthrough);
            requireEquals(reached, host.received.size(), "resolver failures never reached the repository");
            host.restoreResolver();
            host.dropBindings(true);
            var dropped = outcome(() -> host.stubFor(key.token()).publishDocument(host.request()));
            require(dropped instanceof StatusRuntimeException failure
                            && failure.getStatus().getCode() == Status.Code.PERMISSION_DENIED,
                    "a binding-dropping host mapper fails closed: " + dropped);
            host.dropBindings(false);
            requireEquals(reached, host.received.size(), "identity refusals never reached the repository");
            requireEquals(writes, host.writes.snapshot(), "transport refusals make no provider write calls");
            host.requireCommitted(host.publish(Invocation.GRPC, key, "principal", host.request()), host.command(), "principal");
        }
        System.out.println("SCOPED_PUBLICATION_TRANSPORT_FAIL_CLOSED_OK");
    }

    /** Grant-only revocation blocks unfinished creation but never erases a committed receipt; replay needs the live key. */
    private static void grantRevocationSemantics(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            var unfinished = host.anotherUploadCommand(false, "doc-" + UUID.randomUUID());
            var revokedGrant = host.installGrant(key, "principal", unfinished.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            host.revokeGrant(revokedGrant);
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, key, "principal", unfinished.request()), "revoked grant blocks unfinished creation");
            requireEquals(0L, host.successRows(unfinished.command()), "revoked grant left no success row");
            requireEquals(0L, host.recordedVersions(unfinished.command()), "revoked grant left no recorded versions");
            require(!host.documentExists(destination(unfinished.command())), "revoked grant created no document");
            var grant = host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            var response = host.publish(via, key, "principal", host.request());
            host.requireCommitted(response, host.command(), "principal");
            host.revokeGrant(grant);
            requireEquals(response, host.publish(via, key, "principal", host.request()),
                    "grant-only revocation does not erase the committed receipt");
            host.revokeCredential(key, "principal");
            requireRefusal(host, via, RepositoryException.Code.UNAUTHENTICATED,
                    () -> host.publish(via, key, "principal", host.request()), "credential revocation refuses committed replay");
            requireEquals(1L, host.successRows(host.command()), "committed receipt retained as provenance");
        }
        System.out.println("SCOPED_PUBLICATION_GRANT_REVOCATION_" + via + "_OK");
    }

    /** Grant expiry is evaluated on the database clock; an expired grant blocks unfinished creation. */
    private static void grantExpiryBlocksCreation(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            var grant = host.installGrant(key, "principal", host.command(), env.dbNowMicros() + TimeUnit.SECONDS.toMicros(3));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (env.dbNowMicros() <= grant.expiresAtEpochMicros() && System.nanoTime() < deadline) Thread.sleep(25);
            require(env.dbNowMicros() > grant.expiresAtEpochMicros(), "database clock passed the grant deadline");
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, key, "principal", host.request()), "expired grant blocks creation");
            requireEquals(0L, host.successRows(host.command()), "expired grant left no success row");
            requireEquals(0L, host.recordedVersions(host.command()), "expired grant left no recorded versions");
            require(!host.documentExists(destination(host.command())), "expired grant created no document");
        }
        System.out.println("SCOPED_PUBLICATION_GRANT_EXPIRY_" + via + "_OK");
    }

    /** Committed replay requires the original binding at a live generation: rotation refuses both directions. */
    private static void rotationRefusesCommittedReplay(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var keyV1 = host.registerKey("principal");
            host.grantAccounts("principal");
            host.installGrant(keyV1, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            var response = host.publish(via, keyV1, "principal", host.request());
            host.requireCommitted(response, host.command(), "principal");
            var keyV2 = host.retoken("principal", host.rotate(keyV1, "principal"));
            requireRefusal(host, via, RepositoryException.Code.UNAUTHENTICATED,
                    () -> host.publish(via, keyV1, "principal", host.request()), "rotated key cannot replay");
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, keyV2, "principal", host.request()), "new generation does not inherit the operation");
            requireEquals(1L, host.successRows(host.command()), "committed receipt retained");
        }
        System.out.println("SCOPED_PUBLICATION_REPLAY_ROTATION_" + via + "_OK");
    }

    /**
     * Current READ revoked after commit blocks receipt replay over this invocation path; the raw historical
     * content read is checked through the library on both paths. The durable receipt is provenance.
     */
    private static void readRevocationBlocksDelivery(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            var response = host.publish(via, key, "principal", host.request());
            host.requireCommitted(response, host.command(), "principal");
            requireEquals(response, host.publish(via, key, "principal", host.request()), "replay before read revocation");
            var address = destination(host.command());
            var row = new DocumentLedger(env.tx).findByNodeId(DocumentIds.nodeId(address)).orElseThrow();
            row.writeSecurity(DocumentSecurity.getDefaultInstance());
            env.tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:id")
                    .setParameter("policy", row.security).setParameter("id", row.nodeId).executeUpdate(); });
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, key, "principal", host.request()), "receipt replay requires current READ");
            var published = response.getCommitted().getMembers(0);
            var read = outcome(() -> {
                host.history().readRaw(host.libraryCaller(key, "principal"), published.getAddress(),
                        UUID.fromString(published.getRevisionId()), RepositoryReadControl.NONE);
                return null;
            });
            require(read instanceof RepositoryException failure && failure.code() == RepositoryException.Code.NOT_FOUND,
                    "raw historical content read through the library requires current READ: " + read);
            requireEquals(1L, host.successRows(host.command()), "historical ownership is retained provenance");
        }
        System.out.println("SCOPED_PUBLICATION_READ_REVOCATION_" + via + "_OK");
    }

    /** Revocation while a real provider upload is held: the effect settles, never commits, and stays recovery-owned. */
    private static void revocationWhileUploadHeld(Environment env, Path bundle, Invocation via) throws Exception {
        var held = new HeldFirstPutStore(env.opened.store());
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime(env.tx, held);
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            var grant = host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                var publication = workers.submit(() -> outcome(() -> host.publish(via, key, "principal", host.request())));
                try {
                    if (!held.awaitEntered(Duration.ofSeconds(15))) {
                        held.release();
                        throw new AssertionError("real provider upload did not start; publication outcome: "
                                + publication.get(30, TimeUnit.SECONDS));
                    }
                    host.revokeGrant(grant);
                    held.release();
                    var completed = publication.get(30, TimeUnit.SECONDS);
                    require(completed instanceof RuntimeException, "held-then-revoked publication refused: " + completed);
                    requireEquals(RepositoryException.Code.NOT_FOUND, codeOf(via, (RuntimeException) completed),
                            "held-then-revoked publication refusal code");
                } finally { held.release(); }
            }
            require(host.recordedVersions(host.command()) > 0, "settled provider versions remain recorded against the attempt");
            require(!host.documentExists(destination(host.command())), "held-then-revoked publication never committed");
            requireEquals(0L, host.successRows(host.command()), "held-then-revoked publication has no success row");
            requireRefusal(host, via, RepositoryException.Code.NOT_FOUND,
                    () -> host.publish(via, key, "principal", host.request()), "recovery-owned effect cannot be replayed by the client");
        }
        System.out.println("SCOPED_PUBLICATION_UPLOAD_HELD_" + via + "_OK");
    }

    /** Ordering proof one: the authorized commit wins; grant revocation waits for the publisher's transaction. */
    private static void finalCommitWinsAndRevocationWaits(Environment env, Path bundle, Invocation via) throws Exception {
        var host = Host.prepare(env, bundle, false);
        try (var barrier = new CommitBarrier(env.database.dataSource(), host.command().operationId());
                var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            host.openRuntime(barrier.tx(), null);
            host.openTransport(4);
            try {
                var key = host.registerKey("principal");
                host.grantAccounts("principal");
                var grant = host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
                var publication = workers.submit(() -> outcome(() -> host.publish(via, key, "principal", host.request())));
                try {
                    int publisher = barrier.awaitCommit();
                    var revocation = workers.submit(() -> { host.revokeGrant(grant); return true; });
                    CommitBarrier.awaitGrantRevocationWaiter(env.tx, publisher);
                    require(!revocation.isDone(), "revocation waits for the authorized commit");
                    barrier.release();
                    var completed = publication.get(30, TimeUnit.SECONDS);
                    require(completed instanceof PublishDocumentResponse, "authorized commit completed: " + completed);
                    host.requireCommitted((PublishDocumentResponse) completed, host.command(), "principal");
                    requireEquals(Boolean.TRUE, revocation.get(10, TimeUnit.SECONDS), "revocation completed after commit");
                } finally { barrier.release(); }
                var replayed = host.publish(via, key, "principal", host.request());
                host.requireCommitted(replayed, host.command(), "principal");
                requireEquals(1L, host.successRows(host.command()), "one durable receipt");
            } finally {
                // The runtime borrows the barrier's entity manager factory; shut it down first.
                host.close();
            }
        }
        System.out.println("SCOPED_PUBLICATION_COMMIT_WINS_" + via + "_OK");
    }

    /** Ordering proof two: the publisher waits in the final grant check; the revoker's committed outcome (or expiry on the database clock) wins. */
    private static void finalGrantCheckWaitsForRevokerOutcome(Environment env, Path bundle, Invocation via, String ordering) throws Exception {
        var held = new HeldFirstPutStore(env.opened.store());
        try (var host = Host.prepare(env, bundle, false)) {
            // The host bounds lock waits; this scenario's holds are long, so its SQL timeouts must exceed them.
            host.openRuntime(env.tx, held, new SqlTimeouts(Duration.ofSeconds(30), Duration.ofSeconds(60)));
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            long lifetime = ordering.equals("expired") ? TimeUnit.SECONDS.toMicros(20) : GRANT_LIFETIME_MICROS;
            var grant = host.installGrant(key, "principal", host.command(), env.dbNowMicros() + lifetime);
            try (var workers = Executors.newVirtualThreadPerTaskExecutor();
                    var revoker = env.database.dataSource().getConnection()) {
                var publication = workers.submit(() -> outcome(() -> host.publish(via, key, "principal", host.request())));
                try {
                    if (!held.awaitEntered(Duration.ofSeconds(15))) {
                        held.release();
                        throw new AssertionError("admission did not pass to a provider upload; publication outcome: "
                                + publication.get(30, TimeUnit.SECONDS));
                    }
                    revoker.setAutoCommit(false);
                    final int blocker;
                    try (var query = revoker.createStatement(); var rows = query.executeQuery("SELECT pg_backend_pid()")) {
                        rows.next(); blocker = rows.getInt(1);
                    }
                    try (var update = revoker.prepareStatement(
                            "UPDATE repository_creation_grants SET revoked=? WHERE operation_id=?")) {
                        update.setBoolean(1, ordering.equals("revoked"));
                        update.setObject(2, host.command().operationId());
                        requireEquals(1, update.executeUpdate(), "revoker holds the exact grant row");
                    }
                    held.release();
                    CommitBarrier.awaitGrantReaderWaiter(env.tx, blocker);
                    require(!publication.isDone(), "publisher waits in the final grant check");
                    if (ordering.equals("expired")) {
                        require(env.dbNowMicros() < grant.expiresAtEpochMicros(), "grant still live while the publisher waits");
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                        while (env.dbNowMicros() <= grant.expiresAtEpochMicros() && System.nanoTime() < deadline) Thread.sleep(25);
                        require(env.dbNowMicros() > grant.expiresAtEpochMicros(), "database clock passed the grant deadline");
                        require(!publication.isDone(), "publisher still waits after the deadline");
                    }
                    revoker.commit();
                    var completed = publication.get(30, TimeUnit.SECONDS);
                    if (ordering.equals("held-live")) {
                        require(completed instanceof PublishDocumentResponse, "release without revocation commits: " + completed);
                        host.requireCommitted((PublishDocumentResponse) completed, host.command(), "principal");
                        requireEquals(1L, host.successRows(host.command()), "held-live success row");
                    } else {
                        require(completed instanceof RuntimeException, ordering + " publication refused: " + completed);
                        requireEquals(RepositoryException.Code.NOT_FOUND, codeOf(via, (RuntimeException) completed),
                                ordering + " refusal code");
                        require(!host.documentExists(destination(host.command())), ordering + " created no document");
                        requireEquals(0L, host.successRows(host.command()), ordering + " has no success row");
                        require(host.recordedVersions(host.command()) > 0, ordering + " retained recorded provider versions for recovery");
                    }
                } finally {
                    held.release();
                    revoker.rollback();
                }
            }
        }
        System.out.println("SCOPED_PUBLICATION_FINAL_CHECK_" + ordering.toUpperCase().replace('-', '_') + "_" + via + "_OK");
    }

    /** Two independent operations sharing one scoped key both progress; a held pre-commit transaction does not serialize the other. */
    private static void twoOperationsShareOneKey(Environment env, Path bundle, Invocation via) throws Exception {
        var host = Host.prepare(env, bundle, false);
        try (var barrier = new CommitBarrier(env.database.dataSource(), host.command().operationId());
                var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            host.openRuntime(barrier.tx(), null);
            host.openTransport(4);
            try {
                var key = host.registerKey("principal");
                host.grantAccounts("principal");
                var second = host.anotherUploadCommand(false, "doc-" + UUID.randomUUID());
                host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
                host.installGrant(key, "principal", second.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
                var first = workers.submit(() -> outcome(() -> host.publish(via, key, "principal", host.request())));
                try {
                    barrier.awaitCommit();
                    var secondResponse = host.publish(via, key, "principal", second.request());
                    host.requireCommitted(secondResponse, second.command(), "principal");
                    require(!first.isDone(), "first operation still held before its commit");
                    barrier.release();
                    var completed = first.get(30, TimeUnit.SECONDS);
                    require(completed instanceof PublishDocumentResponse, "held operation committed after release: " + completed);
                    host.requireCommitted((PublishDocumentResponse) completed, host.command(), "principal");
                } finally { barrier.release(); }
                requireEquals(1L, host.successRows(host.command()), "first operation success row");
                requireEquals(1L, host.successRows(second.command()), "second operation success row");
                requireEquals((long) host.bodies().size(), host.recordedVersions(host.command()), "first operation recorded versions");
                requireEquals((long) second.bodies().size(), host.recordedVersions(second.command()), "second operation recorded versions");
                host.requireScopedCallersOnly("principal");
            } finally {
                // The runtime borrows the barrier's entity manager factory; shut it down first.
                host.close();
            }
        }
        System.out.println("SCOPED_PUBLICATION_SHARED_KEY_CONCURRENCY_" + via + "_OK");
    }

    /** Exact retries preserve operation identity: the durable receipt returns again with no duplicate effects. */
    private static void exactRetryIdentity(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            var before = host.writes.snapshot();
            var response = host.publish(via, key, "principal", host.request());
            host.requireCommitted(response, host.command(), "principal");
            long effects = host.recordedVersions(host.command());
            var writes = host.writes.snapshot();
            requireEquals((long) host.bodies().size(), writes.calls() - before.calls(),
                    "positive control observes every uploaded part at the real adapter");
            requireEquals((long) host.bodies().size(), writes.returned() - before.returned(),
                    "positive control observes every completed adapter write");
            requireEquals(response, host.publish(via, key, "principal", host.request()), "first exact retry");
            requireEquals(response, host.publish(via, key, "principal", host.request()), "second exact retry");
            requireEquals(1L, host.successRows(host.command()), "retries added no success row");
            requireEquals(effects, host.recordedVersions(host.command()), "retries added no recorded versions");
            requireEquals(writes, host.writes.snapshot(), "exact retries make no new provider write calls");
        }
        System.out.println("SCOPED_PUBLICATION_RETRY_" + via + "_OK");
    }

    /** A contract-invalid request is not an authorization outcome: the exact grant remains usable afterwards. */
    private static void contractViolationIsNotAuthorization(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var key = host.registerKey("principal");
            host.grantAccounts("principal");
            host.installGrant(key, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            var payload = host.request().getPayloads(0);
            byte[] corrupted = payload.getContent().toByteArray();
            corrupted[0] ^= 1;
            var invalid = host.request().toBuilder()
                    .setPayloads(0, payload.toBuilder().setContent(ByteString.copyFrom(corrupted))).build();
            requireRefusal(host, via, RepositoryException.Code.INVALID_ARGUMENT,
                    () -> host.publish(via, key, "principal", invalid), "checksum-invalid request is a contract violation");
            requireEquals(0L, host.selectedAttempts(host.command()), "contract violation reached no staging");
            requireEquals(0L, host.recordedVersions(host.command()), "contract violation left no recorded versions");
            requireEquals(0L, host.successRows(host.command()), "contract violation left no success row");
            host.requireCommitted(host.publish(via, key, "principal", host.request()), host.command(), "principal");
        }
        System.out.println("SCOPED_PUBLICATION_CONTRACT_VS_AUTHZ_" + via + "_OK");
    }

    /**
     * A same-principal key substitution is detected by the exact identity check even when the repository accepts the
     * substituted key's own grant, so binding presence alone cannot satisfy it. The library host builds the caller with
     * the wrong key; over gRPC the token resolves to the wrong key. Each wrong issuer, credential ID, generation,
     * principal, authority or absent binding is also rejected against an actually observed repository caller. Over
     * gRPC, a mapper that replaces the authenticated binding is refused by the production adapter before the repository.
     */
    private static void substitutedBindingIsDetected(Environment env, Path bundle, Invocation via) throws Exception {
        try (var host = Host.prepare(env, bundle, false)) {
            host.openRuntime();
            host.openTransport(4);
            var keyA = host.registerKey("principal");
            var keyB = host.registerKey("principal");
            host.grantAccounts("principal");
            host.installGrant(keyB, "principal", host.command(), env.dbNowMicros() + GRANT_LIFETIME_MICROS);
            int before = host.received.size();
            host.substitute(keyA, keyB.binding());
            Object substituted;
            try {
                substituted = outcome(() -> host.publish(via, keyA, "principal", host.request()));
            } finally {
                host.substitute(keyA, null);
            }
            require(substituted instanceof IdentityMismatch, "exact identity check rejects a substituted key: " + substituted);
            requireEquals(before + 1, host.received.size(), "the substituted invocation reached the repository once");
            var wrong = host.received.get(before).caller();
            require(wrong.credentialBinding().isPresent(), "the substituted call carried a binding, so presence alone would pass");
            requireEquals(Optional.of(keyB.binding()), wrong.credentialBinding(), "the repository received the substituted key");
            requireEquals(1L, host.successRows(host.command()), "the repository accepted the substituted key's own grant");

            var replay = host.publish(via, keyB, "principal", host.request());
            host.requireCommitted(replay, host.command(), "principal");
            var exact = host.received.get(host.received.size() - 1).caller();
            var b = keyB.binding();
            Host.requireIdentity(exact, "principal", Optional.of(b));
            var near = List.of(new RepositoryCredentialBinding("other-issuer", b.credentialId(), b.generation()),
                    new RepositoryCredentialBinding(b.issuer(), UUID.randomUUID(), b.generation()),
                    new RepositoryCredentialBinding(b.issuer(), b.credentialId(), b.generation() + 1),
                    keyA.binding());
            for (var expected : near) {
                var mismatch = outcome(() -> { Host.requireIdentity(exact, "principal", Optional.of(expected)); return null; });
                require(mismatch instanceof IdentityMismatch, "observed binding " + b + " cannot satisfy " + expected);
            }
            require(outcome(() -> { Host.requireIdentity(exact, "other-principal", Optional.of(b)); return null; })
                    instanceof IdentityMismatch, "a different principal fails the identity check");
            require(outcome(() -> { Host.requireIdentity(exact, "principal", Optional.empty()); return null; })
                    instanceof IdentityMismatch, "a bound call fails an unbound expectation");
            require(outcome(() -> { Host.requireIdentity(new RepositoryCaller("principal", true), "principal", Optional.of(b)); return null; })
                    instanceof IdentityMismatch, "process authority fails the identity check");

            if (via == Invocation.GRPC) {
                int reached = host.received.size();
                var writes = host.writes.snapshot();
                host.substituteInMapper(keyA.binding());
                Object remapped;
                try {
                    remapped = outcome(() -> host.stubFor(keyB.token()).publishDocument(host.request()));
                } finally {
                    host.substituteInMapper(null);
                }
                require(remapped instanceof StatusRuntimeException failure
                                && failure.getStatus().getCode() == Status.Code.PERMISSION_DENIED,
                        "a binding-substituting host mapper fails closed: " + remapped);
                requireEquals(reached, host.received.size(), "the remapped call never reached the repository");
                requireEquals(writes, host.writes.snapshot(), "the remapped call made no provider write calls");
            }
        }
        System.out.println("SCOPED_PUBLICATION_IDENTITY_SUBSTITUTION_" + via + "_OK");
    }

    // ---------- shared fixture ----------

    record ScopedKey(RepositoryCredentialBinding binding, String token) {}

    record Observation(RepositoryCaller caller, String operationId) {}

    /** The repository received an identity other than the one provisioned for the invocation. */
    static final class IdentityMismatch extends AssertionError {
        IdentityMismatch(String message) { super(message); }
    }

    record CommandSpec(DocumentPublicationCommand command, PublishDocumentRequest request,
            Map<DocumentUploadPayloads.Key, PartObject> bodies) {}

    static CommandSpec uploadCommand(String account, DriveRecord drive, boolean typed, String docId) {
        return uploadCommand(account, drive, typed, docId, PUBLIC_READ_WRITE);
    }

    static CommandSpec uploadCommand(String account, DriveRecord drive, boolean typed, String docId, DocumentSecurity security) {
        var ownership = OwnershipContext.newBuilder().setAccountId(account).setDatasourceId("source")
                .setSecurity(security).build();
        var document = Document.newBuilder().setDocId(docId).setOwnership(ownership)
                .setSearchMetadata(SearchMetadata.newBuilder().addSemanticResults(
                        SemanticProcessingResult.newBuilder().setResultId("original-0")))
                .setStructuredData(typed ? Any.pack(StringValue.of("scoped typed payload"), "type.test")
                        : Any.newBuilder().setTypeUrl("archive.example/unavailable.Record")
                        .setValue(ByteString.copyFrom(new byte[]{0, (byte) 255, 1})).build()).build();
        var member = DocumentPublicationMember.newBuilder().setMemberId("member-0").setDriveId(drive.driveId.toString())
                .setOwnership(ownership).setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId(account).setGraphId("graph").setGraphAddressId("node").setDocId(docId)));
        var bodies = new HashMap<DocumentUploadPayloads.Key, PartObject>();
        var fragments = DocumentPartCodec.split(document, PartLayouts.document());
        for (int i = 0; i < fragments.size(); i++) {
            var part = fragments.get(i);
            bodies.put(new DocumentUploadPayloads.Key("member-0", i), part);
            member.addParts(DocumentPublicationPart.newBuilder()
                    .setSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                    .setUpload(PublicationUpload.newBuilder().setSizeBytes(part.bytes().length).setSha256(part.sha256())
                            .setContentType("application/protobuf").setWrittenBy(WriteProvenance.newBuilder().setModuleId("producer"))));
        }
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                .setAccountId(account).setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
        return new CommandSpec(command, request(command, bodies, Map.of("member-0", typed)), Map.copyOf(bodies));
    }

    static PublishDocumentRequest request(DocumentPublicationCommand command, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, Boolean> modes) {
        var request = PublishDocumentRequest.newBuilder().setIntent(command.intent());
        modes.forEach((member, typed) -> request.addModes(DocumentPublicationMemberMode.newBuilder().setMemberId(member)
                .setMode(typed ? DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_TYPED
                        : DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_OPAQUE)));
        bodies.forEach((key, body) -> request.addPayloads(DocumentPublicationPayload.newBuilder().setMemberId(key.member())
                .setRevisionOrdinal(key.revisionOrdinal()).setContent(ByteString.copyFrom(body.bytes()))));
        return request.build();
    }

    static DriveRecord insertDrive(Tx tx, String account, String bucket) {
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID(); drive.accountId = account; drive.name = "scoped-" + drive.driveId;
        drive.driveType = "CUSTOM"; drive.provider = "s3"; drive.bucket = bucket;
        new DriveLedger(tx).insert(drive);
        return drive;
    }

    /** Real descriptor-closure admission definition, identical to the typed fixture shape used by the qualified suites. */
    static DocumentSchemaAdmission.Definition definition(com.google.protobuf.Descriptors.Descriptor type) {
        var closure = ai.protomolt.proto.descriptors.DescriptorFingerprints.closure(type);
        var bytes = closure.toByteString();
        var compilation = SchemaCompilationProvenance.newBuilder()
                .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                .setUnknownCompilerReason("synthetic fixture compiler provenance unavailable")
                .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"));
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl("type.test/" + type.getFullName())
                .setArtifactSha256(DocumentPartCodec.sha256Hex(bytes.toByteArray()))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                        .setDescriptorFingerprint(ai.protomolt.proto.descriptors.DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(compilation).build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }

    /** Shared database/store for the probe process; coordinates arrive from the driver through the environment. */
    static final class Environment implements AutoCloseable {
        final LedgerDatabase database;
        final Tx tx;
        final OpenedBlobStore opened;
        final ManagedBackendLedger.Profile profile;
        final String namespace;

        private Environment(LedgerDatabase database, Tx tx, OpenedBlobStore opened,
                ManagedBackendLedger.Profile profile, String namespace) {
            this.database = database; this.tx = tx; this.opened = opened; this.profile = profile; this.namespace = namespace;
        }

        static Environment open() {
            var database = new LedgerDatabase(new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                    System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")));
            var tx = new Tx(database.entityManagerFactory());
            var opened = BlobStores.discover().open("s3", Map.of("endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"),
                    "region", System.getenv("PROTOMOLT_TEST_S3_REGION"), "path-style", "true", "conditional-writes", "false",
                    "access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS"), "secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")));
            String namespace = "scoped-parity-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            opened.ensureNamespace(namespace);
            try (var admin = software.amazon.awssdk.services.s3.S3Client.builder()
                    .endpointOverride(java.net.URI.create(System.getenv("PROTOMOLT_TEST_S3_ENDPOINT")))
                    .region(software.amazon.awssdk.regions.Region.of(System.getenv("PROTOMOLT_TEST_S3_REGION")))
                    .forcePathStyle(true)
                    .httpClientBuilder(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.builder())
                    .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                            software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                                    System.getenv("PROTOMOLT_TEST_S3_ACCESS"), System.getenv("PROTOMOLT_TEST_S3_SECRET")))).build()) {
                admin.putBucketVersioning(request -> request.bucket(namespace).versioningConfiguration(
                        configuration -> configuration.status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
            }
            var profile = new ManagedBackendLedger.Profile(S3BackendIdentity.of(System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"),
                    System.getenv("PROTOMOLT_TEST_S3_REGION"), true), "scoped-parity-realm");
            new ManagedBackendLedger(tx).bind(GENERATION, profile);
            return new Environment(database, tx, opened, profile, namespace);
        }

        long dbNowMicros() {
            return tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint").getSingleResult()).longValue());
        }

        @Override public void close() throws Exception {
            try (opened) { database.close(); }
        }
    }

    /** Pauses an actual success-writing JDBC transaction before commit; all SQL still reaches PostgreSQL. */
    static final class CommitBarrier implements AutoCloseable {
        private final java.util.concurrent.CompletableFuture<Integer> entered = new java.util.concurrent.CompletableFuture<>();
        private final java.util.concurrent.CompletableFuture<Void> release = new java.util.concurrent.CompletableFuture<>();
        private final AtomicBoolean armed = new AtomicBoolean(true);
        private final jakarta.persistence.EntityManagerFactory factory;
        private final Tx tx;

        CommitBarrier(javax.sql.DataSource source, UUID operation) {
            var observed = beforeCommit(source, connection -> {
                if (!armed.get()) return;
                try (var statement = connection.prepareStatement(
                        "SELECT pg_backend_pid() FROM repository_operation_success WHERE operation_id=?")) {
                    statement.setObject(1, operation);
                    try (var rows = statement.executeQuery()) {
                        if (!rows.next() || !armed.compareAndSet(true, false)) return;
                        entered.complete(rows.getInt(1));
                    }
                }
                try { release.get(30, TimeUnit.SECONDS); }
                catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new java.sql.SQLException("Publication barrier interrupted", failure);
                } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failure) {
                    throw new java.sql.SQLException("Publication barrier was not released", failure);
                }
            });
            factory = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", observed, "hibernate.hbm2ddl.auto", "validate"));
            tx = new Tx(factory);
        }

        Tx tx() { return tx; }
        int awaitCommit() throws Exception { return entered.get(30, TimeUnit.SECONDS); }
        void release() { release.complete(null); }

        static void awaitGrantRevocationWaiter(Tx tx, int publisher) throws Exception {
            awaitWaiter(tx, publisher, "%UPDATE repository_creation_grants SET revoked%");
        }

        static void awaitGrantReaderWaiter(Tx tx, int revoker) throws Exception {
            awaitWaiter(tx, revoker, "%lock_repository_creation_grant(%");
        }

        private static void awaitWaiter(Tx tx, int blocker, String query) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                var rows = tx.readOnly(em -> em.createNativeQuery("""
                        SELECT pid FROM pg_stat_activity WHERE :blocker=ANY(pg_blocking_pids(pid))
                          AND wait_event_type='Lock' AND query LIKE :query
                        """).setParameter("blocker", blocker).setParameter("query", query).getResultList());
                if (!rows.isEmpty()) return;
                Thread.sleep(10);
            }
            throw new AssertionError("Expected grant operation did not wait on transaction " + blocker);
        }

        @Override public void close() { release(); factory.close(); }
    }

    @FunctionalInterface interface ConnectionHook { void beforeCommit(java.sql.Connection connection) throws java.sql.SQLException; }

    /** Delegating datasource whose connection proxies run the hook before the real commit. */
    static javax.sql.DataSource beforeCommit(javax.sql.DataSource delegate, ConnectionHook hook) {
        return new javax.sql.DataSource() {
            @Override public java.sql.Connection getConnection() throws java.sql.SQLException { return wrap(delegate.getConnection()); }
            @Override public java.sql.Connection getConnection(String username, String password) throws java.sql.SQLException {
                return wrap(delegate.getConnection(username, password));
            }
            private java.sql.Connection wrap(java.sql.Connection connection) {
                return (java.sql.Connection) java.lang.reflect.Proxy.newProxyInstance(ScopedPublicationProbe.class.getClassLoader(),
                        new Class<?>[]{java.sql.Connection.class}, (proxy, method, args) -> {
                            if (method.getName().equals("commit") && method.getParameterCount() == 0) hook.beforeCommit(connection);
                            return method.invoke(connection, args);
                        });
            }
            @Override public <T> T unwrap(Class<T> type) throws java.sql.SQLException { return delegate.unwrap(type); }
            @Override public boolean isWrapperFor(Class<?> type) throws java.sql.SQLException { return delegate.isWrapperFor(type); }
            @Override public java.io.PrintWriter getLogWriter() throws java.sql.SQLException { return delegate.getLogWriter(); }
            @Override public void setLogWriter(java.io.PrintWriter out) throws java.sql.SQLException { delegate.setLogWriter(out); }
            @Override public void setLoginTimeout(int seconds) throws java.sql.SQLException { delegate.setLoginTimeout(seconds); }
            @Override public int getLoginTimeout() throws java.sql.SQLException { return delegate.getLoginTimeout(); }
            @Override public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
                return delegate.getParentLogger();
            }
        };
    }

    /** Blocks the first provider put before it delegates to the SDK until released; every operation delegates to the real store. */
    static final class HeldFirstPutStore implements BlobStore {
        private final BlobStore delegate;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean armed = new AtomicBoolean(true);

        HeldFirstPutStore(BlobStore delegate) { this.delegate = delegate; }

        boolean awaitEntered(Duration timeout) throws InterruptedException {
            return entered.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        void release() { release.countDown(); }

        private void gate() {
            if (!armed.compareAndSet(true, false)) return;
            entered.countDown();
            try {
                if (!release.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("Held put was not released");
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Held put interrupted", failure);
            }
        }

        @Override public PutResult put(PutSpec spec, byte[] body) { gate(); return delegate.put(spec, body); }
        @Override public PutResult put(PutSpec spec, InputStream body, long contentLength) { gate(); return delegate.put(spec, body, contentLength); }
        @Override public GetResult get(String bucket, String key, String versionId) { return delegate.get(bucket, key, versionId); }
        @Override public GetResult getBounded(String bucket, String key, String versionId, int maxBytes) {
            return delegate.getBounded(bucket, key, versionId, maxBytes);
        }
        @Override public GetResult getForUpdate(String bucket, String key) { return delegate.getForUpdate(bucket, key); }
        @Override public PutResult conditionalPut(PutSpec spec, byte[] body, WriteCondition condition) {
            return delegate.conditionalPut(spec, body, condition);
        }
        @Override public void copy(String srcBucket, String srcKey, String dstBucket, String dstKey) {
            delegate.copy(srcBucket, srcKey, dstBucket, dstKey);
        }
        @Override public boolean delete(String bucket, String key) { return delegate.delete(bucket, key); }
        @Override public BatchDeleteResult deleteAll(String bucket, List<String> keys) { return delegate.deleteAll(bucket, keys); }
        @Override public List<ListedObject> list(String bucket, String prefix) { return delegate.list(bucket, prefix); }
        @Override public void headBucket(String bucket) { delegate.headBucket(bucket); }
        @Override public void headObject(String bucket, String key) { delegate.headObject(bucket, key); }
    }

    /** Counts adapter invocations and normal returns while delegating actual storage behavior. */
    static final class ObservedStore implements BlobStore {
        private final BlobStore delegate;
        private final java.util.concurrent.atomic.AtomicLong calls = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong returned = new java.util.concurrent.atomic.AtomicLong();
        record Writes(long calls, long returned) {}
        ObservedStore(BlobStore delegate) { this.delegate = delegate; }
        Writes snapshot() { return new Writes(calls.get(), returned.get()); }
        private <T> T write(java.util.function.Supplier<T> action) {
            calls.incrementAndGet();
            T result = action.get();
            returned.incrementAndGet();
            return result;
        }
        @Override public PutResult put(PutSpec spec, byte[] body) { return write(() -> delegate.put(spec, body)); }
        @Override public PutResult put(PutSpec spec, InputStream body, long length) { return write(() -> delegate.put(spec, body, length)); }
        @Override public PutResult conditionalPut(PutSpec spec, byte[] body, WriteCondition condition) {
            return write(() -> delegate.conditionalPut(spec, body, condition));
        }
        @Override public void copy(String sourceBucket, String sourceKey, String targetBucket, String targetKey) {
            write(() -> { delegate.copy(sourceBucket, sourceKey, targetBucket, targetKey); return null; });
        }
        @Override public GetResult get(String bucket, String key, String version) { return delegate.get(bucket, key, version); }
        @Override public GetResult getBounded(String bucket, String key, String version, int bound) { return delegate.getBounded(bucket, key, version, bound); }
        @Override public GetResult getForUpdate(String bucket, String key) { return delegate.getForUpdate(bucket, key); }
        @Override public boolean delete(String bucket, String key) { return delegate.delete(bucket, key); }
        @Override public BatchDeleteResult deleteAll(String bucket, List<String> keys) { return delegate.deleteAll(bucket, keys); }
        @Override public List<ListedObject> list(String bucket, String prefix) { return delegate.list(bucket, prefix); }
        @Override public void headBucket(String bucket) { delegate.headBucket(bucket); }
        @Override public void headObject(String bucket, String key) { delegate.headObject(bucket, key); }
    }

    /** One account and journaled host with authenticated in-process transport and synthetic tokens. */
    static final class Host implements AutoCloseable {
        final Environment env;
        final Path bundle;
        final String account;
        final DriveRecord drive;
        final Map<UUID, DocumentUploadPlan.Placement> placements;
        final Map<UUID, DocumentPublicationRuntime.Placement> runtimePlacements;
        final boolean typed;
        final CommandSpec spec;
        /** Every caller the repository boundary received, with the operation it was invoked for. */
        final List<Observation> received = Collections.synchronizedList(new ArrayList<>());
        /** Indices of {@link #received} matched exactly to their invocation's expected identity. */
        private final Set<Integer> verified = java.util.concurrent.ConcurrentHashMap.newKeySet();
        /** Injected host defects: a token or library key presenting another key's binding. */
        private final Map<String, RepositoryCredentialBinding> substitutions = new java.util.concurrent.ConcurrentHashMap<>();
        /** Injected host defect: a transport mapper replacing the authenticated binding. */
        private final AtomicReference<RepositoryCredentialBinding> mapperSubstitution = new AtomicReference<>();
        final AtomicInteger schemaResolutions = new AtomicInteger();
        private final Map<String, AuthenticatedCaller> tokens = Collections.synchronizedMap(new HashMap<>());
        private final Map<String, Set<String>> hostAccounts = Collections.synchronizedMap(new HashMap<>());
        private final AtomicReference<AuthenticatedCallerResolver> resolverDelegate;
        private final AtomicBoolean dropBindings = new AtomicBoolean();
        private ObservedStore writes;
        private PayloadBudget budget;
        private DocumentReadLedger reads;
        private ai.protomolt.proto.repo.engine.DocumentPartReader reader;
        private DocumentPublicationRuntime runtime;
        private DocumentPublicationRepository facade;
        private DocumentPublicationRepository recording;
        private Server server;
        private ManagedChannel channel;
        private ExecutorService executor;
        private DocumentPublicationServiceGrpc.DocumentPublicationServiceBlockingStub plainStub;
        private boolean closed;

        private Host(Environment env, Path bundle, boolean typed, String account, DriveRecord drive, CommandSpec spec) {
            this.env = env; this.bundle = bundle; this.typed = typed; this.account = account; this.drive = drive; this.spec = spec;
            var sampled = DocumentUploadPlan.Placement.sample(drive, GENERATION, env.profile);
            this.placements = Map.of(drive.driveId, sampled);
            this.runtimePlacements = Map.of(drive.driveId, new DocumentPublicationRuntime.Placement(drive, GENERATION, env.profile));
            this.resolverDelegate = new AtomicReference<>(this::resolve);
        }

        static Host prepare(Environment env, Path bundle, boolean typed) {
            var account = "acct-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            var drive = insertDrive(env.tx, account, env.namespace);
            return new Host(env, bundle, typed, account, drive, uploadCommand(account, drive, typed, "doc-" + UUID.randomUUID()));
        }

        CommandSpec anotherUploadCommand(boolean typed, String docId) {
            return uploadCommand(account, drive, typed, docId);
        }

        DocumentPublicationCommand command() { return spec.command(); }
        PublishDocumentRequest request() { return spec.request(); }
        Map<DocumentUploadPayloads.Key, PartObject> bodies() { return spec.bodies(); }

        void openRuntime() throws java.io.IOException { openRuntime(env.tx, null); }

        void openRuntime(Tx tx, BlobStore storeOverride) throws java.io.IOException {
            openRuntime(tx, storeOverride, new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5)));
        }

        void openRuntime(Tx tx, BlobStore storeOverride, SqlTimeouts sqlTimeouts) throws java.io.IOException {
            this.writes = new ObservedStore(storeOverride == null ? env.opened.store() : storeOverride);
            this.budget = new PayloadBudget(64_000_000);
            this.reads = new DocumentReadLedger(tx, UUID.randomUUID());
            activateSchemaPolicy();
            var assessments = new DocumentPublicationRuntime.Assessments(bundle, Duration.ofMinutes(5), Duration.ofSeconds(5));
            DocumentPublicationRuntime.Backends backends = (generation, selected) -> {
                if (!GENERATION.equals(generation) || !env.profile.equals(selected))
                    throw new IllegalStateException("Publication selected an unconfigured backend");
                return new DocumentPublicationRuntime.Backend(env.profile.identity(),
                        new OpenedBlobStore(writes, () -> {}, env.opened.capabilities(),
                                env.opened::ensureNamespace, env.opened.reclaimer()));
            };
            this.reader = new ai.protomolt.proto.repo.engine.DocumentPartReader(
                    (generation, selected) -> env.opened.store(), 4, 1_000_000, budget);
            this.runtime = DocumentPublicationRuntime.journaled(tx, new DriveLedger(tx), reads, reader, budget, backends,
                    LIMITS, sqlTimeouts, 4, Duration.ofMillis(25), LEASE,
                    4, 4_000_000, 100, false, assessments, key -> new RepositoryCaller(key.principal(), true));
            this.facade = runtime.repository(this::select);
            this.recording = (caller, request, control) -> {
                received.add(new Observation(caller, request.getIntent().getOperationId()));
                return facade.publishDocument(caller, request, control);
            };
        }

        private void activateSchemaPolicy() {
            var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId(account)
                    .setMode(typed ? DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED
                            : DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED).setAnyResolvedSchema(true)
                    .setValidationProfile("protomolt-retained-schema-admission/v1")
                    .setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(32).setMaxFragmentBytes(4_000_000)
                            .setMaxRoots(100).setMaxEvidenceBytes(4_000_000).setMaxBindings(20)
                            .setMaxRetainedBytes(16_000_000).setMaxDecodedBytes(1_000_000)).build(), () -> {});
            new DocumentSchemaPolicies(env.tx).activate(policy, 0, () -> {});
        }

        private DocumentPublicationRuntime.PublicationSelection select(RepositoryCaller caller,
                DocumentPublicationCommand command, RepositoryReadControl control) {
            control.check();
            return new DocumentPublicationRuntime.PublicationSelection(runtimePlacements, Map.of(),
                    typed ? Optional.of(definition(Document.getDescriptor())) : Optional.empty(),
                    (scopeCaller, member, scopeControl) -> new DocumentSchemaAdmission.Resolution() {
                        @Override public DocumentSchemaAdmission.Definition select(DocumentSchemaAdmission.Selection occurrence) {
                            schemaResolutions.incrementAndGet();
                            return definition(StringValue.getDescriptor());
                        }
                        @Override public void close() {}
                    });
        }

        void openTransport(int maxConcurrentCalls) throws java.io.IOException {
            var service = new DocumentPublicationGrpcService(recording, this::bind, new PayloadBudget(64_000_000), maxConcurrentCalls);
            this.executor = Executors.newVirtualThreadPerTaskExecutor();
            String name = InProcessServerBuilder.generateName();
            AuthenticatedCallerResolver resolver = token -> resolverDelegate.get().resolveAuthenticated(token);
            this.server = InProcessServerBuilder.forName(name).executor(executor)
                    .maxInboundMessageSize(DocumentPublicationInput.MAX_ENVELOPE_BYTES)
                    .intercept(new ApiTokenServerInterceptor("scoped-operator-fixture-" + UUID.randomUUID(), resolver))
                    .addService(service).build().start();
            this.channel = InProcessChannelBuilder.forName(name).build();
            this.plainStub = DocumentPublicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(60, TimeUnit.SECONDS);
        }

        private RepositoryCaller bind(AuthenticatedCaller authentication) {
            var accounts = hostAccounts.getOrDefault(authentication.caller().name(), Set.of());
            var binding = dropBindings.get() ? Optional.<RepositoryCredentialBinding>empty()
                    : authentication.binding().map(b -> new RepositoryCredentialBinding(b.issuer(), b.credentialId(), b.generation()));
            if (mapperSubstitution.get() != null && binding.isPresent()) binding = Optional.of(mapperSubstitution.get());
            return new RepositoryCaller(authentication.caller().name(), authentication.caller().unrestricted(),
                    authentication.caller().unrestricted() ? Set.of() : accounts, Set.of(), binding);
        }

        void grantAccounts(String principal) { hostAccounts.put(principal, Set.of(account)); }
        void grantAccounts(String principal, Set<String> accounts) { hostAccounts.put(principal, accounts); }
        void dropBindings(boolean drop) { dropBindings.set(drop); }
        void replaceResolver(AuthenticatedCallerResolver replacement) { resolverDelegate.set(replacement); }
        void restoreResolver() { resolverDelegate.set(this::resolve); }

        /** The fixture token table, unless a deliberate substitution makes the token present another key's binding. */
        private Optional<AuthenticatedCaller> resolve(String token) {
            var authentication = Optional.ofNullable(tokens.get(token));
            var substituted = substitutions.get(token);
            if (substituted == null) return authentication;
            return authentication.map(found -> new AuthenticatedCaller(found.caller(), Optional.of(
                    new CredentialBinding(substituted.issuer(), substituted.credentialId(), substituted.generation()))));
        }

        /** Make {@code key}'s invocations present {@code replacement}'s binding on both paths; {@code null} clears it. */
        void substitute(ScopedKey key, RepositoryCredentialBinding replacement) {
            if (replacement == null) substitutions.remove(key.token());
            else substitutions.put(key.token(), replacement);
        }

        void substituteInMapper(RepositoryCredentialBinding replacement) { mapperSubstitution.set(replacement); }

        /** Register a live credential through the process-only authority and mint its synthetic token. */
        ScopedKey registerKey(String principal) {
            var binding = new RepositoryCredentialBinding("scoped-parity-issuer", UUID.randomUUID(), 1);
            new RepositoryCredentialAuthorities(env.tx).register(ADMIN, binding, principal);
            return retoken(principal, binding);
        }

        /** Mint another synthetic token for an already registered identity (e.g. after rotation). */
        ScopedKey retoken(String principal, RepositoryCredentialBinding binding) {
            var token = "scoped-token-" + UUID.randomUUID();
            tokens.put(token, new AuthenticatedCaller(Caller.scoped(principal, Set.of()),
                    Optional.of(new CredentialBinding(binding.issuer(), binding.credentialId(), binding.generation()))));
            return new ScopedKey(binding, token);
        }

        /** A principal-only token with no provisioned key identity; no durable authority is registered. */
        ScopedKey mintUnbound(String principal) {
            var token = "unbound-token-" + UUID.randomUUID();
            tokens.put(token, new AuthenticatedCaller(Caller.scoped(principal, Set.of()), Optional.empty()));
            return new ScopedKey(null, token);
        }

        RepositoryCredentialBinding rotate(ScopedKey key, String principal) {
            return new RepositoryCredentialAuthorities(env.tx).rotate(ADMIN, key.binding(), principal);
        }

        void revokeCredential(ScopedKey key, String principal) {
            new RepositoryCredentialAuthorities(env.tx).revoke(ADMIN, key.binding(), principal);
        }

        RepositoryCreationGrants.Prepared installGrant(ScopedKey key, String principal, DocumentPublicationCommand command,
                long expiresAtEpochMicros) {
            var caller = new RepositoryCaller(principal, false, Set.of(command.intent().getAccountId()), Set.of(),
                    Optional.of(key.binding()));
            var prepared = RepositoryCreationGrants.prepare(caller, command, placements, expiresAtEpochMicros);
            new RepositoryCreationGrants(env.tx, new DriveLedger(env.tx)).install(ADMIN, prepared);
            return prepared;
        }

        void revokeGrant(RepositoryCreationGrants.Prepared grant) {
            new RepositoryCreationGrants(env.tx, new DriveLedger(env.tx)).revoke(ADMIN, grant.key());
        }

        /**
         * Invoke one scoped publication and require that the repository received exactly the provisioned identity
         * requested for THIS invocation: the principal, no process authority, and the key's full binding (or none for
         * a deliberately unbound key). The expectation comes from the provisioned key, never from the mapper output.
         */
        PublishDocumentResponse publish(Invocation via, ScopedKey key, String principal, PublishDocumentRequest request) {
            var expected = Optional.ofNullable(key == null ? null : key.binding());
            int from = received.size();
            PublishDocumentResponse response;
            try {
                response = invoke(via, key, principal, request);
            } catch (RuntimeException failure) {
                try {
                    requireReachedAs(from, principal, expected, request);
                } catch (AssertionError mismatch) {
                    mismatch.addSuppressed(failure);
                    throw mismatch;
                }
                throw failure;
            }
            requireReachedAs(from, principal, expected, request);
            return response;
        }

        private PublishDocumentResponse invoke(Invocation via, ScopedKey key, String principal, PublishDocumentRequest request) {
            if (via == Invocation.LIBRARY) {
                var binding = key == null ? null : substitutions.getOrDefault(key.token(), key.binding());
                return recording.publishDocument(new RepositoryCaller(principal, false,
                        hostAccounts.getOrDefault(principal, Set.of()), Set.of(),
                        Optional.ofNullable(binding)), request, RepositoryReadControl.NONE);
            }
            return stubFor(key.token()).publishDocument(request);
        }

        /** Exactly one repository call for this invocation's operation since {@code from}, carrying the expected identity. */
        private void requireReachedAs(int from, String principal, Optional<RepositoryCredentialBinding> expected,
                PublishDocumentRequest request) {
            String operation = request.getIntent().getOperationId();
            var matches = new ArrayList<Integer>();
            synchronized (received) {
                for (int i = from; i < received.size(); i++) {
                    if (received.get(i).operationId().equals(operation)) matches.add(i);
                }
            }
            if (matches.size() != 1)
                throw new AssertionError("invocation of operation " + operation + " reached the repository "
                        + matches.size() + " times; expected exactly once");
            int index = matches.get(0);
            requireIdentity(received.get(index).caller(), principal, expected);
            verified.add(index);
        }

        /** Exact identity equality; a present but different binding, principal or authority never satisfies it. */
        static void requireIdentity(RepositoryCaller observed, String principal, Optional<RepositoryCredentialBinding> expected) {
            if (observed.processAuthority())
                throw new IdentityMismatch("scoped invocation reached the repository with process authority");
            if (!principal.equals(observed.principalName()))
                throw new IdentityMismatch("repository principal: expected <" + principal + "> but was <"
                        + observed.principalName() + ">");
            if (!expected.equals(observed.credentialBinding()))
                throw new IdentityMismatch("repository credential binding: expected <" + expected + "> but was <"
                        + observed.credentialBinding() + ">");
        }

        /** Fixture seeding through the production publisher with process authority; never counted as a scoped call. */
        PublishDocumentResponse publishAsOperator(PublishDocumentRequest request) {
            return facade.publishDocument(ADMIN, request, RepositoryReadControl.NONE);
        }

        DocumentPublicationServiceGrpc.DocumentPublicationServiceBlockingStub stubFor(String token) {
            var headers = new Metadata();
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
            return plainStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }

        DocumentPublicationServiceGrpc.DocumentPublicationServiceBlockingStub plainStub() { return plainStub; }

        Optional<AuthenticatedCaller> authenticationFor(String token) { return Optional.ofNullable(tokens.get(token)); }

        RepositoryCaller libraryCaller(ScopedKey key, String principal) {
            return new RepositoryCaller(principal, false, hostAccounts.getOrDefault(principal, Set.of()), Set.of(),
                    Optional.ofNullable(key == null ? null : key.binding()));
        }

        ai.protomolt.proto.repo.engine.DocumentHistoricalOperations history() {
            return new ai.protomolt.proto.repo.engine.DocumentHistoricalOperations(reads, reader, budget);
        }

        long successRows(DocumentPublicationCommand command) {
            return env.tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_operation_success WHERE operation_id=:op")
                    .setParameter("op", command.operationId()).getSingleResult()).longValue());
        }

        /** Recorded versions only; ObservedStore separately checks actual adapter invocations. */
        long recordedVersions(DocumentPublicationCommand command) {
            return env.tx.readOnly(em -> ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM document_part_attempt_objects o
                    JOIN document_operation_selections s ON s.attempt_id=o.attempt_id
                    WHERE s.operation_id=:op AND o.provider_version IS NOT NULL
                    """).setParameter("op", command.operationId()).getSingleResult()).longValue());
        }

        long selectedAttempts(DocumentPublicationCommand command) {
            return env.tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_operation_selections WHERE operation_id=:op")
                    .setParameter("op", command.operationId()).getSingleResult()).longValue());
        }

        boolean documentExists(NodeAddress address) {
            return new DocumentLedger(env.tx).findByNodeId(DocumentIds.nodeId(address)).isPresent();
        }

        void requireCommitted(PublishDocumentResponse response, DocumentPublicationCommand command, String principal) {
            require(response.hasCommitted(), "expected a committed receipt: " + response);
            var committed = response.getCommitted();
            requireEquals(command.operationId().toString(), committed.getOperationId(), "receipt operation");
            requireEquals(command.intent().getAccountId(), committed.getAccountId(), "receipt account");
            requireEquals(principal, committed.getPrincipal(), "receipt principal");
            requireEquals(command.sha256(), committed.getCommandSha256(), "receipt command digest");
            requireEquals(command.intent().getMembersCount(), committed.getMembersCount(), "receipt members");
            for (var member : committed.getMembersList()) {
                UUID.fromString(member.getRevisionId());
                require(member.getMutationRevision() > 0, "receipt mutation revision is positive");
            }
        }

        void requireExactReadback(RepositoryCaller reader, DocumentPublishedRevision published,
                Map<DocumentUploadPayloads.Key, PartObject> bodies, boolean typed) throws Exception {
            var revision = UUID.fromString(published.getRevisionId());
            try (var raw = history().readRaw(reader, published.getAddress(), revision, RepositoryReadControl.NONE)) {
                requireEquals(published.getAddress(), raw.address(), "readback address");
                requireEquals(revision, raw.revision(), "readback revision");
                require(raw.publicationRevision() > 0, "readback publication revision is positive");
                requireEquals((long) bodies.size(), (long) raw.fragments().size(), "readback fragment count");
                for (var fragment : raw.fragments()) {
                    var expected = bodies.get(new DocumentUploadPayloads.Key(published.getMemberId(), fragment.revisionOrdinal()));
                    require(expected != null, "readback fragment has a fixture body");
                    var bytes = fragment.bytes();
                    byte[] actual = new byte[bytes.remaining()];
                    bytes.get(actual);
                    if (!java.util.Arrays.equals(expected.bytes(), actual))
                        throw new AssertionError("readback fragment bytes differ from the uploaded body");
                }
            }
            if (typed) {
                try (var validated = history().readValidated(reader, published.getAddress(), revision, RepositoryReadControl.NONE)) {
                    requireEquals("scoped typed payload", validated.document().getStructuredData()
                            .unpack(StringValue.class).getValue(), "retained typed payload");
                    require(!validated.policySha256().isBlank(), "retained admission policy digest");
                }
            }
            require(budget.reservedBytes() == 0, "readback released byte reservations");
        }

        /** Every repository call in this host was a bound scoped call already matched to its invocation's exact key. */
        void requireScopedCallersOnly(String principal) {
            synchronized (received) {
                require(!received.isEmpty(), "the repository observed the scoped caller");
                for (int i = 0; i < received.size(); i++) {
                    var caller = received.get(i).caller();
                    require(!caller.processAuthority(), "scoped calls never carry process authority");
                    requireEquals(principal, caller.principalName(), "repository caller principal");
                    require(caller.credentialBinding().isPresent(), "scoped calls carry a provisioned key");
                    require(verified.contains(i), "repository call " + i + " matched its invocation's exact provisioned key");
                }
            }
        }

        @Override public void close() throws Exception {
            if (closed) return;
            closed = true;
            Throwable failure = null;
            if (channel != null) {
                channel.shutdownNow();
                if (!channel.awaitTermination(10, TimeUnit.SECONDS)) failure = new AssertionError("publication channel did not stop");
            }
            if (server != null) {
                server.shutdownNow();
                if (!server.awaitTermination(10, TimeUnit.SECONDS)) failure = new AssertionError("publication server did not stop");
            }
            if (executor != null) executor.close();
            if (runtime != null) {
                boolean stopped = false;
                for (int pass = 0; pass < 3 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(5));
                if (!stopped && failure == null) failure = new AssertionError("publication runtime did not drain");
            }
            if (reader != null) reader.close();
            if (reads != null) {
                reads.closeForShutdown();
                if (!reads.awaitLocalDrain(Duration.ZERO) && failure == null) failure = new AssertionError("read ledger did not drain");
                reads.attestLocalQuiescence();
            }
            if (budget != null && budget.reservedBytes() != 0 && failure == null)
                failure = new AssertionError("host byte reservations remain: " + budget.reservedBytes());
            if (failure instanceof Exception exception) throw exception;
            if (failure != null) throw new AssertionError(failure);
        }
    }
}
