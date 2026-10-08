package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import com.google.protobuf.StringValue;
import java.lang.reflect.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Actual managed runtime and Git load held across an unquiesced coordinator transfer. */
public final class FencedSchemaWorkerProbe {
    private static final RepositoryCaller ADMIN = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source) throws Exception {
        var bundle = Path.of(System.getenv("PROTOMOLT_TEST_RUNTIME_BUNDLE"));
        var root = Path.of(FencedSchemaWorkerProbe.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
        var directory = Files.createTempDirectory(root, "fenced-schema-");
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var cancelled = new AtomicBoolean();
        var definition = ObservedAssessmentProbe.asset(StringValue.getDescriptor());
        try (var git = GitSchemaRegistryStore.builder().repositoryDir(directory).build();
             var workers = Executors.newVirtualThreadPerTaskExecutor();
             var opened = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(Map.ofEntries(
                Map.entry("endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT")),
                Map.entry("region", System.getenv("PROTOMOLT_TEST_S3_REGION")),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "true"),
                Map.entry("access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS")),
                Map.entry("secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000")))) {
            git.putDescriptorSet(definition.metadata().getArtifactSha256(), definition.descriptors());
            SchemaRegistryStore held = (SchemaRegistryStore) Proxy.newProxyInstance(SchemaRegistryStore.class.getClassLoader(),
                    new Class<?>[]{SchemaRegistryStore.class}, (proxy, method, args) -> {
                        if (method.getName().equals("descriptorSet")) {
                            entered.countDown();
                            if (!release.await(45, TimeUnit.SECONDS)) throw new IllegalStateException("fenced schema gate timeout");
                        }
                        try { return method.invoke(git, args); }
                        catch (InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var resolver = new RegistrySchemaResolver(held,
                    new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 2, 4);
            var budget = new PayloadBudget(64_000_000);
            var timeouts = new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5));
            var bounded = tx.withTimeouts(timeouts);
            var ledger = new DocumentReadLedger(bounded, UUID.randomUUID());
            var readCalls = new AtomicInteger(); var uploadCalls = new AtomicInteger();
            Throwable resolverFailure = null;
            try (var reader = new DocumentPartReader((generation, profile) -> {
                require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact reader backend");
                readCalls.incrementAndGet(); return opened.store();
            }, 2, 16_000_000, budget)) {
                var runtime = DocumentPublicationRuntime.managedJournaled(bounded, new DriveLedger(bounded), ledger,
                        reader, budget, (generation, profile) -> {
                            require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact upload backend");
                            uploadCalls.incrementAndGet(); return new DocumentPublicationRuntime.Backend(profile.identity(), opened);
                        }, new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000),
                        timeouts, 2, Duration.ofMillis(25), Duration.ofSeconds(10), 2, 4_000_000, 10, false,
                        new DocumentPublicationRuntime.Assessments(bundle, Duration.ofMinutes(2), Duration.ofSeconds(1)),
                        (account, principal, operation) -> {
                            require(account.equals("account") && principal.equals(ADMIN.principalName()), "exact process authority");
                            return ADMIN;
                        }, new DocumentPublicationRuntime.ExternalWorkers() {
                            public void closeAdmission() { resolver.close(); }
                            public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
                        });
                Throwable runtimeFailure = null;
                try {
                    var destination = source.candidate().getPartsList().stream().filter(p -> p.hasReuse())
                            .findFirst().orElseThrow().getReuse().getSource();
                    var command = AssessmentMixedReuseProbe.command(source.candidate().toBuilder().setDestination(destination).build());
                    var key = new RepositoryOperationLedger.Key("account", ADMIN.principalName(), command.operationId());
                    var bodies = new HashMap<DocumentPublicationRuntime.PayloadKey, PartObject>();
                    for (int ordinal = 0; ordinal < command.intent().getMembers(0).getPartsCount(); ordinal++) {
                        var part = command.intent().getMembers(0).getParts(ordinal);
                        if (part.hasUpload()) bodies.put(new DocumentPublicationRuntime.PayloadKey("a", ordinal),
                                new PartObject(part.getSlot().getPart(), part.getSlot().getSubKey(),
                                        source.fragments().get(ordinal).toByteArray(), part.getUpload().getSha256()));
                    }
                    var drive = new DriveLedger(tx).findById(source.placement().drive().id()).orElseThrow();
                    var container = ObservedAssessmentProbe.asset(Document.getDescriptor());
                    var control = new RepositoryReadControl() {
                        public boolean isCancelled() { return cancelled.get(); }
                        public long remainingNanos() { return Long.MAX_VALUE; }
                    };
                    var operation = workers.submit(() -> runtime.executeScoped(ADMIN, command,
                            Map.of(drive.driveId, new DocumentPublicationRuntime.Placement(drive, "assessment-s3", provider.profile())),
                            bodies, Map.of(), Map.of("a", DocumentPublicationRuntime.Mode.TYPED), Optional.of(container),
                            (caller, member, scopeControl) -> resolver.open(occurrence ->
                                    new RegistrySchemaResolver.Selected(definition.metadata(), definition.source()), scopeControl::check), control));
                    require(entered.await(10, TimeUnit.SECONDS), "real Git descriptor load started");
                    require(readCalls.get() > 0 && uploadCalls.get() > 0, "real provider upload and retained read preceded schema load");
                    cancelled.set(true);
                    try { operation.get(5, TimeUnit.SECONDS); throw new AssertionError("cancelled publication succeeded"); }
                    catch (ExecutionException expected) {
                        require(expected.getCause() instanceof RepositoryException failure && failure.code() == RepositoryException.Code.CANCELLED,
                                "publication retains cancellation");
                    }
                    require(!runtime.shutdownStep(Duration.ofMillis(100)), "live Git worker blocks initial shutdown");
                    require(count(tx, "repository_coordinator_drains", command.operationId()) == 1, "V90 exists before takeover");
                    require(count(tx, "repository_coordinator_local_drains", command.operationId()) == 0, "no V91 with live worker");
                    require(!resolver.awaitLoads(Duration.ZERO), "cancelled caller left an actual worker active");
                    tx.readOnly(em -> em.createNativeQuery("""
                            SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                             (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                            FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                            WHERE c.operation_id=:id
                            """).setParameter("id", command.operationId()).getSingleResult());
                    var identityRow = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                            SELECT c.claim_epoch,c.claim_token,b.incarnation,o.owner_generation,o.owner_token
                            FROM repository_execution_claims c JOIN repository_coordinator_bindings b
                             USING(account_id,principal,operation_id,claim_epoch)
                            JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                            WHERE c.operation_id=:id
                            """).setParameter("id", command.operationId()).getSingleResult());
                    var identity = new RepositoryCoordinatorDrain.Identity(key, command.sha256(), ((Number) identityRow[0]).longValue(),
                            (UUID) identityRow[1], (UUID) identityRow[2]);
                    var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(identity, UUID.randomUUID(), UUID.randomUUID(),
                            Duration.ofMinutes(5), new RepositoryCoordinatorReservation.OwnerIdentity(((Number) identityRow[3]).longValue(),
                                    (UUID) identityRow[4]));
                    RepositoryCoordinatorExpiration.reserve(bounded, ADMIN, reservation, NONE);
                    var afterTransfer = state(tx, command.operationId());
                    require(RepositoryCoordinatorReservation.confirm(bounded, ADMIN, reservation, NONE).isPresent(), "exact committed reservation");
                    require("EXPIRED_UNQUIESCED".equals(afterTransfer[8]) && "UNKNOWN".equals(afterTransfer[9]),
                            "remote effects remain explicitly unknown");
                    require(!runtime.shutdownStep(Duration.ofMillis(100)), "fenced identity cannot bypass live Git worker");
                    require(!resolver.awaitLoads(Duration.ZERO), "worker still held after claim transfer");
                    require(count(tx, "repository_coordinator_local_drains", command.operationId()) == 0, "transfer invented no V91");
                    require(Arrays.deepEquals(afterTransfer, state(tx, command.operationId())), "shutdown did not alter successor claim or reservation");
                    release.countDown();
                    require(resolver.awaitLoads(Duration.ofSeconds(5)), "actual Git worker completed");
                    require(runtime.shutdownStep(Duration.ofSeconds(5)), "runtime completed after worker drain");
                    require(runtime.shutdownStep(Duration.ZERO), "completed shutdown is idempotent");
                    require(count(tx, "repository_coordinator_local_drains", command.operationId()) == 0, "fenced predecessor never attested V91");
                    require(Arrays.deepEquals(afterTransfer, state(tx, command.operationId())), "shutdown preserved UNKNOWN handoff and claim");
                    require(ledger.outstandingReads() == 0 && budget.reservedBytes() == 0 && resolver.cachedBytes() == 0,
                            "local readers and byte reservations released");
                    require(new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow().mutationRevision
                            == destination.getExpectedMutationRevision(), "cancelled predecessor did not publish");
                    require(git.descriptorSet(definition.metadata().getArtifactSha256()).isPresent(), "borrowed registry remains usable");
                } catch (Exception | Error failure) {
                    runtimeFailure = failure;
                    throw failure;
                } finally {
                    release.countDown();
                    cleanup(runtimeFailure, () -> require(runtime.shutdownStep(Duration.ofSeconds(5)),
                            "runtime cleanup completed before borrowed resources close"));
                }
            } catch (Exception | Error failure) {
                resolverFailure = failure;
                throw failure;
            } finally {
                release.countDown();
                cleanup(resolverFailure, () -> {
                    resolver.close(); require(resolver.awaitLoads(Duration.ofSeconds(5)), "resolver cleanup");
                });
            }
        }
        System.out.println("FENCED_SCHEMA_WORKER_DRAIN_OK");
    }

    private static Object[] state(Tx tx, UUID id) {
        return tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,c.lease_until,r.predecessor_epoch,r.predecessor_token,
                 r.successor_token,r.successor_incarnation,r.command_sha256,r.kind,r.predecessor_remote_state
                FROM repository_execution_claims c JOIN repository_coordinator_reservations r USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id AND r.successor_epoch=c.claim_epoch
                """).setParameter("id", id).getSingleResult());
    }
    private static long count(Tx tx, String table, UUID id) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                .setParameter("id", id).getSingleResult()).longValue());
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
    @FunctionalInterface private interface Cleanup { void run() throws Exception; }
    private static void cleanup(Throwable primary, Cleanup cleanup) throws Exception {
        try { cleanup.run(); }
        catch (Exception | Error failure) {
            if (primary == null) throw failure;
            if (primary != failure) primary.addSuppressed(failure);
        }
    }
}
