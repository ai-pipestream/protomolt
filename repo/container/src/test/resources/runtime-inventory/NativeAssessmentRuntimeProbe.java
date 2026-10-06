package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.Document;
import com.google.protobuf.StringValue;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.*;
import javax.sql.DataSource;

/** Public native runtime composition, real provider I/O, and explicit shutdown ownership. */
public final class NativeAssessmentRuntimeProbe {
    private static final DocumentRevisionAssembly.Limits LIMITS = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);
    static void run(DataSource database, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source) throws Exception {
        for (int scenario : new int[]{1, 2, 3, 4, 6, 11, 12, 13, 14, 16, 10, 0}) {
            boolean journaled = scenario >= 10;
            int mode = scenario % 10;
            var member = source.candidate();
            if (mode == 0 && !journaled) member = member.toBuilder().setDestination(member.getPartsList().stream()
                    .filter(part -> part.hasReuse()).findFirst().orElseThrow().getReuse().getSource()).build();
            if (mode == 0 && journaled) member = member.toBuilder().setDestination(member.getDestination().toBuilder().setIfAbsent(true)
                    .setAddress(member.getDestination().getAddress().toBuilder().setGraphId("journaled-runtime-" + UUID.randomUUID()))).build();
            var command = AssessmentMixedReuseProbe.command(member);
            var armed = new AtomicBoolean(mode == 2 || mode == 3 || mode == 4);
            var faulted = new AtomicBoolean();
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", NativeAssessmentExecutionProbe.faultSource(database, command.operationId(), mode, armed, faulted),
                    "hibernate.hbm2ddl.auto", "validate"));
                    var opened = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(Map.of(
                            "endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), "region", System.getenv("PROTOMOLT_TEST_S3_REGION"),
                            "path-style", "true", "conditional-writes", "true", "access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS"),
                            "secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")))) {
                var tx = new Tx(emf); var drives = new DriveLedger(tx);
                var caller = new RepositoryCaller("principal", true);
                var budget = new PayloadBudget(128_000_000);
                var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
                var backendCalls = new AtomicInteger(); var registryCalls = new AtomicInteger();
                var retry = new AtomicBoolean();
                try (var reader = new DocumentPartReader((generation, profile) -> {
                    require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact read backend");
                    return opened.store();
                }, 2, 16_000_000, budget)) {
                    DocumentPublicationRuntime.Backends backends = (generation, profile) -> {
                        require(!retry.get(), "runtime retry must not upload"); backendCalls.incrementAndGet();
                        require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact upload backend");
                        return new DocumentPublicationRuntime.Backend(profile.identity(), opened);
                    };
                    Path bundle = Path.of(System.getenv("PROTOMOLT_TEST_RUNTIME_BUNDLE"));
                    if (mode == 1) {
                        try {
                            runtime(tx, drives, reads, reader, budget, backends, bundle.resolve("missing-" + UUID.randomUUID()), journaled);
                            throw new AssertionError("Missing observed runtime silently accepted");
                        } catch (java.io.IOException expected) {
                            require(backendCalls.get() == 0 && reads.outstandingReads() == 0,
                                    "failed startup borrows no provider or read session");
                        }
                    }
                    var runtime = runtime(tx, drives, reads, reader, budget, backends, bundle, journaled);
                    try {
                        var bodies = new HashMap<DocumentPublicationRuntime.PayloadKey, PartObject>();
                        for (int ordinal = 0; ordinal < source.candidate().getPartsCount(); ordinal++) {
                            var part = source.candidate().getParts(ordinal);
                            if (part.hasUpload()) bodies.put(new DocumentPublicationRuntime.PayloadKey("a", ordinal),
                                    new PartObject(part.getSlot().getPart(), part.getSlot().getSubKey(),
                                            source.fragments().get(ordinal).toByteArray(), part.getUpload().getSha256()));
                        }
                        var placements = Map.of(source.placement().drive().id(), new DocumentPublicationRuntime.Placement(
                                drives.findById(source.placement().drive().id()).orElseThrow(), "assessment-s3", provider.profile()));
                        var modes = Map.of("a", DocumentPublicationRuntime.Mode.TYPED);
                        var container = Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor()));
                        var definition = mode == 0 ? ObservedAssessmentProbe.asset(StringValue.getDescriptor()) : ObservedAssessmentProbe.invalidSchema();
                        DocumentPublicationRuntime.Schemas schemas = (authenticated, selected, occurrence) -> {
                            require(authenticated == caller && !retry.get(), "authorized first schema resolution only");
                            registryCalls.incrementAndGet();
                            if (mode == 6) {
                                try { require(!runtime.shutdownStep(Duration.ZERO), "shutdown cannot drain an active publication"); }
                                catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
                            }
                            return definition;
                        };
                        Object first = outcome(() -> runtime.execute(caller, command, placements, bodies, Map.of(), modes, container, schemas, RepositoryReadControl.NONE));
                        if (registryCalls.get() != 1 || backendCalls.get() == 0)
                            throw new AssertionError("runtime scenario " + scenario + " did not reach actual admission and uploads",
                                    first instanceof Throwable failure ? failure : null);
                        if (mode == 0) {
                            if (!(first instanceof ai.protomolt.proto.repo.v1.DocumentPublicationResult)) throw new AssertionError("accepted runtime publication", (Throwable) first);
                        } else if (journaled && mode == 6) {
                            boolean refused = false;
                            for (Throwable cause = first instanceof Throwable failure ? failure : null; cause != null; cause = cause.getCause())
                                if (cause instanceof java.sql.SQLException sql && "P0001".equals(sql.getSQLState())
                                        && sql.getMessage().contains("Coordinator is draining; new admission is closed")) refused = true;
                            require(refused, "draining runtime refuses new assessment admission");
                            for (String table : List.of("repository_publication_assessment_starts", "document_assessment_owners", "repository_operation_rejection")) {
                                long rows = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                        "SELECT count(*) FROM " + table + " WHERE operation_id=:op")
                                        .setParameter("op", command.operationId()).getSingleResult()).longValue());
                                require(rows == 0, "drain refusal has no new assessment start, stage, or fabricated rejection receipt");
                            }
                        } else if (mode == 1 || mode == 6) require(first instanceof DocumentPublicationRuntime.Rejected,
                                "runtime preserves durable rejection receipt");
                        else require(faulted.get(), "real commit fault fired");
                        retry.set(true);
                        Object repeated = outcome(() -> runtime.execute(caller, command, Map.of(), Map.of(), Map.of(), modes, container, schemas, RepositoryReadControl.NONE));
                        if (mode == 6) requireCode(repeated, RepositoryException.Code.UNAVAILABLE);
                        else if (mode == 3) requireCode(repeated, RepositoryException.Code.FAILED_PRECONDITION);
                        else if (mode == 0) require(first.equals(repeated), "accepted replay after session retirement");
                        else {
                            require(repeated instanceof DocumentPublicationRuntime.Rejected, "public rejected result on exact retry");
                            var receipt = ((DocumentPublicationRuntime.Rejected) repeated).receipt();
                            require(receipt.hasAssessment(), "public rejection retains evidence binding");
                            if (first instanceof DocumentPublicationRuntime.Rejected rejected)
                                require(rejected.receipt().equals(receipt), "exact rejection replay");
                            require(new DocumentPublicationReplay(tx).observe(caller, command).rejection().orElseThrow().equals(receipt), "receipt is durable");
                        }
                        require(registryCalls.get() == 1, "runtime retry never resolves another schema");
                        if (mode != 0 && mode != 6) {
                            var next = new DocumentPublicationCommand(command.intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
                            retry.set(false);
                            Object nextResult = outcome(() -> runtime.execute(caller, next, placements, bodies, Map.of(), modes, container, schemas, RepositoryReadControl.NONE));
                            if (mode == 3) {
                                requireCode(nextResult, RepositoryException.Code.RESOURCE_EXHAUSTED);
                                require(registryCalls.get() == 1, "uncertain entry is not evicted for a new operation");
                            } else {
                                require(nextResult instanceof DocumentPublicationRuntime.Rejected, "terminal entry frees one-session capacity");
                                require(registryCalls.get() == 2, "new command gets its own admission");
                            }
                        }
                        runtime.close();
                        requireCode(outcome(() -> runtime.execute(caller, command, Map.of(), Map.of(), Map.of(), modes, container, schemas,
                                RepositoryReadControl.NONE)), RepositoryException.Code.UNAVAILABLE);
                    } finally {
                        if (journaled && mode == 3) {
                            var cancelled = new RepositoryReadControl() {
                                public boolean isCancelled() { return true; }
                                public long remainingNanos() { return Long.MAX_VALUE; }
                            };
                            requireCode(outcome(() -> runtime.shutdownStep(Duration.ZERO, cancelled)), RepositoryException.Code.CANCELLED);
                            require(tx.readOnly(em -> ((Number) em.createNativeQuery(
                                    "SELECT count(*) FROM repository_coordinator_drains WHERE operation_id=:op")
                                    .setParameter("op", command.operationId()).getSingleResult()).longValue()) == 0,
                                    "cancelled drain does not fabricate a marker");
                        }
                        boolean stopped = false;
                        for (int pass = 0; pass < 4 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(5));
                        require(stopped && runtime.shutdownStep(Duration.ZERO), "bounded shutdown and repeat shutdown finish");
                    }
                    if (journaled) {
                        long markers = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM repository_coordinator_drains WHERE operation_id=:op")
                                .setParameter("op", command.operationId()).getSingleResult()).longValue());
                        require(markers == (mode == 3 || mode == 6 ? 1 : 0),
                                "runtime drains retained or active registrations, excludes retired terminal entries");
                    }
                    require(reads.outstandingReads() == 0 && budget.reservedBytes() == 0, "runtime releases all read and memory reservations");
                    require(tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT 1").getSingleResult()).intValue()) == 1,
                            "borrowed database remains open after runtime shutdown");
                }
            }
        }
        System.out.println("NATIVE_ASSESSMENT_RUNTIME_OK");
    }
    private static DocumentPublicationRuntime runtime(Tx tx, DriveLedger drives, DocumentReadLedger reads, DocumentPartReader reader,
            PayloadBudget budget, DocumentPublicationRuntime.Backends backends, Path bundle, boolean journaled) throws java.io.IOException {
        if (journaled) return DocumentPublicationRuntime.journaled(tx, drives, reads, reader, budget, backends, LIMITS,
                new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)), 2, Duration.ofMillis(25), Duration.ofMinutes(5),
                1, 4_000_000, 1, false, new DocumentPublicationRuntime.Assessments(bundle, Duration.ofMinutes(2), Duration.ofSeconds(1)),
                key -> {
                    require(key.principal().equals("principal") && key.account().equals("account"), "configured drain authority scope");
                    return new RepositoryCaller("principal", true);
                });
        return new DocumentPublicationRuntime(tx, drives, reads, reader, budget, backends, LIMITS,
                new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)), 2, Duration.ofMillis(25), Duration.ofMinutes(5),
                1, 4_000_000, 1, false, new DocumentPublicationRuntime.Assessments(bundle, Duration.ofMinutes(2), Duration.ofSeconds(1)));
    }
    @FunctionalInterface private interface Call { Object run() throws Exception; }
    private static Object outcome(Call call) throws Exception {
        try { return call.run(); } catch (RuntimeException failure) { return failure; }
    }
    private static void requireCode(Object value, RepositoryException.Code code) {
        if (!(value instanceof RepositoryException failure) || failure.code() != code)
            throw new AssertionError("Expected " + code, value instanceof Throwable failure ? failure : null);
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
