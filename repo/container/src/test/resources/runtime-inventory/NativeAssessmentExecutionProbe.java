package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.Document;
import com.google.protobuf.StringValue;
import java.lang.reflect.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import javax.sql.DataSource;

/** Native session routing over real uploads, retained reads, and transaction acknowledgement faults. */
public final class NativeAssessmentExecutionProbe {
    static void run(Tx observer, DataSource database, AssessmentProviderProbe provider,
            AssessmentMixedReuseProbe.Source source, DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        for (int scenario : new int[]{1, 2, 3, 4, 5, 11, 12, 13, 14, 10}) {
            boolean journaled = scenario >= 10;
            int mode = scenario % 10; // accepted, invalid, stage lost ACK, stage rollback, decision lost ACK, fresh-process recovery
            var member = source.candidate();
            if (mode == 0) {
                // Update an actual versioned publication, not the legacy authorization-only seed row.
                var original = member.getPartsList().stream().filter(part -> part.hasReuse()).findFirst().orElseThrow()
                        .getReuse().getSource();
                member = member.toBuilder().setDestination(original).build();
            }
            var command = AssessmentMixedReuseProbe.command(member);
            var armed = new AtomicBoolean(mode >= 2);
            var faulted = new AtomicBoolean();
            var datasource = faultSource(database, command.operationId(), mode, armed, faulted);
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"))) {
                var tx = new Tx(emf); var drives = new DriveLedger(tx);
                var caller = new RepositoryCaller("principal", true);
                var budget = new PayloadBudget(128_000_000); var payload = new PayloadBudget(16_000_000);
                var limits = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);
                var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
                var placements = Map.of(source.placement().drive().id(), source.placement());
                var session = journaled ? DocumentPublicationSession.journaled(tx, caller, command, placements, Duration.ofMinutes(5), budget)
                        : new DocumentPublicationSession(tx, caller, command, placements, Duration.ofMinutes(5));
                var bodies = new HashMap<DocumentUploadPayloads.Key, PartObject>();
                for (int ordinal = 0; ordinal < source.candidate().getPartsCount(); ordinal++) {
                    var part = source.candidate().getParts(ordinal);
                    if (part.hasUpload()) bodies.put(new DocumentUploadPayloads.Key("a", ordinal),
                            new PartObject(part.getSlot().getPart(), part.getSlot().getSubKey(),
                                    source.fragments().get(ordinal).toByteArray(), part.getUpload().getSha256()));
                }
                var backendCalls = new AtomicInteger(); var resolverCalls = new AtomicInteger();
                var retry = new AtomicBoolean();
                try (var opened = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(Map.of(
                        "endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), "region", System.getenv("PROTOMOLT_TEST_S3_REGION"),
                        "path-style", "true", "conditional-writes", "true", "access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS"),
                        "secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")));
                        var reader = new DocumentPartReader((generation, profile) -> {
                            require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact read backend");
                            return opened.store();
                        }, 2, 16_000_000, payload);
                        var uploads = new DocumentUploadCoordinator(tx, drives, budget, (generation, profile) -> {
                            require(!retry.get(), "retry must not upload"); backendCalls.incrementAndGet();
                            require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact upload backend");
                            return new DocumentUploadCoordinator.Backend(provider.profile().identity(), opened);
                        }, 2, Duration.ofMillis(25), new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)))) {
                    var assessments = new DocumentPublicationAssessmentExecution(tx, drives, reads, reader, budget, limits,
                            observation, Duration.ofMinutes(2), Duration.ofSeconds(1));
                    var execution = new DocumentPublicationExecution(tx, drives, reads, uploads, reader, budget, limits, false, assessments);
                    var modes = Map.of("a", DocumentPublicationCandidate.Mode.TYPED);
                    var container = Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor()));
                    var definition = mode == 0 ? ObservedAssessmentProbe.asset(StringValue.getDescriptor()) : ObservedAssessmentProbe.invalidSchema();
                    DocumentPublicationCandidate.Resolver resolver = (selected, occurrence) -> {
                        require(!retry.get(), "retry must not resolve schemas"); resolverCalls.incrementAndGet();
                        return definition;
                    };
                    Object first;
                    try { first = execution.execute(caller, session, bodies, Map.of(), modes, container, resolver, RepositoryReadControl.NONE); }
                    catch (RuntimeException failure) { first = failure; }
                    require(resolverCalls.get() == 1 && backendCalls.get() > 0, "first attempt used real schema and upload paths");
                    if (mode == 0) {
                        if (!(first instanceof ai.protomolt.proto.repo.v1.DocumentPublicationResult))
                            throw new AssertionError("accepted assessment commits", (Throwable) first);
                    }
                    else if (mode == 1) require(first instanceof DocumentPublicationReplay.Terminated, "invalid assessment durably rejected");
                    else require(faulted.get(), "requested real transaction fault fired");
                    if (mode == 5) NativeAssessmentRestartProbe.run(command,
                            session.admit(caller, RepositoryReadControl.NONE).orElseThrow());
                    retry.set(true);
                    Object repeated;
                    try { repeated = execution.execute(caller, session, Map.of(), Map.of(), modes, container, resolver, RepositoryReadControl.NONE); }
                    catch (RuntimeException failure) { repeated = failure; }
                    if (mode == 0) require(first.equals(repeated), "accepted exact replay");
                    else if (mode == 3) {
                        require(repeated instanceof RepositoryException failure && failure.code() == RepositoryException.Code.FAILED_PRECONDITION,
                                "absent uncertain stage requires explicit recovery");
                    } else {
                        require(repeated instanceof DocumentPublicationReplay.Terminated, "retained rejection or terminal replay");
                        var receipt = ((DocumentPublicationReplay.Terminated) repeated).receipt();
                        require(receipt.hasAssessment(), "rejection binds original assessment");
                        if (journaled) {
                            var started = observer.readOnly(em -> (Object[]) em.createNativeQuery(
                                    "SELECT assessment_id,(extract(epoch from retain_until)*1000000)::bigint FROM repository_publication_assessment_starts WHERE operation_id=:op")
                                    .setParameter("op", command.operationId()).getSingleResult());
                            require(receipt.getAssessment().getAssessmentId().equals(started[0].toString())
                                    && receipt.getAssessment().getRetainUntilEpochMicros() == ((Number) started[1]).longValue(),
                                    "receipt uses journaled assessment identity and deadline");
                        }
                        if (first instanceof DocumentPublicationReplay.Terminated terminal)
                            require(terminal.receipt().equals(receipt), "exact rejection replay");
                    }
                    long stages = observer.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM document_assessment_owners WHERE operation_id=:op")
                            .setParameter("op", command.operationId()).getSingleResult()).longValue());
                    require(stages == (mode == 0 || mode == 3 ? 0 : 1), "no second stage after uncertain creation");
                    if (journaled) {
                        for (var table : List.of("repository_execution_claims", "repository_publication_preparations", "repository_publication_modes")) {
                            long rows = observer.readOnly(em -> ((Number) em.createNativeQuery(
                                    "SELECT count(*) FROM " + table + " WHERE operation_id=:op")
                                    .setParameter("op", command.operationId()).getSingleResult()).longValue());
                            require(rows == 1, "registered execution retains exact " + table);
                        }
                        long starts = observer.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM repository_publication_assessment_starts WHERE operation_id=:op")
                                .setParameter("op", command.operationId()).getSingleResult()).longValue());
                        require(starts == (mode == 0 ? 0 : 1), "journal marker is sticky even when CREATE rolls back");
                    }
                    require(resolverCalls.get() == 1, "no second schema resolution");
                    reader.close(); require(reader.awaitIdle(Duration.ofSeconds(5)), "reader drained");
                    reads.releaseDrained(1);
                    require(reads.outstandingReads() == 0 && budget.reservedBytes() == 0 && payload.reservedBytes() == 0,
                            "one-slot ledger and both budgets drain");
                }
            }
        }
        System.out.println("NATIVE_ASSESSMENT_EXECUTION_OK");
    }

    static DataSource faultSource(DataSource database, UUID operation, int mode, AtomicBoolean armed, AtomicBoolean faulted) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
            var result = invoke(database, method, args);
            if (!method.getName().equals("getConnection")) return result;
            var connection = (Connection) result;
            return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (wrapper, action, arguments) -> {
                boolean inject = false;
                if (action.getName().equals("commit") && armed.get()) {
                    String table = mode == 4 ? "repository_operation_rejection" : "document_assessment_owners";
                    try (var query = connection.prepareStatement("SELECT count(*) FROM " + table + " WHERE operation_id=?")) {
                        query.setObject(1, operation);
                        try (var rows = query.executeQuery()) { rows.next(); inject = rows.getLong(1) > 0 && armed.compareAndSet(true, false); }
                    }
                }
                if (inject && mode == 3) { faulted.set(true); throw new SQLException("Injected assessment pre-commit failure", "08006"); }
                var returned = invoke(connection, action, arguments);
                if (inject) { faulted.set(true); throw new SQLException("Injected assessment lost acknowledgement", "08006"); }
                return returned;
            });
        });
    }
    private static Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
        try { return method.invoke(target, arguments); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
