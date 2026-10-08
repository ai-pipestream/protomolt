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
        restorationDrain(observer, source, observation);
        run(observer, database, provider, source, observation, false);
    }

    /** Real observed executor and SQL journals; missing stage refuses before provider reads. */
    private static void restorationDrain(Tx tx,
            AssessmentMixedReuseProbe.Source source, DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        var command = AssessmentMixedReuseProbe.command(source.candidate());
        var caller = new RepositoryCaller("principal", true);
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var budget = new PayloadBudget(128_000_000);
        var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
        var drives = new DriveLedger(tx);
        var limits = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);
        var placements = Map.of(source.placement().drive().id(), source.placement());
        var modes = Map.of("a", DocumentPublicationCandidate.Mode.TYPED);
        try (var reader = new DocumentPartReader((generation, profile) -> {
                    throw new AssertionError("Unresolved restoration must not open a provider");
                }, 2, 16_000_000, budget);
                var uploads = new DocumentUploadCoordinator(tx, drives, budget,
                        (generation, profile) -> { throw new AssertionError("Cancelled admission must not upload"); },
                        1, Duration.ofMillis(10), new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5)))) {
            var assessments = new DocumentPublicationAssessmentExecution(tx, drives, reads, reader, budget, limits,
                    observation, Duration.ofMinutes(5), Duration.ofSeconds(5));
            var execution = new DocumentPublicationExecution(tx, drives, reads, uploads, reader, budget, limits, false, assessments);
            try (var sessions = DocumentPublicationSessions.journaled(tx, execution, Duration.ofSeconds(5), 1, 4_000_000, budget)) {
                var cancelledAfterOwner = new RepositoryReadControl() {
                    public boolean isCancelled() {
                        return tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM repository_operation_owners WHERE operation_id=:id")
                                .setParameter("id", command.operationId()).getSingleResult()).intValue()) != 0;
                    }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                try {
                    sessions.execute(caller, command, placements, Map.of(), Map.of(), modes, Optional.empty(),
                            (member, occurrence) -> { throw new AssertionError("Cancelled admission must not resolve schemas"); }, cancelledAfterOwner);
                    throw new AssertionError("Cancelled admission returned success");
                } catch (RepositoryException cancelled) {
                    require(cancelled.code() == RepositoryException.Code.CANCELLED, "cancel after real owner admission");
                }
                var claimRow = tx.readOnly(em -> (Object[]) em.createNativeQuery(
                        "SELECT claim_token,lease_until FROM repository_execution_claims WHERE operation_id=:id")
                        .setParameter("id", command.operationId()).getSingleResult());
                var claim = new RepositoryExecutionClaimLedger.Claim(key, command.sha256(), 1, (UUID) claimRow[0], (Instant) claimRow[1]);
                claim = new RepositoryExecutionClaimLedger(tx).renew(claim, Duration.ofMinutes(5));
                tx.readOnly(em -> em.createNativeQuery("SELECT pg_sleep(5.1)").getSingleResult());
                var seeds = DocumentPublicationSeeds.mint(key, command);
                var preparation = new DocumentPublicationPreparationRecord(key, command, seeds, placements, Duration.ofMinutes(5), 1);
                new DocumentPublicationPreparationJournal(tx, budget).save(caller, claim, preparation, RepositoryReadControl.NONE);
                new DocumentPublicationModesJournal(tx, budget).bind(caller, claim, 1, modes, RepositoryReadControl.NONE);
                var owner = new RepositoryOperationLedger(tx).takeOver(key, command, 1, seeds.ownerNonce(), Duration.ofMinutes(5), claim);
                new DocumentAssessmentStartJournal(tx, budget).start(caller, owner, command, UUID.randomUUID(),
                        Duration.ofMinutes(5), RepositoryReadControl.NONE);
                require(sessions.retireSuperseded(caller, command, RepositoryReadControl.NONE), "retire superseded original session");
                require(budget.reservedBytes() == 0, "pre-restoration reservations released");
                try {
                    sessions.resumeStarted(caller, command, owner, RepositoryReadControl.NONE);
                    throw new AssertionError("Uncommitted assessment stage returned success");
                } catch (RepositoryException unresolved) {
                    require(unresolved.code() == RepositoryException.Code.FAILED_PRECONDITION
                            && unresolved.getMessage().equals("Assessment stage outcome is unresolved; explicit recovery is required"),
                            "observed executor refuses unresolved stage after loading restoration");
                }
                require(budget.reservedBytes() > 0 && sessions.retainedSessions() == 1, "loaded restoration retains its reservation");
                sessions.close();
                require(sessions.awaitIdle(Duration.ZERO) && budget.reservedBytes() == 0, "close releases loaded journaled restoration");
                require(sessions.retainedSessions() == 1 && sessions.retainedCommandBytes() > 0, "close retains exact nonterminal identity");
                require(sessions.drainRegistrations(Duration.ZERO, ignored -> caller, RepositoryReadControl.NONE)
                        .equals(new DocumentPublicationSessions.DrainProgress(true, 1, 0)), "closed restoration identity receives drain marker");
            }
            require(reads.outstandingReads() == 0 && budget.reservedBytes() == 0, "restoration cleanup leaves no reads or reservations");
            reads.fence(); reads.attestLocalQuiescence();
        }
        System.out.println("JOURNALED_RESTORATION_DRAIN_OK");
    }

    static void runScoped(Tx observer, DataSource database, AssessmentProviderProbe provider,
            AssessmentMixedReuseProbe.Source source, DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        run(observer, database, provider, source, observation, true);
    }

    private static void run(Tx observer, DataSource database, AssessmentProviderProbe provider,
            AssessmentMixedReuseProbe.Source source, DocumentAssessmentRuntimeObserver.Observation observation, boolean scoped) throws Exception {
        int[] scenarios = scoped ? new int[]{21, 22, 23, 24, 20} : new int[]{1, 2, 3, 4, 5, 11, 12, 13, 14, 21, 22, 23, 24, 20};
        for (int scenario : scenarios) {
            boolean journaled = scenario >= 10;
            boolean managed = scenario >= 20;
            int mode = scenario % 10; // accepted, invalid, stage lost ACK, stage rollback, decision lost ACK, fresh-process recovery
            var member = source.candidate();
            if (mode == 0 || scoped) {
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
                var caller = scoped ? new RepositoryCaller("principal", false, Set.of("account"), Set.of())
                        : new RepositoryCaller("principal", true);
                var budget = new PayloadBudget(128_000_000); var payload = new PayloadBudget(16_000_000);
                var limits = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);
                var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
                var placements = Map.of(source.placement().drive().id(), source.placement());
                var session = managed ? null : journaled
                        ? DocumentPublicationSession.journaled(tx, caller, command, placements, Duration.ofMinutes(5), budget)
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
                try (var opened = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(Map.ofEntries(
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
                Map.entry("socket-timeout-ms", "60000")));
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
                    try (var manager = managed ? DocumentPublicationSessions.journaled(
                            tx, execution, Duration.ofMinutes(5), 1, 4_000_000, budget) : null) {
                    var modes = Map.of("a", DocumentPublicationCandidate.Mode.TYPED);
                    var container = Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor()));
                    var definition = mode == 0 ? ObservedAssessmentProbe.asset(StringValue.getDescriptor()) : ObservedAssessmentProbe.invalidSchema();
                    DocumentPublicationCandidate.Resolver resolver = (selected, occurrence) -> {
                        require(!retry.get(), "retry must not resolve schemas"); resolverCalls.incrementAndGet();
                        return definition;
                    };
                    var wrongAccount = new RepositoryCaller("principal", false, Set.of("other-account"), Set.of());
                    if (scoped) {
                        require(!caller.processAuthority(), "real execution caller has no process authority");
                        denied(() -> manager.execute(wrongAccount, command, placements, bodies, Map.of(), modes, container, resolver, RepositoryReadControl.NONE));
                        require(manager.retainedSessions() == 0 && manager.retainedCommandBytes() == 0,
                                "denied pre-registration request releases capacity");
                        require(backendCalls.get() == 0 && resolverCalls.get() == 0, "denial precedes provider and schema work");
                        for (String table : List.of("repository_execution_claims", "repository_coordinator_bindings", "repository_publication_preparations", "repository_publication_modes",
                                "repository_operation_owners", "repository_publication_assessment_starts")) {
                            long rows = observer.readOnly(em -> ((Number) em.createNativeQuery(
                                    "SELECT count(*) FROM " + table + " WHERE operation_id=:op")
                                    .setParameter("op", command.operationId()).getSingleResult()).longValue());
                            require(rows == 0, "denied request cannot journal " + table);
                        }
                    }
                    Object first;
                    try { first = managed ? manager.execute(caller, command, placements, bodies, Map.of(), modes, container, resolver, RepositoryReadControl.NONE)
                            : execution.execute(caller, session, bodies, Map.of(), modes, container, resolver, RepositoryReadControl.NONE); }
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
                    if (scoped) denied(() -> manager.execute(wrongAccount, command, Map.of(), Map.of(), Map.of(),
                            modes, container, resolver, RepositoryReadControl.NONE));
                    Object repeated;
                    try { repeated = managed ? manager.execute(caller, command, Map.of(), Map.of(), Map.of(), modes, container, resolver, RepositoryReadControl.NONE)
                            : execution.execute(caller, session, Map.of(), Map.of(), modes, container, resolver, RepositoryReadControl.NONE); }
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
                    if (managed && mode != 3) {
                        int uploadsBeforeReplay = backendCalls.get();
                        try (var successor = DocumentPublicationSessions.journaled(
                                tx, execution, Duration.ofMinutes(5), 1, 4_000_000, budget)) {
                            Object replay;
                            try { replay = successor.execute(caller, command, Map.of(), Map.of(), Map.of(),
                                    modes, container, resolver, RepositoryReadControl.NONE); }
                            catch (RuntimeException failure) { replay = failure; }
                            if (mode == 0) require(repeated.equals(replay), "new coordinator replays original committed result");
                            else {
                                require(replay instanceof DocumentPublicationReplay.Terminated,
                                        "new coordinator replays terminal rejection");
                                require(((DocumentPublicationReplay.Terminated) repeated).receipt().equals(
                                        ((DocumentPublicationReplay.Terminated) replay).receipt()),
                                        "new coordinator preserves terminal receipt");
                            }
                            require(successor.retainedSessions() == 0 && successor.retainedCommandBytes() == 0,
                                    "terminal replay in new coordinator retains no session");
                        }
                        require(backendCalls.get() == uploadsBeforeReplay && resolverCalls.get() == 1,
                                "new coordinator terminal replay performs no upload or schema resolution");
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
                    if (managed) {
                        require(manager.retainedSessions() == (mode == 3 ? 1 : 0), "manager retains only uncertain nonterminal stage");
                        require((manager.retainedCommandBytes() > 0) == (mode == 3), "manager terminal eviction releases command capacity");
                    }
                    reader.close(); require(reader.awaitIdle(Duration.ofSeconds(5)), "reader drained");
                    reads.releaseDrained(1);
                    require(reads.outstandingReads() == 0 && budget.reservedBytes() == 0 && payload.reservedBytes() == 0,
                            "one-slot ledger and both budgets drain");
                    }
                }
            }
        }
        System.out.println(scoped ? "SCOPED_NATIVE_ASSESSMENT_EXECUTION_OK" : "NATIVE_ASSESSMENT_EXECUTION_OK");
    }

    @FunctionalInterface private interface CheckedAction { void run() throws Exception; }
    private static void denied(CheckedAction action) throws Exception {
        try { action.run(); }
        catch (RepositoryException failure) {
            require(failure.code() == RepositoryException.Code.NOT_FOUND, "cross-account access hides the target");
            return;
        }
        throw new AssertionError("Scoped request unexpectedly authorized");
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
