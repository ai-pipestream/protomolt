package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/** Real retained validation, versioned storage and SQL decisions on production JARs. */
public final class AssessmentRejectionProbe {
    private static final DocumentRevisionAssembly.Limits LIMITS = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);

    static DocumentSchemaPolicies.Selection run(Tx tx, javax.sql.DataSource database, AssessmentProviderProbe provider,
            RepositoryCaller caller, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selected, DocumentAssessmentCreation.Created stage,
            DocumentSchemaPolicies.Selection current, DocumentAssessmentRuntimeObserver.Observation observation, int scenario) throws Exception {
        var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
        var budget = new PayloadBudget(128_000_000); var payload = new PayloadBudget(16_000_000);
        var reader = new DocumentPartReader((generation, profile) -> {
            require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact rejection provider");
            return provider.store();
        }, 2, 16_000_000, payload);
        var gate = new DocumentAssessmentRejections(tx, Duration.ofSeconds(5));
        try (reader; var capture = reads.captureAssessment(caller, owner, command, selected, stage.assessment(),
                stage.manifestSha256(), stage.retainUntil(), budget, () -> {})) {
            if (scenario == 0) {
                expect(RepositoryException.Code.FAILED_PRECONDITION, () -> gate.reject(caller, owner, command,
                        capture, reader, budget, LIMITS, observation, RepositoryReadControl.NONE));
                require(outcomes(tx, command) == 0, "accepted candidate has no rejection");
                System.out.println("ASSESSMENT_REJECTION_ACCEPTED_REFUSED");
                return current;
            }
            try (var verified = DocumentAssessmentReplay.verify(capture, reader, budget, LIMITS, observation, RepositoryReadControl.NONE)) {
                require(verified.result().firstFailure().isPresent(), "genuine invalid assessment verified");
                require(budget.reservedBytes() > 0 && reads.outstandingReads() == 1, "decision owns evidence budget and pin");
                expect(RepositoryException.Code.FAILED_PRECONDITION,
                        () -> new DocumentAssessmentRejections(tx, Duration.ofDays(1)).decide(caller, owner, command, verified, RepositoryReadControl.NONE));
                expect(RepositoryException.Code.CANCELLED,
                        () -> gate.decide(caller, owner, command, verified, control(() -> true)));
                var wrong = new RepositoryOperationLedger.Owner(owner.key(), owner.generation(), UUID.randomUUID(), owner.leaseUntil());
                try {
                    gate.decide(caller, wrong, command, verified, RepositoryReadControl.NONE);
                    throw new AssertionError("Wrong owner nonce was accepted");
                } catch (RepositoryOperationLedger.OwnerFencedException expected) { }
                require(outcomes(tx, command) == 0, "failed attempts write no terminal result");
                if (scenario == 1) {
                    verifyRuntimeDrift(gate, caller, owner, command, verified);
                    verifyCurrentAccess(tx, gate, owner, command, verified);
                    verifySqlBindingRefusals(tx, owner, command, stage);
                    var before = new AtomicBoolean(); var cancelledBefore = new AtomicBoolean();
                    try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                            Map.of("hibernate.connection.datasource", faultAfterDecisionCommit(database, -1, before, cancelledBefore),
                                    "hibernate.hbm2ddl.auto", "validate"))) {
                        expect(RepositoryException.Code.CANCELLED, () -> new DocumentAssessmentRejections(new Tx(emf), Duration.ofSeconds(5))
                                .decide(caller, owner, command, verified, control(cancelledBefore::get)));
                    }
                    require(before.get() && outcomes(tx, command) == 0, "cancellation after INSERT before COMMIT rolls back");
                }
                if (scenario == 3) {
                    var changed = new DocumentSchemaPolicies(tx).activate(current.policy(), current.revision(), () -> {});
                    try {
                        gate.decide(caller, owner, command, verified, RepositoryReadControl.NONE);
                        throw new AssertionError("Stale policy revision was accepted");
                    } catch (DocumentSchemaPolicies.StalePolicy expected) { }
                    require(outcomes(tx, command) == 0, "stale policy cannot reject");
                    System.out.println("ASSESSMENT_REJECTION_STALE_POLICY_OK");
                    return changed;
                }
                var fired = new AtomicBoolean(); var cancelled = new AtomicBoolean();
                // Each wrapper delegates every actual SQL operation; only the response
                // after the real rejection COMMIT is lost or followed by cancellation.
                var source = faultAfterDecisionCommit(database, scenario, fired, cancelled);
                try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                        Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                    var faulted = new DocumentAssessmentRejections(new Tx(emf), Duration.ofSeconds(5));
                    if (scenario == 1) {
                        try {
                            faulted.decide(caller, owner, command, verified, RepositoryReadControl.NONE);
                            throw new AssertionError("Lost acknowledgement reported success");
                        } catch (RuntimeException failure) {
                            boolean jdbc = false;
                            for (Throwable cause = failure; cause != null; cause = cause.getCause())
                                if (cause instanceof java.sql.SQLException sql && "08006".equals(sql.getSQLState())) jdbc = true;
                            require(jdbc, "lost commit acknowledgment remains actual JDBC failure");
                        }
                    } else {
                        var committed = faulted.decide(caller, owner, command, verified, control(cancelled::get));
                        require(cancelled.get() && committed.rejection().isPresent(), "post-commit cancellation preserves receipt");
                    }
                }
                require(fired.get(), "real rejection COMMIT fault was executed");
                require(outcomes(tx, command) == 1, "one durable rejection, no success");
                var stored = new DocumentPublicationReplay(tx).observe(caller, command).rejection().orElseThrow();
                var binding = stored.getAssessment();
                require(stored.getReasonValue() == 2 && binding.getAssessmentId().equals(stage.assessment().toString())
                        && binding.getManifestSha256().equals(stage.manifestSha256())
                        && binding.getRetainUntilEpochMicros() == stage.retainUntil().getEpochSecond() * 1_000_000 + stage.retainUntil().getNano() / 1000,
                        "durable rejection binds exact retained stage");
                // Replay must precede any owner lease, provider or retained-evidence
                // work; those arguments are deliberately unavailable on this retry.
                var replay = gate.reject(caller, owner, command, null, null, null, null, null, RepositoryReadControl.NONE);
                require(replay.rejection().orElseThrow().equals(stored), "exact retry requires no repeated validation or provider I/O");
                if (scenario == 1) verifyHeaderCorruption(tx, caller, command, stage.manifestSha256());
                System.out.println(scenario == 1 ? "ASSESSMENT_REJECTION_LOST_ACK_OK" : "ASSESSMENT_REJECTION_CANCEL_AFTER_COMMIT_OK");
            }
        } finally {
            require(reader.awaitIdle(Duration.ofSeconds(5)), "rejection provider work drains");
            require(budget.reservedBytes() == 0 && payload.reservedBytes() == 0, "decision releases all byte reservations");
            require(reads.releaseDrained(1) == 1 && reads.outstandingReads() == 0, "decision releases exact SQL read session");
        }
        verifyTerminalReads(tx, provider, caller, owner, command, selected, stage, current, observation, scenario);
        return current;
    }

    private static void verifyTerminalReads(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller,
            RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selected, DocumentAssessmentCreation.Created stage,
            DocumentSchemaPolicies.Selection current, DocumentAssessmentRuntimeObserver.Observation observation, int scenario) throws Exception {
        var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
        var budget = new PayloadBudget(128_000_000); var payload = new PayloadBudget(16_000_000);
        var reader = new DocumentPartReader((generation, profile) -> {
            require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact terminal provider");
            return provider.store();
        }, 2, 16_000_000, payload);
        // A terminal receipt must not revive the former writer's authority.
        try {
            reads.captureAssessment(caller, owner, command, selected, stage.assessment(),
                    stage.manifestSha256(), stage.retainUntil(), budget, () -> {});
            throw new AssertionError("Terminal operation revived live-owner capture");
        } catch (RuntimeException failure) {
            boolean terminal = false;
            for (Throwable cause = failure; cause != null; cause = cause.getCause())
                if (cause instanceof java.sql.SQLException sql && "P0001".equals(sql.getSQLState())
                        && sql.getMessage().contains("Repository operation is terminal")) terminal = true;
            require(terminal, "live capture refused by terminal SQL write fence");
        }
        require(reads.outstandingReads() == 0, "refused live capture consumes no capacity");
        var scoped = scenario == 1 ? new RepositoryCaller(owner.key().principal(), false,
                java.util.Set.of(owner.key().account()), java.util.Set.of()) : caller;
        try (reader; var capture = reads.captureRejectedAssessment(scoped, command, budget, RepositoryReadControl.NONE)) {
            var result = DocumentAssessmentReplay.replay(capture, reader, budget, LIMITS, observation, RepositoryReadControl.NONE);
            require(result.assessment().equals(stage.assessment()) && result.firstFailure().isPresent()
                    && result.manifestSha256().equals(stage.manifestSha256()), "terminal replay reproduces retained invalid candidate");
            expect(RepositoryException.Code.CANCELLED, () -> DocumentAssessmentReplay.replay(capture, reader,
                    budget, LIMITS, observation, control(() -> true)));
            if (scenario == 1) {
                var node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(
                        command.intent().getMembers(0).getDestination().getAddress());
                try {
                    policy(tx, node, "ACCESS_DENY");
                    expect(RepositoryException.Code.NOT_FOUND, () -> DocumentAssessmentReplay.replay(capture, reader,
                            budget, LIMITS, observation, RepositoryReadControl.NONE));
                } finally { policy(tx, node, "ACCESS_READ"); }
                require(DocumentAssessmentReplay.replay(capture, reader, budget, LIMITS, observation,
                        RepositoryReadControl.NONE).equals(result), "restored current access reproduces exact result");
            }
            System.out.println("ASSESSMENT_REJECTION_TERMINAL_READ_OK");
        } finally {
            require(reader.awaitIdle(Duration.ofSeconds(5)), "terminal provider work drains");
            require(budget.reservedBytes() == 0 && payload.reservedBytes() == 0, "terminal read budgets drain");
            require(reads.releaseDrained(1) == 1 && reads.outstandingReads() == 0, "terminal SQL session drains");
        }
    }

    private static javax.sql.DataSource faultAfterDecisionCommit(javax.sql.DataSource delegate, int fault,
            AtomicBoolean fired, AtomicBoolean cancelled) {
        return (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(javax.sql.DataSource.class.getClassLoader(),
                new Class<?>[]{javax.sql.DataSource.class}, (proxy, method, args) -> {
                    Object value = invoke(delegate, method, args);
                    if (!method.getName().equals("getConnection")) return value;
                    var connection = (java.sql.Connection) value;
                    var decision = new AtomicBoolean();
                    return java.lang.reflect.Proxy.newProxyInstance(java.sql.Connection.class.getClassLoader(),
                            new Class<?>[]{java.sql.Connection.class}, (p, operation, parameters) -> {
                                Object result = invoke(connection, operation, parameters);
                                if (operation.getName().equals("prepareStatement") && parameters[0] instanceof String sql
                                        && sql.contains("INSERT INTO repository_operation_rejection")) {
                                    decision.set(true);
                                    if (fault == -1) return java.lang.reflect.Proxy.newProxyInstance(java.sql.PreparedStatement.class.getClassLoader(),
                                            new Class<?>[]{java.sql.PreparedStatement.class}, (s, action, values) -> {
                                                Object written = invoke(result, action, values);
                                                if (action.getName().equals("executeUpdate") && fired.compareAndSet(false, true)) cancelled.set(true);
                                                return written;
                                            });
                                }
                                if (operation.getName().equals("commit") && decision.get() && fault != -1 && fired.compareAndSet(false, true)) {
                                    if (fault == 1) throw new java.sql.SQLException("Injected rejection commit acknowledgement loss", "08006");
                                    cancelled.set(true);
                                }
                                return result;
                            });
                });
    }
    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
    }
    private static void verifyRuntimeDrift(DocumentAssessmentRejections gate, RepositoryCaller caller,
            RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command, DocumentAssessmentReplay.Verified verified) {
        var original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ClassLoader.getPlatformClassLoader());
            try { gate.decide(caller, owner, command, verified, RepositoryReadControl.NONE); throw new AssertionError("Runtime drift accepted"); }
            catch (IllegalStateException expected) { require(expected.getMessage().contains("runtime loader topology"), "observed runtime drift refused"); }
        } finally { Thread.currentThread().setContextClassLoader(original); }
        try {
            Thread.currentThread().interrupt();
            expect(RepositoryException.Code.CANCELLED, () -> gate.decide(caller, owner, command, verified, RepositoryReadControl.NONE));
            require(Thread.currentThread().isInterrupted(), "decision preserves interruption");
        } finally { Thread.interrupted(); }
    }

    private static void verifyHeaderCorruption(Tx tx, RepositoryCaller caller, DocumentPublicationCommand command, String original) {
        try {
            corruptHeader(tx, command, "0".repeat(64));
            expect(RepositoryException.Code.DATA_LOSS, () -> new DocumentPublicationReplay(tx).observe(caller, command));
        } finally { corruptHeader(tx, command, original); }
        require(new DocumentPublicationReplay(tx).observe(caller, command).rejection().orElseThrow()
                .getAssessment().getManifestSha256().equals(original), "restored header replays exact original receipt");
    }

    private static void corruptHeader(Tx tx, DocumentPublicationCommand command, String hash) {
        tx.inTransaction(em -> {
            // Disposable SQL corruption fixture only. Production updates remain
            // forbidden by the immutable rejection trigger.
            em.createNativeQuery("SET LOCAL session_replication_role=replica").executeUpdate();
            require(em.createNativeQuery("UPDATE repository_operation_rejection SET manifest_sha256=decode(:sha,'hex') WHERE operation_id=:op")
                    .setParameter("sha", hash).setParameter("op", command.operationId()).executeUpdate() == 1, "exact header fault/restoration");
        });
    }

    private static void verifyCurrentAccess(Tx tx, DocumentAssessmentRejections gate, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, DocumentAssessmentReplay.Verified verified) {
        // Explicit SQL authorization fixtures, not publication of candidate bytes.
        // These destinations were seeded before policy activation. Do not bypass
        // the policy write gate just to construct an authorization fixture.
        var scoped = new RepositoryCaller(owner.key().principal(), false, java.util.Set.of(owner.key().account()), java.util.Set.of());
        expect(RepositoryException.Code.FAILED_PRECONDITION,
                () -> new DocumentAssessmentRejections(tx, Duration.ofDays(1)).decide(scoped, owner, command, verified, RepositoryReadControl.NONE));
        var address = command.intent().getMembers(0).getDestination().getAddress();
        var node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(address);
        try {
            policy(tx, node, "ACCESS_DENY");
            expect(RepositoryException.Code.NOT_FOUND, () -> gate.decide(scoped, owner, command, verified, RepositoryReadControl.NONE));
            require(outcomes(tx, command) == 0, "revoked destination blocks decision");
        } finally { policy(tx, node, "ACCESS_READ"); }
    }

    private static void policy(Tx tx, UUID node, String access) {
        tx.inTransaction(em -> {
            require(em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:id")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"" + access + "\"}]}")
                    .setParameter("id", node).executeUpdate() == 1, "real destination policy changed");
        });
    }

    private static void verifySqlBindingRefusals(Tx tx, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, DocumentAssessmentCreation.Created stage) {
        for (int scenario = 0; scenario < 4; scenario++) {
            int fault = scenario;
            try {
                tx.inTransaction(em -> {
                    RepositoryOperationLedger.fenceLiveOwner(em, owner);
                    // Deliberately invalid receipt body: this test exercises the SQL
                    // association guard, independently of the protobuf codec.
                    em.createNativeQuery("""
                            INSERT INTO repository_operation_rejection(account_id,principal,operation_id,owner_generation,
                                command_codec,command_version,command_sha256,result_codec,result_version,result_bytes,result_sha256,
                                recorded_at_epoch_micros,disposition,reason,assessment_id,manifest_codec,manifest_version,manifest_sha256,retain_until_epoch_micros)
                            VALUES(:account,:principal,:op,:gen,'document-publication',1,decode(:command,'hex'),
                                'document-publication-rejection',1,decode('00','hex'),sha256(decode('00','hex')),
                                floor(extract(epoch FROM clock_timestamp())*1000000),:disposition,:reason,:id,
                                'document-publication-assessment',1,decode(:manifest,'hex'),:deadline)
                            """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                            .setParameter("op", command.operationId()).setParameter("gen", owner.generation()).setParameter("command", command.sha256())
                            .setParameter("disposition", fault == 3 ? 2 : 1).setParameter("reason", fault == 3 ? 3 : 2)
                            .setParameter("id", fault == 0 ? UUID.randomUUID() : stage.assessment())
                            .setParameter("manifest", fault == 1 ? "0".repeat(64) : stage.manifestSha256())
                            .setParameter("deadline", stage.retainUntil().getEpochSecond() * 1_000_000 + stage.retainUntil().getNano() / 1000 + (fault == 2 ? 1 : 0))
                            .executeUpdate();
                });
                throw new AssertionError("Invalid SQL assessment binding accepted");
            } catch (RuntimeException failure) {
                boolean sql = false;
                String expected = switch (fault) {
                    case 0 -> "query returned no rows";
                    case 1, 2 -> "exact sealed retained assessment";
                    default -> "repository_rejection_assessment_binding";
                };
                for (Throwable cause = failure; cause != null; cause = cause.getCause())
                    if (cause instanceof java.sql.SQLException rejected)
                        sql |= java.util.Set.of("P0001", "P0002", "23514").contains(rejected.getSQLState())
                                && rejected.getMessage().contains(expected);
                require(sql, "SQL binding rejected by guard or constraint");
            }
            require(outcomes(tx, command) == 0, "invalid SQL binding rolled back");
        }
    }
    private static long outcomes(Tx tx, DocumentPublicationCommand command) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT (SELECT count(*) FROM repository_operation_success WHERE operation_id=:op)
                     + (SELECT count(*) FROM repository_operation_rejection WHERE operation_id=:op)
                """).setParameter("op", command.operationId()).getSingleResult()).longValue());
    }
    private static void expect(RepositoryException.Code code, Runnable action) {
        try { action.run(); throw new AssertionError("Expected " + code); }
        catch (RepositoryException failure) { require(failure.code() == code, "exact repository error " + code); }
    }
    private static RepositoryReadControl control(java.util.function.BooleanSupplier cancelled) {
        return new RepositoryReadControl() {
            @Override public boolean isCancelled() { return cancelled.getAsBoolean(); }
            @Override public long remainingNanos() { return Long.MAX_VALUE; }
        };
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
}
