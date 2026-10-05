package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Fault injection around genuine PostgreSQL statements/commit, never fabricated SQL results. */
public final class AssessmentCaptureFaultProbe {
    private enum Fault { LOST_ACK, CANCEL_BEFORE_COMMIT, CANCEL_AFTER_COMMIT }

    static void run(javax.sql.DataSource database, Tx observer, RepositoryCaller caller,
            RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selections,
            DocumentAssessmentCreation.Created retained) {
        for (var fault : Fault.values()) {
            var armed = new AtomicBoolean();
            var cancelled = new AtomicBoolean();
            var source = faultSource(database, fault, armed, cancelled);
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                UUID reader = UUID.randomUUID();
                var ledger = new DocumentReadLedger(new Tx(emf), reader, 1);
                var budget = new PayloadBudget(64_000_000);
                armed.set(true);
                try {
                    ledger.captureAssessment(caller, owner, command, selections, retained.assessment(),
                            retained.manifestSha256(), retained.retainUntil(), budget, () -> {
                                if (cancelled.get()) throw new CancellationException("Injected assessment cancellation");
                            });
                    throw new AssertionError("Faulted assessment capture returned a handle: " + fault);
                } catch (RuntimeException failure) {
                    if (fault == Fault.LOST_ACK) {
                        boolean found = false;
                        for (Throwable cause = failure; cause != null; cause = cause.getCause())
                            if (cause instanceof java.sql.SQLException sql && "08006".equals(sql.getSQLState())) found = true;
                        require(found, "lost acknowledgement must propagate actual JDBC failure");
                    } else require(failure instanceof CancellationException, "cancellation must propagate unchanged");
                }
                require(!armed.get(), "fault actually executed");
                require(budget.reservedBytes() == 0, "failed capture releases verification memory");
                long committed = fault == Fault.CANCEL_BEFORE_COMMIT ? 0 : 1;
                require(sessions(observer, reader) == committed, "database capture outcome matches injection boundary");
                require(ledger.outstandingReads() == committed, "capacity remains owned only when commit was possible");
                require(ledger.releaseDrained(1) == (fault == Fault.CANCEL_AFTER_COMMIT ? 1 : 0),
                        "only acknowledged cancelled capture permits ordinary release");
                if (fault == Fault.LOST_ACK) {
                    try {
                        ledger.captureAssessment(caller, owner, command, selections, retained.assessment(),
                                retained.manifestSha256(), retained.retainUntil(), budget, () -> {});
                        throw new AssertionError("Uncertain capture lost its capacity reservation");
                    } catch (RepositoryException expected) {
                        require(expected.code() == RepositoryException.Code.RESOURCE_EXHAUSTED, "capacity refusal");
                    }
                }
                ledger.fence(); ledger.attestLocalQuiescence();
                require(ledger.recoverQuiescedPins(1) == (fault == Fault.LOST_ACK ? 1 : 0), "exact recovery count");
                require(ledger.reconcileDrained(1) == (fault == Fault.LOST_ACK ? 1 : 0), "exact local reconciliation");
                require(ledger.outstandingReads() == 0 && sessions(observer, reader) == 0, "no residual capture ownership");
                require(budget.reservedBytes() == 0, "recovery leaves no verification memory reserved");
            }
        }
        System.out.println("ASSESSMENT_CAPTURE_FAULTS_OK");
    }

    private static long sessions(Tx tx, UUID reader) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_assessment_read_sessions WHERE reader_incarnation=:reader")
                .setParameter("reader", reader).getSingleResult()).longValue());
    }

    private static javax.sql.DataSource faultSource(javax.sql.DataSource delegate, Fault fault,
            AtomicBoolean armed, AtomicBoolean cancelled) {
        return (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(javax.sql.DataSource.class.getClassLoader(),
                new Class<?>[]{javax.sql.DataSource.class}, (proxy, method, args) -> {
                    Object result = invoke(delegate, method, args);
                    if (!method.getName().equals("getConnection")) return result;
                    var connection = (java.sql.Connection) result;
                    return java.lang.reflect.Proxy.newProxyInstance(java.sql.Connection.class.getClassLoader(),
                            new Class<?>[]{java.sql.Connection.class}, (p, operation, parameters) -> {
                                Object response = invoke(connection, operation, parameters);
                                if (operation.getName().equals("commit") && fault != Fault.CANCEL_BEFORE_COMMIT
                                        && armed.compareAndSet(true, false)) {
                                    if (fault == Fault.LOST_ACK) throw new java.sql.SQLException("Injected assessment commit acknowledgment loss", "08006");
                                    cancelled.set(true);
                                }
                                if (operation.getName().equals("prepareStatement") && parameters[0] instanceof String sql
                                        && sql.contains("INSERT INTO document_assessment_read_sessions")
                                        && fault == Fault.CANCEL_BEFORE_COMMIT) {
                                    var statement = (java.sql.PreparedStatement) response;
                                    return java.lang.reflect.Proxy.newProxyInstance(java.sql.PreparedStatement.class.getClassLoader(),
                                            new Class<?>[]{java.sql.PreparedStatement.class}, (s, action, values) -> {
                                                Object value = invoke(statement, action, values);
                                                if (action.getName().equals("executeUpdate") && armed.compareAndSet(true, false)) cancelled.set(true);
                                                return value;
                                            });
                                }
                                return response;
                            });
                });
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
