package ai.protomolt.proto.repo.container.ledger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;

/** Fault only the transaction that inserted the armed assessment, scoped start, or publication. */
final class HistoricalCreateCommitFault implements AutoCloseable {
    private final AtomicReference<UUID> assessment = new AtomicReference<>();
    private final AtomicReference<RepositoryOperationLedger.Key> startKey = new AtomicReference<>();
    private final AtomicReference<UUID> proposedStart = new AtomicReference<>();
    private final AtomicReference<RepositoryOperationLedger.Owner> publication = new AtomicReference<>();
    private final AtomicReference<RepositoryOperationLedger.Key> publicationKey = new AtomicReference<>();
    private final AtomicReference<UUID> verification = new AtomicReference<>();
    enum RecoveryPhase { RESERVATION, INSTALL }
    private record Recovery(RepositoryOperationLedger.Key key, RecoveryPhase phase) {}
    private final AtomicReference<Recovery> recovery = new AtomicReference<>();
    private Runnable recoveryCommitted;
    private record Rejection(RepositoryOperationLedger.Owner owner, UUID assessment) {}
    private final AtomicReference<Rejection> rejection = new AtomicReference<>();
    private final AtomicBoolean fired = new AtomicBoolean();
    private java.util.function.IntConsumer rejectionCommit;
    private final jakarta.persistence.EntityManagerFactory factory;
    private final Tx tx;
    private final boolean lostAcknowledgement;

    HistoricalCreateCommitFault(DataSource database, boolean lostAcknowledgement) {
        this.lostAcknowledgement = lostAcknowledgement;
        var source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object result = invoke(database, method, args);
                    if (!method.getName().equals("getConnection")) return result;
                    var connection = (Connection) result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                            (wrapper, action, arguments) -> {
                                boolean target = action.getName().equals("commit") && !fired.get()
                                        && (ownsAssessment(connection, assessment.get()) || ownsStart(connection)
                                                || ownsPublication(connection) || ownsVerification(connection) || ownsRejection(connection)
                                                || ownsRecovery(connection));
                                if (target && !fired.compareAndSet(false, true))
                                    throw new AssertionError("Assessment commit fault was entered concurrently");
                                if (target && rejection.get() != null && rejectionCommit != null) {
                                    try (var query = connection.createStatement(); var rows = query.executeQuery("SELECT pg_backend_pid()")) {
                                        rows.next(); rejectionCommit.accept(rows.getInt(1));
                                    }
                                }
                                if (target && !lostAcknowledgement)
                                    throw new SQLException("Injected assessment commit rollback", "40001");
                                Object returned = invoke(connection, action, arguments);
                                if (target && recovery.get() != null && recoveryCommitted != null) recoveryCommitted.run();
                                if (target) throw new SQLException("Injected assessment commit acknowledgement loss", "08006");
                                return returned;
                            });
                });
        factory = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
        tx = new Tx(factory);
    }

    Tx tx() { return tx; }
    void armRecovery(RepositoryOperationLedger.Key key, RecoveryPhase phase, Runnable committed) {
        if (assessment.get() != null || startKey.get() != null || publication.get() != null || publicationKey.get() != null
                || verification.get() != null || rejection.get() != null || !recovery.compareAndSet(null, new Recovery(key, phase)))
            throw new IllegalStateException("Commit fault already armed");
        recoveryCommitted = java.util.Objects.requireNonNull(committed);
    }
    private boolean ownsRecovery(Connection connection) throws SQLException {
        var target = recovery.get();
        if (target == null) return false;
        String table = target.phase() == RecoveryPhase.RESERVATION ? "repository_coordinator_expirations" : "repository_successor_installs";
        try (var statement = connection.prepareStatement("SELECT EXISTS(SELECT 1 FROM " + table
                + " WHERE account_id=? AND principal=? AND operation_id=?"
                + (target.phase() == RecoveryPhase.INSTALL ? " AND install_xid=pg_current_xact_id_if_assigned())"
                        : " AND xmin=CAST(CAST(pg_current_xact_id_if_assigned() AS text) AS xid))"))) {
            statement.setString(1, target.key().account()); statement.setString(2, target.key().principal());
            statement.setObject(3, target.key().operationId());
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    void onRejectionCommit(java.util.function.IntConsumer action) {
        if (rejectionCommit != null || fired.get()) throw new IllegalStateException("Rejection commit hook already used");
        rejectionCommit = java.util.Objects.requireNonNull(action);
    }
    void armRejection(RepositoryOperationLedger.Owner owner, UUID id) {
        if (assessment.get() != null || startKey.get() != null || publication.get() != null || verification.get() != null
                || !rejection.compareAndSet(null, new Rejection(owner, id)))
            throw new IllegalStateException("Commit fault already armed");
    }
    private boolean ownsRejection(Connection connection) throws SQLException {
        var target = rejection.get();
        if (target == null) return false;
        var owner = target.owner();
        try (var statement = connection.prepareStatement("""
                SELECT EXISTS(SELECT 1 FROM repository_operation_rejection
                WHERE account_id=? AND principal=? AND operation_id=? AND owner_generation=? AND assessment_id=?
                  AND creation_xid=pg_current_xact_id_if_assigned())
                """)) {
            statement.setString(1, owner.key().account()); statement.setString(2, owner.key().principal());
            statement.setObject(3, owner.key().operationId()); statement.setLong(4, owner.generation());
            statement.setObject(5, target.assessment());
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    void armVerification(UUID attempt) {
        if (assessment.get() != null || startKey.get() != null || publication.get() != null
                || !verification.compareAndSet(null, attempt)) throw new IllegalStateException("Commit fault already armed");
    }
    private boolean ownsVerification(Connection connection) throws SQLException {
        var attempt = verification.get();
        if (attempt == null) return false;
        try (var statement = connection.prepareStatement("""
                SELECT EXISTS(SELECT 1 FROM document_part_attempt_objects
                WHERE attempt_id=? AND verified AND xmin=CAST(CAST(pg_current_xact_id_if_assigned() AS text) AS xid))
                """)) {
            statement.setObject(1, attempt);
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    void armPublication(RepositoryOperationLedger.Owner owner) {
        if (assessment.get() != null || startKey.get() != null || publicationKey.get() != null || !publication.compareAndSet(null, owner))
            throw new IllegalStateException("Commit fault already armed");
    }
    void armPublication(RepositoryOperationLedger.Key key) {
        if (assessment.get() != null || startKey.get() != null || publication.get() != null
                || verification.get() != null || rejection.get() != null || !publicationKey.compareAndSet(null, key))
            throw new IllegalStateException("Commit fault already armed");
    }
    private boolean ownsPublication(Connection connection) throws SQLException {
        var owner = publication.get();
        var key = owner == null ? publicationKey.get() : owner.key();
        if (key == null) return false;
        try (var statement = connection.prepareStatement("""
                SELECT EXISTS(SELECT 1 FROM repository_operation_success
                WHERE account_id=? AND principal=? AND operation_id=?
                  AND creation_xid=pg_current_xact_id_if_assigned()
                """ + (owner == null ? ")" : " AND owner_generation=?)"))) {
            statement.setString(1, key.account()); statement.setString(2, key.principal());
            statement.setObject(3, key.operationId());
            if (owner != null) statement.setLong(4, owner.generation());
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    void arm(UUID id) {
        if (!assessment.compareAndSet(null, id)) throw new IllegalStateException("Commit fault already armed");
    }
    void armStart(RepositoryOperationLedger.Key key) {
        if (assessment.get() != null || !startKey.compareAndSet(null, key))
            throw new IllegalStateException("Commit fault already armed");
    }
    UUID proposedStart() { return java.util.Objects.requireNonNull(proposedStart.get()); }
    private boolean ownsStart(Connection connection) throws SQLException {
        var key = startKey.get();
        if (key == null) return false;
        try (var statement = connection.prepareStatement("""
                SELECT assessment_id FROM repository_publication_assessment_starts
                WHERE account_id=? AND principal=? AND operation_id=? AND predecessor_generation=0
                  AND started_xid=pg_current_xact_id_if_assigned()
                """)) {
            statement.setString(1, key.account()); statement.setString(2, key.principal());
            statement.setObject(3, key.operationId());
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) return false;
                if (!proposedStart.compareAndSet(null, rows.getObject(1, UUID.class)))
                    throw new AssertionError("Start commit fault selected twice");
                if (rows.next()) throw new AssertionError("More than one scoped assessment start");
                return true;
            }
        }
    }
    boolean lostAcknowledgement() { return lostAcknowledgement; }
    void requireFailure(RuntimeException failure) {
        String state = lostAcknowledgement ? "08006" : "40001";
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sql && state.equals(sql.getSQLState()) && fired.get()) return;
        }
        throw new AssertionError("Expected the targeted assessment commit fault", failure);
    }
    private static boolean ownsAssessment(Connection connection, UUID id) throws SQLException {
        if (id == null) return false;
        try (var statement = connection.prepareStatement("""
                SELECT EXISTS(SELECT 1 FROM document_assessment_owners
                    WHERE assessment_id=? AND creation_xid=pg_current_xact_id_if_assigned())
                """)) {
            statement.setObject(1, id);
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }
    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
    @Override public void close() { factory.close(); }
}
