package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/**
 * Private per-operation coordinator authority for explicitly claimed operation
 * fences. Publication sessions do not yet register claims automatically; holding
 * a claim alone does not authorize recovery or provider I/O.
 * Lock order for integration is claim, operation owner, then dependent domain rows.
 */
final class RepositoryExecutionClaimLedger {
    private final Tx tx;
    RepositoryExecutionClaimLedger(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    record Claim(RepositoryOperationLedger.Key key, String commandSha256, long epoch, UUID token, Instant leaseUntil) {
        Claim {
            Objects.requireNonNull(key); Objects.requireNonNull(token); Objects.requireNonNull(leaseUntil);
            if (epoch < 1 || commandSha256 == null || !commandSha256.matches("[0-9a-f]{64}"))
                throw new IllegalArgumentException("Invalid execution claim identity");
        }
        @Override public String toString() { return "ExecutionClaim[epoch=" + epoch + "]"; }
    }
    static final class Fenced extends RuntimeException {
        Fenced() { super("Execution claim is absent, expired or replaced"); }
    }

    /** Exact admission retry observes the original lease; it never renews it. */
    Claim acquire(RepositoryOperationLedger.Key key, DocumentPublicationCommand command, UUID token, Duration lease) {
        command.requireExecutionSupported();
        return tx.inTransaction(em -> { return acquireInTransaction(em, key, command, token, lease); });
    }

    /** Initial preparation may share this transaction; no provider I/O belongs inside it. */
    static Claim acquireInTransaction(EntityManager em, RepositoryOperationLedger.Key key,
            DocumentPublicationCommand command, UUID token, Duration lease) {
        return acquireInitialInTransaction(em, key, command, token, lease).claim();
    }

    record Acquisition(Claim claim, boolean created) {}

    static Acquisition acquireInitialInTransaction(EntityManager em, RepositoryOperationLedger.Key key,
            DocumentPublicationCommand command, UUID token, Duration lease) {
        scope(key, command);
        return acquireInitialChecked(em, key, command, token, lease);
    }

    /** Private registration only; the enclosing transaction must retain and check exact sources. */
    static Acquisition acquireHistoricalInitialInTransaction(EntityManager em, RepositoryOperationLedger.Key key,
            DocumentPublicationCommand command, UUID token, Duration lease, DocumentHistoricalAssessmentSources sources) {
        try (var work = sources.work()) {
            return acquireHistoricalInitialInTransaction(em, key, command, token, lease, work);
        }
    }

    static Acquisition acquireHistoricalInitialInTransaction(EntityManager em, RepositoryOperationLedger.Key key,
            DocumentPublicationCommand command, UUID token, Duration lease, DocumentHistoricalAssessmentSources.Work sources) {
        if (sources.references(command, () -> {}).isEmpty())
            throw new IllegalArgumentException("Historical registration requires pinned sources");
        if (!key.account().equals(command.intent().getAccountId()) || !key.operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Execution claim differs from command scope");
        return acquireInitialChecked(em, key, command, token, lease);
    }

    private static Acquisition acquireInitialChecked(EntityManager em, RepositoryOperationLedger.Key key,
            DocumentPublicationCommand command, UUID token, Duration lease) {
        Objects.requireNonNull(token); long millis = millis(lease);
        if (!em.getTransaction().isActive() || em.getTransaction().getRollbackOnly())
            throw new IllegalStateException("Execution claim acquisition requires a writable transaction");
        int inserted = bind(em.createNativeQuery("""
                INSERT INTO repository_execution_claims(account_id,principal,operation_id,command_sha256,
                    claim_epoch,claim_token,lease_until,fence_epoch,fence_token)
                VALUES (:account,:principal,:id,:digest,1,:token,clock_timestamp()+(:millis * interval '1 millisecond'),1,:token)
                ON CONFLICT(account_id,principal,operation_id) DO NOTHING
                """), key).setParameter("digest", HexFormat.of().parseHex(command.sha256()))
                .setParameter("token", token).setParameter("millis", millis).executeUpdate();
        var current = readLocked(em, key);
        requireCommand(current, command);
        if (current.epoch != 1 || !current.token.equals(token) || !live(em, key)) throw new Fenced();
        return new Acquisition(current, inserted == 1);
    }

    /** The caller retains predecessor epoch and proposed token across uncertain acknowledgments. */
    Claim takeOver(RepositoryOperationLedger.Key key, DocumentPublicationCommand command,
            long predecessorEpoch, UUID nextToken, Duration lease) {
        scope(key, command); Objects.requireNonNull(nextToken); long millis = millis(lease);
        if (predecessorEpoch < 1 || predecessorEpoch == Long.MAX_VALUE)
            throw new IllegalArgumentException("Invalid predecessor epoch");
        return tx.inTransaction(em -> {
            var current = readLocked(em, key); requireCommand(current, command);
            boolean live = live(em, key);
            if (current.epoch == predecessorEpoch + 1 && current.token.equals(nextToken) && live) return current;
            if (current.epoch != predecessorEpoch || current.token.equals(nextToken) || live) throw new Fenced();
            bind(em.createNativeQuery("""
                    UPDATE repository_execution_claims SET claim_epoch=claim_epoch+1,claim_token=:token,
                        lease_until=clock_timestamp()+(:millis * interval '1 millisecond'),fence_epoch=:epoch,fence_token=:token
                    WHERE account_id=:account AND principal=:principal AND operation_id=:id
                    """), key).setParameter("token", nextToken).setParameter("epoch", predecessorEpoch + 1)
                    .setParameter("millis", millis).executeUpdate();
            return readLocked(em, key);
        });
    }

    Claim renew(Claim expected, Duration lease) {
        return tx.inTransaction(em -> { return renewLive(em, expected, lease); });
    }

    /** Explicit renewal within the caller's transaction, including owner heartbeat updates. */
    static Claim renewLive(EntityManager em, Claim expected, Duration lease) {
        long millis = millis(lease);
        lockLive(em, expected);
        bind(em.createNativeQuery("""
                UPDATE repository_execution_claims SET lease_until=GREATEST(lease_until,
                    clock_timestamp()+(:millis * interval '1 millisecond')),fence_epoch=:epoch,fence_token=:token
                WHERE account_id=:account AND principal=:principal AND operation_id=:id
                """), expected.key).setParameter("millis", millis).setParameter("epoch", expected.epoch)
                .setParameter("token", expected.token).executeUpdate();
        return readLocked(em, expected.key);
    }

    /** First lock in a short mutation transaction; not a reusable preflight grant. */
    static Claim lockLive(EntityManager em, Claim expected) {
        Objects.requireNonNull(expected);
        return lockLive(em, expected.key, expected.commandSha256, expected.epoch, expected.token);
    }

    /** Fence a retained exact identity when acquisition's acknowledgment was lost; never creates or renews it. */
    static Claim lockLive(EntityManager em, RepositoryOperationLedger.Key key, String digest, long epoch, UUID token) {
        Objects.requireNonNull(key); Objects.requireNonNull(token);
        if (!em.getTransaction().isActive() || em.getTransaction().getRollbackOnly())
            throw new IllegalStateException("Execution claim requires a writable transaction");
        try {
            // One JDBC call; the database checks time after obtaining the row lock.
            var rows = bind(em.createNativeQuery("""
                    SELECT lease_until FROM fence_repository_execution_claim(:account,:principal,:id,:digest,:epoch,:token)
                    """), key).setParameter("digest", HexFormat.of().parseHex(digest))
                    .setParameter("epoch", epoch).setParameter("token", token).getResultList();
            if (rows.isEmpty()) throw new Fenced();
            return new Claim(key, digest, epoch, token, instant(rows.getFirst()));
        } catch (RuntimeException | Error failure) {
            try { em.getTransaction().setRollbackOnly(); }
            catch (RuntimeException markingFailure) { if (markingFailure != failure) failure.addSuppressed(markingFailure); }
            throw failure;
        }
    }

    private static Claim readLocked(EntityManager em, RepositoryOperationLedger.Key key) {
        em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
        var rows = bind(em.createNativeQuery("""
                SELECT command_sha256,claim_epoch,claim_token,lease_until FROM repository_execution_claims
                WHERE account_id=:account AND principal=:principal AND operation_id=:id FOR UPDATE
                """), key).getResultList();
        if (rows.isEmpty()) throw new Fenced();
        var row = (Object[]) rows.getFirst();
        return new Claim(key, HexFormat.of().formatHex((byte[]) row[0]), ((Number) row[1]).longValue(),
                (UUID) row[2], instant(row[3]));
    }
    private static boolean live(EntityManager em, RepositoryOperationLedger.Key key) {
        // Separate statement AFTER the row lock, so time cannot be sampled before a lock wait.
        return Boolean.TRUE.equals(bind(em.createNativeQuery("""
                SELECT lease_until > clock_timestamp() FROM repository_execution_claims
                WHERE account_id=:account AND principal=:principal AND operation_id=:id
                """), key).getSingleResult());
    }
    private static Query bind(Query query, RepositoryOperationLedger.Key key) {
        return query.setParameter("account", key.account()).setParameter("principal", key.principal()).setParameter("id", key.operationId());
    }
    private static void scope(RepositoryOperationLedger.Key key, DocumentPublicationCommand command) {
        Objects.requireNonNull(key); Objects.requireNonNull(command);
        command.requireExecutionSupported();
        if (!key.account().equals(command.intent().getAccountId()) || !key.operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Execution claim differs from command scope");
    }
    private static void requireCommand(Claim current, DocumentPublicationCommand command) {
        if (!current.commandSha256.equals(command.sha256())) throw new RepositoryOperationLedger.CommandConflictException();
    }
    private static long millis(Duration lease) {
        Objects.requireNonNull(lease);
        if (lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Execution claim lease requires one second to one day");
        return lease.toMillis();
    }
    private static Instant instant(Object value) {
        if (value instanceof Instant time) return time;
        if (value instanceof java.time.OffsetDateTime time) return time.toInstant();
        if (value instanceof java.sql.Timestamp time) return time.toInstant();
        throw new IllegalStateException("Unsupported claim timestamp type: " + value.getClass().getName());
    }
}
