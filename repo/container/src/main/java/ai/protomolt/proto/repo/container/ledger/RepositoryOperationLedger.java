package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Internal durable command identity and owner fencing. This is not an executable
 * repository API: upload scope, policy authorization, publication
 * and terminal outcomes must be integrated before a public port can use it.
 * No transaction here spans provider I/O. The trusted coordinator supplies scope.
 */
final class RepositoryOperationLedger {
    private final Tx tx;

    RepositoryOperationLedger(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    record Key(String account, String principal, UUID operationId) {
        Key {
            text(account, 200, "account");
            text(principal, 200, "principal");
            Objects.requireNonNull(operationId, "operationId");
        }
    }

    /** Internal storage input from a typed codec, never a public command. */
    record EncodedCommand(String codec, int version, ByteString bytes) {
        EncodedCommand {
            if (codec == null || !codec.matches("[a-z][a-z0-9_.-]{0,127}") || version < 1)
                throw new IllegalArgumentException("Invalid command encoding identity");
            Objects.requireNonNull(bytes, "bytes");
            if (bytes.isEmpty() || bytes.size() > 1048576)
                throw new IllegalArgumentException("Encoded command requires 1 to 1048576 bytes");
        }

        @Override public String toString() {
            return "EncodedCommand[codec=" + codec + ", version=" + version + ", size=" + bytes.size() + "]";
        }
    }

    /** An observation of admission, never proof of a committed/aborted mutation. */
    record Snapshot(Key key, EncodedCommand command, long generation, Instant leaseUntil, Instant createdAt) {}

    record Owner(Key key, long generation, UUID token, Instant leaseUntil,
            Optional<RepositoryExecutionClaimLedger.Claim> executionClaim) {
        Owner(Key key, long generation, UUID token, Instant leaseUntil) {
            this(key, generation, token, leaseUntil, Optional.empty());
        }
        Owner {
            Objects.requireNonNull(key); Objects.requireNonNull(token); Objects.requireNonNull(leaseUntil);
            Objects.requireNonNull(executionClaim);
            if (executionClaim.isPresent() && !executionClaim.orElseThrow().key().equals(key))
                throw new IllegalArgumentException("Execution claim differs from operation owner scope");
            if (generation < 1) throw new IllegalArgumentException("Invalid owner generation");
        }
        Owner withClaim(RepositoryExecutionClaimLedger.Claim claim) {
            return new Owner(key, generation, token, leaseUntil, Optional.of(claim));
        }
        @Override public String toString() { return "Owner[generation=" + generation + ", leaseUntil=" + leaseUntil + "]"; }
    }

    record Admission(Snapshot snapshot, Optional<Owner> owner) {}

    static final class CommandConflictException extends RuntimeException {
        CommandConflictException() { super("Operation identity already belongs to another command"); }
    }

    /** Exact stored command check shared by staging and outcome replay in their own transaction. */
    static void requireCommand(EntityManager em, Key key, DocumentPublicationCommand expected) {
        var rows = bind(em.createNativeQuery("""
                SELECT command_codec,command_version,command,command_sha256 FROM repository_operations
                WHERE account_id=:account AND principal=:principal AND operation_id=:id
                """), key).getResultList();
        if (rows.size() != 1) throw new CommandConflictException();
        Object[] row = (Object[]) rows.getFirst();
        if (!DocumentPublicationCommand.CODEC.equals(row[0])
                || DocumentPublicationCommand.ENCODING_VERSION != ((Number) row[1]).intValue()
                || !expected.canonical().equals(ByteString.copyFrom((byte[]) row[2]))
                || !expected.sha256().equals(java.util.HexFormat.of().formatHex((byte[]) row[3])))
            throw new CommandConflictException();
    }

    static final class OwnerFencedException extends RuntimeException {
        OwnerFencedException() { super("Repository operation owner is absent, expired or replaced"); }
    }

    /** A coordinator must perform authorized result replay instead of acquiring ownership. */
    static final class TerminalOperationException extends RuntimeException {
        TerminalOperationException() { super("Repository operation already completed"); }
    }

    /** Typed document admission; scope binding is checked before opening a transaction. */
    Admission admit(Key key, DocumentPublicationCommand command, UUID ownerNonce, Duration lease) {
        return admit(key, command, ownerNonce, lease, null);
    }

    /**
     * Internal unclaimed historical operation. The host supplies authenticated
     * identity; selection, CREATE and publication check current document authorization. This owner
     * cannot be promoted into a claimed/public execution path by this method.
     */
    Admission admitHistorical(RepositoryCaller caller, Key key, DocumentOperationUploadAdmission.Prepared prepared,
            UUID ownerNonce, Duration lease) {
        var plan = Objects.requireNonNull(prepared).plan();
        var command = plan.command();
        DocumentAdmissionAuthorization.requireCaller(caller, key, command.intent().getAccountId());
        DocumentHistoricalReferenceAdmission.requireComplete(command, plan.historical(), () -> {});
        if (plan.historical().isEmpty()) throw new IllegalArgumentException("Historical assessment requires pinned sources");
        if (!key.account.equals(command.intent().getAccountId()) || !key.operationId.equals(command.operationId()))
            throw new IllegalArgumentException("Publication command differs from operation scope");
        return admit(key, new EncodedCommand(DocumentPublicationCommand.CODEC,
                DocumentPublicationCommand.ENCODING_VERSION, command.canonical()), ownerNonce, lease, null, true);
    }

    Admission admit(Key key, DocumentPublicationCommand command, UUID ownerNonce, Duration lease,
            RepositoryExecutionClaimLedger.Claim claim) {
        Objects.requireNonNull(key); Objects.requireNonNull(command);
        command.requireExecutionSupported();
        if (!key.account.equals(command.intent().getAccountId()) || !key.operationId.equals(command.operationId()))
            throw new IllegalArgumentException("Publication command differs from operation scope");
        return admit(key, new EncodedCommand(DocumentPublicationCommand.CODEC,
                DocumentPublicationCommand.ENCODING_VERSION, command.canonical()), ownerNonce, lease, claim, true);
    }

    /**
     * The coordinator mints and retains its nonce before admission, including
     * across a lost acknowledgement. Exact retries never renew or take ownership.
     */
    Admission admit(Key key, EncodedCommand command, UUID ownerNonce, Duration lease) {
        if (DocumentPublicationCommand.CODEC.equals(command.codec()))
            throw new IllegalArgumentException("Publication codec requires typed command admission");
        return admit(key, command, ownerNonce, lease, null, false);
    }

    private Admission admit(Key key, EncodedCommand command, UUID ownerNonce, Duration lease,
            RepositoryExecutionClaimLedger.Claim claim, boolean typed) {
        Objects.requireNonNull(key); Objects.requireNonNull(command); Objects.requireNonNull(ownerNonce);
        if (DocumentPublicationCommand.CODEC.equals(command.codec()) && !typed)
            throw new IllegalArgumentException("Publication codec requires typed command admission");
        long millis = leaseMillis(lease);
        byte[] bytes = command.bytes.toByteArray();
        byte[] digest = digest(bytes);
        return tx.inTransaction(em -> {
            if (claim != null) {
                if (!key.equals(claim.key()) || !java.util.HexFormat.of().formatHex(digest).equals(claim.commandSha256()))
                    throw new IllegalArgumentException("Execution claim differs from command");
                RepositoryExecutionClaimLedger.lockLive(em, claim);
            }
            int inserted = bind(em.createNativeQuery("""
                    INSERT INTO repository_operations(account_id,principal,operation_id,command_codec,command_version,
                        command,command_sha256)
                    VALUES (:account,:principal,:id,:codec,:version,:command,:digest)
                    ON CONFLICT(account_id,principal,operation_id) DO NOTHING
                    """), key).setParameter("codec", command.codec).setParameter("version", command.version)
                    .setParameter("command", bytes).setParameter("digest", digest).executeUpdate();
            if (inserted == 1) bind(em.createNativeQuery("""
                    INSERT INTO repository_operation_owners(account_id,principal,operation_id,owner_token,owner_generation,lease_until)
                    VALUES (:account,:principal,:id,:owner,1,clock_timestamp()+(:millis * interval '1 millisecond'))
                    """), key).setParameter("owner", ownerNonce).setParameter("millis", millis).executeUpdate();
            // Separate statement: under READ COMMITTED this sees a competing
            // admission that committed while INSERT waited on the unique key.
            var row = read(em, key, true).orElseThrow();
            if (!row.snapshot.command.equals(command) || !Arrays.equals(row.digest, digest))
                throw new CommandConflictException();
            boolean live = live(em, key);
            var owner = row.token.equals(ownerNonce) && live
                    ? Optional.of(claim == null ? row.owner() : row.owner().withClaim(claim)) : Optional.<Owner>empty();
            return new Admission(row.snapshot, owner);
        });
    }

    /** Internal scoped observation; caller authorization is a coordinator obligation. */
    Optional<Snapshot> find(Key key) {
        Objects.requireNonNull(key);
        return tx.readOnly(em -> read(em, key, false).map(Row::snapshot));
    }

    /**
     * First lock in a coordinator transaction, before any domain/attempt locks.
     * Checks database time after waiting. It grants no policy or command binding;
     * those checks and the terminal outcome must join this same transaction.
     */
    static Owner lockLiveOwner(EntityManager em, Owner expected) {
        Objects.requireNonNull(expected);
        if (!em.getTransaction().isActive() || em.getTransaction().getRollbackOnly())
            throw new IllegalStateException("Operation fence requires an active writable transaction");
        try {
            if (expected.executionClaim.isPresent())
                RepositoryExecutionClaimLedger.lockLive(em, expected.executionClaim.orElseThrow());
            var current = readOwner(em, expected.key, true).orElseThrow(OwnerFencedException::new);
            boolean live = live(em, expected.key);
            if (current.generation != expected.generation || !current.token.equals(expected.token) || !live)
                throw new OwnerFencedException();
            return new Owner(current.key, current.generation, current.token, current.leaseUntil, expected.executionClaim);
        } catch (RuntimeException | Error failure) {
            // A caller catching a fence failure cannot commit later domain work.
            try { em.getTransaction().setRollbackOnly(); }
            catch (RuntimeException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
            throw failure;
        }
    }

    /**
     * Establish SQL-checkable proof before dependent row locks. One owner-only
     * update; no command bytes or lease extension. The V34 guard checks liveness
     * after a lock wait and V35 stamps the actual transaction ID.
     */
    static Owner fenceLiveOwner(EntityManager em, Owner expected) {
        Objects.requireNonNull(expected);
        if (!em.getTransaction().isActive() || em.getTransaction().getRollbackOnly())
            throw new IllegalStateException("Operation fence requires an active writable transaction");
        try {
            if (expected.executionClaim.isPresent())
                RepositoryExecutionClaimLedger.lockLive(em, expected.executionClaim.orElseThrow());
            var rows = bind(em.createNativeQuery("""
                    UPDATE repository_operation_owners SET write_fence_xid=pg_current_xact_id()
                    WHERE account_id=:account AND principal=:principal AND operation_id=:id
                      AND owner_generation=:generation AND owner_token=:token AND lease_until > clock_timestamp()
                    RETURNING owner_token,owner_generation,lease_until
                    """), expected.key).setParameter("generation", expected.generation)
                    .setParameter("token", expected.token).getResultList();
            if (rows.isEmpty()) throw new OwnerFencedException();
            Object[] row = (Object[]) rows.getFirst();
            return new Owner(expected.key, ((Number) row[1]).longValue(), (UUID) row[0], instant(row[2]), expected.executionClaim);
        } catch (RuntimeException | Error failure) {
            try { em.getTransaction().setRollbackOnly(); }
            catch (RuntimeException markingFailure) {
                if (markingFailure != failure) failure.addSuppressed(markingFailure);
            }
            throw failure;
        }
    }

    Owner renew(Owner owner, Duration lease) {
        Objects.requireNonNull(owner);
        long millis = leaseMillis(lease);
        return tx.inTransaction(em -> {
            lockLiveOwner(em, owner);
            // Both leases advance or neither does. Claim locking remains before owner locking.
            var claim = owner.executionClaim.map(value -> RepositoryExecutionClaimLedger.renewLive(em, value, lease));
            int changed = bind(em.createNativeQuery("""
                    UPDATE repository_operation_owners SET lease_until=GREATEST(lease_until,
                        clock_timestamp()+(:millis * interval '1 millisecond'))
                    WHERE account_id=:account AND principal=:principal AND operation_id=:id
                      AND lease_until > clock_timestamp()
                    """), owner.key).setParameter("millis", millis).executeUpdate();
            if (changed != 1) throw new OwnerFencedException();
            var renewed = readOwner(em, owner.key, false).orElseThrow();
            return new Owner(renewed.key, renewed.generation, renewed.token, renewed.leaseUntil, claim);
        });
    }

    /** CAS takeover; the same nonce can reconcile a lost takeover acknowledgement. */
    Owner takeOver(Key key, long expectedGeneration, UUID nextNonce, Duration lease) {
        return takeOver(key, expectedGeneration, nextNonce, lease, null);
    }

    /**
     * Observe permanent fencing only. Missing or earlier generations do not prove
     * supersession; expiry of the same nonce does not permit its retirement.
     */
    boolean isSuperseded(Key key, DocumentPublicationCommand command, long generation, UUID nonce) {
        Objects.requireNonNull(key); Objects.requireNonNull(command); Objects.requireNonNull(nonce);
        if (generation < 1 || !key.account.equals(command.intent().getAccountId()) || !key.operationId.equals(command.operationId()))
            throw new IllegalArgumentException("Ownership observation differs from operation scope");
        return tx.inTransaction(em -> {
            var found = readOwner(em, key, true);
            if (found.isEmpty()) return false;
            requireCommand(em, key, command);
            var row = found.orElseThrow();
            return row.generation > generation || (row.generation == generation && !row.token.equals(nonce));
        });
    }

    /** Confirm the exact expired recovery owner; the later takeover must still win its CAS. */
    void requireExpiredRecovery(Key key, DocumentPublicationCommand command, long generation, UUID nonce) {
        Objects.requireNonNull(key); Objects.requireNonNull(command); Objects.requireNonNull(nonce);
        if (generation < 1 || !key.account.equals(command.intent().getAccountId()) || !key.operationId.equals(command.operationId()))
            throw new IllegalArgumentException("Recovery observation differs from operation scope");
        tx.inTransaction(em -> {
            var row = readOwner(em, key, true).orElseThrow(OwnerFencedException::new);
            requireCommand(em, key, command);
            boolean active = live(em, key);
            if (row.generation != generation || !row.token.equals(nonce) || active) throw new OwnerFencedException();
            return null;
        });
    }

    /**
     * Document recovery verifies the immutable command under the same owner lock
     * as takeover, including an exact retry after an uncertain acknowledgement.
     * Trusted caller authorization remains the host's responsibility.
     */
    Owner takeOver(Key key, DocumentPublicationCommand command, long expectedGeneration, UUID nextNonce, Duration lease) {
        return takeOver(key, command, expectedGeneration, nextNonce, lease, null);
    }

    Owner takeOver(Key key, DocumentPublicationCommand command, long expectedGeneration, UUID nextNonce, Duration lease,
            RepositoryExecutionClaimLedger.Claim claim) {
        Objects.requireNonNull(key); Objects.requireNonNull(command);
        command.requireExecutionSupported();
        if (!key.account.equals(command.intent().getAccountId()) || !key.operationId.equals(command.operationId()))
            throw new IllegalArgumentException("Publication command differs from operation scope");
        if (claim != null && (!key.equals(claim.key()) || !command.sha256().equals(claim.commandSha256())))
            throw new IllegalArgumentException("Execution claim differs from takeover command");
        return takeOver(key, expectedGeneration, nextNonce, lease, command, claim);
    }

    private Owner takeOver(Key key, long expectedGeneration, UUID nextNonce, Duration lease,
            DocumentPublicationCommand command) {
        return takeOver(key, expectedGeneration, nextNonce, lease, command, null);
    }

    private Owner takeOver(Key key, long expectedGeneration, UUID nextNonce, Duration lease,
            DocumentPublicationCommand command, RepositoryExecutionClaimLedger.Claim claim) {
        Objects.requireNonNull(key); Objects.requireNonNull(nextNonce);
        if (expectedGeneration < 1 || expectedGeneration == Long.MAX_VALUE)
            throw new IllegalArgumentException("Invalid takeover generation");
        long millis = leaseMillis(lease);
        return tx.inTransaction(em -> {
            if (claim != null) RepositoryExecutionClaimLedger.lockLive(em, claim);
            var row = readOwner(em, key, true).orElseThrow(OwnerFencedException::new);
            if (command != null) requireCommand(em, key, command);
            boolean live = live(em, key);
            if (row.generation == expectedGeneration + 1 && row.token.equals(nextNonce) && live)
                return claim == null ? row : row.withClaim(claim);
            if (row.generation != expectedGeneration || row.token.equals(nextNonce) || live)
                throw new OwnerFencedException();
            bind(em.createNativeQuery("""
                    UPDATE repository_operation_owners SET owner_token=:owner,owner_generation=owner_generation+1,
                        lease_until=clock_timestamp()+(:millis * interval '1 millisecond')
                    WHERE account_id=:account AND principal=:principal AND operation_id=:id
                    """), key).setParameter("owner", nextNonce).setParameter("millis", millis).executeUpdate();
            var taken = readOwner(em, key, false).orElseThrow();
            return claim == null ? taken : taken.withClaim(claim);
        });
    }

    private record Row(Snapshot snapshot, UUID token, byte[] digest) {
        Owner owner() { return new Owner(snapshot.key, snapshot.generation, token, snapshot.leaseUntil); }
    }

    private static Optional<Row> read(EntityManager em, Key key, boolean lock) {
        var rows = bind(em.createNativeQuery("""
                SELECT command_codec,command_version,command,command_sha256,owner_token,owner_generation,
                    lease_until,created_at FROM repository_operations c JOIN repository_operation_owners o
                    USING(account_id,principal,operation_id)
                WHERE c.account_id=:account AND c.principal=:principal AND c.operation_id=:id
                """ + (lock ? " FOR UPDATE OF o" : "")), key).getResultList();
        if (rows.isEmpty()) return Optional.empty();
        Object[] row = (Object[]) rows.getFirst();
        var command = new EncodedCommand((String) row[0], ((Number) row[1]).intValue(), ByteString.copyFrom((byte[]) row[2]));
        var snapshot = new Snapshot(key, command, ((Number) row[5]).longValue(),
                instant(row[6]), instant(row[7]));
        return Optional.of(new Row(snapshot, (UUID) row[4], (byte[]) row[3]));
    }

    /** Heartbeats and takeover never transfer the potentially large command body. */
    private static Optional<Owner> readOwner(EntityManager em, Key key, boolean lock) {
        var rows = bind(em.createNativeQuery("""
                SELECT owner_token,owner_generation,lease_until FROM repository_operation_owners
                WHERE account_id=:account AND principal=:principal AND operation_id=:id
                """ + (lock ? " FOR UPDATE" : "")), key).getResultList();
        if (rows.isEmpty()) return Optional.empty();
        Object[] row = (Object[]) rows.getFirst();
        return Optional.of(new Owner(key, ((Number) row[1]).longValue(), (UUID) row[0], instant(row[2])));
    }

    private static Instant instant(Object value) {
        if (value instanceof Instant instant) return instant;
        if (value instanceof java.time.OffsetDateTime time) return time.toInstant();
        if (value instanceof java.sql.Timestamp time) return time.toInstant();
        throw new IllegalStateException("Unsupported database timestamp type: " + value.getClass().getName());
    }

    private static boolean live(EntityManager em, Key key) {
        // Evaluate DB time after the owner row lock, never before a lock wait.
        var state = (Object[]) bind(em.createNativeQuery("""
                SELECT lease_until > clock_timestamp(), EXISTS(SELECT 1 FROM repository_operation_success s
                    WHERE s.account_id=o.account_id AND s.principal=o.principal AND s.operation_id=o.operation_id)
                    OR EXISTS(SELECT 1 FROM repository_operation_rejection r
                    WHERE r.account_id=o.account_id AND r.principal=o.principal AND r.operation_id=o.operation_id)
                FROM repository_operation_owners o
                WHERE o.account_id=:account AND o.principal=:principal AND o.operation_id=:id
                """), key).getSingleResult();
        if (Boolean.TRUE.equals(state[1])) throw new TerminalOperationException();
        return Boolean.TRUE.equals(state[0]);
    }

    private static jakarta.persistence.Query bind(jakarta.persistence.Query query, Key key) {
        return query.setParameter("account", key.account).setParameter("principal", key.principal)
                .setParameter("id", key.operationId);
    }

    private static byte[] digest(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException missing) { throw new IllegalStateException("SHA-256 is unavailable", missing); }
    }

    private static long leaseMillis(Duration lease) {
        Objects.requireNonNull(lease);
        if (lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Operation lease requires one second to one day");
        return lease.toMillis();
    }

    private static void text(String value, int max, String name) {
        if (value == null || value.isBlank() || value.length() > max || value.indexOf('\0') >= 0)
            throw new IllegalArgumentException("Invalid " + name);
    }
}
