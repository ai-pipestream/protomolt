package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
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

    record Owner(Key key, long generation, UUID token, Instant leaseUntil) {
        Owner {
            Objects.requireNonNull(key); Objects.requireNonNull(token); Objects.requireNonNull(leaseUntil);
            if (generation < 1) throw new IllegalArgumentException("Invalid owner generation");
        }
        @Override public String toString() { return "Owner[generation=" + generation + ", leaseUntil=" + leaseUntil + "]"; }
    }

    record Admission(Snapshot snapshot, Optional<Owner> owner) {}

    static final class CommandConflictException extends RuntimeException {
        CommandConflictException() { super("Operation identity already belongs to another command"); }
    }

    static final class OwnerFencedException extends RuntimeException {
        OwnerFencedException() { super("Repository operation owner is absent, expired or replaced"); }
    }

    /** Typed document admission; scope binding is checked before opening a transaction. */
    Admission admit(Key key, DocumentPublicationCommand command, UUID ownerNonce, Duration lease) {
        Objects.requireNonNull(key); Objects.requireNonNull(command);
        if (!key.account.equals(command.intent().getAccountId()) || !key.operationId.equals(command.operationId()))
            throw new IllegalArgumentException("Publication command differs from operation scope");
        return admit(key, new EncodedCommand(DocumentPublicationCommand.CODEC,
                DocumentPublicationCommand.ENCODING_VERSION, command.canonical()), ownerNonce, lease);
    }

    /**
     * The coordinator mints and retains its nonce before admission, including
     * across a lost acknowledgement. Exact retries never renew or take ownership.
     */
    Admission admit(Key key, EncodedCommand command, UUID ownerNonce, Duration lease) {
        Objects.requireNonNull(key); Objects.requireNonNull(command); Objects.requireNonNull(ownerNonce);
        long millis = leaseMillis(lease);
        byte[] bytes = command.bytes.toByteArray();
        byte[] digest = digest(bytes);
        return tx.inTransaction(em -> {
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
            var owner = row.token.equals(ownerNonce) && live(em, key)
                    ? Optional.of(row.owner()) : Optional.<Owner>empty();
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
            var current = readOwner(em, expected.key, true).orElseThrow(OwnerFencedException::new);
            if (current.generation != expected.generation || !current.token.equals(expected.token) || !live(em, expected.key))
                throw new OwnerFencedException();
            return current;
        } catch (RuntimeException | Error failure) {
            // A caller catching a fence failure cannot commit later domain work.
            try { em.getTransaction().setRollbackOnly(); }
            catch (RuntimeException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
            throw failure;
        }
    }

    Owner renew(Owner owner, Duration lease) {
        Objects.requireNonNull(owner);
        long millis = leaseMillis(lease);
        return tx.inTransaction(em -> {
            lockLiveOwner(em, owner);
            int changed = bind(em.createNativeQuery("""
                    UPDATE repository_operation_owners SET lease_until=GREATEST(lease_until,
                        clock_timestamp()+(:millis * interval '1 millisecond'))
                    WHERE account_id=:account AND principal=:principal AND operation_id=:id
                      AND lease_until > clock_timestamp()
                    """), owner.key).setParameter("millis", millis).executeUpdate();
            if (changed != 1) throw new OwnerFencedException();
            return readOwner(em, owner.key, false).orElseThrow();
        });
    }

    /** CAS takeover; the same nonce can reconcile a lost takeover acknowledgement. */
    Owner takeOver(Key key, long expectedGeneration, UUID nextNonce, Duration lease) {
        Objects.requireNonNull(key); Objects.requireNonNull(nextNonce);
        if (expectedGeneration < 1 || expectedGeneration == Long.MAX_VALUE)
            throw new IllegalArgumentException("Invalid takeover generation");
        long millis = leaseMillis(lease);
        return tx.inTransaction(em -> {
            var row = readOwner(em, key, true).orElseThrow(OwnerFencedException::new);
            if (row.generation == expectedGeneration + 1 && row.token.equals(nextNonce) && live(em, key))
                return row;
            if (row.generation != expectedGeneration || row.token.equals(nextNonce) || live(em, key))
                throw new OwnerFencedException();
            bind(em.createNativeQuery("""
                    UPDATE repository_operation_owners SET owner_token=:owner,owner_generation=owner_generation+1,
                        lease_until=clock_timestamp()+(:millis * interval '1 millisecond')
                    WHERE account_id=:account AND principal=:principal AND operation_id=:id
                    """), key).setParameter("owner", nextNonce).setParameter("millis", millis).executeUpdate();
            return readOwner(em, key, false).orElseThrow();
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
        return (Boolean) bind(em.createNativeQuery("""
                SELECT lease_until > clock_timestamp() FROM repository_operation_owners
                WHERE account_id=:account AND principal=:principal AND operation_id=:id
                """), key).getSingleResult();
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
