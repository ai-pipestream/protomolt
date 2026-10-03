package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.archive.v1.ArchiveMutationReceipt;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationState;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Timestamp;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * Atomic logical mutation and immutable admission receipt. Internal API: the
 * caller must authorize the current principal before admission AND replay.
 * This entry-scoped transaction is not a content-repository session commit.
 */
public final class ArchiveMutationLedger {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private final Tx tx;

    public ArchiveMutationLedger(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /** Apply a reviewed archive command, including logical counters and cleanup targets. */
    public ArchiveMutationReceipt execute(String principal, ArchiveMutationCommand command, long sampledRevision) {
        return admit(principal, command, sampledRevision,
                (em, entry) -> ArchiveDestructiveMutations.apply(em, entry, command));
    }

    public record LogicalOutcome(boolean entryDeleted, long versionsRemoved,
            long versionsTombstoned, Set<UUID> targets) {
        public LogicalOutcome { targets = Set.copyOf(targets); }
    }

    /**
     * Execute SQL changes once, under the sampled entry revision. The callback
     * receives the locked entry (null when absent) and this transaction's EM.
     * It must derive targets from removed references, maintain archive counters,
     * and perform no provider I/O or nested transactions. A missing entry can
     * only produce a no-op; handlers may instead report NOT_FOUND.
     */
    public ArchiveMutationReceipt admit(String principal, ArchiveMutationCommand command,
            long sampledRevision, BiFunction<EntityManager, ArchiveEntryRecord, LogicalOutcome> mutation) {
        requireText(principal, "principal");
        Objects.requireNonNull(command);
        Objects.requireNonNull(mutation);
        if (sampledRevision < 0) throw new IllegalArgumentException("Negative entry revision");
        checkCancellation();
        String account = command.address().getAccountId();
        return tx.inTransaction(em -> {
            checkCancellation();
            // Hash collisions only serialize unrelated operations. The SQL primary
            // key and exact command bytes, not this hash, establish identity.
            String lockKey = account.length() + ":" + account + principal.length() + ":" + principal + command.operationId();
            em.createNativeQuery("SELECT 1 FROM pg_advisory_xact_lock(hashtextextended(:key,0))")
                    .setParameter("key", lockKey).getSingleResult();
            var stored = read(em, principal, account, command.operationId());
            if (stored.isPresent()) {
                var row = stored.get();
                if (!command.sha256().equals(row[0]) || !Arrays.equals(command.canonical().toByteArray(), (byte[]) row[1]))
                    throw new OperationConflictException("Operation identity already belongs to another command");
                return decode((byte[]) row[2]);
            }
            UUID entryId = ArchiveIds.entryUuid(command.address());
            var entry = em.find(ArchiveEntryRecord.class, entryId, LockModeType.PESSIMISTIC_WRITE);
            if ((entry == null ? 0 : entry.mutationRevision) != sampledRevision)
                throw new ArchiveLedger.VersionConflictException("Archive entry changed before mutation admission");
            if (entry != null && (!entry.accountId.equals(account)
                    || !entry.archive.equals(command.address().getArchive())
                    || !entry.entryId.equals(command.address().getEntryId())))
                throw new IllegalStateException("Archive entry identity does not match its address");
            var previouslyReferenced = Set.copyOf(em.unwrap(org.hibernate.Session.class)
                    .createNativeQuery("SELECT DISTINCT object_id FROM archive_version_object_refs WHERE entry_uuid=:entry", UUID.class)
                    .setParameter("entry", entryId).getResultList());
            var outcome = Objects.requireNonNull(mutation.apply(em, entry));
            checkCancellation();
            if (entry == null && (outcome.entryDeleted() || outcome.versionsRemoved() != 0
                    || outcome.versionsTombstoned() != 0 || !outcome.targets().isEmpty()))
                throw new IllegalArgumentException("An absent entry cannot produce a mutation outcome");
            em.flush();
            for (UUID target : outcome.targets().stream().sorted().toList()) {
                var upload = ArchiveUploadLedger.lock(em, target);
                var binding = ArchiveObjectLedger.find(em, target).orElseThrow();
                var location = binding.location();
                if (!location.entryUuid().equals(entryId) || !location.accountId().equals(account)
                        || !location.archive().equals(command.address().getArchive()))
                    throw new IllegalArgumentException("Mutation target is outside the entry scope");
                if (!Set.of("LIVE", "DELETING", "DELETED").contains(upload.state()))
                    throw new IllegalArgumentException("Mutation target was not published");
                // DELETED can also describe an abandoned upload. Only references
                // observed before this mutation prove that it owned the object.
                if (!previouslyReferenced.contains(target))
                    throw new IllegalArgumentException("Mutation target was not referenced before this command");
                Number refs = (Number) em.createNativeQuery("SELECT count(*) FROM archive_version_object_refs WHERE object_id=:id")
                        .setParameter("id", target).getSingleResult();
                if (refs.longValue() != 0) throw new IllegalArgumentException("Mutation target has retained references");
            }
            Instant now = em.unwrap(org.hibernate.Session.class)
                    .createNativeQuery("SELECT clock_timestamp()", Instant.class).getSingleResult();
            var receipt = ArchiveMutationReceipt.newBuilder().setOperationId(command.operationId().toString())
                    .setAddress(command.address()).setKind(command.kind()).setCommandSha256(command.sha256())
                    .setEntryDeleted(outcome.entryDeleted()).setVersionsRemoved(outcome.versionsRemoved())
                    .setVersionsTombstoned(outcome.versionsTombstoned()).setObjectsTargeted(outcome.targets().size())
                    .setObjectsPending(outcome.targets().size()).setStatusRevision(1)
                    .setState(outcome.targets().isEmpty() ? ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED
                            : ArchiveMutationState.ARCHIVE_MUTATION_STATE_ADMITTED)
                    .setObservedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano())).build();
            if (!VALIDATOR.validate(receipt).valid()) throw new IllegalArgumentException("Invalid archive mutation outcome");
            checkCancellation();
            em.createNativeQuery("""
                    INSERT INTO archive_mutations(account_id,principal,operation_id,command_sha256,command,admission_receipt,sampled_revision)
                    VALUES (:account,:principal,:id,:sha,:command,:receipt,:revision)
                    """).setParameter("account", account).setParameter("principal", principal).setParameter("id", command.operationId())
                    .setParameter("sha", command.sha256()).setParameter("command", command.canonical().toByteArray())
                    .setParameter("receipt", receipt.toByteArray()).setParameter("revision", sampledRevision).executeUpdate();
            for (UUID target : outcome.targets()) {
                em.createNativeQuery("""
                        INSERT INTO archive_mutation_targets(account_id,principal,operation_id,object_id)
                        VALUES (:account,:principal,:id,:target)
                        """).setParameter("account", account).setParameter("principal", principal)
                        .setParameter("id", command.operationId()).setParameter("target", target).executeUpdate();
            }
            return receipt;
        });
    }

    /** Initial admission only, not a fresh physical observation. Authorize before lookup. */
    public Optional<ArchiveMutationReceipt> find(String principal, String account, UUID operationId) {
        requireText(principal, "principal");
        requireText(account, "account");
        Objects.requireNonNull(operationId);
        return tx.readOnly(em -> read(em, principal, account, operationId).map(row -> decode((byte[]) row[2])));
    }

    private static Optional<Object[]> read(EntityManager em, String principal, String account, UUID operationId) {
        var rows = em.createNativeQuery("""
                SELECT command_sha256,command,admission_receipt FROM archive_mutations
                WHERE account_id=:account AND principal=:principal AND operation_id=:id
                """).setParameter("account", account).setParameter("principal", principal).setParameter("id", operationId).getResultList();
        return rows.isEmpty() ? Optional.empty() : Optional.of((Object[]) rows.getFirst());
    }

    private static ArchiveMutationReceipt decode(byte[] bytes) {
        try {
            var receipt = ArchiveMutationReceipt.parseFrom(bytes);
            if (!VALIDATOR.validate(receipt).valid()) throw new IllegalStateException("Invalid stored archive mutation receipt");
            return receipt;
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalStateException("Malformed stored archive mutation receipt", malformed);
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank() || value.length() > 200)
            throw new IllegalArgumentException(name + " must contain between 1 and 200 characters");
    }

    public static final class OperationConflictException extends RuntimeException {
        public OperationConflictException(String message) { super(message); }
    }

    public static final class EntryMissingException extends RuntimeException {
        public EntryMissingException() { super("Archive entry does not exist"); }
    }

    public static final class MigrationRequiredException extends RuntimeException {
        public MigrationRequiredException() { super("Unbound archive content requires explicit storage identity migration before mutation"); }
    }

    private static void checkCancellation() {
        if (Thread.currentThread().isInterrupted())
            throw new java.util.concurrent.CancellationException("Archive mutation cancelled before admission");
    }
}
