package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.archive.v1.ArchiveMutationReceipt;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationState;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Current cleanup snapshots, serialized per operation. Authorize before lookup. */
public final class ArchiveMutationObservations {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private final Tx tx;

    public ArchiveMutationObservations(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    public Optional<ArchiveMutationReceipt> observe(String principal, String account, UUID operationId) {
        if (principal == null || principal.isBlank() || principal.length() > 200
                || account == null || account.isBlank() || account.length() > 200)
            throw new IllegalArgumentException("Bounded account and trusted principal are required");
        Objects.requireNonNull(operationId);
        return tx.inTransaction(em -> {
            var admissions = em.createNativeQuery("""
                    SELECT admission_receipt FROM archive_mutations
                    WHERE account_id=:account AND principal=:principal AND operation_id=:id
                    """).setParameter("account", account).setParameter("principal", principal)
                    .setParameter("id", operationId).getResultList();
            if (admissions.isEmpty()) return Optional.empty();
            var admission = decode((byte[]) admissions.getFirst());
            em.createNativeQuery("""
                    INSERT INTO archive_mutation_observations(account_id,principal,operation_id,status_revision,receipt)
                    VALUES (:account,:principal,:id,1,:receipt) ON CONFLICT DO NOTHING
                    """).setParameter("account", account).setParameter("principal", principal)
                    .setParameter("id", operationId).setParameter("receipt", admission.toByteArray()).executeUpdate();
            var stored = (Object[]) em.createNativeQuery("""
                    SELECT status_revision,receipt FROM archive_mutation_observations
                    WHERE account_id=:account AND principal=:principal AND operation_id=:id FOR UPDATE
                    """).setParameter("account", account).setParameter("principal", principal)
                    .setParameter("id", operationId).getSingleResult();
            long revision = ((Number) stored[0]).longValue();
            var previous = decode((byte[]) stored[1]);
            if (previous.getStatusRevision() != revision)
                throw new IllegalStateException("Stored archive observation revision differs from receipt");
            var logical = previous.toBuilder().setState(admission.getState())
                    .setObjectsPending(admission.getObjectsPending()).setObjectsConfirmedAbsent(admission.getObjectsConfirmedAbsent())
                    .clearErrorCode().setObservedAt(admission.getObservedAt()).setStatusRevision(admission.getStatusRevision()).build();
            if (!logical.equals(admission)) throw new IllegalStateException("Archive observation differs from its logical admission");
            // A single statement observes all targets at one MVCC snapshot. Do not
            // read each target separately or mix observations from different times.
            var snapshot = (Object[]) em.createNativeQuery("""
                    SELECT count(*),count(*) FILTER (WHERE u.state='DELETED'),
                           bool_or(u.cleanup_attempts>0),
                           min(u.cleanup_error) FILTER (WHERE u.state<>'DELETED'),
                           count(*) FILTER (WHERE u.state NOT IN ('LIVE','DELETING','DELETED'))
                    FROM archive_mutation_targets t JOIN archive_object_uploads u USING(object_id)
                    WHERE t.account_id=:account AND t.principal=:principal AND t.operation_id=:id
                    """).setParameter("account", account).setParameter("principal", principal)
                    .setParameter("id", operationId).getSingleResult();
            long targets = ((Number) snapshot[0]).longValue();
            if (targets != admission.getObjectsTargeted())
                throw new IllegalStateException("Archive mutation target count differs from admission");
            long confirmed = ((Number) snapshot[1]).longValue();
            boolean attempted = Boolean.TRUE.equals(snapshot[2]);
            String error = (String) snapshot[3];
            if (((Number) snapshot[4]).longValue() != 0)
                throw new IllegalStateException("Archive mutation target has invalid cleanup state");
            long pending = targets - confirmed;
            var state = pending == 0 ? ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED
                    : error != null ? ArchiveMutationState.ARCHIVE_MUTATION_STATE_RETRY_REQUIRED
                    : attempted ? ArchiveMutationState.ARCHIVE_MUTATION_STATE_RECLAIMING
                    : ArchiveMutationState.ARCHIVE_MUTATION_STATE_ADMITTED;
            var candidate = admission.toBuilder().setState(state).setObjectsPending(pending)
                    .setObjectsConfirmedAbsent(confirmed).clearErrorCode()
                    .setObservedAt(previous.getObservedAt()).setStatusRevision(revision);
            if (error != null) candidate.setErrorCode(error);
            if (candidate.build().equals(previous)) return Optional.of(previous);
            Instant now = em.unwrap(org.hibernate.Session.class)
                    .createNativeQuery("SELECT clock_timestamp()", Instant.class).getSingleResult();
            var receipt = candidate.setStatusRevision(Math.incrementExact(revision))
                    .setObservedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano())).build();
            if (!VALIDATOR.validate(receipt).valid()) throw new IllegalStateException("Invalid archive cleanup observation");
            int changed = em.createNativeQuery("""
                    UPDATE archive_mutation_observations SET status_revision=:next,receipt=:receipt
                    WHERE account_id=:account AND principal=:principal AND operation_id=:id AND status_revision=:previous
                    """).setParameter("next", receipt.getStatusRevision()).setParameter("receipt", receipt.toByteArray())
                    .setParameter("account", account).setParameter("principal", principal).setParameter("id", operationId)
                    .setParameter("previous", revision).executeUpdate();
            if (changed != 1) throw new IllegalStateException("Archive observation changed under its row lock");
            return Optional.of(receipt);
        });
    }

    private static ArchiveMutationReceipt decode(byte[] bytes) {
        try {
            var receipt = ArchiveMutationReceipt.parseFrom(bytes);
            if (!VALIDATOR.validate(receipt).valid()) throw new IllegalStateException("Invalid stored archive receipt");
            return receipt;
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalStateException("Malformed stored archive receipt", malformed);
        }
    }
}
