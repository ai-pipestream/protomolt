package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Recover original coordinates under a live owner; never grants byte-read or decision authority. */
final class DocumentAssessmentDiscovery {
    record Original(DocumentAssessmentCreation.Created stage,
                    Map<String, DocumentAssessmentRetainedSlots.UploadSelection> selections) {
        Original { Objects.requireNonNull(stage); selections = Map.copyOf(selections); }
    }

    private final Tx tx;
    DocumentAssessmentDiscovery(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * The host must retain the original private owner nonce. This lookup recovers
     * only the unique stage in that exact generation, never a latest stage or a
     * predecessor. Call captureAssessment with these coordinates to verify the
     * manifest, retained slots and immutable selections before reading evidence.
     * Empty is not proof of rollback or permission to create another stage after
     * an uncertain write. Expired, released or inconsistent rows fail explicitly.
     * Current policy and observed runtime are checked by the later decision gate.
     */
    Optional<Original> discover(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Runnable control) {
        Objects.requireNonNull(owner); Objects.requireNonNull(command); active(control);
        if (!owner.key().operationId().equals(command.operationId())
                || !owner.key().account().equals(command.intent().getAccountId()))
            throw new IllegalArgumentException("Assessment discovery differs from operation scope");
        DocumentAdmissionAuthorization.requireCaller(caller, owner, command.intent().getAccountId());
        return tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            RepositoryOperationLedger.requireCommand(em, owner.key(), command);
            DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
            active(control);
            var rows = em.createNativeQuery("""
                    SELECT assessment_id,command_codec,command_version,encode(command_sha256,'hex'),
                        encode(manifest_sha256,'hex'),CAST(floor(extract(epoch FROM retain_until)*1000000) AS bigint),
                        sealed AND release_xid IS NULL AND retain_until>clock_timestamp()
                    FROM document_assessment_owners WHERE account_id=:account AND principal=:principal
                        AND operation_id=:op AND owner_generation=:generation LIMIT 2 FOR UPDATE
                    """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                    .setParameter("op", command.operationId()).setParameter("generation", owner.generation()).getResultList();
            if (rows.isEmpty()) {
                active(control);
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                return Optional.empty();
            }
            if (rows.size() != 1) throw unavailable();
            var row = (Object[]) rows.getFirst();
            if (!DocumentPublicationCommand.CODEC.equals(row[1])
                    || ((Number) row[2]).intValue() != DocumentPublicationCommand.ENCODING_VERSION
                    || !command.sha256().equals(row[3]) || !Boolean.TRUE.equals(row[6])) throw unavailable();
            long micros = ((Number) row[5]).longValue();
            var deadline = Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000), Math.floorMod(micros, 1_000_000) * 1000);
            var identity = new DocumentAssessmentSlotSnapshot.Identity((UUID) row[0], owner.key(), owner.generation(),
                    command.sha256(), (String) row[4], deadline);
            var selections = DocumentAssessmentRetainedSlots.retainedSelections(em, identity, () -> active(control));
            active(control);
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            if (!Boolean.TRUE.equals(em.createNativeQuery("""
                    SELECT sealed AND release_xid IS NULL AND retain_until>clock_timestamp()
                    FROM document_assessment_owners WHERE assessment_id=:id
                    """).setParameter("id", identity.assessment()).getSingleResult())) throw unavailable();
            return Optional.of(new Original(new DocumentAssessmentCreation.Created(identity.assessment(),
                    identity.manifestSha256(), deadline), selections));
        });
    }

    private static IllegalStateException unavailable() {
        return new IllegalStateException("Original retained assessment is unavailable or inconsistent");
    }
    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Assessment discovery interrupted");
        Objects.requireNonNull(control).run();
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Assessment discovery interrupted");
    }
}
