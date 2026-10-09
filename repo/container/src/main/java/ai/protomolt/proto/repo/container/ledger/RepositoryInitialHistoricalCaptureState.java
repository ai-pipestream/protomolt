package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.security.MessageDigest;
import java.util.*;

/** Private disposal evidence only. The caller must first stop and join initial registration. */
final class RepositoryInitialHistoricalCaptureState {
    private RepositoryInitialHistoricalCaptureState() {}
    enum State { ABSENT, REGISTERED }

    static State classify(Tx tx, PayloadBudget budget, RepositoryCaller coordinator,
            DocumentPublicationPreparationRecord record, Map<String, DocumentPublicationCandidate.Mode> modes,
            RepositoryCoordinatorDrain.Identity expected, DocumentPreparationCaptureDrain.Identity capture,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(coordinator, record.key(), record.key().account());
        if (!coordinator.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Initial capture classification requires private process authority");
        if (record.predecessorGeneration() != 0 || expected.epoch() != 1 || capture.generation() != 0
                || !expected.equals(capture.owner()) || !record.key().equals(expected.key())
                || !record.command().sha256().equals(expected.commandSha256()))
            throw inconsistent();
        var fixedModes = Map.copyOf(modes);
        try (var reserved = budget.reserve(3L * DocumentPublicationPreparationCodec.MAX_BYTES
                + DocumentPublicationModesJournal.MAX_BYTES)) {
            DocumentPublicationModesJournal.encode(record.command(), fixedModes);
            var encoded = DocumentPublicationPreparationCodec.encode(record);
            var digest = DocumentPublicationPreparationJournal.digest(encoded);
            var state = tx.inTransaction(em -> {
                em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
                // The exact registration has joined. A missing row is not a lock on a future INSERT.
                var claims = scope(em.createNativeQuery("""
                        SELECT command_sha256,claim_token FROM repository_execution_claims
                        WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE
                        """), record).getResultList();
                control.check();
                long preparation = count(em, record, "repository_publication_preparations",
                        "predecessor_generation=0 AND owner_nonce=:nonce", "nonce", record.seeds().ownerNonce());
                long choices = count(em, record, "repository_publication_modes",
                        "predecessor_generation=0 AND owner_nonce=:nonce", "nonce", record.seeds().ownerNonce());
                long binding = count(em, record, "repository_coordinator_bindings",
                        "claim_epoch=1 AND claim_token=:token", "token", expected.token());
                long roots = count(em, record, "repository_preparation_history_sets",
                        "predecessor_generation=0 AND preparation_sha256=:sha", "sha", digest);
                long batches = captureCount(em, record, "repository_preparation_pin_batches", capture);
                long owners = captureCount(em, record, "repository_preparation_pin_owners", capture);
                long pins = captureCount(em, record, "repository_preparation_source_pins", capture);
                if (preparation == 0 && choices == 0 && binding == 0 && roots == 0
                        && batches == 0 && owners == 0 && pins == 0) {
                    if (claims.isEmpty()) {
                        // No initial claim can leave even foreign durable registration fragments.
                        for (var table : List.of("repository_execution_scopes", "repository_operations", "repository_coordinator_bindings",
                                "repository_publication_preparations", "repository_publication_modes",
                                "repository_preparation_history_sets", "repository_preparation_history_roots",
                                "repository_preparation_pin_batches", "repository_preparation_pin_owners",
                                "repository_preparation_source_pins")) {
                            if (count(em, record, table, "true", null, null) != 0) throw inconsistent();
                        }
                    } else if (expected.token().equals(((Object[]) claims.getFirst())[1])) {
                        throw inconsistent();
                    }
                    control.check();
                    return State.ABSENT; // Another initial winner never lends this capture its identity.
                }
                if (claims.size() != 1 || preparation != 1 || choices != 1 || binding != 1
                        || roots != 1 || batches != 1 || owners != 1 || pins < 1
                        || !MessageDigest.isEqual((byte[]) ((Object[]) claims.getFirst())[0],
                                HexFormat.of().parseHex(expected.commandSha256()))) throw inconsistent();
                requireRegistration(em, record, fixedModes, encoded.toByteArray(), expected);
                requireCapture(em, record, capture, pins);
                if (count(em, record, "repository_preparation_root_releases", "predecessor_generation=0", null, null) != 0) {
                    DocumentPreparationRootReleases.requireReleased(em, coordinator, record, control);
                } else {
                    DocumentHistoricalRetentionBinding.require(em, record, digest, true);
                }
                control.check();
                return State.REGISTERED;
            });
            control.check();
            return state;
        }
    }

    private static void requireRegistration(EntityManager em, DocumentPublicationPreparationRecord record,
            Map<String, DocumentPublicationCandidate.Mode> modes, byte[] encoded,
            RepositoryCoordinatorDrain.Identity expected) {
        var rows = scope(em.createNativeQuery("""
                SELECT p.preparation_bytes,p.command_bytes,p.command_sha256,m.owner_nonce,m.modes::text,b.incarnation
                FROM repository_publication_preparations p
                JOIN repository_publication_modes m USING(account_id,principal,operation_id,predecessor_generation)
                JOIN repository_coordinator_bindings b USING(account_id,principal,operation_id)
                WHERE p.account_id=:a AND p.principal=:p AND p.operation_id=:o
                  AND p.predecessor_generation=0 AND b.claim_epoch=1 AND b.claim_token=:token
                """), record).setParameter("token", expected.token()).getResultList();
        if (rows.size() != 1) throw inconsistent();
        var row = (Object[]) rows.getFirst();
        if (!MessageDigest.isEqual(encoded, (byte[]) row[0])
                || !MessageDigest.isEqual(record.command().canonical().toByteArray(), (byte[]) row[1])
                || !MessageDigest.isEqual(HexFormat.of().parseHex(record.command().sha256()), (byte[]) row[2])
                || !modes.equals(DocumentPublicationModesJournal.decodeModes(record, new Object[] {row[3], row[4]}))
                || !expected.incarnation().equals(row[5])) throw inconsistent();
        RepositoryOperationLedger.requireCommand(em, record.key(), record.command());
    }

    private static void requireCapture(EntityManager em, DocumentPublicationPreparationRecord record,
            DocumentPreparationCaptureDrain.Identity capture, long pins) {
        var rows = scope(em.createNativeQuery("""
                SELECT b.initial_capture,b.sealed,b.expected_count,b.creation_xid::text,h.creation_xid::text,
                  o.claim_epoch,o.claim_token,o.incarnation,
                  (SELECT sha256(convert_to('protomolt/preparation-pins/v1' || chr(10) ||
                    coalesce(string_agg(p.reader_incarnation::text || '/' || p.pin_id::text || '/' || p.object_id::text || '/' ||
                      p.node_id::text || '/' || p.revision_id::text || '/' || p.publication_revision::text || chr(10),
                      '' ORDER BY p.pin_id),''),'UTF8'))
                   FROM repository_preparation_source_pins p
                   WHERE p.account_id=b.account_id AND p.principal=b.principal AND p.operation_id=b.operation_id
                     AND p.predecessor_generation=b.predecessor_generation AND p.pins_sha256=b.pins_sha256)
                FROM repository_preparation_pin_batches b
                JOIN repository_preparation_pin_owners o USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                JOIN repository_preparation_history_sets h USING(account_id,principal,operation_id,predecessor_generation)
                WHERE b.account_id=:a AND b.principal=:p AND b.operation_id=:o
                  AND b.predecessor_generation=0 AND b.pins_sha256=:pins
                """), record).setParameter("pins", HexFormat.of().parseHex(capture.pinsSha256())).getResultList();
        if (rows.size() != 1) throw inconsistent();
        var row = (Object[]) rows.getFirst();
        if (!Boolean.TRUE.equals(row[0]) || !Boolean.TRUE.equals(row[1]) || ((Number) row[2]).longValue() != pins
                || !row[3].equals(row[4]) || ((Number) row[5]).longValue() != 1
                || !capture.owner().token().equals(row[6]) || !capture.owner().incarnation().equals(row[7])
                || !MessageDigest.isEqual(HexFormat.of().parseHex(capture.pinsSha256()), (byte[]) row[8])) throw inconsistent();
    }

    private static long captureCount(EntityManager em, DocumentPublicationPreparationRecord record,
            String table, DocumentPreparationCaptureDrain.Identity capture) {
        return count(em, record, table, "predecessor_generation=0 AND pins_sha256=:pins", "pins",
                HexFormat.of().parseHex(capture.pinsSha256()));
    }
    private static long count(EntityManager em, DocumentPublicationPreparationRecord record,
            String table, String predicate, String parameter, Object value) {
        var query = scope(em.createNativeQuery("SELECT count(*) FROM " + table
                + " WHERE account_id=:a AND principal=:p AND operation_id=:o AND " + predicate), record);
        if (parameter != null) query.setParameter(parameter, value);
        return ((Number) query.getSingleResult()).longValue();
    }
    private static Query scope(Query query, DocumentPublicationPreparationRecord record) {
        return query.setParameter("a", record.key().account()).setParameter("p", record.key().principal())
                .setParameter("o", record.key().operationId());
    }
    private static RepositoryException inconsistent() {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Initial historical capture evidence is incomplete or inconsistent");
    }
}
