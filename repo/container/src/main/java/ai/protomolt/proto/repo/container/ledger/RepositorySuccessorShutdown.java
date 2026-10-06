package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import java.util.HexFormat;
import java.util.Objects;
import java.util.TreeMap;

/** Read-only local shutdown evidence, never an execution grant or remote drain marker. */
final class RepositorySuccessorShutdown {
    private RepositorySuccessorShutdown() {}

    enum State { UNRESOLVED, CAPACITY_UNAVAILABLE, UNACTIVATED, ACTIVATED, TERMINAL }

    /**
     * Observes one exact retained successor under the claim lock. Lease expiry is irrelevant:
     * this neither starts work nor establishes permanent fencing. The host must close admission,
     * drain accepted work and revalidate before disposing of its local state.
     */
    static State inspect(Tx tx, PayloadBudget budget, RepositoryCaller caller,
            DocumentSuccessorFingerprint fingerprint, RepositoryReadControl control) {
        return inspect(tx,budget,caller,fingerprint,control,false);
    }

    /** Private exact-generation terminal evidence; no receipt decoding or current document disclosure. */
    static boolean terminal(Tx tx, PayloadBudget budget, RepositoryCaller caller,
            DocumentSuccessorFingerprint fingerprint, RepositoryReadControl control) {
        return inspect(tx,budget,caller,fingerprint,control,true)==State.TERMINAL;
    }

    private static State inspect(Tx tx, PayloadBudget budget, RepositoryCaller caller,
            DocumentSuccessorFingerprint fingerprint, RepositoryReadControl control, boolean terminalOnly) {
        Objects.requireNonNull(control).check();
        var proposal = fingerprint.reservation();
        var predecessor = proposal.predecessor();
        var key = predecessor.key();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Successor shutdown proof requires private process authority");
        final PayloadBudget.Lease capacity;
        try {
            capacity = budget.reserve(DocumentPublicationModesJournal.MAX_BYTES);
        } catch (PayloadBudget.CapacityExceededException unavailable) {
            // An accepted worker may still own these bytes. No proof or fallback is inferred.
            control.check();
            return State.CAPACITY_UNAVAILABLE;
        }
        try (var reserved = capacity) {
            Object[] captured = tx.inTransaction(em -> {
                control.check();
                em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
                var claims = em.createNativeQuery("""
                        SELECT command_sha256,claim_epoch,claim_token FROM repository_execution_claims
                        WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE
                        """).setParameter("a", key.account()).setParameter("p", key.principal())
                        .setParameter("o", key.operationId()).getResultList();
                if (claims.isEmpty()) return null;
                var claim = (Object[]) claims.getFirst();
                if (!predecessor.commandSha256().equals(HexFormat.of().formatHex((byte[]) claim[0])))
                    throw new RepositoryException(RepositoryException.Code.CONFLICT, "Shutdown claim command changed");
                if (((Number) claim[1]).longValue() != predecessor.epoch()+1
                        || !proposal.successorToken().equals(claim[2])) return null;
                if (RepositoryCoordinatorReservation.read(em, proposal).isEmpty()) return null;
                var rows = em.createNativeQuery("""
                        SELECT CASE WHEN octet_length(m.modes::text)<=1048576 THEN m.modes::text END,
                         e.claim_epoch IS NULL AND b.claim_epoch IS NULL,
                         COALESCE(e.claim_token=i.successor_token AND e.incarnation=i.successor_incarnation
                          AND e.owner_generation=i.predecessor_generation+1 AND e.owner_nonce=i.owner_nonce
                          AND e.command_sha256=i.command_sha256 AND e.preparation_sha256=i.preparation_sha256
                          AND e.modes_sha256=i.modes_sha256 AND b.claim_token=i.successor_token
                          AND b.incarnation=i.successor_incarnation,FALSE),
                         (s.operation_id IS NOT NULL AND r.operation_id IS NULL
                          AND s.owner_generation=i.predecessor_generation+1 AND s.command_sha256=i.command_sha256)
                         OR (r.operation_id IS NOT NULL AND s.operation_id IS NULL
                          AND r.owner_generation=i.predecessor_generation+1 AND r.command_sha256=i.command_sha256)
                        FROM repository_successor_installs i
                        JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                        JOIN repository_publication_modes m ON m.account_id=i.account_id AND m.principal=i.principal
                         AND m.operation_id=i.operation_id AND m.predecessor_generation=i.predecessor_generation
                        LEFT JOIN repository_successor_executions e ON e.account_id=i.account_id AND e.principal=i.principal
                         AND e.operation_id=i.operation_id AND e.claim_epoch=i.successor_epoch
                        LEFT JOIN repository_coordinator_bindings b ON b.account_id=i.account_id AND b.principal=i.principal
                         AND b.operation_id=i.operation_id AND b.claim_epoch=i.successor_epoch
                        LEFT JOIN repository_operation_success s ON s.account_id=i.account_id AND s.principal=i.principal
                         AND s.operation_id=i.operation_id
                        LEFT JOIN repository_operation_rejection r ON r.account_id=i.account_id AND r.principal=i.principal
                         AND r.operation_id=i.operation_id
                        WHERE i.account_id=:a AND i.principal=:p AND i.operation_id=:o
                         AND i.predecessor_epoch=:previousEpoch AND i.successor_epoch=:epoch
                         AND i.successor_token=:token AND i.successor_incarnation=:incarnation
                         AND i.command_sha256=:command AND i.predecessor_preparation_sha256=:previous
                         AND i.preparation_sha256=:next AND o.owner_generation=i.predecessor_generation+1
                         AND o.owner_token=i.owner_nonce AND m.owner_nonce=i.owner_nonce
                         AND i.modes_sha256=sha256(convert_to(m.modes::text,'UTF8'))
                        """).setParameter("a", key.account()).setParameter("p", key.principal())
                        .setParameter("o", key.operationId()).setParameter("previousEpoch", predecessor.epoch())
                        .setParameter("epoch", predecessor.epoch()+1).setParameter("token", proposal.successorToken())
                        .setParameter("incarnation", proposal.successorIncarnation())
                        .setParameter("command", HexFormat.of().parseHex(predecessor.commandSha256()))
                        .setParameter("previous", fingerprint.previous().toByteArray())
                        .setParameter("next", fingerprint.next().toByteArray()).getResultList();
                control.check();
                return rows.isEmpty() ? null : (Object[]) rows.getFirst();
            });
            control.check();
            if (captured == null) return State.UNRESOLVED;
            if (captured[0] == null) throw new RepositoryException(RepositoryException.Code.DATA_LOSS,
                    "Stored publication modes exceed shutdown proof limit");
            // PostgreSQL jsonb::text is not the compact, sorted Java fingerprint encoding.
            if (!modeDigest((String) captured[0]).equals(fingerprint.modes())) return State.UNRESOLVED;
            control.check();
            if (terminalOnly) return Boolean.TRUE.equals(captured[2]) && Boolean.TRUE.equals(captured[3])
                    ? State.TERMINAL : State.UNRESOLVED;
            if (Boolean.TRUE.equals(captured[1])) return State.UNACTIVATED;
            return Boolean.TRUE.equals(captured[2]) ? State.ACTIVATED : State.UNRESOLVED;
        }
    }

    private static ByteString modeDigest(String encoded) {
        try {
            var parsed = JsonParser.parseString(encoded).getAsJsonObject();
            if (parsed.size() == 0 || parsed.size() > 10000) throw new IllegalArgumentException("Invalid mode count");
            var sorted = new TreeMap<String, DocumentPublicationCandidate.Mode>();
            parsed.entrySet().forEach(entry -> {
                var value = entry.getValue();
                if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString())
                    throw new IllegalArgumentException("Invalid mode value");
                sorted.put(entry.getKey(), DocumentPublicationCandidate.Mode.valueOf(value.getAsString()));
            });
            var canonical = new JsonObject();
            sorted.forEach((member, mode) -> canonical.addProperty(member, mode.name()));
            return ByteString.copyFrom(DocumentPublicationPreparationJournal.digest(ByteString.copyFromUtf8(canonical.toString())));
        } catch (IllegalArgumentException | IllegalStateException malformed) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Stored publication modes are invalid");
        }
    }
}
