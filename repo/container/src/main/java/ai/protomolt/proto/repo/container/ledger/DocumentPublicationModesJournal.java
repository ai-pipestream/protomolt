package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Immutable private mode choices; does not activate recovered sessions or provider work. */
final class DocumentPublicationModesJournal {
    private static final int MAX_BYTES = 1024 * 1024;
    private final Tx tx;
    private final PayloadBudget budget;
    private final DocumentPublicationPreparationJournal preparations;

    DocumentPublicationModesJournal(Tx tx, PayloadBudget budget) {
        this.tx = Objects.requireNonNull(tx); this.budget = Objects.requireNonNull(budget);
        preparations = new DocumentPublicationPreparationJournal(tx, budget);
    }

    /** Stored choices grant no execution authority; the current claim is rechecked before delivery. */
    Optional<Map<String, DocumentPublicationCandidate.Mode>> load(RepositoryCaller caller,
            RepositoryExecutionClaimLedger.Claim claim, long predecessor, RepositoryReadControl control) {
        requireProcess(caller, claim, control);
        return loadRetained(caller, claim, predecessor, control);
    }

    /** Scoped callers receive only a comparison result, never private recovery state. */
    void requireObservedModes(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Map<String, DocumentPublicationCandidate.Mode> observed,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, owner, command.intent().getAccountId());
        if (!owner.key().operationId().equals(command.operationId())) throw new IllegalArgumentException("Mode command differs from owner");
        if (owner.executionClaim().isEmpty()) return;
        var claim = owner.executionClaim().orElseThrow();
        boolean journaled = tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            RepositoryOperationLedger.requireCommand(em, owner.key(), command);
            return ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM repository_publication_preparations WHERE account_id=:a AND principal=:p
                      AND operation_id=:o AND predecessor_generation=:g
                    """).setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                    .setParameter("o", owner.key().operationId()).setParameter("g", owner.generation()-1).getSingleResult()).intValue()==1;
        });
        if (!journaled) return; // Explicit claim-only primitive, without a preparation journal.
        var fixed = loadRetained(caller, claim, owner.generation()-1, control).orElseThrow(() ->
                new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Fixed publication modes are absent"));
        if (!fixed.equals(observed)) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Observed publication modes differ from fixed modes");
        tx.inTransaction(em -> { RepositoryOperationLedger.fenceLiveOwner(em, owner); return null; });
        control.check();
    }

    private Optional<Map<String, DocumentPublicationCandidate.Mode>> loadRetained(RepositoryCaller caller,
            RepositoryExecutionClaimLedger.Claim claim, long predecessor, RepositoryReadControl control) {
        try (var reservation = budget.reserve(MAX_BYTES);
             var loaded = preparations.load(caller, claim, predecessor, control).orElseThrow(() ->
                     new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Publication preparation is absent"))) {
            var row = tx.inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                var rows = em.createNativeQuery("""
                        SELECT owner_nonce, CASE WHEN octet_length(modes::text)<=1048576 THEN modes::text END
                        FROM repository_publication_modes
                        WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                        """).setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                        .setParameter("o", claim.key().operationId()).setParameter("g", predecessor).getResultList();
                return rows.isEmpty() ? null : (Object[]) rows.getFirst();
            });
            control.check();
            if (row == null) return Optional.empty();
            Map<String, DocumentPublicationCandidate.Mode> modes;
            try {
                if (!loaded.record().seeds().ownerNonce().equals(row[0]) || row[1] == null)
                    throw new IllegalArgumentException("Stored mode binding differs");
                var parsed = JsonParser.parseString((String) row[1]).getAsJsonObject();
                var copy = new TreeMap<String, DocumentPublicationCandidate.Mode>();
                parsed.entrySet().forEach(entry -> {
                    if (!entry.getValue().isJsonPrimitive() || !entry.getValue().getAsJsonPrimitive().isString())
                        throw new IllegalArgumentException("Stored mode is not a string");
                    copy.put(entry.getKey(), DocumentPublicationCandidate.Mode.valueOf(entry.getValue().getAsString()));
                });
                if (!copy.keySet().equals(loaded.record().command().intent().getMembersList().stream()
                        .map(member -> member.getMemberId()).collect(Collectors.toSet())))
                    throw new IllegalArgumentException("Stored modes differ from command members");
                modes = Map.copyOf(copy);
            } catch (IllegalArgumentException | IllegalStateException malformed) {
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Private publication modes are invalid");
            }
            control.check();
            tx.inTransaction(em -> { RepositoryExecutionClaimLedger.lockLive(em, claim); return null; });
            control.check();
            return Optional.of(modes);
        }
    }

    /** Exact retry only. The caller must retain choices when the acknowledgment is uncertain. */
    void bind(RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim, long predecessor,
            Map<String, DocumentPublicationCandidate.Mode> requested, RepositoryReadControl control) {
        requireProcess(caller, claim, control);
        var modes = Map.copyOf(requested);
        if (modes.isEmpty() || modes.size()>10000) throw new IllegalArgumentException("Invalid publication mode count");
        try (var reservation = budget.reserve(MAX_BYTES);
             var loaded = preparations.load(caller, claim, predecessor, control).orElseThrow(() ->
                     new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Publication preparation is absent"))) {
            var preparation = loaded.record();
            var members = preparation.command().intent().getMembersList().stream()
                    .map(member -> member.getMemberId()).collect(Collectors.toSet());
            if (!members.equals(modes.keySet())) throw new IllegalArgumentException("Admission modes differ from command members");
            var json = new JsonObject();
            new TreeMap<>(modes).forEach((member, mode) -> json.addProperty(member, mode.name()));
            String encoded = json.toString();
            if (encoded.getBytes(StandardCharsets.UTF_8).length>MAX_BYTES)
                throw new IllegalArgumentException("Publication modes exceed byte limit");
            control.check();
            tx.inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                em.createNativeQuery("""
                        INSERT INTO repository_publication_modes(account_id,principal,operation_id,predecessor_generation,owner_nonce,modes)
                        VALUES (:a,:p,:o,:g,:owner,CAST(:modes AS jsonb))
                        ON CONFLICT(account_id,principal,operation_id,predecessor_generation) DO NOTHING
                        """)
                        .setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                        .setParameter("o", claim.key().operationId()).setParameter("g", predecessor)
                        .setParameter("owner", preparation.seeds().ownerNonce()).setParameter("modes", encoded).executeUpdate();
                control.check(); return null;
            });
            control.check();
        }
    }

    private static void requireProcess(RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, claim.key(), claim.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Private recovery requires process authority");
    }
}
