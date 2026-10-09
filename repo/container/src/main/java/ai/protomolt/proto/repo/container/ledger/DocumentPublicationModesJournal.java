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
    static final int MAX_BYTES = 1024 * 1024;
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
        return loadRetained(caller, claim, predecessor, control, null);
    }

    /** Same-session host authority, separate from the authenticated document caller. */
    Optional<Map<String, DocumentPublicationCandidate.Mode>> loadOwned(DocumentPublicationRegistration.JournalAccess access,
            RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim, long predecessor, RepositoryReadControl control) {
        Objects.requireNonNull(access).require(caller, claim, predecessor, control);
        return loadRetained(caller, claim, predecessor, control, access);
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
        PayloadBudget.Lease[] reservations = {null, null};
        try {
            var captured = tx.inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                RepositoryOperationLedger.requireCommand(em, owner.key(), command);
                var preparation = DocumentPublicationPreparationJournal.capture(em, claim, owner.generation()-1, size -> {
                    reservations[0] = budget.reserve(MAX_BYTES);
                    reservations[1] = budget.reserve(size);
                }, control);
                if (preparation == null) return null; // Explicit claim-only primitive, without a preparation journal.
                return new Captured(preparation, readModes(em, claim, owner.generation()-1));
            });
            control.check();
            if (captured == null) return;
            // Keep integrity/error ordering: validate preparation before missing or malformed modes.
            var preparation = DocumentPublicationPreparationJournal.decode(captured.preparation(), reservations[1].bytes(),
                    claim.key(), claim.commandSha256(), owner.generation()-1);
            control.check();
            if (captured.modes() == null) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Fixed publication modes are absent");
            var fixed = decodeModes(preparation, captured.modes());
            control.check();
            if (!fixed.equals(observed)) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Observed publication modes differ from fixed modes");
            tx.inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                RepositoryOperationLedger.requireCommand(em, owner.key(), command);
                control.check(); return null;
            });
            control.check();
        } finally {
            if (reservations[1] != null) reservations[1].close();
            if (reservations[0] != null) reservations[0].close();
        }
    }

    private record Captured(Object[] preparation, Object[] modes) {}

    /** Compare immutable bindings after current authorization, with the owner locked in this transaction. */
    static void requireBoundModes(jakarta.persistence.EntityManager em, RepositoryOperationLedger.Key key,
            DocumentPublicationCommand command, long generation, String encoded) {
        var rows = em.createNativeQuery("""
                SELECT p.owner_nonce=m.owner_nonce AND p.owner_nonce=o.owner_token
                    AND o.owner_generation=:generation AND p.command_codec=:codec AND p.command_version=:version
                    AND encode(p.command_sha256,'hex')=:digest AND p.command_bytes=:command,
                  (SELECT array_agg(key ORDER BY key) FROM jsonb_each(m.modes)) =
                    (SELECT array_agg(key ORDER BY key) FROM jsonb_each(CAST(:modes AS jsonb)))
                    AND NOT EXISTS (SELECT 1 FROM jsonb_each(m.modes) e
                      WHERE e.value NOT IN ('"TYPED"'::jsonb,'"OPAQUE"'::jsonb)),
                  m.modes=CAST(:modes AS jsonb)
                FROM repository_publication_preparations p JOIN repository_publication_modes m
                  USING(account_id,principal,operation_id,predecessor_generation)
                JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE p.account_id=:account AND p.principal=:principal AND p.operation_id=:operation
                  AND p.predecessor_generation=:predecessor
                """).setParameter("generation", generation).setParameter("predecessor", generation-1)
                .setParameter("codec", DocumentPublicationCommand.CODEC)
                .setParameter("version", DocumentPublicationCommand.ENCODING_VERSION)
                .setParameter("digest", command.sha256()).setParameter("command", command.canonical().toByteArray())
                .setParameter("modes", encoded).setParameter("account", key.account())
                .setParameter("principal", key.principal()).setParameter("operation", key.operationId()).getResultList();
        if (rows.size()!=1 || !Boolean.TRUE.equals(((Object[]) rows.getFirst())[0])
                || !Boolean.TRUE.equals(((Object[]) rows.getFirst())[1]))
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Terminal publication mode binding is invalid");
        if (!Boolean.TRUE.equals(((Object[]) rows.getFirst())[2]))
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Publication modes differ from fixed modes");
    }

    /** Compare immutable predecessor choices before V98; its SQL still arbitrates the current claim. */
    void requireSupersessionModes(RepositoryCaller authority, RepositoryCaller caller,
            DocumentPublicationCommand command, RepositoryCoordinatorReservation.SupersededUnactivated proposal,
            Map<String,DocumentPublicationCandidate.Mode> requested, RepositoryReadControl control) {
        RepositoryCoordinatorReservation.require(authority,proposal,control);
        var key=proposal.predecessor().key();
        DocumentAdmissionAuthorization.requireCaller(caller,key,key.account());
        if (!command.operationId().equals(key.operationId()) || !command.sha256().equals(proposal.predecessor().commandSha256()))
            throw new IllegalArgumentException("Supersession modes require the exact command");
        String encoded=encode(command,requested);
        tx.inTransaction(em -> {
            control.check();
            DocumentAdmissionAuthorization.authorizeRejection(em,caller,command);
            var rows=em.createNativeQuery("""
                    SELECT p.owner_nonce=m.owner_nonce AND p.owner_nonce=:nonce
                        AND encode(p.command_sha256,'hex')=:command
                        AND encode(p.preparation_sha256,'hex')=:preparation,
                      m.modes=CAST(:modes AS jsonb)
                    FROM repository_publication_preparations p JOIN repository_publication_modes m
                      USING(account_id,principal,operation_id,predecessor_generation)
                    WHERE p.account_id=:account AND p.principal=:principal AND p.operation_id=:operation
                      AND p.predecessor_generation=:generation
                    """).setParameter("nonce",proposal.owner().nonce()).setParameter("command",command.sha256())
                    .setParameter("preparation",proposal.preparationSha256()).setParameter("modes",encoded)
                    .setParameter("account",key.account()).setParameter("principal",key.principal())
                    .setParameter("operation",key.operationId()).setParameter("generation",proposal.owner().generation()-1)
                    .getResultList();
            if (rows.size()!=1 || !Boolean.TRUE.equals(((Object[])rows.getFirst())[0]))
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Supersession mode binding is invalid");
            if (!Boolean.TRUE.equals(((Object[])rows.getFirst())[1]))
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,"Recovery modes differ from fixed modes");
            control.check(); return null;
        });
    }

    private Optional<Map<String, DocumentPublicationCandidate.Mode>> loadRetained(RepositoryCaller caller,
            RepositoryExecutionClaimLedger.Claim claim, long predecessor, RepositoryReadControl control,
            DocumentPublicationRegistration.JournalAccess access) {
        try (var reservation = budget.reserve(MAX_BYTES);
             var loaded = preparations.load(caller, claim, predecessor, control).orElseThrow(() ->
                     new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Publication preparation is absent"))) {
            if (access != null) access.requirePreparation(loaded.record());
            var row = tx.inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                return readModes(em, claim, predecessor);
            });
            control.check();
            if (row == null) return Optional.empty();
            var modes = decodeModes(loaded.record(), row);
            control.check();
            tx.inTransaction(em -> { RepositoryExecutionClaimLedger.lockLive(em, claim); return null; });
            control.check();
            return Optional.of(modes);
        }
    }

    private static Object[] readModes(jakarta.persistence.EntityManager em, RepositoryExecutionClaimLedger.Claim claim, long predecessor) {
        var rows = em.createNativeQuery("""
                SELECT owner_nonce, CASE WHEN octet_length(modes::text)<=1048576 THEN modes::text END
                FROM repository_publication_modes
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                """).setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                .setParameter("o", claim.key().operationId()).setParameter("g", predecessor).getResultList();
        return rows.isEmpty() ? null : (Object[]) rows.getFirst();
    }

    static Map<String, DocumentPublicationCandidate.Mode> decodeModes(DocumentPublicationPreparationRecord preparation, Object[] row) {
        try {
            if (!preparation.seeds().ownerNonce().equals(row[0]) || row[1] == null)
                throw new IllegalArgumentException("Stored mode binding differs");
            var parsed = JsonParser.parseString((String) row[1]).getAsJsonObject();
            var copy = new TreeMap<String, DocumentPublicationCandidate.Mode>();
            parsed.entrySet().forEach(entry -> {
                if (!entry.getValue().isJsonPrimitive() || !entry.getValue().getAsJsonPrimitive().isString())
                    throw new IllegalArgumentException("Stored mode is not a string");
                copy.put(entry.getKey(), DocumentPublicationCandidate.Mode.valueOf(entry.getValue().getAsString()));
            });
            if (!copy.keySet().equals(preparation.command().intent().getMembersList().stream()
                    .map(member -> member.getMemberId()).collect(Collectors.toSet())))
                throw new IllegalArgumentException("Stored modes differ from command members");
            return Map.copyOf(copy);
        } catch (IllegalArgumentException | IllegalStateException malformed) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Private publication modes are invalid");
        }
    }

    /** Exact retry only. The caller must retain choices when the acknowledgment is uncertain. */
    void bind(RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim, long predecessor,
            Map<String, DocumentPublicationCandidate.Mode> requested, RepositoryReadControl control) {
        requireProcess(caller, claim, control);
        bindRetained(caller, claim, predecessor, requested, control, null);
    }

    void bindOwned(DocumentPublicationRegistration.JournalAccess access, RepositoryCaller caller,
            RepositoryExecutionClaimLedger.Claim claim, long predecessor,
            Map<String, DocumentPublicationCandidate.Mode> requested, RepositoryReadControl control) {
        Objects.requireNonNull(access).require(caller, claim, predecessor, control);
        bindRetained(caller, claim, predecessor, requested, control, access);
    }

    private void bindRetained(RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim, long predecessor,
            Map<String, DocumentPublicationCandidate.Mode> requested, RepositoryReadControl control,
            DocumentPublicationRegistration.JournalAccess access) {
        var modes = Map.copyOf(requested);
        if (modes.isEmpty() || modes.size()>10000) throw new IllegalArgumentException("Invalid publication mode count");
        try (var reservation = budget.reserve(MAX_BYTES);
             var loaded = preparations.load(caller, claim, predecessor, control).orElseThrow(() ->
                     new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Publication preparation is absent"))) {
            var preparation = loaded.record();
            if (access != null) access.requirePreparation(preparation);
            String encoded = encode(preparation.command(), modes);
            control.check();
            tx.inTransaction(em -> {
                insert(em, claim, preparation, encoded);
                control.check(); return null;
            });
            control.check();
        }
    }

    static String encode(DocumentPublicationCommand command, Map<String, DocumentPublicationCandidate.Mode> requested) {
        var modes = Map.copyOf(requested);
        if (modes.isEmpty() || modes.size()>10000) throw new IllegalArgumentException("Invalid publication mode count");
        var members = command.intent().getMembersList().stream().map(member -> member.getMemberId()).collect(Collectors.toSet());
        if (!members.equals(modes.keySet())) throw new IllegalArgumentException("Admission modes differ from command members");
        var json = new JsonObject();
        new TreeMap<>(modes).forEach((member, mode) -> json.addProperty(member, mode.name()));
        String encoded = json.toString();
        if (encoded.getBytes(StandardCharsets.UTF_8).length>MAX_BYTES)
            throw new IllegalArgumentException("Publication modes exceed byte limit");
        return encoded;
    }

    static void insert(jakarta.persistence.EntityManager em, RepositoryExecutionClaimLedger.Claim claim,
            DocumentPublicationPreparationRecord preparation, String encoded) {
        RepositoryExecutionClaimLedger.lockLive(em, claim);
        em.createNativeQuery("""
                        INSERT INTO repository_publication_modes(account_id,principal,operation_id,predecessor_generation,owner_nonce,modes)
                        VALUES (:a,:p,:o,:g,:owner,CAST(:modes AS jsonb))
                        ON CONFLICT(account_id,principal,operation_id,predecessor_generation) DO NOTHING
                        """)
                        .setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                        .setParameter("o", claim.key().operationId()).setParameter("g", preparation.predecessorGeneration())
                        .setParameter("owner", preparation.seeds().ownerNonce()).setParameter("modes", encoded).executeUpdate();
    }

    private static void requireProcess(RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, claim.key(), claim.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Private recovery requires process authority");
    }
}
