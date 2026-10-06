package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import com.google.gson.JsonObject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.time.Duration;
import java.util.*;

/** Atomic private registration only. Installed owners still have no execution grant. */
final class RepositorySuccessorInstall {
    private RepositorySuccessorInstall() {}

    record Plan(RepositoryCoordinatorHandoff.Proposal handoff, DocumentPublicationPreparationRecord previous,
            DocumentPublicationPreparationRecord next, Map<String, DocumentPublicationCandidate.Mode> modes) {
        Plan {
            Objects.requireNonNull(handoff); Objects.requireNonNull(previous); Objects.requireNonNull(next);
            modes = Map.copyOf(modes);
            if (!previous.key().equals(next.key()) || !next.key().equals(handoff.predecessor().key())
                    || !previous.command().sha256().equals(next.command().sha256())
                    || !next.command().sha256().equals(handoff.predecessor().commandSha256())
                    || next.predecessorGeneration() != previous.predecessorGeneration() + 1)
                throw new IllegalArgumentException("Successor preparation differs from predecessor");
            var ids = new HashSet<UUID>(); ids.add(previous.seeds().ownerNonce());
            ids.addAll(previous.seeds().attempts().values()); ids.addAll(previous.seeds().uploadTokens().values());
            var proposed = new ArrayList<UUID>(); proposed.add(next.seeds().ownerNonce());
            proposed.addAll(next.seeds().attempts().values()); proposed.addAll(next.seeds().uploadTokens().values());
            if (proposed.stream().anyMatch(ids::contains)) throw new IllegalArgumentException("Successor identities must be fresh");
            var members = next.command().intent().getMembersList().stream().map(m -> m.getMemberId())
                    .collect(java.util.stream.Collectors.toSet());
            if (!members.equals(modes.keySet())) throw new IllegalArgumentException("Successor modes differ from members");
        }
        @Override public String toString() { return "SuccessorInstall[private]"; }
    }

    static Plan prepare(RepositoryCoordinatorHandoff.Proposal handoff, DocumentPublicationPreparationRecord previous,
            Duration lease, Map<String, DocumentPublicationCandidate.Mode> modes) {
        return new Plan(handoff, previous, new DocumentPublicationPreparationRecord(previous.key(), previous.command(),
                DocumentPublicationSeeds.mint(previous.key(), previous.command()), previous.placements(), lease,
                Math.addExact(previous.predecessorGeneration(), 1)), modes);
    }

    static void install(Tx tx, PayloadBudget budget, RepositoryCaller caller, Plan plan, RepositoryReadControl control) {
        require(caller, plan, control);
        if (RepositoryCoordinatorHandoff.confirm(tx, caller, plan.handoff(), control).isEmpty())
            throw new IllegalArgumentException("Successor handoff is not committed");
        try (var reserved = budget.reserve(2L * DocumentPublicationPreparationCodec.MAX_BYTES + 1024 * 1024)) {
            var oldBytes = DocumentPublicationPreparationCodec.encode(plan.previous());
            var bytes = DocumentPublicationPreparationCodec.encode(plan.next());
            var oldSha = DocumentPublicationPreparationJournal.digest(oldBytes);
            var sha = DocumentPublicationPreparationJournal.digest(bytes);
            var json = new JsonObject(); new TreeMap<>(plan.modes()).forEach((key, mode) -> json.addProperty(key, mode.name()));
            var modes = json.toString();
            if (modes.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024 * 1024)
                throw new IllegalArgumentException("Successor modes exceed byte limit");
            if (confirm(tx, caller, plan, oldSha, sha, modes, control)) return;
            try {
                tx.inTransaction(em -> {
                    control.check();
                    var p = plan.next(); var h = plan.handoff();
                    scope(em.createNativeQuery("""
                            INSERT INTO repository_successor_installs
                            (account_id,principal,operation_id,predecessor_epoch,successor_epoch,successor_token,
                             successor_incarnation,predecessor_generation,predecessor_nonce,predecessor_preparation_sha256,
                             owner_nonce,command_sha256,preparation_sha256,modes_sha256,install_xid)
                            VALUES(:a,:p,:o,:epoch,:successor,:token,:incarnation,:g,:old,:oldSha,:owner,:command,:sha,
                             sha256(convert_to(CAST(:modes AS jsonb)::text,'UTF8')),pg_current_xact_id())
                            """), p.key()).setParameter("epoch", h.predecessor().epoch())
                            .setParameter("successor", h.predecessor().epoch()+1).setParameter("token", h.successorToken())
                            .setParameter("incarnation", h.successorIncarnation()).setParameter("g", p.predecessorGeneration())
                            .setParameter("old", plan.previous().seeds().ownerNonce()).setParameter("oldSha", oldSha)
                            .setParameter("owner", p.seeds().ownerNonce()).setParameter("command", HexFormat.of().parseHex(p.command().sha256()))
                            .setParameter("sha", sha).setParameter("modes", modes).executeUpdate();
                    // Check current read access, including historical inputs and creation scope.
                    // Write policy and schema assessment remain required before later execution.
                    DocumentAdmissionAuthorization.authorizeRejection(em, caller, p.command());
                    control.check();
                    scope(em.createNativeQuery("""
                            INSERT INTO repository_publication_preparations
                            (account_id,principal,operation_id,predecessor_generation,owner_nonce,command_codec,
                             command_version,command_bytes,command_sha256,preparation_bytes,preparation_sha256)
                            VALUES(:a,:p,:o,:g,:owner,:codec,:version,:command,:commandSha,:bytes,:sha)
                            """), p.key()).setParameter("g", p.predecessorGeneration()).setParameter("owner", p.seeds().ownerNonce())
                            .setParameter("codec", DocumentPublicationCommand.CODEC).setParameter("version", DocumentPublicationCommand.ENCODING_VERSION)
                            .setParameter("command", p.command().canonical().toByteArray()).setParameter("commandSha", HexFormat.of().parseHex(p.command().sha256()))
                            .setParameter("bytes", bytes.toByteArray()).setParameter("sha", sha).executeUpdate();
                    scope(em.createNativeQuery("""
                            INSERT INTO repository_publication_modes(account_id,principal,operation_id,predecessor_generation,owner_nonce,modes)
                            VALUES(:a,:p,:o,:g,:owner,CAST(:modes AS jsonb))
                            """), p.key()).setParameter("g", p.predecessorGeneration()).setParameter("owner", p.seeds().ownerNonce())
                            .setParameter("modes", modes).executeUpdate();
                    control.check();
                    int changed = scope(em.createNativeQuery("""
                            UPDATE repository_operation_owners SET owner_generation=owner_generation+1,owner_token=:owner,
                             lease_until=clock_timestamp()+(:lease*interval '1 millisecond')
                            WHERE account_id=:a AND principal=:p AND operation_id=:o AND owner_generation=:g AND owner_token=:old
                            """), p.key()).setParameter("owner", p.seeds().ownerNonce()).setParameter("lease", p.lease().toMillis())
                            .setParameter("g", p.predecessorGeneration()).setParameter("old", plan.previous().seeds().ownerNonce()).executeUpdate();
                    if (changed != 1) throw new IllegalStateException("Successor owner compare-and-set failed");
                    control.check();
                });
                control.check();
            } catch (RuntimeException failure) {
                try { if (confirm(tx, caller, plan, oldSha, sha, modes, control)) return; }
                catch (RuntimeException confirmation) { if (confirmation != failure) failure.addSuppressed(confirmation); }
                throw failure;
            }
        }
    }

    private static boolean confirm(Tx tx, RepositoryCaller caller, Plan plan, byte[] oldSha, byte[] sha,
            String modes, RepositoryReadControl control) {
        require(caller, plan, control);
        boolean result = tx.inTransaction(em -> {
            var p = plan.next(); var h = plan.handoff();
            var rows = scope(em.createNativeQuery("""
                    SELECT predecessor_epoch,successor_token,successor_incarnation,predecessor_generation,predecessor_nonce,
                     predecessor_preparation_sha256,owner_nonce,command_sha256,preparation_sha256,
                     modes_sha256=sha256(convert_to(CAST(:modes AS jsonb)::text,'UTF8'))
                    FROM repository_successor_installs WHERE account_id=:a AND principal=:p AND operation_id=:o AND successor_epoch=:epoch
                    """), p.key()).setParameter("epoch", h.predecessor().epoch()+1).setParameter("modes", modes).getResultList();
            if (rows.isEmpty()) return false;
            var row = (Object[]) rows.getFirst();
            if (((Number) row[0]).longValue()!=h.predecessor().epoch() || !row[1].equals(h.successorToken())
                    || !row[2].equals(h.successorIncarnation()) || ((Number) row[3]).longValue()!=p.predecessorGeneration()
                    || !row[4].equals(plan.previous().seeds().ownerNonce()) || !Arrays.equals((byte[])row[5],oldSha)
                    || !row[6].equals(p.seeds().ownerNonce()) || !HexFormat.of().formatHex((byte[])row[7]).equals(p.command().sha256())
                    || !Arrays.equals((byte[])row[8],sha) || !Boolean.TRUE.equals(row[9]))
                throw new IllegalArgumentException("Successor install differs from committed proposal");
            return true;
        });
        control.check(); return result;
    }

    private static Query scope(Query query, RepositoryOperationLedger.Key key) {
        return query.setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId());
    }
    private static void require(RepositoryCaller caller, Plan plan, RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(plan);
        var key=plan.next().key(); DocumentAdmissionAuthorization.requireCaller(caller,key,key.account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Successor installation requires private process authority");
    }
}
