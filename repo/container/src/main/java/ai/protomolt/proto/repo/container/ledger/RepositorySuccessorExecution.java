package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/** Private activation only. Completion confirms a durable fact, not a live execution lease. */
final class RepositorySuccessorExecution {
    private RepositorySuccessorExecution() {}

    /**
     * Both callers must be resolved by the trusted host, not decoded from saved request rights.
     * An exact retry may confirm an earlier commit after revocation, drain or lease expiry.
     * It grants no session: execution must reacquire live fences and current authorization.
     */
    static void activate(Tx tx, PayloadBudget budget, RepositoryCaller caller, RepositoryCaller executionCaller,
            RepositorySuccessorInstall.Plan plan, RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(plan);
        var next = plan.next();
        DocumentAdmissionAuthorization.requireCaller(caller, next.key(), next.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Successor activation requires private process authority");
        DocumentAdmissionAuthorization.requireCaller(executionCaller, next.key(), next.key().account());
        if (RepositoryCoordinatorHandoff.confirm(tx, caller, plan.handoff(), control).isEmpty())
            throw new IllegalArgumentException("Successor handoff is not committed");
        // Encode outside SQL locks. This bounds encoded bytes, not the entire parsed object graph.
        try (var reserved = budget.reserve(2L * DocumentPublicationPreparationCodec.MAX_BYTES + 1024 * 1024)) {
            var previousSha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(plan.previous()));
            var sha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(next));
            var modes = RepositorySuccessorInstall.encodeModes(plan);
            if (!RepositorySuccessorInstall.confirm(tx, caller, plan, previousSha, sha, modes, control))
                throw new IllegalArgumentException("Successor install is not committed");
            if (confirm(tx, plan, sha, modes, control)) return;
            var prepared = next.prepare().plan();
            var authorization = DocumentAdmissionAuthorization.prepare(prepared, prepared.historical());
            control.check();
            try {
                tx.inTransaction(em -> {
                    control.check();
                    // The guard locks the exact current claim, then its installed owner.
                    // Plain INSERT: a concurrent exact winner is confirmed after rollback.
                    scope(em.createNativeQuery("""
                            INSERT INTO repository_successor_executions(account_id,principal,operation_id,
                             claim_epoch,claim_token,incarnation,owner_generation,owner_nonce,
                             command_sha256,preparation_sha256,modes_sha256,activation_xid)
                            VALUES(:a,:p,:o,:epoch,:token,:incarnation,:generation,:owner,:command,:sha,
                             sha256(convert_to(CAST(:modes AS jsonb)::text,'UTF8')),pg_current_xact_id())
                            """), plan).setParameter("epoch", plan.handoff().predecessor().epoch()+1)
                            .setParameter("token", plan.handoff().successorToken())
                            .setParameter("incarnation", plan.handoff().successorIncarnation())
                            .setParameter("generation", next.predecessorGeneration()+1)
                            .setParameter("owner", next.seeds().ownerNonce())
                            .setParameter("command", HexFormat.of().parseHex(next.command().sha256()))
                            .setParameter("sha", sha).setParameter("modes", modes).executeUpdate();
                    DocumentAdmissionAuthorization.lockAndAuthorize(em, executionCaller, prepared, authorization);
                    control.check();
                    scope(em.createNativeQuery("""
                            INSERT INTO repository_coordinator_bindings(account_id,principal,operation_id,claim_epoch,claim_token,incarnation)
                            VALUES(:a,:p,:o,:epoch,:token,:incarnation)
                            """), plan).setParameter("epoch", plan.handoff().predecessor().epoch()+1)
                            .setParameter("token", plan.handoff().successorToken())
                            .setParameter("incarnation", plan.handoff().successorIncarnation()).executeUpdate();
                    control.check();
                });
                control.check();
            } catch (RuntimeException failure) {
                try { if (confirm(tx, plan, sha, modes, control)) return; }
                catch (RuntimeException confirmation) { if (confirmation != failure) failure.addSuppressed(confirmation); }
                throw failure;
            }
        }
    }

    record Attached(RepositoryOperationLedger.Owner owner, boolean assessmentStarted) {}

    /** Current execution attachment, distinct from immutable activation readback. Never renews. */
    static Attached attach(Tx tx, PayloadBudget budget, RepositoryCaller caller,
            RepositorySuccessorInstall.Plan plan, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        var next = plan.next(); var key = next.key();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        try (var reserved = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES + 1024L * 1024)) {
            var sha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(next));
            var modes = RepositorySuccessorInstall.encodeModes(plan);
            var prepared = next.prepare().plan();
            var authorization = DocumentAdmissionAuthorization.prepare(prepared, prepared.historical());
            var attached = tx.inTransaction(em -> {
                var claim = RepositoryExecutionClaimLedger.lockLive(em, key, next.command().sha256(),
                        plan.handoff().predecessor().epoch()+1, plan.handoff().successorToken());
                if (!read(em, plan, sha, modes)) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Successor activation is not committed");
                var owner = RepositoryOperationLedger.lockLiveOwner(em, key, next.predecessorGeneration()+1,
                        next.seeds().ownerNonce(), java.util.Optional.of(claim));
                RepositoryOperationLedger.requireCommand(em, key, next.command());
                DocumentAdmissionAuthorization.lockAndAuthorize(em, caller, prepared, authorization);
                boolean started = (Boolean) scope(em.createNativeQuery("""
                        SELECT EXISTS(SELECT 1 FROM repository_publication_assessment_starts
                         WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g)
                        """), plan).setParameter("g", next.predecessorGeneration()).getSingleResult();
                control.check();
                return new Attached(owner, started);
            });
            control.check(); return attached;
        }
    }

    /** Exact immutable readback; never stamps an execution fence or extends a lease. */
    private static boolean confirm(Tx tx, RepositorySuccessorInstall.Plan plan, byte[] sha,
            String modes, RepositoryReadControl control) {
        control.check();
        boolean found = tx.inTransaction(em -> { return read(em, plan, sha, modes); });
        control.check(); return found;
    }

    private static boolean read(EntityManager em, RepositorySuccessorInstall.Plan plan, byte[] sha, String modes) {
        var rows = scope(em.createNativeQuery("""
                SELECT e.claim_token,e.incarnation,e.owner_generation,e.owner_nonce,e.command_sha256,e.preparation_sha256,
                 e.modes_sha256=sha256(convert_to(CAST(:modes AS jsonb)::text,'UTF8')),b.claim_token,b.incarnation
                FROM repository_successor_executions e LEFT JOIN repository_coordinator_bindings b
                 USING(account_id,principal,operation_id,claim_epoch)
                WHERE e.account_id=:a AND e.principal=:p AND e.operation_id=:o AND e.claim_epoch=:epoch
                """), plan).setParameter("epoch", plan.handoff().predecessor().epoch()+1)
                .setParameter("modes", modes).getResultList();
        if (rows.isEmpty()) return false;
        var row = (Object[]) rows.getFirst();
        var next = plan.next(); var handoff = plan.handoff();
        if (!handoff.successorToken().equals(row[0]) || !handoff.successorIncarnation().equals(row[1])
                || ((Number) row[2]).longValue()!=next.predecessorGeneration()+1 || !next.seeds().ownerNonce().equals(row[3])
                || !HexFormat.of().formatHex((byte[]) row[4]).equals(next.command().sha256())
                || !Arrays.equals((byte[]) row[5],sha) || !Boolean.TRUE.equals(row[6])
                || !handoff.successorToken().equals(row[7]) || !handoff.successorIncarnation().equals(row[8]))
            throw new IllegalArgumentException("Successor activation differs from committed proposal");
        return true;
    }

    private static Query scope(Query query, RepositorySuccessorInstall.Plan plan) {
        var key = plan.next().key();
        return query.setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId());
    }
}
