package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejection;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;

/** Private terminal decision only. Does not activate execution, release sources or perform provider I/O. */
final class RepositoryHistoricalLimitDecisions {
    private final Tx tx;
    private final PayloadBudget budget;
    RepositoryHistoricalLimitDecisions(Tx tx, PayloadBudget budget) {
        this.tx = Objects.requireNonNull(tx); this.budget = Objects.requireNonNull(budget);
    }

    /** Empty means no proven limit. Exceptions are never classified as terminal outcomes. */
    Optional<DocumentPublicationRejection> decide(RepositoryCaller coordinator, RepositoryCaller caller,
            RepositorySuccessorInstall.Plan plan, DocumentPublicationPreparationRecord retention, RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(plan); Objects.requireNonNull(retention);
        var next = plan.next(); var key = next.key();
        DocumentAdmissionAuthorization.requireCaller(coordinator, key, key.account());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (!coordinator.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Recovery limit decision requires private process authority");
        if (!retention.key().equals(key) || !retention.command().sha256().equals(next.command().sha256())
                || retention.predecessorGeneration() >= next.predecessorGeneration())
            throw new IllegalArgumentException("Recovery limit retained scope differs");
        try (var reserved = budget.reserve(3L * DocumentPublicationPreparationCodec.MAX_BYTES + 1024 * 1024)) {
            var nextSha = digest(next); var retainedSha = digest(retention);
            var modes = RepositorySuccessorInstall.encodeModes(plan);
            if (!RepositorySuccessorInstall.confirm(tx, coordinator, plan, digest(plan.previous()), nextSha, modes, control))
                throw unavailable("Recovery limit successor is not installed");
            return tx.inTransaction(em -> {
                control.check();
                // Lock without renewing or acquiring a write fence. Replay must work after lease expiry.
                scope(em.createNativeQuery("SELECT claim_epoch FROM repository_execution_claims WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE"), key).getSingleResult();
                scope(em.createNativeQuery("SELECT owner_generation FROM repository_operation_owners WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE"), key).getSingleResult();
                var observed = DocumentPublicationReplay.observe(em, caller, next.command(), key);
                control.check();
                if (observed.state() == DocumentPublicationReplay.State.TERMINATED) {
                    var receipt = observed.rejection().orElseThrow();
                    if (receipt.getReasonValue()!=4 || receipt.getOwnerGeneration()!=next.predecessorGeneration()+1
                            || !matches(em, plan, retention, nextSha, retainedSha, modes))
                        throw new RepositoryException(RepositoryException.Code.CONFLICT, "Recovery limit decision identity differs");
                    return Optional.of(receipt);
                }
                if (observed.state()!=DocumentPublicationReplay.State.PENDING)
                    throw unavailable("Recovery limit requires a pending operation");
                boolean live = !scope(em.createNativeQuery("""
                        SELECT 1 FROM repository_execution_claims c JOIN repository_operation_owners w USING(account_id,principal,operation_id)
                        WHERE c.account_id=:a AND c.principal=:p AND c.operation_id=:o
                         AND c.claim_epoch=:epoch AND c.claim_token=:token AND c.command_sha256=:command
                         AND w.owner_generation=:generation AND w.owner_token=:nonce
                         AND c.lease_until>clock_timestamp() AND w.lease_until>clock_timestamp()
                         AND NOT EXISTS(SELECT 1 FROM repository_successor_executions e WHERE e.account_id=c.account_id
                          AND e.principal=c.principal AND e.operation_id=c.operation_id AND e.claim_epoch=c.claim_epoch)
                        """), key).setParameter("epoch",plan.reservation().predecessor().epoch()+1)
                        .setParameter("token",plan.reservation().successorToken()).setParameter("command",HexFormat.of().parseHex(next.command().sha256()))
                        .setParameter("generation",next.predecessorGeneration()+1).setParameter("nonce",next.seeds().ownerNonce()).getResultList().isEmpty();
                if (!live) throw unavailable("Recovery limit requires exact live unactivated ownership");
                DocumentAdmissionAuthorization.authorizeRejection(em, caller, next.command());
                if (DocumentPreparationHistoryRoots.coverage(em, retention, retainedSha)!=DocumentPreparationHistoryRoots.Coverage.EXACT)
                    throw unavailable("Recovery limit retained history is unavailable");
                var proof = inspect(em, plan, retention, nextSha, retainedSha);
                control.check();
                if (proof.isEmpty()) return Optional.empty();
                insert(em, plan, retention, nextSha, retainedSha, modes, proof.orElseThrow());
                var receipt = rejection(em, next);
                control.check();
                return Optional.of(receipt);
            });
        }
    }

    private record Proof(String kind, int count, int depth, long endpoint, byte[] endpointSha) {}

    private static Optional<Proof> inspect(EntityManager em, RepositorySuccessorInstall.Plan plan,
            DocumentPublicationPreparationRecord retention, byte[] nextSha, byte[] retainedSha) {
        var key = plan.next().key();
        var row = (Object[]) scope(em.createNativeQuery("""
                WITH RECURSIVE ancestry(g,sha,depth) AS (
                 SELECT CAST(:g AS bigint),CAST(:sha AS bytea),0
                 UNION ALL
                 SELECT i.predecessor_generation-1,i.predecessor_preparation_sha256,a.depth+1
                 FROM ancestry a JOIN repository_successor_installs i ON i.predecessor_generation=a.g AND i.preparation_sha256=a.sha
                  AND i.account_id=:a AND i.principal=:p AND i.operation_id=:o AND i.command_sha256=:command
                 WHERE a.g>:retained AND a.depth<65)
                SELECT g,sha,depth FROM ancestry ORDER BY depth DESC LIMIT 1
                """), key).setParameter("g", plan.next().predecessorGeneration()).setParameter("sha", nextSha)
                .setParameter("command", HexFormat.of().parseHex(plan.next().command().sha256()))
                .setParameter("retained", retention.predecessorGeneration()).getSingleResult();
        long endpoint = ((Number) row[0]).longValue(); byte[] sha = (byte[]) row[1]; int depth = ((Number) row[2]).intValue();
        if (endpoint==retention.predecessorGeneration() && !MessageDigest.isEqual(sha, retainedSha)
                || endpoint>retention.predecessorGeneration() && depth<65)
            throw unavailable("Recovery limit ancestry is incomplete or differs");
        if (depth==65) return Optional.of(new Proof("ANCESTRY", 65, depth, endpoint, sha));
        long count = ((Number) scope(em.createNativeQuery("""
                SELECT count(*) FROM repository_preparation_pin_batches
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:retained
                """), key).setParameter("retained", retention.predecessorGeneration()).getSingleResult()).longValue();
        if (count>16) throw unavailable("Recovery capture count is corrupt");
        return count==16 ? Optional.of(new Proof("CAPTURES",16,depth,endpoint,sha)) : Optional.empty();
    }

    private static void insert(EntityManager em, RepositorySuccessorInstall.Plan plan, DocumentPublicationPreparationRecord retention,
            byte[] nextSha, byte[] retainedSha, String modes, Proof proof) {
        expected(scope(em.createNativeQuery("""
                INSERT INTO repository_recovery_limit_decisions(account_id,principal,operation_id,claim_epoch,claim_token,incarnation,
                 owner_generation,owner_nonce,command_sha256,preparation_sha256,modes_sha256,retention_generation,retention_sha256,
                 limit_kind,observed_count,inspected_depth,endpoint_generation,endpoint_sha256)
                VALUES(:a,:p,:o,:epoch,:token,:incarnation,:generation,:nonce,:command,:sha,
                 sha256(convert_to(CAST(:modes AS jsonb)::text,'UTF8')),:retained,:retainedSha,:kind,:count,:depth,:endpoint,:endpointSha)
                """), plan.next().key()),plan,retention,nextSha,retainedSha,modes)
                .setParameter("kind",proof.kind()).setParameter("count",proof.count()).setParameter("depth",proof.depth())
                .setParameter("endpoint",proof.endpoint()).setParameter("endpointSha",proof.endpointSha()).executeUpdate();
    }

    private static boolean matches(EntityManager em, RepositorySuccessorInstall.Plan plan, DocumentPublicationPreparationRecord retention,
            byte[] nextSha, byte[] retainedSha, String modes) {
        return !expected(scope(em.createNativeQuery("""
                SELECT 1 FROM repository_recovery_limit_decisions d JOIN repository_operation_rejection r
                 USING(account_id,principal,operation_id) WHERE account_id=:a AND principal=:p AND operation_id=:o
                 AND d.claim_epoch=:epoch AND d.claim_token=:token AND d.incarnation=:incarnation AND d.owner_generation=:generation
                 AND d.owner_nonce=:nonce AND d.command_sha256=:command AND d.preparation_sha256=:sha
                 AND d.modes_sha256=sha256(convert_to(CAST(:modes AS jsonb)::text,'UTF8'))
                 AND d.retention_generation=:retained AND d.retention_sha256=:retainedSha
                 AND r.creation_xid=d.creation_xid AND r.owner_generation=d.owner_generation
                 AND r.command_sha256=d.command_sha256 AND r.reason=4 AND r.disposition=1
                """),plan.next().key()),plan,retention,nextSha,retainedSha,modes).getResultList().isEmpty();
    }

    private static DocumentPublicationRejection rejection(EntityManager em, DocumentPublicationPreparationRecord next) {
        long at = ((Number) em.createNativeQuery("SELECT floor(extract(epoch FROM clock_timestamp())*1000000)").getSingleResult()).longValue();
        var key = next.key();
        var receipt = DocumentPublicationRejection.newBuilder().setAccountId(key.account()).setPrincipal(key.principal())
                .setOperationId(key.operationId().toString()).setOwnerGeneration(next.predecessorGeneration()+1)
                .setCommandCodec(DocumentPublicationCommand.CODEC).setCommandEncodingVersion(DocumentPublicationCommand.ENCODING_VERSION)
                .setCommandSha256(next.command().sha256()).setRecordedAtEpochMicros(at).setDispositionValue(1).setReasonValue(4).build();
        var encoded = DocumentPublicationRejectionCodec.encode(next.command(),receipt,key.principal(),receipt.getOwnerGeneration());
        scope(em.createNativeQuery("""
                INSERT INTO repository_operation_rejection(account_id,principal,operation_id,owner_generation,command_codec,command_version,
                 command_sha256,result_codec,result_version,result_bytes,result_sha256,recorded_at_epoch_micros,disposition,reason)
                VALUES(:a,:p,:o,:generation,'document-publication',1,:command,'document-publication-rejection',1,:bytes,:sha,:at,1,4)
                """),key).setParameter("generation",receipt.getOwnerGeneration())
                .setParameter("command",HexFormat.of().parseHex(next.command().sha256()))
                .setParameter("bytes",encoded.bytes().toByteArray()).setParameter("sha",HexFormat.of().parseHex(encoded.sha256()))
                .setParameter("at",at).executeUpdate();
        return receipt;
    }

    private static Query expected(Query q, RepositorySuccessorInstall.Plan plan, DocumentPublicationPreparationRecord retention,
            byte[] nextSha, byte[] retainedSha, String modes) {
        return q.setParameter("epoch",plan.reservation().predecessor().epoch()+1).setParameter("token",plan.reservation().successorToken())
                .setParameter("incarnation",plan.reservation().successorIncarnation()).setParameter("generation",plan.next().predecessorGeneration()+1)
                .setParameter("nonce",plan.next().seeds().ownerNonce()).setParameter("command",HexFormat.of().parseHex(plan.next().command().sha256()))
                .setParameter("sha",nextSha).setParameter("modes",modes).setParameter("retained",retention.predecessorGeneration())
                .setParameter("retainedSha",retainedSha);
    }
    private static Query scope(Query q, RepositoryOperationLedger.Key key) {
        return q.setParameter("a",key.account()).setParameter("p",key.principal()).setParameter("o",key.operationId());
    }
    private static byte[] digest(DocumentPublicationPreparationRecord record) {
        return DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(record));
    }
    private static RepositoryException unavailable(String message) {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,message);
    }
}
