package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/** Private terminal qualification for retention cleanup. Never grants deletion or execution. */
final class DocumentPreparationTerminalEvidence {
    sealed interface Evidence permits Outcome, Abandonment {}
    record Outcome(String kind,long generation,String resultSha256,String creationXid) implements Evidence {
        @Override public String toString() { return "PreparationTerminalOutcome[private]"; }
    }
    record Abandonment(UUID claimToken,UUID ownerNonce) implements Evidence {
        @Override public String toString() { return "PreparationAbandonment[private]"; }
    }

    private final DocumentPublicationPreparationRecord record;
    private final byte[] preparationSha;

    /** Caller reserves bounded preparation/result memory before construction and retains it through qualification. */
    DocumentPreparationTerminalEvidence(DocumentPublicationPreparationRecord record) {
        this.record=Objects.requireNonNull(record);
        this.preparationSha=DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(record));
    }

    /** Caller retains the transaction and next locks the root header and digest-ordered capture batches. */
    Evidence lockAndRequire(EntityManager em,RepositoryCaller caller,RepositoryReadControl control) {
        control.check();
        DocumentAdmissionAuthorization.requireCaller(caller,record.key(),record.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Retention terminal qualification requires private process authority");
        var claims=scope(em.createNativeQuery("""
                SELECT command_sha256,claim_epoch,claim_token,CAST(require_repository_read_committed() AS text)
                FROM repository_execution_claims WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE
                """)).getResultList();
        if (claims.isEmpty()) throw incomplete("Retained execution claim is absent");
        var claim=(Object[])claims.getFirst();
        if (!record.command().sha256().equals(hex(claim[0]))) throw corrupt();
        var preparations=scope(em.createNativeQuery("""
                SELECT preparation_sha256,command_sha256,owner_nonce FROM repository_publication_preparations
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g FOR UPDATE
                """)).setParameter("g",record.predecessorGeneration()).getResultList();
        if (preparations.isEmpty()) throw incomplete("Retained preparation is absent");
        var preparation=(Object[])preparations.getFirst();
        if (!MessageDigest.isEqual(preparationSha,(byte[])preparation[0])
                || !record.command().sha256().equals(hex(preparation[1]))
                || !record.seeds().ownerNonce().equals(preparation[2])) throw corrupt();
        var outcomes=scope(em.createNativeQuery("""
                SELECT 'SUCCESS',owner_generation,command_codec,command_version,command_sha256,
                  result_codec,result_version,CASE WHEN octet_length(result_bytes)<=1048576 THEN result_bytes END,
                  result_sha256,creation_xid::text
                FROM repository_operation_success WHERE account_id=:a AND principal=:p AND operation_id=:o
                UNION ALL
                SELECT 'REJECTION',owner_generation,command_codec,command_version,command_sha256,
                  result_codec,result_version,CASE WHEN octet_length(result_bytes)<=4096 THEN result_bytes END,
                  result_sha256,creation_xid::text
                FROM repository_operation_rejection WHERE account_id=:a AND principal=:p AND operation_id=:o
                """)).getResultList();
        var abandoned=scope(em.createNativeQuery("""
                SELECT predecessor_generation,owner_nonce,preparation_sha256,claim_epoch,claim_token
                FROM repository_publication_abandonments WHERE account_id=:a AND principal=:p AND operation_id=:o
                """)).getResultList();
        if (outcomes.size()+abandoned.size()>1) throw corrupt();
        if (!abandoned.isEmpty()) {
            var row=(Object[])abandoned.getFirst();
            if (record.predecessorGeneration()!=0 || ((Number)row[0]).longValue()!=0
                    || !record.seeds().ownerNonce().equals(row[1]) || !MessageDigest.isEqual(preparationSha,(byte[])row[2])
                    || ((Number)row[3]).longValue()!=1 || ((Number)claim[1]).longValue()!=1 || !claim[2].equals(row[4])) throw corrupt();
            boolean exact=(Boolean)scope(em.createNativeQuery("""
                    SELECT EXISTS(SELECT 1 FROM repository_coordinator_bindings WHERE account_id=:a AND principal=:p
                      AND operation_id=:o AND claim_epoch=1 AND claim_token=:token)
                    AND NOT EXISTS(SELECT 1 FROM repository_operations WHERE account_id=:a AND principal=:p AND operation_id=:o)
                    AND NOT EXISTS(SELECT 1 FROM repository_operation_owners WHERE account_id=:a AND principal=:p AND operation_id=:o)
                    AND NOT EXISTS(SELECT 1 FROM repository_publication_assessment_starts WHERE account_id=:a AND principal=:p AND operation_id=:o)
                    """)).setParameter("token",row[4]).getSingleResult();
            if (!exact) throw corrupt();
            control.check();
            return new Abandonment((UUID)row[4],(UUID)row[1]);
        }
        if (outcomes.isEmpty()) throw incomplete("Retained operation has no terminal outcome");
        var row=(Object[])outcomes.getFirst();
        long generation=((Number)row[1]).longValue();
        if (generation<=record.predecessorGeneration() || !DocumentPublicationCommand.CODEC.equals(row[2])
                || ((Number)row[3]).intValue()!=DocumentPublicationCommand.ENCODING_VERSION
                || !record.command().sha256().equals(hex(row[4])) || !(row[7] instanceof byte[])) throw corrupt();
        boolean exact=(Boolean)scope(em.createNativeQuery("""
                SELECT EXISTS(SELECT 1 FROM repository_operation_owners o JOIN repository_publication_preparations p
                  ON p.account_id=o.account_id AND p.principal=o.principal AND p.operation_id=o.operation_id
                  AND p.predecessor_generation=o.owner_generation-1 AND p.owner_nonce=o.owner_token
                  JOIN repository_operations op ON op.account_id=o.account_id AND op.principal=o.principal AND op.operation_id=o.operation_id
                  WHERE o.account_id=:a AND o.principal=:p AND o.operation_id=:o AND o.owner_generation=:generation
                  AND p.command_sha256=:command AND op.command_sha256=:command
                  AND op.command_codec='document-publication' AND op.command_version=1)
                """)).setParameter("generation",generation).setParameter("command",row[4]).getSingleResult();
        if (!exact) throw corrupt();
        try {
            var bytes=ByteString.copyFrom((byte[])row[7]);
            if (row[0].equals("SUCCESS")) {
                var result=DocumentPublicationResultCodec.decode(record.command(),record.key().principal(),generation,
                        (String)row[5],((Number)row[6]).intValue(),bytes,hex(row[8]));
                if (!DocumentPublicationResultCodec.encode(record.command(),result,record.key().principal(),generation).bytes().equals(bytes)) throw corrupt();
                int members=((Number)scope(em.createNativeQuery("""
                        SELECT member_count FROM repository_operation_success WHERE account_id=:a AND principal=:p AND operation_id=:o
                        """)).getSingleResult()).intValue();
                if (result.getMembersCount()!=members) throw corrupt();
            } else {
                var receipt=DocumentPublicationRejectionCodec.decode(record.command(),record.key().principal(),generation,
                        (String)row[5],((Number)row[6]).intValue(),bytes,hex(row[8]));
                var projection=(Object[])scope(em.createNativeQuery("""
                        SELECT owner_generation,result_codec,result_version,result_bytes,encode(result_sha256,'hex'),
                          recorded_at_epoch_micros,disposition,reason,command_codec,command_version,encode(command_sha256,'hex'),
                          assessment_id,manifest_codec,manifest_version,encode(manifest_sha256,'hex'),retain_until_epoch_micros
                        FROM repository_operation_rejection WHERE account_id=:a AND principal=:p AND operation_id=:o
                        """)).getSingleResult();
                DocumentPublicationReplay.requireRejectionProjection(receipt,projection);
                if (receipt.getReasonValue()==4) {
                    boolean paired=(Boolean)scope(em.createNativeQuery("""
                            SELECT EXISTS(SELECT 1 FROM repository_recovery_limit_decisions
                            WHERE account_id=:a AND principal=:p AND operation_id=:o AND owner_generation=:generation
                              AND command_sha256=:command AND creation_xid::text=:xid)
                            """)).setParameter("generation",generation).setParameter("command",row[4])
                            .setParameter("xid",row[9]).getSingleResult();
                    if (!paired) throw corrupt();
                }
            }
        } catch (InvalidProtocolBufferException | IllegalArgumentException malformed) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Retained terminal receipt is invalid",malformed);
        }
        control.check();
        return new Outcome((String)row[0],generation,hex(row[8]),(String)row[9]);
    }

    private Query scope(Query q) {
        return q.setParameter("a",record.key().account()).setParameter("p",record.key().principal()).setParameter("o",record.key().operationId());
    }
    private static String hex(Object value) { return HexFormat.of().formatHex((byte[])value); }
    private static RepositoryException incomplete(String message) { return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,message); }
    private static RepositoryException corrupt() { return new RepositoryException(RepositoryException.Code.DATA_LOSS,"Retained terminal evidence differs from its preparation"); }
}
