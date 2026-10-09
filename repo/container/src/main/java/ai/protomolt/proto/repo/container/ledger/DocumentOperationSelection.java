package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import java.util.UUID;

/** Explicit initial and retry selection persistence; no provider adoption or terminal replay API. */
final class DocumentOperationSelection {
    private DocumentOperationSelection() {}

    record Expected(long revision, UUID attempt) {
        Expected {
            if (revision < 1 || revision == Long.MAX_VALUE || attempt == null)
                throw new IllegalArgumentException("Retry requires a replaceable revision and an exact attempt");
        }
    }

    static void replace(EntityManager em, RepositoryOperationLedger.Owner owner, String member,
            Expected expected, UUID next) {
        em.createNativeQuery("""
                INSERT INTO document_operation_selection_attempts(account_id,principal,operation_id,owner_generation,
                    member_id,selection_revision,attempt_id,previous_attempt_id)
                VALUES (:account,:principal,:operation,:generation,:member,:revision,:next,:previous)
                """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation())
                .setParameter("member", member).setParameter("revision", expected.revision()+1)
                .setParameter("next", next).setParameter("previous", expected.attempt()).executeUpdate();
        int changed = em.createNativeQuery("""
                UPDATE document_operation_selection_current SET selection_revision=:next
                WHERE account_id=:account AND principal=:principal AND operation_id=:operation
                    AND owner_generation=:generation AND member_id=:member AND selection_revision=:previous
                """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation())
                .setParameter("member", member).setParameter("next", expected.revision()+1)
                .setParameter("previous", expected.revision()).executeUpdate();
        if (changed != 1) throw new DocumentPartAttemptLedger.FenceException("Document selection compare-and-set conflict");
    }

    static String encode(DocumentUploadPlan.Prepared plan) {
        var members=ListValue.newBuilder();
        for (var member : plan.members()) {
            var placement=member.placement();
            var row=Struct.newBuilder()
                    .putFields("member_id", text(member.intent().getMemberId()))
                    .putFields("node_id", text(member.nodeId().toString()))
                    .putFields("sampled_revision", text(Long.toString(member.intent().getDestination().getExpectedMutationRevision())))
                    .putFields("drive_id", text(placement.drive().id().toString()))
                    .putFields("backend_generation", text(placement.generation()))
                    .putFields("storage_realm", text(placement.profile().storageRealm()))
                    .putFields("storage_namespace", text(placement.drive().namespace()))
                    .putFields("upload_count", text(Integer.toString(member.attempt().map(a -> a.uploads().size()).orElse(0))));
            member.attempt().ifPresent(a -> row.putFields("attempt_id",text(a.id().toString())));
            members.addValues(Value.newBuilder().setStructValue(row));
        }
        try { return JsonFormat.printer().omittingInsignificantWhitespace().print(members); }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new IllegalArgumentException("Cannot encode document operation selections",invalid);
        }
    }

    /** The caller already holds the owner write fence and sampled drive locks. */
    static void insert(EntityManager em, RepositoryOperationLedger.Owner owner, String encoded, int count) {
        int inserted=em.createNativeQuery("""
                INSERT INTO document_operation_selections(account_id,principal,operation_id,owner_generation,
                    member_id,node_id,sampled_revision,drive_id,drive_snapshot,drive_sha256,backend_generation,
                    storage_realm,storage_namespace,upload_count,attempt_id)
                SELECT :account,:principal,:operation,:generation,q.member_id,q.node_id,q.sampled_revision,
                    q.drive_id,document_operation_drive_snapshot(d),document_operation_drive_digest_v1(d),
                    q.backend_generation,q.storage_realm,q.storage_namespace,
                    q.upload_count,q.attempt_id
                FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(member_id text,node_id uuid,sampled_revision bigint,
                    drive_id uuid,backend_generation text,storage_realm text,storage_namespace text,
                    upload_count integer,attempt_id uuid)
                JOIN drives d ON d.drive_id=q.drive_id
                ORDER BY q.member_id
                """).setParameter("account",owner.key().account()).setParameter("principal",owner.key().principal())
                .setParameter("operation",owner.key().operationId()).setParameter("generation",owner.generation())
                .setParameter("rows",encoded).executeUpdate();
        if (inserted!=count) throw new DocumentPartAttemptLedger.FenceException("Document operation selection is incomplete");
    }

    private static Value text(String value) { return Value.newBuilder().setStringValue(value).build(); }
}
