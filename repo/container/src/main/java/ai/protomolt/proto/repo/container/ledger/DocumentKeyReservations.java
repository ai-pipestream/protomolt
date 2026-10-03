package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import java.util.List;
import java.util.UUID;

/** Permanent exclusion between pre-admission writers and managed attempt keys. */
final class DocumentKeyReservations {
    private DocumentKeyReservations() {}

    static void reserve(EntityManager em, List<String> keys, UUID attempt) {
        var values = ListValue.newBuilder();
        for (String key : keys) {
            if (key == null || !ai.protomolt.proto.repo.codec.RepositoryNamespaces.isDocumentPart(key))
                throw new IllegalArgumentException("Document reservation requires document part keys");
            values.addValues(Value.newBuilder().setStringValue(key));
        }
        String json;
        try { json = JsonFormat.printer().omittingInsignificantWhitespace().print(values); }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new IllegalArgumentException("Cannot encode document key reservation", invalid);
        }
        em.createNativeQuery("SELECT 1 FROM reserve_document_keys(CAST(:keys AS jsonb),CAST(:attempt AS uuid),false)",Integer.class)
                .setParameter("keys",json).setParameter("attempt",attempt).getSingleResult();
    }
}
