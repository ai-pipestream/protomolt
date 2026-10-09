package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Projects requested provenance through retained part ordinals without parsing whole source manifests. */
final class DocumentRetainedManifestEntries {
    private static final int BATCH_SIZE=32;
    private static final int MAX_ENTRY_BYTES=1_048_576;
    private static final long MAX_TOTAL_BYTES=64L*1_048_576;
    private record SourceSlot(UUID node,DocumentPublicationSlot slot) {}
    private record Batch(String json,int count) {}
    record Prepared(List<Batch> batches,Map<SourceSlot,PublicationObjectIdentity> expected) {
        Prepared { batches=List.copyOf(batches); expected=Map.copyOf(expected); }
    }
    private DocumentRetainedManifestEntries() {}

    static Prepared prepare(DocumentUploadPlan.Prepared plan,Runnable control) {
        var expected=new LinkedHashMap<SourceSlot,PublicationObjectIdentity>();
        for (var member:plan.members()) for (var part:member.intent().getPartsList()) {
            control.run();
            if (!part.hasReuse()) continue;
            var reuse=part.getReuse();
            var key=new SourceSlot(DocumentIds.nodeId(reuse.getSource().getAddress()),reuse.getSourceSlot());
            var previous=expected.putIfAbsent(key,reuse.getObject());
            if (previous!=null && !previous.equals(reuse.getObject())) throw conflict();
        }
        if (expected.size()>10_000) throw new IllegalArgumentException("Retained manifest request exceeds part bound");
        var batches=new ArrayList<Batch>();
        var values=ListValue.newBuilder();
        for (var entry:expected.entrySet()) {
            control.run();
            values.addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                    .putFields("node",text(entry.getKey().node().toString()))
                    .putFields("part",text(Integer.toString(entry.getKey().slot().getPartValue())))
                    .putFields("sub_key",text(entry.getKey().slot().getSubKey()))
                    .putFields("object_id",text(entry.getValue().getObjectId()))));
            if (values.getValuesCount()==BATCH_SIZE) { batches.add(encode(values)); values.clear(); }
        }
        if (values.getValuesCount()>0) batches.add(encode(values));
        return new Prepared(batches,expected);
    }

    /** Caller holds document/current-revision and physical retention fences for the full command. */
    static Map<UUID,Map<DocumentPublicationSlot,PartManifestEntry>> read(EntityManager em,Prepared prepared,Runnable control) {
        var result=new HashMap<UUID,Map<DocumentPublicationSlot,PartManifestEntry>>();
        long total=0;
        for (var batch:prepared.batches()) {
            control.run();
            var rows=em.createNativeQuery("""
                    WITH requested AS MATERIALIZED (
                      SELECT q.node,q.part,q.sub_key,
                        (d.part_manifest->'parts'->p.revision_ordinal)::text AS entry
                      FROM jsonb_to_recordset(CAST(:requests AS jsonb)) q(node uuid,part integer,sub_key text,object_id uuid)
                      JOIN document_revision_current c ON c.node_id=q.node
                      JOIN document_revision_parts p ON p.revision_id=c.revision_id AND p.object_id=q.object_id
                        AND p.part=q.part AND p.sub_key=q.sub_key
                      JOIN documents d ON d.node_id=q.node
                    )
                    SELECT node,part,sub_key,octet_length(entry),
                      CASE WHEN octet_length(entry)<=:maximum THEN entry ELSE NULL END
                    FROM requested
                    """).setParameter("requests",batch.json()).setParameter("maximum",MAX_ENTRY_BYTES).getResultList();
            if (rows.size()!=batch.count()) throw conflict();
            for (Object value:rows) {
                control.run();
                Object[] row=(Object[])value;
                if (row[3]==null) throw conflict();
                long bytes=((Number)row[3]).longValue();
                if (bytes>MAX_ENTRY_BYTES || bytes>MAX_TOTAL_BYTES-total)
                    throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,"Retained manifest metadata exceeds publication budget");
                total+=bytes;
                var slot=DocumentPublicationSlot.newBuilder().setPartValue(((Number)row[1]).intValue()).setSubKey((String)row[2]).build();
                var key=new SourceSlot((UUID)row[0],slot);
                var object=prepared.expected().get(key);
                if (object==null || row[4]==null) throw conflict();
                var parsed=PartManifestEntry.newBuilder();
                try { JsonFormat.parser().merge((String)row[4],parsed); }
                catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
                    throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Retained manifest entry is malformed",invalid);
                }
                var entry=parsed.build();
                if (entry.getPart()!=slot.getPart() || !entry.getSubKey().equals(slot.getSubKey())
                        || entry.getState()!=PartState.PART_STATE_PRESENT || !entry.getObjectKey().equals(object.getObjectKey())
                        || entry.getSizeBytes()!=object.getSizeBytes() || !entry.getSha256().equals(object.getSha256())) throw conflict();
                if (result.computeIfAbsent(key.node(),ignored->new HashMap<>()).put(slot,entry)!=null) throw conflict();
            }
        }
        return result;
    }
    private static Batch encode(ListValue.Builder values) {
        try { return new Batch(JsonFormat.printer().omittingInsignificantWhitespace().print(values),values.getValuesCount()); }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) { throw new IllegalArgumentException("Cannot encode retained manifest request",invalid); }
    }
    private static Value text(String value) { return Value.newBuilder().setStringValue(value).build(); }
    private static DocumentPartAttemptLedger.FenceException conflict() {
        return new DocumentPartAttemptLedger.FenceException("Retained manifest entry differs from publication command");
    }
}
