package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.v1.DocumentPublicationSlot;
import ai.protomolt.proto.repo.v1.PublicationObjectIdentity;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Point-in-time SQL evidence for reuse declarations, not permission to perform
 * physical reuse. Requires the caller's source authorization and revision locks.
 * Provider qualification and atomic revision-reference publication remain separate.
 */
final class DocumentReuseAdmission {
    private DocumentReuseAdmission() {}
    private static final int BATCH_SIZE = 256;
    private record Slot(UUID node, DocumentPublicationSlot slot) {}
    private record Batch(String json, int size) {}
    static final class Prepared {
        private final List<Batch> sources;
        private final List<Batch> claims;
        private Prepared(List<Batch> sources, List<Batch> claims) {
            this.sources = List.copyOf(sources); this.claims = List.copyOf(claims);
        }
    }

    /** All encoding and deduplication occurs before database locks. */
    static Prepared prepare(DocumentUploadPlan.Prepared plan) {
        var claims = new LinkedHashMap<Slot, PublicationObjectIdentity>();
        var nodes = new TreeSet<UUID>();
        for (var member : plan.members()) for (var part : member.intent().getPartsList()) {
            if (!part.hasReuse()) continue;
            var reuse = part.getReuse();
            UUID node = DocumentIds.nodeId(reuse.getSource().getAddress());
            var prior = claims.putIfAbsent(new Slot(node, reuse.getSourceSlot()), reuse.getObject());
            if (prior != null && !prior.equals(reuse.getObject()))
                throw new IllegalArgumentException("Contradictory retained object identities for the same source slot");
            nodes.add(node);
        }
        var sourceRows = nodes.stream().map(node -> Struct.newBuilder()
                .putFields("node_id", text(node.toString())).build()).toList();
        var claimRows = new ArrayList<Struct>(claims.size());
        claims.forEach((slot, object) -> {
            var row = Struct.newBuilder().putFields("node_id", text(slot.node.toString()))
                    .putFields("part", text(Integer.toString(slot.slot.getPartValue())))
                    .putFields("sub_key", text(slot.slot.getSubKey()))
                    .putFields("object_id", text(object.getObjectId()))
                    .putFields("generation", text(object.getBackendGeneration()))
                    .putFields("realm", text(object.getStorageRealm()))
                    .putFields("namespace", text(object.getNamespace()))
                    .putFields("object_key", text(object.getObjectKey()))
                    .putFields("size", text(Long.toString(object.getSizeBytes())))
                    .putFields("sha256", text(object.getSha256()))
                    .putFields("content_type", text(object.getContentType()));
            // Missing JSON property maps to SQL NULL, distinct from an empty or sentinel version.
            if (object.hasProviderVersion()) row.putFields("version", text(object.getProviderVersion()));
            claimRows.add(row.build());
        });
        return new Prepared(encode(sourceRows), encode(claimRows));
    }

    static void requireBoundSources(EntityManager em, Prepared prepared) {
        // Compare each source body once, not once per reused part in that body.
        for (var batch : prepared.sources) {
            long matched = ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(node_id uuid)
                    JOIN documents d ON d.node_id=q.node_id
                    JOIN document_revision_current current_revision ON current_revision.node_id=d.node_id
                    JOIN document_revision_publications r ON r.revision_id=current_revision.revision_id AND r.node_id=d.node_id
                    LEFT JOIN document_part_publications p ON p.node_id=d.node_id
                    LEFT JOIN document_part_publication_history h ON h.attempt_id=p.attempt_id AND h.node_id=d.node_id
                    LEFT JOIN document_part_attempts a ON a.attempt_id=p.attempt_id
                    WHERE r.projection_sealed AND r.body=document_publication_body(d)
                        AND CASE WHEN r.native_binding IS NOT NULL THEN
                            p.attempt_id IS NULL AND EXISTS(SELECT 1 FROM document_revision_commits c
                                JOIN repository_operation_success s USING(account_id,principal,operation_id,owner_generation)
                                WHERE c.revision_id=r.revision_id AND c.account_id=d.account_id)
                        ELSE r.legacy_attempt_id=p.attempt_id AND h.body=r.body
                            AND a.node_id=d.node_id AND a.account_id=d.account_id
                            AND a.plan_kind='FULL_REVISION' AND a.state='VERIFIED'
                            AND NOT EXISTS(SELECT 1 FROM document_part_attempt_cleanup c WHERE c.attempt_id=a.attempt_id) END
                    """).setParameter("rows", batch.json).getSingleResult()).longValue();
            if (matched != batch.size) refuse();
        }
        for (var batch : prepared.claims) {
            long matched = ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(
                        node_id uuid,part integer,sub_key text,object_id uuid,generation text,realm text,
                        namespace text,object_key text,version text,size bigint,sha256 text,content_type text)
                    JOIN document_revision_current p ON p.node_id=q.node_id
                    JOIN documents d ON d.node_id=p.node_id
                    JOIN document_revision_publications h ON h.revision_id=p.revision_id AND h.node_id=q.node_id AND h.projection_sealed
                    JOIN document_revision_parts part ON part.revision_id=h.revision_id AND part.part=q.part AND part.sub_key=q.sub_key
                    JOIN repository_physical_locations l ON l.object_id=part.object_id AND l.source_kind='DOCUMENT_PART'
                    JOIN document_part_attempt_objects o ON o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
                        AND o.physical_object_id=l.object_id AND o.part=q.part
                        AND o.sub_key_digest=sha256(convert_to(q.sub_key,'UTF8')) AND o.sub_key=q.sub_key
                    JOIN document_part_attempts a ON a.attempt_id=o.attempt_id AND a.account_id=d.account_id
                    JOIN repository_object_retention r ON r.object_id=l.object_id AND NOT r.reclaiming AND NOT r.retiring
                    JOIN repository_object_references historical ON historical.object_id=l.object_id
                        AND historical.owner_kind='DOCUMENT_HISTORY' AND historical.owner_id=p.revision_id
                        AND historical.owner_revision=h.publication_revision
                    JOIN repository_object_references current_ref ON current_ref.object_id=l.object_id
                        AND current_ref.owner_kind='DOCUMENT_CURRENT' AND current_ref.owner_id=p.node_id
                        AND current_ref.owner_revision=h.publication_revision
                    WHERE a.state='VERIFIED' AND o.verified
                        AND NOT EXISTS(SELECT 1 FROM document_part_attempt_cleanup c WHERE c.attempt_id=a.attempt_id)
                        AND l.object_id=q.object_id AND l.backend_generation=q.generation
                        AND a.backend_generation=l.backend_generation AND a.storage_realm=l.storage_realm
                        AND l.storage_realm=q.realm AND o.storage_realm=l.storage_realm
                        AND l.storage_namespace=q.namespace AND o.storage_namespace=l.storage_namespace
                        AND l.object_key=q.object_key AND o.object_key=l.object_key
                        AND o.provider_version IS NOT DISTINCT FROM q.version
                        AND o.expected_size=q.size AND o.expected_sha256=q.sha256 AND o.content_type=q.content_type
                    """).setParameter("rows", batch.json).getSingleResult()).longValue();
            if (matched != batch.size) refuse();
        }
    }

    /** Same transaction and source locks as authorization; no provider calls or retention acquisition. */
    static DocumentRetainedReadPlan capture(EntityManager em, Prepared prepared,
            DocumentUploadPlan.Prepared plan, RepositoryOperationLedger.Owner owner) {
        requireBoundSources(em, prepared);
        var generations = new java.util.HashSet<String>();
        for (var member : plan.members()) for (var part : member.intent().getPartsList())
            if (part.hasReuse()) generations.add(part.getReuse().getObject().getBackendGeneration());
        var profiles = ManagedBackendLedger.requireAll(em, generations);
        var entries = new ArrayList<DocumentRetainedReadPlan.Entry>();
        for (var member : plan.members()) {
            for (int ordinal = 0; ordinal < member.intent().getPartsCount(); ordinal++) {
                var part = member.intent().getParts(ordinal);
                if (!part.hasReuse()) continue;
                var source = part.getReuse();
                var object = source.getObject();
                var profile = profiles.get(object.getBackendGeneration());
                if (!profile.storageRealm().equals(object.getStorageRealm())) refuse();
                entries.add(new DocumentRetainedReadPlan.Entry(member.intent().getMemberId(), ordinal,
                        part.getSlot(), source, new DocumentPublicationLedger.Binding(
                                object.getBackendGeneration(), profile, object.getNamespace())));
            }
        }
        return new DocumentRetainedReadPlan(plan.command(), owner, entries);
    }

    private static Value text(String value) { return Value.newBuilder().setStringValue(value).build(); }
    private static List<Batch> encode(List<Struct> rows) {
        var batches = new ArrayList<Batch>();
        for (int start = 0; start < rows.size(); start += BATCH_SIZE) {
            var items = rows.subList(start, Math.min(start + BATCH_SIZE, rows.size()));
            var list = ListValue.newBuilder();
            items.forEach(row -> list.addValues(Value.newBuilder().setStructValue(row)));
            try { batches.add(new Batch(JsonFormat.printer().omittingInsignificantWhitespace().print(list), items.size())); }
            catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                throw new IllegalArgumentException("Cannot encode retained object claims", failure);
            }
        }
        return batches;
    }
    private static void refuse() {
        throw new DocumentPartAttemptLedger.FenceException("Reused part does not match a retained current managed source binding");
    }
}
