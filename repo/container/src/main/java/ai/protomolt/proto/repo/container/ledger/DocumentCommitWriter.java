package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.container.lifecycle.DocumentEventFactory;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentPublicationRowKind;
import ai.protomolt.proto.repo.v1.DocumentPublicationSlot;
import ai.protomolt.proto.repo.v1.DocumentPublishedRevision;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Builds all row snapshots before any write, then persists one member of an already-fenced batch. */
final class DocumentCommitWriter {
    private DocumentCommitWriter() {}
    record Candidate(String member, UUID revision, long selection, DocumentRecord row, String references, byte[] resolution) {}

    static Candidate prepare(DocumentUploadPlan.Member member, DocumentCommandContent content,
            DocumentCommitParts.Bound parts, Map<UUID,DocumentRecord> locked,
            Map<UUID,Map<DocumentPublicationSlot,PartManifestEntry>> sourceEntries,
            Instant now, Runnable control) {
        var intent=member.intent();
        var prior=locked.get(member.nodeId());
        long previous=0;
        if (prior!=null) {
            var manifest=prior.readManifest();
            if (manifest==null || manifest.getDocVersion()<=0)
                throw new DocumentPartAttemptLedger.FenceException("Native publication requires an explicit previous manifest version");
            previous=manifest.getDocVersion();
        }
        var manifest=DocumentManifest.newBuilder().setAddress(intent.getDestination().getAddress()).setDocVersion(Math.addExact(previous,1));
        var refs=ListValue.newBuilder();
        var timestamp=Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        DocumentCommitParts.Physical core=null;
        long size=0;
        for (int ordinal=0;ordinal<intent.getPartsCount();ordinal++) {
            control.run();
            var declaration=intent.getParts(ordinal);
            var entry=PartManifestEntry.newBuilder().setPart(declaration.getSlot().getPart()).setSubKey(declaration.getSlot().getSubKey());
            if (declaration.hasEmpty()) entry.setState(PartState.PART_STATE_EMPTY);
            else {
                var physical=parts.parts().get(new DocumentCommitParts.Slot(intent.getMemberId(),ordinal));
                if (declaration.hasReuse()) {
                    UUID source=DocumentIds.nodeId(declaration.getReuse().getSource().getAddress());
                    var retained=sourceEntries.getOrDefault(source,Map.of()).get(declaration.getReuse().getSourceSlot());
                    if (retained==null || retained.getState()!=PartState.PART_STATE_PRESENT)
                        throw new DocumentPartAttemptLedger.FenceException("Retained manifest slot is unavailable");
                    entry=retained.toBuilder(); // Preserve known last-write time and producer provenance.
                } else {
                    entry.setUpdatedAt(timestamp);
                    if (declaration.getUpload().hasWrittenBy())
                        entry.setWrittenBy(declaration.getUpload().getWrittenBy());
                }
                entry.setState(PartState.PART_STATE_PRESENT).setObjectKey(physical.key())
                        .setSizeBytes(physical.size()).setSha256(physical.sha256());
                size=Math.addExact(size,physical.size());
                if (physical.part()==DocumentPart.DOCUMENT_PART_CORE_VALUE) core=physical;
                refs.addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                        .putFields("ordinal",text(Integer.toString(ordinal))).putFields("part",text(Integer.toString(physical.part())))
                        .putFields("sub_key",text(physical.subKey())).putFields("object_id",text(physical.id().toString()))));
            }
            manifest.addParts(entry);
        }
        if (core==null) throw new IllegalArgumentException("Native publication requires a verified CORE");
        var row=new DocumentRecord();
        var address=intent.getDestination().getAddress();
        var ownership=intent.getOwnership();
        row.nodeId=member.nodeId(); row.docId=address.getDocId(); row.accountId=address.getAccountId();
        row.graphId=address.getGraphId(); row.graphAddressId=address.getGraphAddressId();
        row.rowKind=intent.getRowKind()==DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_INTAKE ? DocumentRowKind.INTAKE : DocumentRowKind.PIPELINE;
        row.clusterId=intent.hasClusterId() ? intent.getClusterId() : null;
        row.datasourceId=ownership.getDatasourceId(); row.connectorId=ownership.hasConnectorId() ? ownership.getConnectorId() : null;
        row.driveName=member.placement().drive().name(); row.objectKey=null;
        row.versionId=core.version(); row.etag=core.etag()==null ? "" : core.etag(); row.sizeBytes=size;
        row.contentType=core.contentType();
        var document=content.assembly().document();
        row.filename=document.hasSearchMetadata() && document.getSearchMetadata().hasTitle() ? document.getSearchMetadata().getTitle() : row.docId;
        if (row.filename.codePointCount(0,row.filename.length())>500) throw new IllegalArgumentException("Document filename exceeds 500 characters");
        row.writeSecurity(ownership.getSecurity()); row.writeManifest(manifest.build());
        row.checksum=DocumentPartCodec.rootChecksumFromManifest(manifest.build());
        row.deleteSourceBlobsOnSettle=intent.getDeleteSourceBlobsOnSettle();
        row.sourceBlobDeleteReason=intent.hasSourceBlobDeleteReason() ? intent.getSourceBlobDeleteReason() : null;
        row.crawlId=intent.hasCrawlId() ? intent.getCrawlId() : null;
        row.createdAt=prior==null ? now : prior.createdAt; row.updatedAt=now;
        if (prior!=null) { row.reprocessCount=prior.reprocessCount; row.lastReprocessedAt=prior.lastReprocessedAt; }
        try {
            return new Candidate(intent.getMemberId(),UUID.randomUUID(),parts.selections().get(intent.getMemberId()),row,
                    JsonFormat.printer().omittingInsignificantWhitespace().print(refs),
                    content.structuredResolution().map(value -> value.toByteArray()).orElse(null));
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Cannot encode native revision references",failure);
        }
    }

    static DocumentPublishedRevision write(EntityManager em, RepositoryOperationLedger.Owner owner, Candidate candidate, int ordinal, boolean deliver) {
        return write(em, owner, candidate, ordinal, deliver, "OPAQUE", null, () -> {});
    }

    static DocumentPublishedRevision write(EntityManager em, RepositoryOperationLedger.Owner owner, Candidate candidate,
            int ordinal, boolean deliver, String decision, DocumentSchemaRetention retention, Runnable control) {
        if (!("TYPED".equals(decision) && retention != null) && !("OPAQUE".equals(decision) && retention == null))
            throw new IllegalArgumentException("Publication decision differs from its retention writer");
        control.run();
        var row=em.merge(candidate.row()); em.flush(); em.refresh(row);
        var event=deliver ? DocumentEventFactory.saved(row,row.updatedAt) : DocumentEventFactory.savedWithoutDelivery(row,row.updatedAt);
        em.createNativeQuery("""
                INSERT INTO document_revision_commits(revision_id,node_id,publication_revision,account_id,principal,operation_id,
                    owner_generation,member_id,member_ordinal,selection_revision,event_id,metadata_version,admission_mode,structured_resolution)
                VALUES(:revision,:node,:mutation,:account,:principal,:operation,:generation,:member,:ordinal,:selection,:event,1,:decision,:resolution)
                """).setParameter("revision",candidate.revision()).setParameter("node",row.nodeId).setParameter("mutation",row.mutationRevision)
                .setParameter("account",owner.key().account()).setParameter("principal",owner.key().principal())
                .setParameter("operation",owner.key().operationId()).setParameter("generation",owner.generation())
                .setParameter("member",candidate.member()).setParameter("ordinal",ordinal)
                .setParameter("selection",candidate.selection()).setParameter("event",event.eventId)
                .setParameter("resolution",candidate.resolution()).setParameter("decision",decision).executeUpdate();
        em.createNativeQuery("""
                INSERT INTO document_revision_publications(revision_id,node_id,publication_revision,body,published_at,native_binding)
                SELECT :revision,node_id,mutation_revision,document_publication_body(documents),clock_timestamp(),:revision
                FROM documents WHERE node_id=:node
                """).setParameter("revision",candidate.revision()).setParameter("node",row.nodeId).executeUpdate();
        em.createNativeQuery("""
                INSERT INTO document_revision_parts(revision_id,revision_ordinal,part,sub_key,object_id)
                SELECT :revision,q.ordinal,q.part,q.sub_key,q.object_id
                FROM jsonb_to_recordset(CAST(:parts AS jsonb)) q(ordinal integer,part integer,sub_key text,object_id uuid)
                """).setParameter("revision",candidate.revision()).setParameter("parts",candidate.references()).executeUpdate();
        if (retention != null) retention.write(em, owner, candidate.revision(), control);
        control.run();
        em.createNativeQuery("UPDATE document_revision_publications SET projection_sealed=true WHERE revision_id=:revision")
                .setParameter("revision",candidate.revision()).executeUpdate();
        em.createNativeQuery("""
                INSERT INTO document_revision_current(node_id,revision_id) VALUES(:node,:revision)
                ON CONFLICT(node_id) DO UPDATE SET revision_id=EXCLUDED.revision_id
                """).setParameter("node",row.nodeId).setParameter("revision",candidate.revision()).executeUpdate();
        em.createNativeQuery("DELETE FROM document_part_publications WHERE node_id=:node").setParameter("node",row.nodeId).executeUpdate();
        em.persist(event);
        return DocumentPublishedRevision.newBuilder().setMemberId(candidate.member()).setAddress(row.readManifest().getAddress())
                .setRevisionId(candidate.revision().toString()).setMutationRevision(row.mutationRevision).build();
    }

    private static Value text(String value) { return Value.newBuilder().setStringValue(value).build(); }
}
