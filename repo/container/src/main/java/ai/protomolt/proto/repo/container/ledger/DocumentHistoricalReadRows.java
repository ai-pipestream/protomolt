package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.PartState;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.UUID;
import java.util.stream.Collectors;

/** Native historical metadata only; no schema artifacts or provider I/O under document locks. */
final class DocumentHistoricalReadRows {
    private DocumentHistoricalReadRows() {}
    private static final long MAX_MANIFEST_BYTES = 64L * 1024 * 1024;

    /** Caller holds current document READ authorization and the active-reader lock. */
    static DocumentHistoricalReadPlan capture(EntityManager em, NodeAddress address, UUID revision) {
        var headers = em.createNativeQuery("""
                SELECT r.publication_revision,r.native_binding,r.projection_sealed,
                 (c.node_id=r.node_id AND c.account_id=:account AND c.publication_revision=r.publication_revision
                  AND c.creation_xid=r.projection_xid AND s.creation_xid=c.creation_xid),
                 octet_length((r.body->'part_manifest')::text),
                 CASE WHEN octet_length((r.body->'part_manifest')::text)<=:limit THEN (r.body->'part_manifest')::text END,
                 r.body->>'version_id',r.body->>'etag',r.body->>'size_bytes',r.body->>'checksum',
                 (r.body->>'node_id'=CAST(r.node_id AS text) AND r.body->>'account_id'=:account
                  AND r.body->>'doc_id'=:doc AND r.body->>'graph_id'=:graph AND r.body->>'graph_address_id'=:graphAddress),
                 (SELECT count(*) FROM document_revision_parts p WHERE p.revision_id=r.revision_id),
                 c.metadata_version,octet_length(c.metadata_snapshot::text),
                 CASE WHEN octet_length(c.metadata_snapshot::text)<=:metadataLimit THEN c.metadata_snapshot::text END
                FROM document_revision_publications r
                LEFT JOIN document_revision_commits c ON c.revision_id=r.revision_id
                LEFT JOIN repository_operation_success s USING(account_id,principal,operation_id,owner_generation)
                WHERE r.revision_id=:revision AND r.node_id=:node
                """).setParameter("account", address.getAccountId()).setParameter("revision", revision)
                .setParameter("node", DocumentIds.nodeId(address)).setParameter("limit", MAX_MANIFEST_BYTES)
                .setParameter("metadataLimit", DocumentHistoricalMetadata.MAX_BYTES)
                .setParameter("doc", address.getDocId()).setParameter("graph", address.getGraphId())
                .setParameter("graphAddress", address.getGraphAddressId()).getResultList();
        if (headers.isEmpty()) throw new RepositoryException(RepositoryException.Code.NOT_FOUND, "Document revision is unavailable");
        Object[] h = (Object[]) headers.getFirst();
        if (h[1] == null) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Historical reader requires a native revision");
        if (!revision.equals(h[1]) || !Boolean.TRUE.equals(h[2]) || !Boolean.TRUE.equals(h[3]) || !Boolean.TRUE.equals(h[10]))
            throw invalid("Historical revision binding is invalid");
        if (h[4] instanceof Number bytes && bytes.longValue() > MAX_MANIFEST_BYTES)
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Historical manifest exceeds read capacity");
        if (!(h[5] instanceof String encoded)) throw invalid("Historical manifest is missing");
        if (h[13] instanceof Number bytes && bytes.longValue() > DocumentHistoricalMetadata.MAX_BYTES)
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Historical metadata exceeds read capacity");
        if (!(h[12] instanceof Integer version) || !(h[14] instanceof String metadataJson))
            throw invalid("Historical metadata is missing");
        var metadata = DocumentHistoricalMetadata.decode(version, metadataJson, address.getAccountId());
        var builder = DocumentManifest.newBuilder();
        try { JsonFormat.parser().merge(encoded, builder); }
        catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical manifest is malformed", failure);
        }
        var manifest = builder.build();
        if (!manifest.getAddress().equals(address) || manifest.getPartsCount() > DocumentPublicationCommand.MAX_PARTS)
            throw invalid("Historical manifest identity or count is invalid");
        var rows = em.unwrap(org.hibernate.Session.class).createNativeQuery("""
                SELECT r.revision_ordinal,r.part,r.sub_key,l.object_key,o.expected_size,o.expected_sha256,
                 o.provider_version,o.etag,l.backend_generation,l.storage_namespace,l.storage_realm,o.content_type,l.object_id
                FROM document_revision_parts r
                JOIN repository_physical_locations l ON l.object_id=r.object_id AND l.source_kind='DOCUMENT_PART'
                JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id
                 AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal AND o.verified
                 AND o.part=r.part AND o.sub_key=r.sub_key AND o.object_key=l.object_key
                 AND o.storage_namespace=l.storage_namespace AND o.storage_realm=l.storage_realm
                JOIN document_part_attempts a ON a.attempt_id=o.attempt_id AND a.state='VERIFIED'
                 AND a.backend_generation=l.backend_generation AND a.storage_realm=l.storage_realm
                WHERE r.revision_id=:revision ORDER BY r.revision_ordinal
                """, Object[].class).setParameter("revision", revision)
                .setMaxResults(DocumentPublicationCommand.MAX_PARTS + 1).getResultList();
        long present = manifest.getPartsList().stream().filter(p -> p.getState() == PartState.PART_STATE_PRESENT).count();
        if (rows.size() != present || ((Number) h[11]).longValue() != present)
            throw invalid("Historical physical part set is incomplete");
        var profiles = ManagedBackendLedger.requireAll(em, rows.stream().map(r -> (String) r[8]).collect(Collectors.toSet()));
        var entries = new ArrayList<DocumentHistoricalReadPlan.Entry>();
        var verified = new ArrayList<DocumentPartPublication.VerifiedPart>();
        for (var row : rows) {
            int ordinal = ((Number) row[0]).intValue();
            if (ordinal < 0 || ordinal >= manifest.getPartsCount()) throw invalid("Historical part ordinal is invalid");
            var slot = manifest.getParts(ordinal);
            var part = new DocumentPublicationLedger.Part(DocumentPart.forNumber(((Number) row[1]).intValue()),
                    (String) row[2], (String) row[3], ((Number) row[4]).longValue(), (String) row[5],
                    (String) row[6], (String) row[7], (String) row[11]);
            if (slot.getState() != PartState.PART_STATE_PRESENT || slot.getPart() != part.part()
                    || !slot.getSubKey().equals(part.subKey()) || part.contentType() == null || part.contentType().isBlank())
                throw invalid("Historical part differs from manifest");
            var profile = profiles.get((String) row[8]);
            if (!profile.storageRealm().equals(row[10])) throw invalid("Historical backend realm differs from physical binding");
            entries.add(new DocumentHistoricalReadPlan.Entry(ordinal, (UUID) row[12],
                    new DocumentPublicationLedger.BoundPart(part,
                            new DocumentPublicationLedger.Binding((String) row[8], profile, (String) row[9]))));
            verified.add(new DocumentPartPublication.VerifiedPart(verified.size(), part.part(), part.subKey(),
                    part.key(), part.size(), part.sha256(), part.providerVersion(), part.etag()));
        }
        // Validate the historical body, never the current document's content or provider fields.
        var historical = new DocumentRecord();
        historical.accountId = address.getAccountId(); historical.docId = address.getDocId();
        historical.graphId = address.getGraphId(); historical.graphAddressId = address.getGraphAddressId();
        historical.status = DocumentStatus.AVAILABLE; historical.partManifest = encoded;
        historical.versionId = (String) h[6]; historical.etag = (String) h[7]; historical.checksum = (String) h[9];
        try {
            historical.sizeBytes = Long.parseLong((String) h[8]);
            DocumentPartPublication.validate(historical, verified, manifest);
        } catch (NumberFormatException | DocumentPartAttemptLedger.FenceException failure) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical body differs from physical bindings", failure);
        }
        return new DocumentHistoricalReadPlan(address, revision, ((Number) h[0]).longValue(), manifest, metadata, entries);
    }
    private static RepositoryException invalid(String message) {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, message);
    }
}
