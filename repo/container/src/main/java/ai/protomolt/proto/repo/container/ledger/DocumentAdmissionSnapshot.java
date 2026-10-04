package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import jakarta.persistence.FlushModeType;
import java.util.Objects;

/** Freezes a candidate through the persisted snapshot codecs, without inserting a document. */
record DocumentAdmissionSnapshot(String body, String metadata) {
    DocumentAdmissionSnapshot {
        Objects.requireNonNull(body);
        Objects.requireNonNull(metadata);
    }

    static DocumentAdmissionSnapshot prepare(EntityManager em, DocumentRecord row) {
        Objects.requireNonNull(em); Objects.requireNonNull(row);
        // Explicit casts give null parameters stable SQL types and preserve BIGINT
        // precision. The composite applies the same column types as a documents row.
        // No table read needs pending entity writes; never auto-flush before admission.
        Object[] snapshot = (Object[]) em.createNativeQuery("""
                WITH snapshots AS MATERIALIZED (
                SELECT document_publication_body(d) AS body,document_revision_metadata_v1(d) AS metadata
                FROM jsonb_populate_record(NULL::documents,jsonb_build_object(
                  'node_id',CAST(:node AS uuid),'account_id',CAST(:account AS text),
                  'doc_id',CAST(:doc AS text),'graph_id',CAST(:graph AS text),'graph_address_id',CAST(:address AS text),
                  'drive_name',CAST(:drive AS text),'object_key',CAST(:object AS text),'version_id',CAST(:version AS text),
                  'etag',CAST(:etag AS text),'size_bytes',CAST(:size AS bigint),'checksum',CAST(:checksum AS text),
                  'part_manifest',CAST(:manifest AS jsonb),'row_kind',CAST(:kind AS text),'cluster_id',CAST(:cluster AS text),
                  'datasource_id',CAST(:datasource AS text),'connector_id',CAST(:connector AS text),
                  'content_type',CAST(:content AS text),'filename',CAST(:filename AS text),'security',CAST(:security AS jsonb),
                  'delete_source_blobs_on_settle',CAST(:deleteSource AS boolean),'source_blob_delete_reason',CAST(:reason AS text),
                  'crawl_id',CAST(:crawl AS text),'created_at',CAST(:created AS timestamptz),'updated_at',CAST(:updated AS timestamptz)
                )) d)
                SELECT CASE WHEN octet_length(body::text)<=16777216 THEN body::text END,
                       CASE WHEN octet_length(metadata::text)<=1048576 THEN metadata::text END FROM snapshots
                """).setFlushMode(FlushModeType.COMMIT).setParameter("node", row.nodeId).setParameter("account", row.accountId)
                .setParameter("doc", row.docId).setParameter("graph", row.graphId).setParameter("address", row.graphAddressId)
                .setParameter("drive", row.driveName).setParameter("object", row.objectKey).setParameter("version", row.versionId)
                .setParameter("etag", row.etag).setParameter("size", row.sizeBytes).setParameter("checksum", row.checksum)
                .setParameter("manifest", row.partManifest).setParameter("kind", row.rowKind).setParameter("cluster", row.clusterId)
                .setParameter("datasource", row.datasourceId).setParameter("connector", row.connectorId)
                .setParameter("content", row.contentType).setParameter("filename", row.filename).setParameter("security", row.security)
                .setParameter("deleteSource", row.deleteSourceBlobsOnSettle).setParameter("reason", row.sourceBlobDeleteReason)
                .setParameter("crawl", row.crawlId).setParameter("created", Objects.requireNonNull(row.createdAt, "createdAt"))
                .setParameter("updated", Objects.requireNonNull(row.updatedAt, "updatedAt")).getSingleResult();
        if (snapshot[0] == null || snapshot[1] == null)
            throw new IllegalArgumentException("Document admission snapshot exceeds body or metadata byte bound");
        return new DocumentAdmissionSnapshot((String) snapshot[0], (String) snapshot[1]);
    }
}
