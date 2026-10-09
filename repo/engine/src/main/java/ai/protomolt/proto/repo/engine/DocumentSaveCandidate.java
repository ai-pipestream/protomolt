package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRowKind;
import ai.protomolt.proto.repo.container.ledger.DocumentStatus;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SaveDocumentRequest;
import java.time.Instant;
import java.util.UUID;

/** Pure candidate construction after authorization; no storage or transaction side effects. */
final class DocumentSaveCandidate {
    private DocumentSaveCandidate() {}

    static DocumentRecord build(RepositoryCaller caller, SaveResolution.Resolved r, SaveDocumentRequest request,
            DriveRecord drive, UUID nodeId, String basePrefix, DocumentManifest manifest, String rootChecksum,
            long totalSize, String coreEtag, String coreVersionId, DocumentRecord existing) {
        OwnershipContext ownership = r.doc().getOwnership();
        DocumentRecord row = new DocumentRecord();
        row.nodeId = nodeId;
        row.docId = r.address().getDocId();
        row.graphAddressId = r.address().getGraphAddressId();
        row.graphId = r.address().getGraphId();
        row.rowKind = r.rowKind();
        row.clusterId = r.clusterId();
        row.accountId = r.address().getAccountId();
        row.datasourceId = caller.processAuthority() ? ownership.getDatasourceId() : existing.datasourceId;
        row.connectorId = !request.getConnectorId().isBlank() ? request.getConnectorId()
                : (ownership.hasConnectorId() ? ownership.getConnectorId() : null);
        row.checksum = rootChecksum;
        row.driveName = drive.name;
        row.objectKey = basePrefix;
        row.versionId = coreVersionId;
        row.etag = coreEtag != null ? coreEtag : "";
        row.sizeBytes = totalSize;
        row.contentType = DocumentOperations.PART_CONTENT_TYPE;
        row.filename = r.doc().hasSearchMetadata() && r.doc().getSearchMetadata().hasTitle()
                ? r.doc().getSearchMetadata().getTitle() : r.address().getDocId();
        row.writeManifest(manifest);
        if (caller.processAuthority()) row.writeSecurity(ownership.hasSecurity() ? ownership.getSecurity() : null);
        else row.security = existing.security; // Current destination policy is never sourced from copied body provenance.
        boolean intake = DocumentRowKind.INTAKE.equals(r.rowKind());
        row.deleteSourceBlobsOnSettle = intake && request.getDeleteSourceBlobsOnSettle();
        row.sourceBlobDeleteReason = intake && !request.getSourceBlobDeleteReason().isBlank()
                ? request.getSourceBlobDeleteReason() : null;
        row.status = DocumentStatus.AVAILABLE;
        row.crawlId = request.hasCrawlId() && !request.getCrawlId().isBlank()
                ? request.getCrawlId() : null;
        Instant now = Instant.now();
        if (existing != null) {
            row.createdAt = existing.createdAt;
            row.reprocessCount = existing.reprocessCount;
            row.lastReprocessedAt = existing.lastReprocessedAt;
        } else {
            row.createdAt = now;
        }
        // Body rewrite: the staleness guard moves. Deliberately explicit —
        // nothing else bumps updated_at (see DocumentRecord's class Javadoc).
        row.updatedAt = now;
        return row;
    }
}
