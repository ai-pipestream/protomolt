package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.ledger.DocumentRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentRowKind;
import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SaveDocumentResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static ai.pipestream.proto.repo.service.GrpcErrors.invalidArgument;

/**
 * Where a save lands, decided from the request alone. Nothing here touches the ledger,
 * object storage or the clock beyond minting a document id, which is what makes the
 * storage-identity rules readable on their own and testable without a database.
 *
 * <p>The rules themselves are the point. A document's address is four segments, and none
 * of them is inferred: the {@code graph_address} oneof arm states the origin outright, and
 * every rejection names the field that is wrong. A blank identity is never quietly filled
 * in with a default, because a wrong address stores a document where nobody will look for
 * it and no error is raised at the time.
 */
final class SaveResolution {

    private SaveResolution() {
    }

    /**
     * Everything validation and defaulting settled about a save: the document (always
     * carrying a doc id) plus the canonical {@link NodeAddress} of the storage identity,
     * the row kind and the cluster hint. The address's {@code graph_id} is never blank on
     * either kind, so blank-graph rows are unrepresentable.
     */
    record Resolved(Document doc, NodeAddress address, String rowKind, String clusterId) {
    }

    /**
     * Validation + defaulting half of a save: ownership/account required, a blank doc id
     * mints a fresh UUID (the caller is intake and will persist the returned coordinates),
     * and the {@code graph_address} oneof arm is the EXPLICIT origin discriminator with
     * {@code graph_id} required on both arms.
     *
     * @throws io.grpc.StatusRuntimeException INVALID_ARGUMENT naming the offending field
     */
    static Resolved resolve(SaveDocumentRequest request) {
        if (!request.hasDocument()) {
            throw invalidArgument("document is required");
        }
        Document doc = request.getDocument();
        if (doc.getDocId().isBlank()) {
            doc = doc.toBuilder().setDocId(UUID.randomUUID().toString()).build();
        }
        if (!doc.hasOwnership()) {
            throw invalidArgument("document.ownership is required");
        }
        OwnershipContext ownership = doc.getOwnership();
        String accountId = ownership.getAccountId();
        if (accountId.isBlank()) {
            throw invalidArgument("document.ownership.account_id is required");
        }
        if (request.getDrive().isBlank()) {
            throw invalidArgument("drive is required");
        }
        String requestGraphId = request.hasGraphId() ? request.getGraphId() : "";

        return switch (request.getGraphAddressCase()) {
            case USE_DATASOURCE_ID -> intake(request, doc, ownership, accountId, requestGraphId);
            case GRAPH_LOCATION_ID -> pipeline(request, doc, accountId, requestGraphId);
            default -> throw invalidArgument(
                    "exactly one graph address arm (use_datasource_id or graph_location_id) must be set");
        };
    }

    /**
     * An intake save. The address is the document's {@code ownership.datasource_id}, the
     * graph is the account's own intake graph, and a cluster id is a category error: the
     * intake layer is its own single-node graph.
     */
    private static Resolved intake(SaveDocumentRequest request, Document doc,
            OwnershipContext ownership, String accountId, String requestGraphId) {
        if (request.hasClusterId() && !request.getClusterId().isBlank()) {
            throw invalidArgument("cluster_id must be absent on intake saves"
                    + " (use_datasource_id); the intake layer is its own single-node graph");
        }
        String expected = intakeGraphId(accountId);
        if (requestGraphId.isBlank()) {
            throw invalidArgument("graph_id is required on intake saves"
                    + " (use_datasource_id): expected \"" + expected + "\"");
        }
        if (!expected.equals(requestGraphId)) {
            throw invalidArgument("graph_id must equal the account's intake graph \""
                    + expected + "\" on intake saves (got \"" + requestGraphId + "\")");
        }
        String datasourceId = ownership.getDatasourceId();
        if (datasourceId.isBlank()) {
            throw invalidArgument("document.ownership.datasource_id is required on intake saves"
                    + " (use_datasource_id) - it is the storage address");
        }
        return new Resolved(doc, address(doc, datasourceId, accountId, requestGraphId),
                DocumentRowKind.INTAKE, null);
    }

    /** A pipeline save at the named graph node; the graph is the owning graph and required. */
    private static Resolved pipeline(SaveDocumentRequest request, Document doc,
            String accountId, String requestGraphId) {
        String graphAddressId = request.getGraphLocationId();
        if (graphAddressId.isBlank()) {
            throw invalidArgument("graph_location_id must not be blank");
        }
        if (requestGraphId.isBlank()) {
            throw invalidArgument("graph_id is required on pipeline saves"
                    + " (graph_location_id=\"" + graphAddressId + "\")");
        }
        if (requestGraphId.startsWith(INTAKE_GRAPH_PREFIX)) {
            throw invalidArgument("graph_id must not be an intake graph id on pipeline saves"
                    + " (got \"" + requestGraphId + "\")");
        }
        String clusterId = request.hasClusterId() && !request.getClusterId().isBlank()
                ? request.getClusterId() : null;
        return new Resolved(doc, address(doc, graphAddressId, accountId, requestGraphId),
                DocumentRowKind.PIPELINE, clusterId);
    }

    /** The prefix marking a graph id as the intake layer's rather than a pipeline's. */
    static final String INTAKE_GRAPH_PREFIX = "intake:";

    /** The one intake graph an account has. */
    static String intakeGraphId(String accountId) {
        return INTAKE_GRAPH_PREFIX + accountId;
    }

    private static NodeAddress address(Document doc, String graphAddressId, String accountId,
            String graphId) {
        return NodeAddress.newBuilder()
                .setDocId(doc.getDocId())
                .setGraphAddressId(graphAddressId)
                .setAccountId(accountId)
                .setGraphId(graphId)
                .build();
    }

    /** The row's canonical storage address, rebuilt from its identity columns. */
    static NodeAddress addressOf(DocumentRecord row) {
        return NodeAddress.newBuilder()
                .setDocId(row.docId)
                .setGraphAddressId(row.graphAddressId)
                .setAccountId(row.accountId)
                .setGraphId(row.graphId)
                .build();
    }

    static SaveDocumentResponse saveResponse(DocumentRecord row, String rootChecksum) {
        return SaveDocumentResponse.newBuilder()
                .setNodeId(row.nodeId.toString())
                .setDrive(row.driveName)
                .setStoragePrefix(row.objectKey)
                .setSizeBytes(row.sizeBytes)
                .setChecksum(rootChecksum)
                .setCreatedAtEpochMs(row.createdAt.toEpochMilli())
                .setDeduplicated(false)
                .setAddress(addressOf(row))
                .build();
    }

    /** Part-object key root: {@code <drive.prefix>/documents/<accountId>/<nodeId>}. */
    static String basePrefix(DriveRecord drive, String accountId, UUID nodeId) {
        return DriveKeys.under(drive, "documents/" + accountId + "/" + nodeId);
    }

    /** Provider metadata stamped on every part object for observability. */
    static Map<String, String> s3Metadata(Resolved r) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("doc-id", r.address().getDocId());
        metadata.put("account-id", r.address().getAccountId());
        metadata.put("graph-id", r.address().getGraphId());
        metadata.put("row-kind", r.rowKind());
        metadata.put("graph-address-id", r.address().getGraphAddressId());
        return metadata;
    }

    /** The row's current manifest doc_version, or 0 when absent or unparseable. */
    static long manifestVersion(DocumentRecord row) {
        if (row == null) {
            return 0;
        }
        try {
            DocumentManifest manifest = row.readManifest();
            return manifest == null ? 0 : manifest.getDocVersion();
        } catch (RuntimeException e) {
            return 0;
        }
    }
}
