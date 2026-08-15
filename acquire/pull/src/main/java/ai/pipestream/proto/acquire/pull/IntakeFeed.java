package ai.pipestream.proto.acquire.pull;

import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import ai.pipestream.proto.repo.v1.Document;
import java.util.Map;

/**
 * The submission seam between a pull connector and the intake door: one assembled document in,
 * the intake receipt out. The production implementation is {@link GrpcIntakeFeed}; tests inject
 * fakes to exercise pull logic without a transport.
 */
public interface IntakeFeed extends AutoCloseable {

    /**
     * Submits one document through the intake door.
     *
     * @param document the assembled document (see {@link PullDocuments#document})
     * @param datasourceId the datasource the document belongs to
     * @param drive the target drive, or blank for intake's default
     * @param metadata source-provenance metadata recorded with the save
     * @return the intake receipt
     */
    IngestDocumentResponse submit(Document document, String datasourceId, String drive,
                                  Map<String, String> metadata);

    @Override
    default void close() {
    }
}
