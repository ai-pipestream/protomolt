package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.PublishDocumentRequest;
import ai.protomolt.proto.repo.v1.PublishDocumentResponse;

/**
 * Internal execution boundary retained for admission, receipt and recovery
 * integration. Application document writes use the parts-based DocumentService;
 * this interface does not define an alternative public document transport.
 */
public interface DocumentPublicationRepository {
    /** Complete uploads and mode choices are required on retries, including terminal receipt replay. */
    PublishDocumentResponse publishDocument(RepositoryCaller caller, PublishDocumentRequest request,
            RepositoryReadControl control);
}
