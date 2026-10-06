package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.PublishDocumentRequest;
import ai.protomolt.proto.repo.v1.PublishDocumentResponse;

/** Managed publication using the same contract for library and transport callers. */
public interface DocumentPublicationRepository {
    /** Complete uploads and mode choices are required on retries, including terminal receipt replay. */
    PublishDocumentResponse publishDocument(RepositoryCaller caller, PublishDocumentRequest request,
            RepositoryReadControl control);
}
