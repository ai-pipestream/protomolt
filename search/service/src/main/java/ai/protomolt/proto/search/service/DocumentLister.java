package ai.protomolt.proto.search.service;

import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;

/**
 * Lists stored documents for replay. The gRPC implementation is
 * {@link GrpcDocumentFetcher}; tests inject in-memory listers.
 */
@FunctionalInterface
public interface DocumentLister {

    /**
     * One page of the listing.
     *
     * @param request the listing request (account, scope, page token)
     * @return the page
     * @throws io.grpc.StatusRuntimeException when the repository refuses the
     *         listing
     */
    ListDocumentsResponse list(ListDocumentsRequest request);
}
