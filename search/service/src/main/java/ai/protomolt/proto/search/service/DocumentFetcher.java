package ai.protomolt.proto.search.service;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;

/**
 * Fetches the stored document an index request addresses. The gRPC
 * implementation is {@link GrpcDocumentFetcher}; tests inject in-memory
 * fetchers, keeping the service's logic transport-free.
 */
@FunctionalInterface
public interface DocumentFetcher {

    /**
     * The stored document at {@code address}, with CORE and PARSED parts.
     *
     * @param address the stored document's address
     * @return the document
     * @throws io.grpc.StatusRuntimeException when the repository refuses the
     *         read; the service surfaces the repository's status
     */
    Document fetch(NodeAddress address);
}
