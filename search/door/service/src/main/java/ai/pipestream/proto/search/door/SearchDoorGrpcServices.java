package ai.pipestream.proto.search.door;

import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.search.v1.DeleteDocumentRequest;
import ai.pipestream.proto.search.v1.DeleteDocumentResponse;
import ai.pipestream.proto.search.v1.IndexDocumentRequest;
import ai.pipestream.proto.search.v1.IndexDocumentResponse;
import ai.pipestream.proto.search.v1.ListSubjectsRequest;
import ai.pipestream.proto.search.v1.ListSubjectsResponse;
import ai.pipestream.proto.search.v1.SearchIndexServiceGrpc;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.SearchResponse;
import ai.pipestream.proto.search.v1.SearchServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The door's two gRPC services over one {@link LuceneSearchStore}.
 * Validation failures map onto {@code INVALID_ARGUMENT} (the request named
 * something outside the served surface) and {@code FAILED_PRECONDITION}
 * (the request needs a lane the subject does not serve); repository
 * statuses pass through on the indexing side.
 */
final class SearchDoorGrpcServices {

    private static final Logger LOG = LoggerFactory.getLogger(SearchDoorGrpcServices.class);

    private SearchDoorGrpcServices() {
    }

    /** The indexing half. */
    static final class Index extends SearchIndexServiceGrpc.SearchIndexServiceImplBase {

        private final LuceneSearchStore store;
        private final DocumentFetcher fetcher;

        Index(LuceneSearchStore store, DocumentFetcher fetcher) {
            this.store = store;
            this.fetcher = fetcher;
        }

        @Override
        public void indexDocument(IndexDocumentRequest request,
                StreamObserver<IndexDocumentResponse> observer) {
            try {
                if (request.getMappingSubject().isBlank()) {
                    throw new IllegalArgumentException("mapping_subject is required");
                }
                if (!request.hasAddress()) {
                    throw new IllegalArgumentException("address is required");
                }
                Document document = fetcher.fetch(request.getAddress());
                LuceneSearchStore.IndexResult result =
                        store.index(request.getMappingSubject(), document);
                observer.onNext(IndexDocumentResponse.newBuilder()
                        .setDocId(result.docId())
                        .setChunksIndexed(result.chunksIndexed())
                        .setPolicyDigest(result.policyDigest())
                        .build());
                observer.onCompleted();
            } catch (StatusRuntimeException e) {
                // The repository refused the read; its status is the answer.
                observer.onError(e);
            } catch (IllegalArgumentException e) {
                observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage())
                        .asRuntimeException());
            } catch (RuntimeException e) {
                LOG.error("index failed", e);
                observer.onError(Status.INTERNAL.withDescription(e.getMessage())
                        .asRuntimeException());
            }
        }

        @Override
        public void deleteDocument(DeleteDocumentRequest request,
                StreamObserver<DeleteDocumentResponse> observer) {
            try {
                if (request.getMappingSubject().isBlank()) {
                    throw new IllegalArgumentException("mapping_subject is required");
                }
                int chunksDeleted =
                        store.delete(request.getMappingSubject(), request.getDocId());
                observer.onNext(DeleteDocumentResponse.newBuilder()
                        .setDocId(request.getDocId())
                        .setChunksDeleted(chunksDeleted)
                        .build());
                observer.onCompleted();
            } catch (IllegalArgumentException e) {
                observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage())
                        .asRuntimeException());
            } catch (RuntimeException e) {
                LOG.error("delete failed", e);
                observer.onError(Status.INTERNAL.withDescription(e.getMessage())
                        .asRuntimeException());
            }
        }
    }

    /** The query half. */
    static final class Search extends SearchServiceGrpc.SearchServiceImplBase {

        private final LuceneSearchStore store;

        Search(LuceneSearchStore store) {
            this.store = store;
        }

        @Override
        public void search(SearchRequest request, StreamObserver<SearchResponse> observer) {
            try {
                if (request.getMappingSubject().isBlank()) {
                    throw new IllegalArgumentException("mapping_subject is required");
                }
                observer.onNext(SearchResponse.newBuilder()
                        .addAllHits(store.search(request.getMappingSubject(), request))
                        .build());
                observer.onCompleted();
            } catch (IllegalArgumentException e) {
                observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage())
                        .asRuntimeException());
            } catch (IllegalStateException e) {
                observer.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage())
                        .asRuntimeException());
            } catch (RuntimeException e) {
                LOG.error("search failed", e);
                observer.onError(Status.INTERNAL.withDescription(e.getMessage())
                        .asRuntimeException());
            }
        }

        @Override
        public void listSubjects(ListSubjectsRequest request,
                StreamObserver<ListSubjectsResponse> observer) {
            observer.onNext(ListSubjectsResponse.newBuilder()
                    .addAllSubjects(store.describeSubjects())
                    .build());
            observer.onCompleted();
        }
    }
}
