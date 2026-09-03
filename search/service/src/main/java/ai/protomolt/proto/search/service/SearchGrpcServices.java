package ai.protomolt.proto.search.service;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.search.v1.DeleteDocumentRequest;
import ai.protomolt.proto.search.v1.DeleteDocumentResponse;
import ai.protomolt.proto.search.v1.IndexDocumentRequest;
import ai.protomolt.proto.search.v1.IndexDocumentResponse;
import ai.protomolt.proto.search.v1.ListSubjectsRequest;
import ai.protomolt.proto.search.v1.ListSubjectsResponse;
import ai.protomolt.proto.search.v1.SearchIndexServiceGrpc;
import ai.protomolt.proto.search.v1.SearchRequest;
import ai.protomolt.proto.search.v1.SearchResponse;
import ai.protomolt.proto.search.v1.SearchServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The service's two gRPC services over one {@link LuceneSearchStore}.
 * Validation failures map onto {@code INVALID_ARGUMENT} (the request named
 * something outside the served surface) and {@code FAILED_PRECONDITION}
 * (the request needs a lane the subject does not serve); repository
 * statuses pass through on the indexing side.
 */
final class SearchGrpcServices {

    private static final Logger LOG = LoggerFactory.getLogger(SearchGrpcServices.class);

    private SearchGrpcServices() {
    }

    /** The indexing half. */
    static final class Index extends SearchIndexServiceGrpc.SearchIndexServiceImplBase {

        private final LuceneSearchStore store;
        private final DocumentFetcher fetcher;
        // The optional document gate: fetched documents validate against
        // their declared rules (typically over the mounted taxonomy catalog)
        // before anything indexes. Null keeps the historical behavior.
        private final ai.protomolt.proto.validate.ProtoValidator documentGate;
        // The optional screening mount, consulted per request because mounts
        // swap on the config lane. Null means screening was never configured;
        // a non-null supplier returning null means the mount is not live yet,
        // which refuses fail-closed.
        private final java.util.function.Supplier<ai.protomolt.proto.screening.Screener>
                screening;

        Index(LuceneSearchStore store, DocumentFetcher fetcher) {
            this(store, fetcher, null, null);
        }

        Index(LuceneSearchStore store, DocumentFetcher fetcher,
                ai.protomolt.proto.validate.ProtoValidator documentGate) {
            this(store, fetcher, documentGate, null);
        }

        Index(LuceneSearchStore store, DocumentFetcher fetcher,
                ai.protomolt.proto.validate.ProtoValidator documentGate,
                java.util.function.Supplier<ai.protomolt.proto.screening.Screener> screening) {
            this.store = store;
            this.fetcher = fetcher;
            this.documentGate = documentGate;
            this.screening = screening;
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
                if (documentGate != null) {
                    ai.protomolt.proto.validate.ValidationResult verdict =
                            documentGate.validate(document);
                    if (!verdict.valid()) {
                        // The request was fine; the STORED document fails its
                        // declared rules as mounted right now, so the refusal
                        // is a precondition, named violation by violation.
                        String reasons = verdict.violations().stream()
                                .map(violation -> violation.path() + ": "
                                        + violation.message()
                                        + " (" + violation.ruleId() + ")")
                                .collect(java.util.stream.Collectors.joining("; "));
                        observer.onError(Status.FAILED_PRECONDITION.withDescription(
                                "stored document violates its declared rules: " + reasons)
                                .asRuntimeException());
                        return;
                    }
                }
                IndexDocumentResponse.Builder response = IndexDocumentResponse.newBuilder();
                if (screening != null) {
                    ai.protomolt.proto.screening.Screener screener = screening.get();
                    if (screener == null) {
                        // Configured but no mount live yet: fail-closed, the
                        // taxonomy gate's boot stance.
                        observer.onError(Status.FAILED_PRECONDITION.withDescription(
                                "screening is configured but no mount is live;"
                                        + " refusing fail-closed").asRuntimeException());
                        return;
                    }
                    ai.protomolt.proto.screening.Screener.Verdict verdict;
                    try {
                        verdict = screener.screen(document);
                    } catch (ai.protomolt.proto.screening.Screener
                            .ScreeningRefusedException e) {
                        observer.onError(Status.FAILED_PRECONDITION
                                .withDescription(e.getMessage()).asRuntimeException());
                        return;
                    }
                    document = (Document) verdict.message();
                    for (ai.protomolt.proto.screening.Screener.Finding finding
                            : verdict.findings()) {
                        response.addScreenedFields(finding.path());
                        response.setScreeningModelVersion(finding.modelVersion());
                        response.setScreeningThreshold(finding.threshold());
                    }
                }
                LuceneSearchStore.IndexResult result =
                        store.index(request.getMappingSubject(), document);
                observer.onNext(response
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
