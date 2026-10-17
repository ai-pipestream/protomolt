package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.spi.BlobRepository;
import ai.protomolt.proto.repo.spi.DocumentRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryOperationControl;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobResponse;
import ai.protomolt.proto.repo.v1.DeleteBlobRequest;
import ai.protomolt.proto.repo.v1.DeleteBlobResponse;
import ai.protomolt.proto.repo.v1.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v1.DeleteDocumentResponse;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateResponse;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
import ai.protomolt.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.protomolt.proto.repo.v1.GetDocumentManifestRequest;
import ai.protomolt.proto.repo.v1.GetDocumentManifestResponse;
import ai.protomolt.proto.repo.v1.GetDocumentRequest;
import ai.protomolt.proto.repo.v1.GetDocumentResponse;
import ai.protomolt.proto.repo.v1.ListDocumentsRequest;
import ai.protomolt.proto.repo.v1.ListDocumentsResponse;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
import ai.protomolt.proto.repo.v1.SaveDocumentRequest;
import ai.protomolt.proto.repo.v1.SaveDocumentResponse;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v1.SemanticProcessingResult;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the v1 {@link DocumentGrpcService} refuses {@code SearchMetadata.segmentations}
 * and {@code SearchMetadata.embedding_sets} with {@code INVALID_ARGUMENT} before touching the
 * underlying repository (#328).
 */
class V1SemanticFieldsRefusalTest {

    @Test
    void v1SaveRefusesSegmentationsAndEmbeddingSetsWithoutWriting() throws Exception {
        AtomicInteger savesCalled = new AtomicInteger();
        DocumentRepository repository = new StubDocumentRepository(savesCalled);
        BlobRepository blobs = new StubBlobRepository();
        DocumentGrpcService service = new DocumentGrpcService(repository, blobs,
                caller -> new RepositoryCaller(caller.name(), caller.unrestricted()));

        ServerInterceptor callerInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                Context ctx = Context.current().withValue(
                        CallerContexts.CALLER, Caller.scoped("test-operator", Set.of()));
                return Contexts.interceptCall(ctx, call, headers, next);
            }
        };

        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(ServerInterceptors.intercept(service, callerInterceptor))
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        try {
            var stub = DocumentServiceGrpc.newBlockingStub(channel);

            SaveDocumentRequest withSegmentation = SaveDocumentRequest.newBuilder()
                    .setDocument(Document.newBuilder()
                            .setDocId("doc-seg")
                            .setSearchMetadata(SearchMetadata.newBuilder()
                                    .addSegmentations(Segmentation.newBuilder()
                                            .setSegmentationId("seg-1")
                                            .build())))
                    .build();

            assertThatThrownBy(() -> stub.saveDocument(withSegmentation))
                    .isInstanceOf(StatusRuntimeException.class)
                    .satisfies(error -> {
                        Status status = ((StatusRuntimeException) error).getStatus();
                        assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                        assertThat(status.getDescription()).contains("Segmentations and embedding sets");
                    });
            assertThat(savesCalled.get()).isZero();

            SaveDocumentRequest withEmbeddingSet = SaveDocumentRequest.newBuilder()
                    .setDocument(Document.newBuilder()
                            .setDocId("doc-emb")
                            .setSearchMetadata(SearchMetadata.newBuilder()
                                    .addEmbeddingSets(EmbeddingSet.newBuilder()
                                            .setSegmentationId("seg-1")
                                            .build())))
                    .build();

            assertThatThrownBy(() -> stub.saveDocument(withEmbeddingSet))
                    .isInstanceOf(StatusRuntimeException.class)
                    .satisfies(error -> {
                        Status status = ((StatusRuntimeException) error).getStatus();
                        assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                        assertThat(status.getDescription()).contains("Segmentations and embedding sets");
                    });
            assertThat(savesCalled.get()).isZero();

            SaveDocumentRequest validV1 = SaveDocumentRequest.newBuilder()
                    .setDocument(Document.newBuilder()
                            .setDocId("doc-ok")
                            .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Valid v1 doc")))
                    .build();

            SaveDocumentResponse response = stub.saveDocument(validV1);
            assertThat(response.getNodeId()).isEqualTo("saved-doc-ok");
            assertThat(savesCalled.get()).isEqualTo(1);

            SaveDocumentRequest legacySemanticResults = SaveDocumentRequest.newBuilder()
                    .setDocument(Document.newBuilder()
                            .setDocId("doc-legacy-sem")
                            .setSearchMetadata(SearchMetadata.newBuilder()
                                    .addSemanticResults(SemanticProcessingResult.newBuilder()
                                            .setResultId("res-1")
                                            .build())))
                    .build();

            SaveDocumentResponse legacyResponse = stub.saveDocument(legacySemanticResults);
            assertThat(legacyResponse.getNodeId()).isEqualTo("saved-doc-legacy-sem");
            assertThat(savesCalled.get()).isEqualTo(2);
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    private static final class StubDocumentRepository implements DocumentRepository {
        private final AtomicInteger savesCalled;

        private StubDocumentRepository(AtomicInteger savesCalled) {
            this.savesCalled = savesCalled;
        }

        @Override
        public SaveDocumentResponse saveDocument(
                RepositoryCaller caller, SaveDocumentRequest request, RepositoryOperationControl control) {
            savesCalled.incrementAndGet();
            return SaveDocumentResponse.newBuilder()
                    .setNodeId("saved-" + request.getDocument().getDocId())
                    .build();
        }

        @Override
        public GetDocumentResponse getDocument(
                RepositoryCaller caller, GetDocumentRequest request, RepositoryReadControl control) {
            throw new UnsupportedOperationException();
        }

        @Override
        public GetDocumentResponse getDocumentByReference(
                RepositoryCaller caller, GetDocumentByReferenceRequest request, RepositoryReadControl control) {
            throw new UnsupportedOperationException();
        }

        @Override
        public GetDocumentManifestResponse getDocumentManifest(
                RepositoryCaller caller, GetDocumentManifestRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DeleteDocumentResponse deleteDocument(RepositoryCaller caller, DeleteDocumentRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListDocumentsResponse listDocuments(RepositoryCaller caller, ListDocumentsRequest request) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class StubBlobRepository implements BlobRepository {
        @Override
        public GetBlobResponse get(RepositoryCaller caller, GetBlobRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PutBlobResponse put(RepositoryCaller caller, PutBlobRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DeleteBlobResponse delete(RepositoryCaller caller, DeleteBlobRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public GetBlobForUpdateResponse getForUpdate(RepositoryCaller caller, GetBlobForUpdateRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompareAndPutBlobResponse compareAndPut(RepositoryCaller caller, CompareAndPutBlobRequest request) {
            throw new UnsupportedOperationException();
        }
    }
}
