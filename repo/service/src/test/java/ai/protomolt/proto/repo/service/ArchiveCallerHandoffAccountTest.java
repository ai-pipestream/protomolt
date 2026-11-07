package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The archive-definition document hop applies the same account rule as the archive doors:
 * a scoped caller's blank account id never reaches the document service.
 */
class ArchiveCallerHandoffAccountTest {
    private static final String ACCOUNT = "acct";
    private static final RepositoryCaller SCOPED =
            new RepositoryCaller("alice", false, Set.of(ACCOUNT), Set.of());

    private final List<String> reached = new CopyOnWriteArrayList<>();
    private Server server;
    private ManagedChannel channel;
    private DocumentServiceGrpc.DocumentServiceBlockingStub documents;

    @BeforeEach
    void start() throws IOException {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new DocumentServiceGrpc.DocumentServiceImplBase() {
                    @Override
                    public void getDocument(GetDocumentRequest request,
                            StreamObserver<GetDocumentResponse> observer) {
                        reached.add(request.getAddress().getAccountId());
                        observer.onNext(GetDocumentResponse.getDefaultInstance());
                        observer.onCompleted();
                    }

                    @Override
                    public void listDocuments(ListDocumentsRequest request,
                            StreamObserver<ListDocumentsResponse> observer) {
                        reached.add(request.getAccountId());
                        observer.onNext(ListDocumentsResponse.getDefaultInstance());
                        observer.onCompleted();
                    }
                })
                .build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        documents = DocumentServiceGrpc.newBlockingStub(
                ArchiveCallerHandoff.interceptBindingsChannel(channel, new RepoCallerBindings()));
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void aBlankAccountIdIsRefusedBeforeTheDocumentService() {
        for (String blank : List.of("", "   ")) {
            assertRefused(() -> asScoped(() -> documents.getDocument(get(blank))), blank);
            assertRefused(() -> asScoped(() -> documents.listDocuments(list(blank))), blank);
        }
        assertThat(reached).isEmpty();
    }

    @Test
    void anotherAccountIsRefusedBeforeTheDocumentService() {
        assertRefused(() -> asScoped(() -> documents.getDocument(get("other"))), "other");
        assertRefused(() -> asScoped(() -> documents.listDocuments(list("other"))), "other");
        assertThat(reached).isEmpty();
    }

    @Test
    void theBoundAccountReachesTheDocumentService() throws IOException {
        asScoped(() -> documents.getDocument(get(ACCOUNT)));
        asScoped(() -> documents.listDocuments(list(ACCOUNT)));
        assertThat(reached).containsExactly(ACCOUNT, ACCOUNT);
    }

    private static GetDocumentRequest get(String account) {
        return GetDocumentRequest.newBuilder()
                .setAddress(NodeAddress.newBuilder().setAccountId(account)).build();
    }

    private static ListDocumentsRequest list(String account) {
        return ListDocumentsRequest.newBuilder().setAccountId(account).build();
    }

    private static void asScoped(Runnable read) throws IOException {
        ArchiveCallerHandoff.callWithCaller(SCOPED, null, () -> {
            read.run();
            return null;
        });
    }

    private static void assertRefused(ThrowingCallable read, String account) {
        assertThatThrownBy(read).isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
            assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(failure.getStatus().getDescription())
                    .isEqualTo("The caller is not bound to account '" + account + "'");
        });
    }
}
