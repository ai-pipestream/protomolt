package ai.pipestream.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.pipestream.proto.repo.v1.GetDocumentResponse;
import ai.pipestream.proto.repo.v1.NodeAddress;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The fetcher's call-deadline contract over an in-process repo server: a
 * hung repo call surfaces DEADLINE_EXCEEDED inside the deadline instead of
 * parking the caller forever, the one-arg constructor keeps the default
 * deadline, and a missing or non-positive timeout is refused. The server
 * must not use directExecutor(), or deadline cancellation is never
 * delivered to a handler that blocks.
 */
class GrpcDocumentFetcherTest {

    static final Duration CALL_TIMEOUT = Duration.ofMillis(200);
    static final CountDownLatch hung = new CountDownLatch(1);
    static Server server;
    static String target;

    static final class FakeDocumentService extends DocumentServiceGrpc.DocumentServiceImplBase {
        @Override
        public void getDocumentByReference(GetDocumentByReferenceRequest request,
                StreamObserver<GetDocumentResponse> observer) {
            if ("doc-hung".equals(request.getAddress().getDocId())) {
                // Never answers until released: the deadline is the only
                // way out.
                try {
                    hung.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            observer.onNext(GetDocumentResponse.newBuilder()
                    .setDocument(Document.newBuilder()
                            .setDocId(request.getAddress().getDocId()))
                    .build());
            observer.onCompleted();
        }
    }

    @BeforeAll
    static void startServer() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(new FakeDocumentService())
                .build()
                .start();
        target = "inprocess:" + name;
    }

    @AfterAll
    static void stopServer() {
        hung.countDown();
        server.shutdownNow();
    }

    @Test
    void aHungRepoCallFailsWithDeadlineExceeded() {
        try (GrpcDocumentFetcher fetcher = new GrpcDocumentFetcher(target, CALL_TIMEOUT)) {
            NodeAddress address = NodeAddress.newBuilder().setDocId("doc-hung").build();
            assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                    assertThatThrownBy(() -> fetcher.fetch(address))
                            .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                                    assertThat(e.getStatus().getCode())
                                            .isEqualTo(Status.Code.DEADLINE_EXCEEDED)));
        }
    }

    @Test
    void theOneArgConstructorFetchesWithItsDefaultDeadline() {
        try (GrpcDocumentFetcher fetcher = new GrpcDocumentFetcher(target)) {
            Document document =
                    fetcher.fetch(NodeAddress.newBuilder().setDocId("doc-1").build());
            assertThat(document.getDocId()).isEqualTo("doc-1");
        }
    }

    @Test
    void aNonPositiveOrMissingTimeoutIsRefused() {
        assertThatThrownBy(() -> new GrpcDocumentFetcher(target, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rpcTimeout");
        assertThatThrownBy(() -> new GrpcDocumentFetcher(target, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rpcTimeout");
        assertThatThrownBy(() -> new GrpcDocumentFetcher(target, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rpcTimeout");
    }

    @Test
    void aBlankTargetIsRefused() {
        assertThatThrownBy(() -> new GrpcDocumentFetcher(" ", Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("target");
    }
}
