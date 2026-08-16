package ai.pipestream.proto.search.door;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.junit.jupiter.api.Test;

/**
 * The fetcher's deadline contract: every repo call carries one, so a hung
 * repository fails the calling step instead of parking a worker forever.
 */
class GrpcDocumentFetcherTest {

    @Test
    void refusalsNameTheProblem() {
        assertThatThrownBy(() -> new GrpcDocumentFetcher(" ", Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("target");
        assertThatThrownBy(() -> new GrpcDocumentFetcher("inprocess:x", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rpcTimeout");
        assertThatThrownBy(() -> new GrpcDocumentFetcher("inprocess:x", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rpcTimeout");
    }

    @Test
    void aHungRepositoryTripsTheDeadlineInsteadOfParkingForever() throws Exception {
        String name = InProcessServerBuilder.generateName();
        CountDownLatch release = new CountDownLatch(1);
        Server server = InProcessServerBuilder.forName(name)
                .addService(new DocumentServiceGrpc.DocumentServiceImplBase() {
                    @Override
                    public void getDocumentByReference(GetDocumentByReferenceRequest request,
                            StreamObserver<GetDocumentResponse> observer) {
                        // Never answers until released: the deadline is the
                        // only way out.
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                })
                .build()
                .start();
        try (GrpcDocumentFetcher fetcher =
                new GrpcDocumentFetcher("inprocess:" + name, Duration.ofMillis(200))) {
            assertThatThrownBy(() -> fetcher.fetch(NodeAddress.getDefaultInstance()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                            assertThat(e.getStatus().getCode())
                                    .isEqualTo(Status.Code.DEADLINE_EXCEEDED));
        } finally {
            release.countDown();
            server.shutdownNow();
        }
    }
}
