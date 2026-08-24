package ai.pipestream.proto.acquire.pull;

import ai.pipestream.proto.intake.v1.IngestDocumentRequest;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import ai.pipestream.proto.intake.v1.IntakeServiceGrpc;
import ai.pipestream.proto.repo.v1.Document;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Which transport a {@code host:port} intake target gets. The connector's API key rides
 * every call, so the default has to be TLS: a plaintext channel would put that credential,
 * and the document bodies it carries, in the clear for anything on the path. Plaintext
 * stays reachable for a trusted network or a TLS-terminating sidecar, as an explicit
 * choice.
 */
class GrpcIntakeFeedTransportTest {

    private static final String API_KEY = "connector-key";

    /** Answers anything, so a call that arrives proves the transport agreed. */
    private static final class AcceptingIntake extends IntakeServiceGrpc.IntakeServiceImplBase {
        @Override
        public void ingestDocument(IngestDocumentRequest request,
                                   StreamObserver<IngestDocumentResponse> observer) {
            observer.onNext(IngestDocumentResponse.getDefaultInstance());
            observer.onCompleted();
        }
    }

    private Server plaintextServer;
    private String target;

    @BeforeEach
    void startPlaintextIntake() throws IOException {
        plaintextServer = NettyServerBuilder.forPort(0)
                .addService(new AcceptingIntake())
                .build()
                .start();
        target = "127.0.0.1:" + plaintextServer.getPort();
    }

    @AfterEach
    void stopPlaintextIntake() {
        plaintextServer.shutdownNow();
    }

    private static void submit(GrpcIntakeFeed feed) {
        feed.submit(Document.newBuilder().setDocId("doc-1").build(),
                "datasource-1", "intake", Map.of());
    }

    @Test
    void aHostPortTargetUsesTlsByDefault() {
        // The server speaks plaintext, so a TLS client cannot complete a call against it.
        // Reaching it anyway would mean the channel had quietly downgraded.
        try (GrpcIntakeFeed feed = new GrpcIntakeFeed(target, API_KEY)) {
            assertThatThrownBy(() -> submit(feed))
                    .isInstanceOf(StatusRuntimeException.class);
        }
    }

    @Test
    void plaintextIsAvailableAsAnExplicitChoice() {
        try (GrpcIntakeFeed feed = new GrpcIntakeFeed(target, API_KEY, true)) {
            assertThat(feed.submit(Document.newBuilder().setDocId("doc-1").build(),
                    "datasource-1", "intake", Map.of())).isNotNull();
        }
    }

    @Test
    void theOptOutIsReadFromTheEnvironmentAndDefaultsToTls() {
        assertThat(GrpcIntakeFeed.plaintextRequested(Map.of())).isFalse();
        assertThat(GrpcIntakeFeed.plaintextRequested(null)).isFalse();
        assertThat(GrpcIntakeFeed.plaintextRequested(
                Map.of(GrpcIntakeFeed.ENV_PLAINTEXT, "true"))).isTrue();
        // Anything that is not "true" keeps the encrypted default rather than guessing.
        assertThat(GrpcIntakeFeed.plaintextRequested(
                Map.of(GrpcIntakeFeed.ENV_PLAINTEXT, "yes"))).isFalse();
    }
}
