package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.ConfluenceServiceGrpc;
import com.google.protobuf.DescriptorProtos;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ConfluenceProxyServer#startNetty} end to end: a real Netty socket,
 * the health service reporting SERVING once listening, and reflection
 * serving the facade's descriptor to descriptor-driven clients.
 */
class ConfluenceProxyServerTest {

    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void stopStack() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    private void startOnEphemeralPort() throws Exception {
        server = ConfluenceProxyServer.startNetty(
                new ConfluenceServiceGrpc.ConfluenceServiceImplBase() {
                }, 0);
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .build();
    }

    @Test
    void bindsTheEphemeralPortAndServesHealth() throws Exception {
        startOnEphemeralPort();

        assertThat(server.getPort()).isPositive();
        HealthCheckResponse response = HealthGrpc.newBlockingStub(channel)
                .check(HealthCheckRequest.getDefaultInstance());
        assertThat(response.getStatus())
                .isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
    }

    @Test
    void reflectionServesTheFacadeDescriptorOverTcp() throws Exception {
        startOnEphemeralPort();

        ServerReflectionGrpc.ServerReflectionStub reflection =
                ServerReflectionGrpc.newStub(channel);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<ServerReflectionResponse> response = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        StreamObserver<ServerReflectionRequest> requests = reflection.serverReflectionInfo(
                new StreamObserver<>() {
                    @Override
                    public void onNext(ServerReflectionResponse value) {
                        response.set(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        error.set(t);
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });
        requests.onNext(ServerReflectionRequest.newBuilder()
                .setFileContainingSymbol(
                        "ai.pipestream.proto.acquire.confluence.v1.ConfluenceService")
                .build());
        requests.onCompleted();

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isNull();
        assertThat(response.get().hasFileDescriptorResponse()).isTrue();
        List<String> files = new ArrayList<>();
        for (com.google.protobuf.ByteString bytes
                : response.get().getFileDescriptorResponse().getFileDescriptorProtoList()) {
            files.add(DescriptorProtos.FileDescriptorProto.parseFrom(bytes).getName());
        }
        assertThat(files).contains(
                "ai/pipestream/proto/acquire/confluence/v1/confluence_service.proto");
    }
}
