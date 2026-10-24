package ai.protomolt.proto.grpc.invoke;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReflectionClientTest {

    @Test
    void rejectsUnboundedTimeoutBeforeOpeningAReflectionStream() {
        assertThatThrownBy(() -> ReflectionClient.discover(null,
                ReflectionClient.MAX_TIMEOUT_MS + 1))
                .isInstanceOf(ReflectionException.class)
                .hasMessageContaining("must be from 1 to");
    }

    @Test
    void stalledServerReflectionStreamTimesOutAndCancelsServerCall() throws Exception {
        CountDownLatch cancelledOnServer = new CountDownLatch(1);
        String serverName = InProcessServerBuilder.generateName();
        Server stalled = InProcessServerBuilder.forName(serverName)
                .addService(new ServerReflectionGrpc.ServerReflectionImplBase() {
                    @Override
                    public StreamObserver<ServerReflectionRequest> serverReflectionInfo(
                            StreamObserver<ServerReflectionResponse> responseObserver) {
                        Context.current().addListener(
                                ctx -> cancelledOnServer.countDown(),
                                Runnable::run);
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(ServerReflectionRequest value) {
                                // Intentionally stall and never respond.
                            }

                            @Override
                            public void onError(Throwable t) {
                                cancelledOnServer.countDown();
                            }

                            @Override
                            public void onCompleted() {
                            }
                        };
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
        try {
            assertThatThrownBy(() -> ReflectionClient.discover(channel, 50))
                    .isInstanceOf(ReflectionException.class)
                    .hasMessageContaining("Reflection timed out after 50ms");
            assertThat(cancelledOnServer.await(2, TimeUnit.SECONDS)).isTrue();
        } finally {
            channel.shutdownNow();
            stalled.shutdownNow();
        }
    }
}
