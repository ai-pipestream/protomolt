package ai.protomolt.proto.grpc.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.CallOptions;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.InsecureServerCredentials;
import io.grpc.KnownLength;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Message size limits on policy channels, over a real Netty transport so gRPC serializes and
 * measures every message. The server answers with as many bytes as the request's first four
 * bytes ask for and counts the requests it receives.
 */
@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class OutboundMessageLimitTest {

    private static final MethodDescriptor<byte[], byte[]> ECHO = MethodDescriptor
            .<byte[], byte[]>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("test.Sized/Echo")
            .setRequestMarshaller(new Bytes())
            .setResponseMarshaller(new Bytes())
            .build();

    private static final class SizedStream extends ByteArrayInputStream implements KnownLength {
        SizedStream(byte[] bytes) {
            super(bytes);
        }
    }

    private final AtomicInteger received = new AtomicInteger();
    private Server server;
    private String target;

    @BeforeEach
    void start() throws IOException {
        server = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                .maxInboundMessageSize(64 * 1024 * 1024)
                .addService(ServerServiceDefinition.builder("test.Sized")
                        .addMethod(ECHO, ServerCalls.asyncUnaryCall((request, observer) -> {
                            received.incrementAndGet();
                            observer.onNext(new byte[ByteBuffer.wrap(request).getInt()]);
                            observer.onCompleted();
                        }))
                        .build())
                .build()
                .start();
        target = "127.0.0.1:" + server.getPort();
    }

    @AfterEach
    void stop() throws InterruptedException {
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void anOversizedRequestFailsBeforeTheServerSeesIt() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.builder()
                .maxOutboundMessageBytes(1024).build();
        ManagedChannel channel = open(policy);
        try {
            assertThat(call(channel, CallOptions.DEFAULT, 1024, 8)).hasSize(8);
            assertNotSent(() -> call(channel, CallOptions.DEFAULT, 1025, 8));
            assertThat(received).hasValue(1);
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void anOversizedResponseFailsTheCall() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.builder()
                .maxInboundMessageBytes(1024).build();
        ManagedChannel channel = open(policy);
        try {
            assertThat(call(channel, CallOptions.DEFAULT, 8, 1024)).hasSize(1024);
            assertExhausted(() -> call(channel, CallOptions.DEFAULT, 8, 1025));
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void aCallerMayLowerTheLimitsButNotRaiseThem() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.builder()
                .maxInboundMessageBytes(1024).maxOutboundMessageBytes(1024).build();
        ManagedChannel channel = open(policy);
        try {
            CallOptions wider = CallOptions.DEFAULT
                    .withMaxInboundMessageSize(Integer.MAX_VALUE)
                    .withMaxOutboundMessageSize(Integer.MAX_VALUE);
            assertExhausted(() -> call(channel, wider, 8, 2048));
            assertNotSent(() -> call(channel, wider, 2048, 8));

            CallOptions narrower = CallOptions.DEFAULT
                    .withMaxInboundMessageSize(256)
                    .withMaxOutboundMessageSize(256);
            assertExhausted(() -> call(channel, narrower, 8, 512));
            assertNotSent(() -> call(channel, narrower, 512, 8));
            assertThat(call(channel, narrower, 256, 256)).hasSize(256);
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void theDefaultIsTheGrpcFourMebibyteLimitInBothDirections() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.defaults();
        assertThat(policy.maxInboundMessageBytes()).isEqualTo(4 * 1024 * 1024);
        assertThat(policy.maxOutboundMessageBytes()).isEqualTo(4 * 1024 * 1024);
        ManagedChannel channel = open(policy);
        try {
            assertNotSent(() -> call(channel, CallOptions.DEFAULT, 4 * 1024 * 1024 + 1, 8));
            assertThat(received).hasValue(0);
            assertExhausted(() -> call(channel, CallOptions.DEFAULT, 8, 4 * 1024 * 1024 + 1));
        } finally {
            channel.shutdownNow();
        }
        assertThatThrownBy(() -> OutboundChannelPolicy.builder().maxInboundMessageBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OutboundChannelPolicy.builder().maxOutboundMessageBytes(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private ManagedChannel open(OutboundChannelPolicy policy) {
        return policy.open(target, false, canonical -> Grpc.newChannelBuilder(
                        canonical, InsecureChannelCredentials.create())
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build());
    }

    /** Sends {@code requestBytes} bytes asking for a {@code responseBytes}-byte answer. */
    private static byte[] call(ManagedChannel channel, CallOptions options, int requestBytes,
                               int responseBytes) {
        byte[] request = new byte[requestBytes];
        ByteBuffer.wrap(request).putInt(responseBytes);
        return ClientCalls.blockingUnaryCall(channel, ECHO,
                options.withDeadlineAfter(10, TimeUnit.SECONDS), request);
    }

    /** gRPC fails an oversized response with RESOURCE_EXHAUSTED. */
    private static void assertExhausted(Runnable call) {
        assertThatThrownBy(call::run)
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                        assertThat(failure.getStatus().getCode())
                                .isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
    }

    /** The policy refuses an oversized request before framing it, naming both sizes. */
    private void assertNotSent(Runnable call) {
        int before = received.get();
        assertThatThrownBy(call::run)
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                    assertThat(failure.getStatus().getCode())
                            .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                    assertThat(failure.getStatus().getDescription())
                            .contains("exceeds the outbound limit of");
                });
        assertThat(received).hasValue(before);
    }

    /** Known-length streams, as the protobuf marshaller produces, so gRPC sizes them up front. */
    private static final class Bytes implements MethodDescriptor.Marshaller<byte[]> {
        @Override
        public InputStream stream(byte[] value) {
            return new SizedStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
            try {
                return stream.readAllBytes();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
