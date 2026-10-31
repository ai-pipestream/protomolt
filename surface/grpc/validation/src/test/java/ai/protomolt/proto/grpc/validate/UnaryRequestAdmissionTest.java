package ai.protomolt.proto.grpc.validate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies {@link UnaryRequestAdmission} over a real Netty transport, including
 * pre-half-close held-frame observation and header-time reservation under
 * {@code grpc-java 1.85.1} delayed unary decoding.
 */
class UnaryRequestAdmissionTest {

    private static final String ECHO = "test.Ingress/Echo";
    private static final String STREAM = "test.Ingress/Stream";
    private static final String OPEN_ECHO = "test.Plumbing/Ping";
    private static final BytesValue VALUE =
            BytesValue.newBuilder().setValue(ByteString.copyFromUtf8("content")).build();

    private static final class Fixture implements AutoCloseable {
        final AtomicInteger parses = new AtomicInteger();
        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger streams = new AtomicInteger();
        final AtomicLong inboundUncompressedBytes = new AtomicLong();
        final CountDownLatch received = new CountDownLatch(1);
        final AtomicReference<Status> transportStatus = new AtomicReference<>();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final UnaryRequestAdmission admission;
        final MethodDescriptor<BytesValue, BytesValue> method;
        final MethodDescriptor<BytesValue, BytesValue> stream;
        final MethodDescriptor<BytesValue, BytesValue> openMethod;
        final Server server;
        final ManagedChannel channel;

        Fixture(boolean block) throws Exception {
            this(block, 0);
        }

        Fixture(boolean block, int responseLimit) throws Exception {
            admission = new UnaryRequestAdmission(
                    2048L + responseLimit,
                    1024,
                    1,
                    Set.of(ECHO),
                    responseLimit == 0 ? Map.of() : Map.of(ECHO, responseLimit),
                    Set.of("test.Plumbing"));
            var proto = ProtoUtils.marshaller(BytesValue.getDefaultInstance());
            var observed = new MethodDescriptor.Marshaller<BytesValue>() {
                @Override
                public InputStream stream(BytesValue value) {
                    return proto.stream(value);
                }

                @Override
                public BytesValue parse(InputStream input) {
                    parses.incrementAndGet();
                    return proto.parse(input);
                }
            };
            method = MethodDescriptor.<BytesValue, BytesValue>newBuilder()
                    .setFullMethodName(ECHO)
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setRequestMarshaller(observed)
                    .setResponseMarshaller(proto)
                    .build();
            stream = method.toBuilder()
                    .setFullMethodName(STREAM)
                    .setType(MethodDescriptor.MethodType.CLIENT_STREAMING)
                    .build();
            openMethod = MethodDescriptor.<BytesValue, BytesValue>newBuilder()
                    .setFullMethodName(OPEN_ECHO)
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setRequestMarshaller(proto)
                    .setResponseMarshaller(proto)
                    .build();
            var service = ServerServiceDefinition.builder("test.Ingress")
                    .addMethod(method, ServerCalls.asyncUnaryCall((request, observer) -> {
                        calls.incrementAndGet();
                        entered.countDown();
                        try {
                            if (block && !release.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError("Handler not released");
                            }
                            observer.onNext(request);
                            observer.onCompleted();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            observer.onError(Status.CANCELLED.asRuntimeException());
                        }
                    }))
                    .addMethod(stream, ServerCalls.asyncClientStreamingCall(observer -> {
                        streams.incrementAndGet();
                        throw new AssertionError("Rejected stream constructed a handler");
                    }))
                    .build();
            var plumbing = ServerServiceDefinition.builder("test.Plumbing")
                    .addMethod(openMethod, ServerCalls.asyncUnaryCall((request, observer) -> {
                        observer.onNext(request);
                        observer.onCompleted();
                    }))
                    .build();
            var builder = NettyServerBuilder.forPort(0)
                    .addService(service)
                    .addService(plumbing)
                    .addStreamTracerFactory(new ServerStreamTracer.Factory() {
                        @Override
                        public ServerStreamTracer newServerStreamTracer(
                                String name, Metadata headers) {
                            return new ServerStreamTracer() {
                                @Override
                                public void inboundUncompressedSize(long bytes) {
                                    inboundUncompressedBytes.addAndGet(bytes);
                                    received.countDown();
                                }

                                @Override
                                public void streamClosed(Status status) {
                                    transportStatus.set(status);
                                }
                            };
                        }
                    });
            admission.configure(builder);
            server = builder.build().start();
            channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
                    .usePlaintext()
                    .build();
        }

        BytesValue call(BytesValue value, CallOptions options) {
            return ClientCalls.blockingUnaryCall(
                    channel, method, options.withDeadlineAfter(5, TimeUnit.SECONDS), value);
        }

        @Override
        public void close() throws Exception {
            release.countDown();
            admission.close();
            channel.shutdownNow();
            assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            server.shutdownNow();
            assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(admission.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 256})
    void cancellationCannotReleaseAllowanceWhileSynchronousHandlerStillUsesRequest(
            int responseLimit) throws Exception {
        try (var f = new Fixture(true, responseLimit)) {
            var pending = ClientCalls.futureUnaryCall(
                    f.channel.newCall(f.method, CallOptions.DEFAULT), VALUE);
            assertThat(f.entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(f.admission.reservedBytes()).isEqualTo(2048L + responseLimit);
            assertThat(pending.cancel(true)).isTrue();
            assertThatThrownBy(() -> f.call(VALUE, CallOptions.DEFAULT))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                            assertThat(e.getStatus().getCode())
                                    .isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
            assertThat(f.parses.get()).isEqualTo(1);
            assertThat(f.admission.awaitIdle(Duration.ofMillis(30))).isFalse();
            f.release.countDown();
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(f.admission.reservedBytes()).isZero();
        }
    }

    @Test
    void codeHoldingTheAdmissionsMonitorCannotStallAdmissionOrDrain() throws Exception {
        try (var f = new Fixture(true)) {
            var held = new CountDownLatch(1);
            var letGo = new CountDownLatch(1);
            var holder = new Thread(() -> {
                synchronized (f.admission) {
                    held.countDown();
                    try {
                        letGo.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            holder.start();
            try {
                assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();
                var pending = ClientCalls.futureUnaryCall(
                        f.channel.newCall(f.method, CallOptions.DEFAULT), VALUE);
                assertThat(f.entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(f.admission.activeCalls()).isEqualTo(1);
                f.release.countDown();
                assertThat(pending.get(5, TimeUnit.SECONDS)).isEqualTo(VALUE);
                assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
                assertThat(f.admission.reservedBytes()).isZero();
            } finally {
                letGo.countDown();
                holder.join(5_000);
            }
        }
    }

    @Test
    void cancellationBeforeHalfCloseDrainsReservationWithoutDecoding() throws Exception {
        try (var f = new Fixture(false)) {
            var call = f.channel.newCall(f.method, CallOptions.DEFAULT);
            call.start(new ClientCall.Listener<>() { }, new Metadata());
            call.request(1);
            call.sendMessage(VALUE);
            assertThat(f.received.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(f.parses.get()).isZero();
            assertThat(f.inboundUncompressedBytes.get())
                    .isEqualTo(VALUE.getSerializedSize())
                    .isLessThanOrEqualTo(1024);
            assertThat(f.admission.reservedBytes())
                    .isEqualTo(2048)
                    .isGreaterThanOrEqualTo(f.inboundUncompressedBytes.get());
            assertThatThrownBy(() -> f.call(VALUE, CallOptions.DEFAULT))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                            assertThat(e.getStatus().getCode())
                                    .isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
            call.cancel("Test cancellation before half-close", null);
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(f.parses.get()).isZero();
            assertThat(f.calls.get()).isZero();
            assertThat(f.admission.reservedBytes()).isZero();
            assertThat(f.call(VALUE, CallOptions.DEFAULT)).isEqualTo(VALUE);
        }
    }

    @Test
    void oversizedFrameBeforeHalfCloseIsRejectedWithoutBufferingOrDecoding() throws Exception {
        try (var f = new Fixture(false)) {
            var oversized = BytesValue.newBuilder()
                    .setValue(ByteString.copyFrom(new byte[2048]))
                    .build();
            var result = new CompletableFuture<Status>();
            var call = f.channel.newCall(
                    f.method, CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS));
            call.start(new ClientCall.Listener<>() {
                @Override
                public void onClose(Status status, Metadata trailers) {
                    result.complete(status);
                }
            }, new Metadata());
            call.request(1);
            call.sendMessage(oversized);
            assertThat(result.get(5, TimeUnit.SECONDS).getCode())
                    .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(f.inboundUncompressedBytes.get()).isZero();
            assertThat(f.parses.get()).isZero();
            assertThat(f.calls.get()).isZero();
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(f.admission.reservedBytes()).isZero();
            assertThat(f.call(VALUE, CallOptions.DEFAULT)).isEqualTo(VALUE);
        }
    }

    @Test
    void streamsFailBeforeParsingOrConstructingHandlerWhileOpenPlumbingPasses() throws Exception {
        try (var f = new Fixture(false)) {
            var failed = new CompletableFuture<Throwable>();
            var sender = ClientCalls.asyncClientStreamingCall(
                    f.channel.newCall(f.stream, CallOptions.DEFAULT),
                    new StreamObserver<>() {
                        @Override
                        public void onNext(BytesValue ignored) {
                            failed.completeExceptionally(new AssertionError("Unexpected result"));
                        }

                        @Override
                        public void onError(Throwable error) {
                            failed.complete(error);
                        }

                        @Override
                        public void onCompleted() {
                            failed.completeExceptionally(
                                    new AssertionError("Unexpected completion"));
                        }
                    });
            sender.onNext(VALUE);
            sender.onCompleted();
            assertThat(Status.fromThrowable(failed.get(5, TimeUnit.SECONDS)).getCode())
                    .isEqualTo(Status.Code.UNIMPLEMENTED);
            assertThat(f.streams.get()).isZero();
            assertThat(f.parses.get()).isZero();
            assertThat(f.admission.reservedBytes()).isZero();

            BytesValue ping = ClientCalls.blockingUnaryCall(
                    f.channel,
                    f.openMethod,
                    CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS),
                    VALUE);
            assertThat(ping).isEqualTo(VALUE);
        }
    }
}
