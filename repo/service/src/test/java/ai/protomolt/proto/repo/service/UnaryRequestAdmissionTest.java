package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.InputStream;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

/** Real Netty transport with a synthetic protobuf echo service; no repository/provider claim. */
class UnaryRequestAdmissionTest {
    static final String ECHO = "test.Ingress/Echo";
    static final String STREAM = "test.Ingress/Stream";
    static final BytesValue VALUE = BytesValue.newBuilder().setValue(ByteString.copyFromUtf8("content")).build();

    static final class Fixture implements AutoCloseable {
        final AtomicInteger parses = new AtomicInteger();
        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger streams = new AtomicInteger();
        final CountDownLatch parsed = new CountDownLatch(1);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final PayloadBudget budget;
        final UnaryRequestAdmission admission;
        final MethodDescriptor<BytesValue, BytesValue> method;
        final MethodDescriptor<BytesValue, BytesValue> stream;
        final GrpcServerLifetime server;
        final ManagedChannel channel;

        Fixture(boolean block) throws Exception {
            this(block, 0);
        }
        Fixture(boolean block, int responseLimit) throws Exception {
            budget = new PayloadBudget(2048 + responseLimit);
            admission = new UnaryRequestAdmission(budget, 1024, 1, Set.of(ECHO),
                    responseLimit == 0 ? java.util.Map.of() : java.util.Map.of(ECHO, responseLimit));
            var proto = ProtoUtils.marshaller(BytesValue.getDefaultInstance());
            var observed = new MethodDescriptor.Marshaller<BytesValue>() {
                public InputStream stream(BytesValue value) { return proto.stream(value); }
                public BytesValue parse(InputStream input) { parses.incrementAndGet(); parsed.countDown(); return proto.parse(input); }
            };
            method = MethodDescriptor.<BytesValue, BytesValue>newBuilder().setFullMethodName(ECHO)
                    .setType(MethodDescriptor.MethodType.UNARY).setRequestMarshaller(observed).setResponseMarshaller(proto).build();
            stream = method.toBuilder().setFullMethodName(STREAM).setType(MethodDescriptor.MethodType.CLIENT_STREAMING).build();
            var service = ServerServiceDefinition.builder("test.Ingress")
                    .addMethod(method, ServerCalls.asyncUnaryCall((request, observer) -> {
                        calls.incrementAndGet(); entered.countDown();
                        try {
                            if (block && !release.await(10, TimeUnit.SECONDS)) throw new AssertionError("Handler not released");
                            observer.onNext(request); observer.onCompleted();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt(); observer.onError(Status.CANCELLED.asRuntimeException());
                        }
                    }))
                    .addMethod(stream, ServerCalls.asyncClientStreamingCall(observer -> {
                        streams.incrementAndGet(); throw new AssertionError("Rejected stream constructed a handler");
                    })).build();
            var builder = NettyServerBuilder.forPort(0).addService(service);
            admission.configure(builder);
            server = GrpcServerLifetime.start(builder);
            channel = NettyChannelBuilder.forAddress("127.0.0.1", server.server().getPort()).usePlaintext().build();
        }

        BytesValue call(BytesValue value, CallOptions options) {
            return ClientCalls.blockingUnaryCall(channel, method, options.withDeadlineAfter(5, TimeUnit.SECONDS), value);
        }

        @Override public void close() throws Exception {
            release.countDown(); admission.close(); channel.shutdownNow();
            assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            server.close();
            assertThat(admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest @ValueSource(ints = {0, 256})
    void cancellationCannotReleaseAllowanceWhileSynchronousHandlerStillUsesRequest(int responseLimit) throws Exception {
        try (var f = new Fixture(true, responseLimit)) {
            var pending = ClientCalls.futureUnaryCall(f.channel.newCall(f.method, CallOptions.DEFAULT), VALUE);
            assertThat(f.entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(f.budget.reservedBytes()).isEqualTo(2048 + responseLimit);
            assertThat(pending.cancel(true)).isTrue();
            assertThatThrownBy(() -> f.call(VALUE, CallOptions.DEFAULT))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
            assertThat(f.parses.get()).isEqualTo(1);
            assertThat(f.admission.awaitIdle(Duration.ofMillis(30))).isFalse();
            f.release.countDown();
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(f.budget.reservedBytes()).isZero();
        }
    }

    @Test void oversizedResponseIsNeverDeliveredAndCapacityCanBeReused() throws Exception {
        try (var f = new Fixture(false, VALUE.getSerializedSize() - 1)) {
            assertThatThrownBy(() -> f.call(VALUE, CallOptions.DEFAULT))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(f.budget.reservedBytes()).isZero();
            assertThat(f.call(BytesValue.getDefaultInstance(), CallOptions.DEFAULT)).isEqualTo(BytesValue.getDefaultInstance());
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void oversizedMessagesIncludingCompressedExpansionNeverReachHandler(boolean compressed) throws Exception {
        try (var f = new Fixture(false)) {
            var value = BytesValue.newBuilder().setValue(ByteString.copyFrom(new byte[2048])).build();
            var options = compressed ? CallOptions.DEFAULT.withCompression("gzip") : CallOptions.DEFAULT;
            assertThatThrownBy(() -> f.call(value, options)).isInstanceOfSatisfying(StatusRuntimeException.class,
                    e -> assertThat(e.getStatus().getCode()).isEqualTo(compressed ? Status.Code.UNKNOWN : Status.Code.RESOURCE_EXHAUSTED));
            // grpc-core 1.84 converts exceptions during compressed protobuf parsing to UNKNOWN.
            // The decompressed size check still prevents handler execution.
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(f.calls.get()).isZero();
        }
    }

    @Test void streamsFailBeforeParsingOrConstructingHandler() throws Exception {
        try (var f = new Fixture(false)) {
            var failed = new CompletableFuture<Throwable>();
            var sender = ClientCalls.asyncClientStreamingCall(f.channel.newCall(f.stream, CallOptions.DEFAULT),
                    new StreamObserver<BytesValue>() {
                        public void onNext(BytesValue ignored) { failed.completeExceptionally(new AssertionError("Unexpected result")); }
                        public void onError(Throwable error) { failed.complete(error); }
                        public void onCompleted() { failed.completeExceptionally(new AssertionError("Unexpected completion")); }
                    });
            sender.onNext(VALUE); sender.onCompleted();
            assertThat(Status.fromThrowable(failed.get(5, TimeUnit.SECONDS)).getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
            assertThat(f.streams.get()).isZero(); assertThat(f.parses.get()).isZero();
            assertThat(f.budget.reservedBytes()).isZero();
        }
    }

    @Test void successfulCallsDrainAndClosedAdmissionRejectsBeforeDecode() throws Exception {
        try (var f = new Fixture(false)) {
            assertThat(f.call(VALUE, CallOptions.DEFAULT)).isEqualTo(VALUE);
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            f.admission.close();
            assertThatThrownBy(() -> f.call(VALUE, CallOptions.DEFAULT)).isInstanceOfSatisfying(StatusRuntimeException.class,
                    e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
            assertThat(f.parses.get()).isEqualTo(1);
        }
    }

    @Test void malformedProtobufReleasesReservationWithoutInvokingHandler() throws Exception {
        try (var f = new Fixture(false)) {
            var malformed = MethodDescriptor.<byte[], BytesValue>newBuilder()
                    .setFullMethodName(ECHO).setType(MethodDescriptor.MethodType.UNARY)
                    .setResponseMarshaller(ProtoUtils.marshaller(BytesValue.getDefaultInstance()))
                    .setRequestMarshaller(new MethodDescriptor.Marshaller<byte[]>() {
                        public InputStream stream(byte[] value) { return new java.io.ByteArrayInputStream(value); }
                        public byte[] parse(InputStream input) { throw new AssertionError("Client-only request marshaller"); }
                    }).build();
            assertThatThrownBy(() -> ClientCalls.blockingUnaryCall(f.channel, malformed,
                    CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS), new byte[]{10, 127}))
                    .isInstanceOf(StatusRuntimeException.class);
            assertThat(f.parses.get()).isEqualTo(1);
            assertThat(f.calls.get()).isZero();
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(f.budget.reservedBytes()).isZero();
        }
    }

    @Test void cancellationBeforeHalfCloseDrainsParsedRequest() throws Exception {
        try (var f = new Fixture(false)) {
            var call = f.channel.newCall(f.method, CallOptions.DEFAULT);
            call.start(new ClientCall.Listener<>() {}, new Metadata());
            call.request(1); call.sendMessage(VALUE);
            assertThat(f.parsed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(f.budget.reservedBytes()).isEqualTo(2048);
            call.cancel("Test cancellation before half-close", null);
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(f.calls.get()).isZero();
        }
    }

    @Test void serverShutdownDoesNotReleaseBlockedHandlerReservation() throws Exception {
        try (var f = new Fixture(true)) {
            var pending = ClientCalls.futureUnaryCall(f.channel.newCall(f.method, CallOptions.DEFAULT), VALUE);
            assertThat(f.entered.await(5, TimeUnit.SECONDS)).isTrue();
            f.admission.close();
            f.server.server().shutdownNow();
            assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
            assertThat(f.admission.awaitIdle(Duration.ofMillis(30))).isFalse();
            assertThat(f.budget.reservedBytes()).isEqualTo(2048);
            f.release.countDown();
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
        }
    }

    @Test void duplicateUnaryMessagesAreRejectedAndBothDecodeAllowancesDrain() throws Exception {
        try (var f = new Fixture(false)) {
            // Only the client descriptor permits two sends; the server remains unary.
            var descriptor = f.method.toBuilder().setType(MethodDescriptor.MethodType.BIDI_STREAMING).build();
            var result = new CompletableFuture<Status>();
            var call = f.channel.newCall(descriptor, CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS));
            call.start(new ClientCall.Listener<>() {
                @Override public void onClose(Status status, Metadata trailers) { result.complete(status); }
            }, new Metadata());
            call.request(1); call.sendMessage(VALUE); call.sendMessage(VALUE); call.halfClose();
            assertThat(result.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(Status.Code.INTERNAL);
            assertThat(f.parses.get()).isEqualTo(2);
            assertThat(f.calls.get()).isZero();
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(f.budget.reservedBytes()).isZero();
        }
    }

    @Test void validCompressedRequestRunsAndReleasesAllowance() throws Exception {
        try (var f = new Fixture(false)) {
            assertThat(f.call(VALUE, CallOptions.DEFAULT.withCompression("gzip"))).isEqualTo(VALUE);
            assertThat(f.admission.awaitIdle(Duration.ofSeconds(5))).isTrue();
            assertThat(f.calls.get()).isEqualTo(1);
        }
    }
}
