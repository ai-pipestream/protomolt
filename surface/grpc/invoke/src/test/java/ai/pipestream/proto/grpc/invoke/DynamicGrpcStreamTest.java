package ai.pipestream.proto.grpc.invoke;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The streaming primitives behind future connectors: an uncapped, flow-controlled
 * server-stream handle and client-streaming invocation, all over dynamic messages.
 */
class DynamicGrpcStreamTest {

    private static final String PROTO = """
            syntax = "proto3";
            package feed.test;
            message Open { int64 limit = 1; string pad = 2; }
            message Tick { int64 seq = 1; string pad = 2; }
            message Total { int64 sum = 1; }
            service Feed {
              rpc Count(Open) returns (stream Tick);
              rpc Watch(Open) returns (stream Tick);
              rpc Silent(Open) returns (stream Tick);
              rpc Fail(Open) returns (stream Tick);
              rpc Sum(stream Tick) returns (Total);
              rpc One(Open) returns (Tick);
            }
            """;

    private static final String PAD = "x".repeat(8 * 1024);

    private static FileDescriptor file;
    private static Server server;
    private static ManagedChannel channel;
    private static final AtomicInteger watchEmitted = new AtomicInteger();
    private static final CountDownLatch watchCancelled = new CountDownLatch(1);

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("feed/test/feed.proto", PROTO, "test").build());
        file = compiled.descriptorFor("feed/test/feed.proto").orElseThrow();
        ServiceDescriptor service = file.findServiceByName("Feed");

        var count = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Count"));
        var watch = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Watch"));
        var silent = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Silent"));
        var fail = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Fail"));
        var sum = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Sum"));
        var one = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("One"));

        io.grpc.ServiceDescriptor grpcService = io.grpc.ServiceDescriptor
                .newBuilder(service.getFullName())
                .setSchemaDescriptor((io.grpc.protobuf.ProtoFileDescriptorSupplier) () -> file)
                .addMethod(count).addMethod(watch).addMethod(silent)
                .addMethod(fail).addMethod(sum).addMethod(one)
                .build();

        ServerServiceDefinition definition = ServerServiceDefinition.builder(grpcService)
                .addMethod(count, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    long limit = limitOf(request);
                    for (long i = 0; i < limit; i++) {
                        out.onNext(tick(i, ""));
                    }
                    out.onCompleted();
                }))
                .addMethod(watch, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    // An endless producer that respects transport readiness; padded
                    // messages keep the transport window's lookahead small and countable.
                    ServerCallStreamObserver<DynamicMessage> observer =
                            (ServerCallStreamObserver<DynamicMessage>) out;
                    observer.setOnCancelHandler(watchCancelled::countDown);
                    observer.setOnReadyHandler(() -> {
                        while (observer.isReady() && !observer.isCancelled()) {
                            observer.onNext(tick(watchEmitted.getAndIncrement(), PAD));
                        }
                    });
                }))
                .addMethod(silent, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    // Never emits, never completes: the quiet-interval case.
                    ((ServerCallStreamObserver<DynamicMessage>) out).setOnCancelHandler(() -> { });
                }))
                .addMethod(fail, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    out.onNext(tick(0, ""));
                    out.onNext(tick(1, ""));
                    out.onError(Status.FAILED_PRECONDITION
                            .withDescription("boom after two").asRuntimeException());
                }))
                .addMethod(sum, ServerCalls.asyncClientStreamingCall(out -> new StreamObserver<>() {
                    long total;

                    @Override
                    public void onNext(DynamicMessage value) {
                        total += (long) value.getField(
                                value.getDescriptorForType().findFieldByName("seq"));
                    }

                    @Override
                    public void onError(Throwable t) {
                        // Client hung up; nothing to do.
                    }

                    @Override
                    public void onCompleted() {
                        DynamicMessage.Builder response = DynamicMessage.newBuilder(
                                file.findMessageTypeByName("Total"));
                        response.setField(response.getDescriptorForType().findFieldByName("sum"),
                                total);
                        out.onNext(response.build());
                        out.onCompleted();
                    }
                }))
                .addMethod(one, ServerCalls.asyncUnaryCall((request, out) -> {
                    out.onNext(tick(42, ""));
                    out.onCompleted();
                }))
                .build();

        server = InProcessServerBuilder.forName("dynamic-stream-test")
                .addService(definition)
                .build()
                .start();
        channel = InProcessChannelBuilder.forName("dynamic-stream-test").build();
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    private static long limitOf(DynamicMessage request) {
        return (long) request.getField(request.getDescriptorForType().findFieldByName("limit"));
    }

    private static DynamicMessage tick(long seq, String pad) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(
                file.findMessageTypeByName("Tick"));
        builder.setField(builder.getDescriptorForType().findFieldByName("seq"), seq);
        if (!pad.isEmpty()) {
            builder.setField(builder.getDescriptorForType().findFieldByName("pad"), pad);
        }
        return builder.build();
    }

    private static DynamicMessage open(long limit) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(
                file.findMessageTypeByName("Open"));
        builder.setField(builder.getDescriptorForType().findFieldByName("limit"), limit);
        return builder.build();
    }

    private static long seqOf(DynamicMessage tick) {
        return (long) tick.getField(tick.getDescriptorForType().findFieldByName("seq"));
    }

    private DynamicGrpcStream openStream(String method, long limit) {
        return DynamicGrpcCalls.openServerStream(channel,
                file.findServiceByName("Feed").findMethodByName(method),
                open(limit), CallOptions.DEFAULT, new Metadata());
    }

    @Test
    void finiteStreamDrainsInBatchesAndCloses() {
        try (DynamicGrpcStream stream = openStream("Count", 25)) {
            List<DynamicMessage> first = stream.take(10, Duration.ofSeconds(5));
            List<DynamicMessage> second = stream.take(10, Duration.ofSeconds(5));
            List<DynamicMessage> rest = stream.take(10, Duration.ofSeconds(5));
            assertThat(first).hasSize(10);
            assertThat(second).hasSize(10);
            assertThat(rest).hasSize(5);
            assertThat(seqOf(rest.get(4))).isEqualTo(24);
            assertThat(stream.take(10, Duration.ofMillis(100))).isEmpty();
            assertThat(stream.isClosed()).isTrue();
            assertThat(stream.terminalStatus().isOk()).isTrue();
        }
    }

    @Test
    void openEndedStreamStaysBoundedAndCancelsCleanly() throws Exception {
        watchEmitted.set(0);
        try (DynamicGrpcStream stream = openStream("Watch", 0)) {
            List<DynamicMessage> taken = stream.take(5, Duration.ofSeconds(5));
            assertThat(taken).hasSize(5);

            // The producer is endless; consumption is what meters it. Allow the transport
            // window's lookahead, but the server must not have run away.
            Thread.sleep(200);
            assertThat(watchEmitted.get()).isLessThan(50);

            stream.cancel();
            assertThat(watchCancelled.await(5, TimeUnit.SECONDS))
                    .as("server sees the cancellation")
                    .isTrue();
        }
    }

    @Test
    void quietIntervalReturnsEmptyNotError() {
        try (DynamicGrpcStream stream = openStream("Silent", 0)) {
            long start = System.nanoTime();
            List<DynamicMessage> quiet = stream.take(10, Duration.ofMillis(300));
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertThat(quiet).isEmpty();
            assertThat(elapsedMs).isBetween(250L, 5_000L);
            assertThat(stream.isClosed()).isFalse();
        }
    }

    @Test
    void failureSurfacesAfterPriorMessagesDrain() {
        try (DynamicGrpcStream stream = openStream("Fail", 0)) {
            List<DynamicMessage> drained = stream.take(10, Duration.ofSeconds(5));
            assertThat(drained).hasSize(2);
            assertThatThrownBy(() -> stream.take(1, Duration.ofSeconds(5)))
                    .isInstanceOf(StatusRuntimeException.class)
                    .hasMessageContaining("boom after two");
            assertThat(stream.isClosed()).isTrue();
        }
    }

    @Test
    void clientStreamingSumsTheBatch() {
        Iterator<DynamicMessage> requests = IntStream.range(0, 100)
                .mapToObj(i -> tick(i, ""))
                .iterator();
        DynamicMessage total = DynamicGrpcCalls.callClientStreaming(channel,
                file.findServiceByName("Feed").findMethodByName("Sum"),
                requests, CallOptions.DEFAULT, new Metadata());
        assertThat((long) total.getField(
                total.getDescriptorForType().findFieldByName("sum")))
                .isEqualTo(4950);
    }

    @Test
    void wrongShapesAreRejected() {
        var one = file.findServiceByName("Feed").findMethodByName("One");
        var sum = file.findServiceByName("Feed").findMethodByName("Sum");
        assertThatThrownBy(() -> DynamicGrpcCalls.openServerStream(
                channel, one, open(1), CallOptions.DEFAULT, new Metadata()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DynamicGrpcCalls.callClientStreaming(
                channel, one, List.<DynamicMessage>of().iterator(),
                CallOptions.DEFAULT, new Metadata()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DynamicGrpcCalls.openServerStream(
                channel, sum, open(1), CallOptions.DEFAULT, new Metadata()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
