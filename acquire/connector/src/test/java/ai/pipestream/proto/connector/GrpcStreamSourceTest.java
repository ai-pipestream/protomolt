package ai.pipestream.proto.connector;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A server-streaming method as a push source: finite streams drain in order and complete,
 * endless streams respect pause and resume, failures surface after the messages that beat
 * them, and close cancels the call the server is watching.
 */
class GrpcStreamSourceTest {

    private static final String PROTO = """
            syntax = "proto3";
            package feed.test;
            message Open { int64 limit = 1; }
            message Tick { int64 seq = 1; string pad = 2; }
            service Feed {
              rpc Count(Open) returns (stream Tick);
              rpc Watch(Open) returns (stream Tick);
              rpc Fail(Open) returns (stream Tick);
            }
            """;

    private static final String PAD = "x".repeat(8 * 1024);

    private static FileDescriptor file;
    private static Server server;
    private static ManagedChannel channel;
    private static final AtomicInteger watchEmitted = new AtomicInteger();
    private static final CountDownLatch watchCancelled = new CountDownLatch(1);

    /** Records arrivals and terminal signals without a pump in the way. */
    private static final class RecordingListener implements StreamSource.Listener {
        final LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>();
        final AtomicInteger received = new AtomicInteger();
        final AtomicInteger terminals = new AtomicInteger();
        final CountDownLatch terminal = new CountDownLatch(1);
        volatile Throwable error;

        @Override
        public void onMessage(Message message) {
            received.incrementAndGet();
            messages.add(message);
        }

        @Override
        public void onComplete() {
            terminals.incrementAndGet();
            terminal.countDown();
        }

        @Override
        public void onError(Throwable error) {
            this.error = error;
            terminals.incrementAndGet();
            terminal.countDown();
        }
    }

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("feed/test/feed.proto", PROTO, "test").build());
        file = compiled.descriptorFor("feed/test/feed.proto").orElseThrow();
        ServiceDescriptor service = file.findServiceByName("Feed");

        var count = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Count"));
        var watch = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Watch"));
        var fail = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Fail"));

        io.grpc.ServiceDescriptor grpcService = io.grpc.ServiceDescriptor
                .newBuilder(service.getFullName())
                .setSchemaDescriptor((io.grpc.protobuf.ProtoFileDescriptorSupplier) () -> file)
                .addMethod(count).addMethod(watch).addMethod(fail)
                .build();

        ServerServiceDefinition definition = ServerServiceDefinition.builder(grpcService)
                .addMethod(count, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    for (long i = 0; i < limitOf(request); i++) {
                        out.onNext(tick(i));
                    }
                    out.onCompleted();
                }))
                .addMethod(watch, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    // An endless producer that respects transport readiness; padded
                    // messages keep the transport window's lookahead small.
                    ServerCallStreamObserver<DynamicMessage> observer =
                            (ServerCallStreamObserver<DynamicMessage>) out;
                    observer.setOnCancelHandler(watchCancelled::countDown);
                    observer.setOnReadyHandler(() -> {
                        while (observer.isReady() && !observer.isCancelled()) {
                            observer.onNext(tick(watchEmitted.getAndIncrement()));
                        }
                    });
                }))
                .addMethod(fail, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    out.onNext(tick(0));
                    out.onNext(tick(1));
                    out.onError(Status.FAILED_PRECONDITION
                            .withDescription("boom after two").asRuntimeException());
                }))
                .build();

        server = InProcessServerBuilder.forName("grpc-stream-source-test")
                .addService(definition)
                .build()
                .start();
        channel = InProcessChannelBuilder.forName("grpc-stream-source-test").build();
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    private static long limitOf(DynamicMessage request) {
        return (long) request.getField(request.getDescriptorForType().findFieldByName("limit"));
    }

    private static DynamicMessage tick(long seq) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(file.findMessageTypeByName("Tick"));
        builder.setField(builder.getDescriptorForType().findFieldByName("seq"), seq);
        builder.setField(builder.getDescriptorForType().findFieldByName("pad"), PAD);
        return builder.build();
    }

    private static DynamicMessage open(long limit) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(file.findMessageTypeByName("Open"));
        builder.setField(builder.getDescriptorForType().findFieldByName("limit"), limit);
        return builder.build();
    }

    private static long seqOf(Message tick) {
        return (long) ((DynamicMessage) tick).getField(
                ((DynamicMessage) tick).getDescriptorForType().findFieldByName("seq"));
    }

    private GrpcSourcePlan plan(String method, long limit) {
        return new GrpcSourcePlan(channel,
                file.findServiceByName("Feed").findMethodByName(method), open(limit));
    }

    private static void awaitClosed(SourcePump pump) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!pump.isClosed() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    @Test
    void finiteStreamDrainsInOrderAndCompletes() throws Exception {
        SourcePump pump = new SourcePump(8);
        pump.attach(new GrpcStreamSource().open(plan("Count", 25), pump));

        for (int i = 0; i < 25; i++) {
            Message next = pump.take(Duration.ofSeconds(5));
            assertThat(next).as("tick %d arrives", i).isNotNull();
            assertThat(seqOf(next)).isEqualTo(i);
        }
        awaitClosed(pump);
        assertThat(pump.take(Duration.ofMillis(200))).isNull();
        assertThat(pump.isCompleted()).isTrue();
        assertThat(pump.isClosed()).isTrue();
        pump.close();
    }

    @Test
    void pauseStopsInflowResumeRestartsAndCloseCancels() throws Exception {
        watchEmitted.set(0);
        RecordingListener listener = new RecordingListener();
        StreamSource.Handle handle = new GrpcStreamSource().open(plan("Watch", 0), listener);

        Message first = listener.messages.poll(5, TimeUnit.SECONDS);
        assertThat(first).as("the watch produces").isNotNull();
        handle.pause();

        // Once the in-flight batch lands, production stops: two samples 300ms apart agree.
        Thread.sleep(300);
        int atPause = listener.received.get();
        Thread.sleep(300);
        assertThat(listener.received.get() - atPause).as("paused inflow").isLessThanOrEqualTo(1);

        handle.resume();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (listener.received.get() < atPause + 5 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(listener.received.get()).as("resumed inflow").isGreaterThanOrEqualTo(atPause + 5);

        handle.close();
        assertThat(listener.terminal.await(5, TimeUnit.SECONDS))
                .as("close terminates the stream").isTrue();
        assertThat(listener.terminals.get()).as("exactly one terminal signal").isEqualTo(1);
        assertThat(watchCancelled.await(5, TimeUnit.SECONDS))
                .as("the server sees the cancellation").isTrue();
    }

    @Test
    void closeDuringActiveInflowTerminatesOnceAndJoins() throws Exception {
        watchEmitted.set(0);
        RecordingListener listener = new RecordingListener();
        StreamSource.Handle handle = new GrpcStreamSource().open(plan("Watch", 0), listener);

        Message first = listener.messages.poll(5, TimeUnit.SECONDS);
        assertThat(first).as("the watch produces").isNotNull();

        // No pause: the pump thread is mid-take or mid-batch when close lands. close()
        // must still join the thread promptly and fire exactly one terminal signal.
        long start = System.nanoTime();
        handle.close();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).as("close joins promptly").isLessThan(6_000);
        assertThat(listener.terminal.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(listener.terminals.get()).as("exactly one terminal signal").isEqualTo(1);
        assertThat(watchCancelled.await(5, TimeUnit.SECONDS))
                .as("the server sees the cancellation").isTrue();
    }

    @Test
    void failureSurfacesAfterPriorMessagesDrain() throws Exception {
        SourcePump pump = new SourcePump(8);
        pump.attach(new GrpcStreamSource().open(plan("Fail", 0), pump));

        assertThat(seqOf(pump.take(Duration.ofSeconds(5)))).isEqualTo(0);
        assertThat(seqOf(pump.take(Duration.ofSeconds(5)))).isEqualTo(1);
        assertThatThrownBy(() -> pump.take(Duration.ofSeconds(5)))
                .isInstanceOf(SourceException.class)
                .cause().isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("boom after two");
        assertThat(pump.isClosed()).isTrue();
        pump.close();
    }
}
