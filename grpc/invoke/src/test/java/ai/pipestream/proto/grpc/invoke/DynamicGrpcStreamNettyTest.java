package ai.pipestream.proto.grpc.invoke;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The streaming primitives over a real socket: deadline expiry, cross-thread cancellation,
 * server death mid-stream, and metadata propagation — the failure modes a long-lived
 * connector consumer actually meets.
 */
class DynamicGrpcStreamNettyTest {

    private static final String PROTO = """
            syntax = "proto3";
            package feed.net;
            message Open { int64 limit = 1; }
            message Tick { int64 seq = 1; }
            service NetFeed {
              rpc Slow(Open) returns (stream Tick);
              rpc Echo(Open) returns (stream Tick);
            }
            """;

    private static final Metadata.Key<String> API_TOKEN =
            Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);

    private static FileDescriptor file;
    private static Server server;
    private static ManagedChannel channel;
    private static final AtomicReference<String> seenToken = new AtomicReference<>();

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("feed/net/feed.proto", PROTO, "test").build());
        file = compiled.descriptorFor("feed/net/feed.proto").orElseThrow();
        ServiceDescriptor service = file.findServiceByName("NetFeed");

        var slow = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Slow"));
        var echo = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Echo"));
        io.grpc.ServiceDescriptor grpcService = io.grpc.ServiceDescriptor
                .newBuilder(service.getFullName())
                .setSchemaDescriptor((io.grpc.protobuf.ProtoFileDescriptorSupplier) () -> file)
                .addMethod(slow).addMethod(echo)
                .build();

        ServerServiceDefinition definition = ServerServiceDefinition.builder(grpcService)
                .addMethod(slow, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    // One message, then silence: deadline and cancel tests take it from here.
                    ((ServerCallStreamObserver<DynamicMessage>) out).setOnCancelHandler(() -> { });
                    out.onNext(tick(0));
                }))
                .addMethod(echo, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    out.onNext(tick(1));
                    out.onNext(tick(2));
                    // Leave the stream open; the server-death test kills the process side.
                    ((ServerCallStreamObserver<DynamicMessage>) out).setOnCancelHandler(() -> { });
                }))
                .build();

        server = ServerBuilder.forPort(0)
                .addService(definition)
                .intercept(new io.grpc.ServerInterceptor() {
                    @Override
                    public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(
                            io.grpc.ServerCall<ReqT, RespT> call, Metadata headers,
                            io.grpc.ServerCallHandler<ReqT, RespT> next) {
                        seenToken.set(headers.get(API_TOKEN));
                        return next.startCall(call, headers);
                    }
                })
                .build()
                .start();
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .build();
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        if (server != null) {
            server.shutdownNow();
        }
    }

    private static DynamicMessage tick(long seq) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(
                file.findMessageTypeByName("Tick"));
        builder.setField(builder.getDescriptorForType().findFieldByName("seq"), seq);
        return builder.build();
    }

    private static DynamicMessage open() {
        return DynamicMessage.newBuilder(file.findMessageTypeByName("Open")).build();
    }

    @Test
    void deadlineExpirySurfacesAfterDrain() {
        DynamicGrpcStream stream = DynamicGrpcCalls.openServerStream(channel,
                file.findServiceByName("NetFeed").findMethodByName("Slow"),
                open(), CallOptions.DEFAULT.withDeadlineAfter(500, TimeUnit.MILLISECONDS),
                new Metadata());
        List<DynamicMessage> first = stream.take(1, Duration.ofSeconds(5));
        assertThat(first).hasSize(1);
        StatusRuntimeException expired = catchThrowableOfType(StatusRuntimeException.class,
                () -> {
                    // The deadline fires while we wait; drain until it surfaces.
                    for (int i = 0; i < 20; i++) {
                        stream.take(1, Duration.ofSeconds(1));
                    }
                });
        assertThat(expired.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
        assertThat(stream.isClosed()).isTrue();
    }

    @Test
    void cancelFromAnotherThreadUnblocksTake() throws Exception {
        DynamicGrpcStream stream = DynamicGrpcCalls.openServerStream(channel,
                file.findServiceByName("NetFeed").findMethodByName("Slow"),
                open(), CallOptions.DEFAULT, new Metadata());
        assertThat(stream.take(1, Duration.ofSeconds(5))).hasSize(1);

        CountDownLatch unblocked = new CountDownLatch(1);
        AtomicReference<Throwable> outcome = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            try {
                // Blocks: the server has gone quiet. Cancel must end this promptly.
                stream.take(1, Duration.ofSeconds(30));
            } catch (Throwable t) {
                outcome.set(t);
            } finally {
                unblocked.countDown();
            }
        });
        consumer.start();
        Thread.sleep(200);
        stream.cancel();
        assertThat(unblocked.await(5, TimeUnit.SECONDS))
                .as("take() unblocked by cancel")
                .isTrue();
        assertThat(outcome.get()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) outcome.get()).getStatus().getCode())
                .isEqualTo(Status.Code.CANCELLED);
    }

    @Test
    void metadataHeadersReachTheServer() {
        Metadata headers = new Metadata();
        headers.put(API_TOKEN, "stream-secret");
        try (DynamicGrpcStream stream = DynamicGrpcCalls.openServerStream(channel,
                file.findServiceByName("NetFeed").findMethodByName("Slow"),
                open(), CallOptions.DEFAULT, headers)) {
            assertThat(stream.take(1, Duration.ofSeconds(5))).hasSize(1);
            assertThat(seenToken.get()).isEqualTo("stream-secret");
        }
    }

    @Test
    void serverDeathSurfacesAsUnavailableAfterDrain() throws Exception {
        // A dedicated server so killing it does not affect the other tests.
        Server doomed = ServerBuilder.forPort(0)
                .addService(serverFor("Echo"))
                .build()
                .start();
        ManagedChannel doomedChannel = ManagedChannelBuilder
                .forAddress("127.0.0.1", doomed.getPort())
                .usePlaintext()
                .build();
        try {
            DynamicGrpcStream stream = DynamicGrpcCalls.openServerStream(doomedChannel,
                    file.findServiceByName("NetFeed").findMethodByName("Echo"),
                    open(), CallOptions.DEFAULT, new Metadata());
            assertThat(stream.take(2, Duration.ofSeconds(5))).hasSize(2);

            doomed.shutdownNow();
            assertThatThrownBy(() -> {
                for (int i = 0; i < 20; i++) {
                    stream.take(1, Duration.ofSeconds(1));
                }
            })
                    .isInstanceOf(StatusRuntimeException.class)
                    .satisfies(t -> assertThat(((StatusRuntimeException) t).getStatus().getCode())
                            .isIn(Status.Code.UNAVAILABLE, Status.Code.CANCELLED));
            assertThat(stream.isClosed()).isTrue();
        } finally {
            doomedChannel.shutdownNow();
            doomed.shutdownNow();
        }
    }

    private static ServerServiceDefinition serverFor(String methodName) {
        ServiceDescriptor service = file.findServiceByName("NetFeed");
        var method = DynamicGrpcCalls.methodDescriptor(service.findMethodByName(methodName));
        io.grpc.ServiceDescriptor grpcService = io.grpc.ServiceDescriptor
                .newBuilder(service.getFullName())
                .setSchemaDescriptor((io.grpc.protobuf.ProtoFileDescriptorSupplier) () -> file)
                .addMethod(method)
                .build();
        return ServerServiceDefinition.builder(grpcService)
                .addMethod(method, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    out.onNext(tick(1));
                    out.onNext(tick(2));
                    ((ServerCallStreamObserver<DynamicMessage>) out).setOnCancelHandler(() -> { });
                }))
                .build();
    }
}
