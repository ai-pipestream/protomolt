package ai.pipestream.proto.grpc.validate;

import ai.pipestream.proto.quality.QualityReport;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The contract enforced at the call boundary, both directions. A valid request reaches the
 * handler; an invalid one is refused with INVALID_ARGUMENT naming every violation, and the
 * handler never runs — the same guarantee the serde gives a topic. The client-side interceptor
 * fails the same call locally, before the network. Quality dimensions measure per request and
 * can gate with FAILED_PRECONDITION.
 */
class ValidatingInterceptorTest {

    private static final String PROTO = """
            syntax = "proto3";
            package grpc.validate.test.v1;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            import "ai/pipestream/proto/quality/v1/quality.proto";
            message Ping {
              option (ai.pipestream.proto.quality.v1.quality) = {
                dimension: { id: "detailed"
                             cel: "clamp(double(this.note.size()) / 10.0, 0.0, 1.0)" }
              };
              string id = 1 [(ai.pipestream.proto.validate.v1.field).string.min_len = 2];
              string note = 2;
            }
            message Pong { string id = 1; }
            """;

    private static Descriptor pingType;
    private static Descriptor pongType;
    private static MethodDescriptor<DynamicMessage, DynamicMessage> pingMethod;
    private static final AtomicInteger SERVERS = new AtomicInteger();

    @BeforeAll
    static void compile() throws Exception {
        ClassLoader loader = ValidatingInterceptorTest.class.getClassLoader();
        String validateProto = new String(loader.getResourceAsStream(
                "ai/pipestream/proto/validate/v1/validate.proto").readAllBytes());
        String qualityProto = new String(loader.getResourceAsStream(
                "ai/pipestream/proto/quality/v1/quality.proto").readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/validate/v1/validate.proto", validateProto, "test")
                .add("ai/pipestream/proto/quality/v1/quality.proto", qualityProto, "test")
                .add("grpc/validate/test/v1/echo.proto", PROTO, "test")
                .build());
        var file = compiled.descriptorFor("grpc/validate/test/v1/echo.proto").orElseThrow();
        pingType = file.findMessageTypeByName("Ping");
        pongType = file.findMessageTypeByName("Pong");
        pingMethod = MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(
                        "grpc.validate.test.v1.Echo", "Ping"))
                .setRequestMarshaller(ProtoUtils.marshaller(
                        DynamicMessage.getDefaultInstance(pingType)))
                .setResponseMarshaller(ProtoUtils.marshaller(
                        DynamicMessage.getDefaultInstance(pongType)))
                .build();
    }

    private static DynamicMessage ping(String id, String note) {
        return DynamicMessage.newBuilder(pingType)
                .setField(pingType.findFieldByName("id"), id)
                .setField(pingType.findFieldByName("note"), note)
                .build();
    }

    /** An in-process echo service behind the given server interceptor, torn down by the caller. */
    private record Fixture(Server server, ManagedChannel channel, AtomicInteger handled)
            implements AutoCloseable {

        @Override
        public void close() {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    private static Fixture serve(ServerInterceptor interceptor,
                                 io.grpc.ClientInterceptor... clientInterceptors)
            throws IOException {
        String name = "validating-interceptor-" + SERVERS.incrementAndGet();
        AtomicInteger handled = new AtomicInteger();
        ServerServiceDefinition echo = ServerServiceDefinition
                .builder("grpc.validate.test.v1.Echo")
                .addMethod(pingMethod, ServerCalls.asyncUnaryCall((request, observer) -> {
                    handled.incrementAndGet();
                    observer.onNext(DynamicMessage.newBuilder(pongType)
                            .setField(pongType.findFieldByName("id"),
                                    request.getField(pingType.findFieldByName("id")))
                            .build());
                    observer.onCompleted();
                }))
                .build();
        Server server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(ServerInterceptors.intercept(echo, interceptor))
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor()
                .intercept(clientInterceptors)
                .build();
        return new Fixture(server, channel, handled);
    }

    private static DynamicMessage call(ManagedChannel channel, DynamicMessage request) {
        return ClientCalls.blockingUnaryCall(channel, pingMethod, CallOptions.DEFAULT, request);
    }

    @Test
    void validRequestsReachTheHandler() throws IOException {
        try (Fixture fixture = serve(ValidatingServerInterceptor.create())) {
            DynamicMessage pong = call(fixture.channel(), ping("A-1", "fine"));
            assertThat(pong.getField(pongType.findFieldByName("id"))).isEqualTo("A-1");
            assertThat(fixture.handled().get()).isEqualTo(1);
        }
    }

    /** The whole point: the handler never sees a message that violates its own contract. */
    @Test
    void invalidRequestsAreRefusedBeforeTheHandler() throws IOException {
        try (Fixture fixture = serve(ValidatingServerInterceptor.create())) {
            assertThatThrownBy(() -> call(fixture.channel(), ping("A", "")))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.INVALID_ARGUMENT);
                        assertThat(e.getStatus().getDescription())
                                .contains("string.min_len");
                    });
            assertThat(fixture.handled().get()).isZero();
        }
    }

    @Test
    void qualityMeasuresPerCallAndGatesBelowTheFloor() throws IOException {
        Map<String, QualityReport> reports = new ConcurrentHashMap<>();
        ValidatingServerInterceptor interceptor = ValidatingServerInterceptor.builder()
                .qualityFloor(0.5)
                .onQuality(reports::put)
                .build();
        try (Fixture fixture = serve(interceptor)) {
            // note is 10+ chars: dimension 'detailed' scores 1.0, above the floor.
            call(fixture.channel(), ping("A-1", "0123456789"));
            assertThat(reports).containsKey("grpc.validate.test.v1.Echo/Ping");
            assertThat(reports.get("grpc.validate.test.v1.Echo/Ping").composite())
                    .isEqualTo(1.0);

            // A terse note scores 0.1: refused as FAILED_PRECONDITION, handler untouched.
            assertThatThrownBy(() -> call(fixture.channel(), ping("A-2", "x")))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                            assertThat(e.getStatus().getCode())
                                    .isEqualTo(Status.Code.FAILED_PRECONDITION));
            assertThat(fixture.handled().get()).isEqualTo(1);
        }
    }

    /** The client half: the same refusal, before the request ever reaches the wire. */
    @Test
    void clientInterceptorFailsInvalidRequestsLocally() throws IOException {
        // The server side does NOT validate here; only the client interceptor stands guard.
        try (Fixture fixture = serve(passthrough(), ValidatingClientInterceptor.create())) {
            assertThatThrownBy(() -> call(fixture.channel(), ping("A", "")))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.INVALID_ARGUMENT);
                        assertThat(e.getStatus().getDescription())
                                .contains("string.min_len");
                    });
            assertThat(fixture.handled().get()).isZero();

            // And a valid request sails through the same channel.
            assertThat(call(fixture.channel(), ping("A-1", "fine"))).isNotNull();
        }
    }

    private static ServerInterceptor passthrough() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(
                    io.grpc.ServerCall<ReqT, RespT> call, io.grpc.Metadata headers,
                    io.grpc.ServerCallHandler<ReqT, RespT> next) {
                return next.startCall(call, headers);
            }
        };
    }
}
