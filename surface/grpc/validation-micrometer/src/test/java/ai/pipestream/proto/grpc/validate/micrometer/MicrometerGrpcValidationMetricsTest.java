package ai.pipestream.proto.grpc.validate.micrometer;

import ai.pipestream.proto.grpc.validate.ValidatingServerInterceptor;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End to end through real ServiceLoader discovery: this module on the test classpath means a
 * plain {@code ValidatingServerInterceptor.create()} — no metrics configuration anywhere —
 * lands counters in the global registry for every request it judges.
 */
class MicrometerGrpcValidationMetricsTest {

    private static final String PROTO = """
            syntax = "proto3";
            package grpc.metrics.test.v1;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Ping {
              string id = 1 [(ai.pipestream.proto.validate.v1.field).string.min_len = 2];
            }
            """;

    private static Descriptor pingType;
    private static MethodDescriptor<DynamicMessage, DynamicMessage> pingMethod;
    private static SimpleMeterRegistry registry;
    private static Server server;
    private static ManagedChannel channel;

    @BeforeAll
    static void start() throws Exception {
        ClassLoader loader = MicrometerGrpcValidationMetricsTest.class.getClassLoader();
        String validateProto = new String(loader.getResourceAsStream(
                "ai/pipestream/proto/validate/v1/validate.proto").readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/validate/v1/validate.proto", validateProto, "test")
                .add("grpc/metrics/test/v1/echo.proto", PROTO, "test")
                .build());
        pingType = compiled.descriptorFor("grpc/metrics/test/v1/echo.proto").orElseThrow()
                .findMessageTypeByName("Ping");
        pingMethod = MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(
                        "grpc.metrics.test.v1.Echo", "Ping"))
                .setRequestMarshaller(ProtoUtils.marshaller(
                        DynamicMessage.getDefaultInstance(pingType)))
                .setResponseMarshaller(ProtoUtils.marshaller(
                        DynamicMessage.getDefaultInstance(pingType)))
                .build();

        registry = new SimpleMeterRegistry();
        Metrics.addRegistry(registry);

        ServerServiceDefinition echo = ServerServiceDefinition
                .builder("grpc.metrics.test.v1.Echo")
                .addMethod(pingMethod, ServerCalls.asyncUnaryCall((request, observer) -> {
                    observer.onNext(request);
                    observer.onCompleted();
                }))
                .build();
        server = InProcessServerBuilder.forName("grpc-metrics-test").directExecutor()
                .addService(ServerInterceptors.intercept(echo,
                        ValidatingServerInterceptor.create()))
                .build().start();
        channel = InProcessChannelBuilder.forName("grpc-metrics-test").directExecutor().build();
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        server.shutdownNow();
        Metrics.removeRegistry(registry);
        registry.close();
    }

    private static DynamicMessage ping(String id) {
        return DynamicMessage.newBuilder(pingType)
                .setField(pingType.findFieldByName("id"), id)
                .build();
    }

    @Test
    void countsJudgedRequestsWithNoConfigurationAtAll() {
        ClientCalls.blockingUnaryCall(channel, pingMethod, CallOptions.DEFAULT, ping("ok"));
        assertThatThrownBy(() -> ClientCalls.blockingUnaryCall(
                channel, pingMethod, CallOptions.DEFAULT, ping("x")))
                .isInstanceOf(StatusRuntimeException.class);

        assertThat(registry.counter("protomolt.grpc.validation.requests",
                "side", "server", "method", "grpc.metrics.test.v1.Echo/Ping",
                "type", "grpc.metrics.test.v1.Ping").count()).isEqualTo(1.0);
        assertThat(registry.counter("protomolt.grpc.validation.rejections",
                "side", "server", "method", "grpc.metrics.test.v1.Echo/Ping",
                "type", "grpc.metrics.test.v1.Ping").count()).isEqualTo(1.0);
        assertThat(registry.counter("protomolt.grpc.validation.violations",
                "side", "server", "method", "grpc.metrics.test.v1.Echo/Ping",
                "type", "grpc.metrics.test.v1.Ping", "rule", "string.min_len").count())
                .isEqualTo(1.0);
    }

    @Test
    void rendersEveryEventKindDirectly() {
        SimpleMeterRegistry local = new SimpleMeterRegistry();
        MicrometerGrpcValidationMetrics metrics = new MicrometerGrpcValidationMetrics(local);

        metrics.onValidated("client", "svc/M", "a.B");
        metrics.onRejected("client", "svc/M", "a.B", List.of("int32.gte"));
        metrics.onQualityScored("svc/M", "a.B", 0.75, Map.of("d", 0.75));
        metrics.onQualityRejected("svc/M", "a.B", 0.1);

        assertThat(local.counter("protomolt.grpc.validation.requests",
                "side", "client", "method", "svc/M", "type", "a.B").count()).isEqualTo(1.0);
        assertThat(local.counter("protomolt.grpc.validation.rejections",
                "side", "client", "method", "svc/M", "type", "a.B").count()).isEqualTo(1.0);
        assertThat(local.counter("protomolt.grpc.validation.violations",
                "side", "client", "method", "svc/M", "type", "a.B", "rule", "int32.gte")
                .count()).isEqualTo(1.0);
        assertThat(local.summary("protomolt.grpc.quality.score",
                "method", "svc/M", "type", "a.B").mean()).isEqualTo(0.75);
        assertThat(local.summary("protomolt.grpc.quality.dimension",
                "method", "svc/M", "type", "a.B", "dimension", "d").mean()).isEqualTo(0.75);
        assertThat(local.counter("protomolt.grpc.quality.rejections",
                "method", "svc/M", "type", "a.B").count()).isEqualTo(1.0);
    }
}
