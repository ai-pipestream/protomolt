package ai.pipestream.proto.grpc.invoke;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReflectActionTest {

    private static final String PROTO = """
            syntax = "proto3";
            package reflect.test;
            import "google/protobuf/timestamp.proto";
            message Ping { string text = 1; }
            message Pong { string text = 1; google.protobuf.Timestamp at = 2; }
            service PingService {
              rpc Echo(Ping) returns (Pong);
            }
            """;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Server reflectiveServer;
    private static Server bareServer;
    private static String reflectiveName;
    private static String bareName;
    private static ReflectAction action;

    @BeforeAll
    static void startServers() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("reflect/test/ping.proto", PROTO, "test").build());
        FileDescriptor file = compiled.descriptorFor("reflect/test/ping.proto").orElseThrow();
        ServiceDescriptor service = file.findServiceByName("PingService");

        io.grpc.MethodDescriptor<DynamicMessage, DynamicMessage> echo =
                DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Echo"));
        // Reflection lists only services whose gRPC descriptor carries a proto schema supplier,
        // which is what generated stubs attach; attach the reflected file the same way.
        io.grpc.ServiceDescriptor grpcDescriptor = io.grpc.ServiceDescriptor
                .newBuilder(service.getFullName())
                .setSchemaDescriptor((io.grpc.protobuf.ProtoFileDescriptorSupplier) () -> file)
                .addMethod(echo)
                .build();
        ServerServiceDefinition definition = ServerServiceDefinition.builder(grpcDescriptor)
                .addMethod(echo, ServerCalls.asyncUnaryCall((DynamicMessage request,
                        io.grpc.stub.StreamObserver<DynamicMessage> out) ->
                        out.onError(io.grpc.Status.UNIMPLEMENTED.asRuntimeException())))
                .build();

        reflectiveName = InProcessServerBuilder.generateName();
        reflectiveServer = InProcessServerBuilder.forName(reflectiveName)
                .addService(definition)
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();

        // A server with the same app service but no reflection registered.
        bareName = InProcessServerBuilder.generateName();
        bareServer = InProcessServerBuilder.forName(bareName)
                .addService(definition)
                .build()
                .start();

        action = new ReflectAction(target -> InProcessChannelBuilder.forName(target).build());
    }

    @AfterAll
    static void stop() {
        reflectiveServer.shutdownNow();
        bareServer.shutdownNow();
    }

    private ObjectNode input(String target) {
        ObjectNode input = MAPPER.createObjectNode();
        input.put("target", target);
        input.put("deadlineMs", 10_000);
        return input;
    }

    @Test
    void reflectsServicesAndDescriptorSet() throws Exception {
        ObjectNode result = action.execute(input(reflectiveName), ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(servicesOf(result)).contains("reflect.test.PingService");
        assertThat(result.get("descriptorSetBase64").asText()).isNotEmpty();
        // The transitive well-known-type dependency is resolved into the set.
        assertThat(result.get("fileCount").asInt()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void reflectedDescriptorSetDrivesTheOtherActions() throws Exception {
        ObjectNode result = action.execute(input(reflectiveName), ActionContext.create());
        String descriptorSet = result.get("descriptorSetBase64").asText();

        // The descriptor set reflected off the wire is a valid schema source for the catalog.
        ObjectNode listInput = MAPPER.createObjectNode();
        listInput.putObject("schema").put("descriptorSetBase64", descriptorSet);
        var types = ai.pipestream.proto.actions.ActionCatalog
                .defaults(ActionContext.create())
                .execute("list-types", listInput);
        assertThat(types.get("types").findValuesAsText("fullName"))
                .contains("reflect.test.Ping", "reflect.test.Pong");
    }

    @Test
    void serverWithoutReflectionReturnsOkFalse() throws Exception {
        ObjectNode result = action.execute(input(bareName), ActionContext.create());
        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("error").asText()).isNotEmpty();
    }

    private static List<String> servicesOf(JsonNode result) {
        List<String> names = new java.util.ArrayList<>();
        result.get("services").forEach(n -> names.add(n.asText()));
        return names;
    }
}
