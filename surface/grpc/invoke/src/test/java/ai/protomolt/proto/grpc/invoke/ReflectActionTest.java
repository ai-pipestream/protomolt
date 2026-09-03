package ai.protomolt.proto.grpc.invoke;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
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
    private static Server legacyReflectiveServer;
    private static Server deniedStableServer;
    private static Server bareServer;
    private static String reflectiveName;
    private static String legacyReflectiveName;
    private static String deniedStableName;
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

        legacyReflectiveName = InProcessServerBuilder.generateName();
        legacyReflectiveServer = InProcessServerBuilder.forName(legacyReflectiveName)
                .addService(definition)
                .addService(ProtoReflectionService.newInstance())
                .build()
                .start();

        deniedStableName = InProcessServerBuilder.generateName();
        deniedStableServer = InProcessServerBuilder.forName(deniedStableName)
                .addService(definition)
                .addService(ServerInterceptors.intercept(
                        ProtoReflectionServiceV1.newInstance(),
                        new ServerInterceptor() {
                            @Override
                            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                                    ServerCall<ReqT, RespT> call, Metadata headers,
                                    ServerCallHandler<ReqT, RespT> next) {
                                call.close(Status.PERMISSION_DENIED
                                        .withDescription("stable reflection denied"),
                                        new Metadata());
                                return new ServerCall.Listener<>() {
                                };
                            }
                        }))
                .addService(ProtoReflectionService.newInstance())
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
        legacyReflectiveServer.shutdownNow();
        deniedStableServer.shutdownNow();
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
        ObjectNode result = dispatch(action, input(reflectiveName));
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(servicesOf(result)).contains("reflect.test.PingService");
        assertThat(result.get("descriptorSetBase64").asText()).isNotEmpty();
        // The transitive well-known-type dependency is resolved into the set.
        assertThat(result.get("fileCount").asInt()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void fallsBackToLegacyReflectionAndResolvesDescriptorSet() throws Exception {
        ObjectNode result = dispatch(action, input(legacyReflectiveName));

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(servicesOf(result)).contains("reflect.test.PingService");
        assertThat(result.get("descriptorSetBase64").asText()).isNotEmpty();
        assertThat(result.get("fileCount").asInt()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void doesNotDowngradeAfterStableReflectionPermissionFailure() throws Exception {
        ObjectNode result = dispatch(action, input(deniedStableName));

        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("error").asText()).contains("PERMISSION_DENIED");
    }

    @Test
    void reflectedDescriptorSetDrivesTheOtherActions() throws Exception {
        ObjectNode result = dispatch(action, input(reflectiveName));
        String descriptorSet = result.get("descriptorSetBase64").asText();

        // The descriptor set reflected off the wire is a valid schema source for the catalog.
        ObjectNode listInput = MAPPER.createObjectNode();
        listInput.putObject("schema").put("descriptorSetBase64", descriptorSet);
        var types = ai.protomolt.proto.actions.ActionCatalog
                .defaults(ActionContext.create())
                .execute("list-types", listInput);
        assertThat(types.get("types").findValuesAsText("fullName"))
                .contains("reflect.test.Ping", "reflect.test.Pong");
    }

    @Test
    void serverWithoutReflectionReturnsOkFalse() throws Exception {
        ObjectNode result = dispatch(action, input(bareName));
        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("error").asText()).isNotEmpty();
    }

    private static List<String> servicesOf(JsonNode result) {
        List<String> names = new java.util.ArrayList<>();
        result.get("services").forEach(n -> names.add(n.asText()));
        return names;
    }

    /**
     * Dispatches the way every surface does: through a catalog holding the verb, which is
     * where the request contract is checked before the verb runs.
     */
    private static ObjectNode dispatch(ProtoAction verb, ObjectNode input)
            throws ActionException {
        return ActionCatalog.defaults(ActionContext.create())
                .replace(verb).execute(verb.name(), input);
    }

}
