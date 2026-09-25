package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.ProtoFileDescriptorSupplier;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.ServerCalls;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;

class ReflectedAnyActionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROTO = """
            syntax = "proto3";
            package reflected.any.v1;
            import "google/protobuf/any.proto";
            message Evidence { string label = 1; }
            message ExchangeRequest { google.protobuf.Any evidence = 1; }
            message ExchangeResponse { google.protobuf.Any evidence = 1; }
            service AnyService { rpc Exchange(ExchangeRequest) returns (ExchangeResponse); }
            """;

    @TempDir
    Path directory;
    private Server server;
    private String serverName;

    @AfterEach
    void stop() {
        if (server != null) server.shutdownNow();
    }

    @Test
    void catalogParsesAndRendersAnyTypesFromTheReflectedMethodClosure() throws Exception {
        CompiledProtos protos = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("reflected/any/v1/service.proto", PROTO, "test").build());
        FileDescriptor file = protos.descriptorFor("reflected/any/v1/service.proto").orElseThrow();
        ServiceDescriptor service = file.findServiceByName("AnyService");
        var protoExchange = service.findMethodByName("Exchange");
        var exchange = DynamicGrpcCalls.methodDescriptor(protoExchange);
        var grpcService = io.grpc.ServiceDescriptor.newBuilder(service.getFullName())
                .setSchemaDescriptor((ProtoFileDescriptorSupplier) () -> file)
                .addMethod(exchange)
                .build();
        serverName = "reflected-any-" + UUID.randomUUID();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerServiceDefinition.builder(grpcService)
                        .addMethod(exchange, ServerCalls.asyncUnaryCall((DynamicMessage request,
                                io.grpc.stub.StreamObserver<DynamicMessage> response) -> {
                            DynamicMessage reply = DynamicMessage.newBuilder(protoExchange.getOutputType())
                                    .setField(protoExchange.getOutputType().findFieldByName("evidence"),
                                            request.getField(protoExchange.getInputType()
                                                    .findFieldByName("evidence")))
                                    .build();
                            response.onNext(reply);
                            response.onCompleted();
                        }))
                        .build())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();

        var repository = new FileSystemServiceProfileRepository(directory);
        ActionCatalog catalog = ServiceWorkspaceActions.register(ActionCatalog.defaults(
                        ActionContext.create()), repository,
                (ignored, tls) -> InProcessChannelBuilder.forName(serverName).build());

        catalog.execute("service-register", profile());
        String verb = "closure-local-exchange";
        ObjectNode request = MAPPER.createObjectNode();
        request.set("evidence",
                MAPPER.createObjectNode()
                        .put("@type", "type.googleapis.com/reflected.any.v1.Evidence")
                        .put("label", "closure-payload"));
        ObjectNode result = catalog.execute(verb, request);

        assertThat(result.path("evidence").path("@type").asText())
                .isEqualTo("type.googleapis.com/reflected.any.v1.Evidence");
        assertThat(result.path("evidence").path("label").asText()).isEqualTo("closure-payload");
    }

    private static ObjectNode profile() {
        ObjectNode input = MAPPER.createObjectNode();
        ObjectNode profile = input.putObject("profile");
        profile.put("name", "closure-local");
        profile.put("description", "Reflected Any closure test");
        ObjectNode endpoint = profile.putArray("endpoints").addObject();
        endpoint.put("name", "local");
        endpoint.put("host", "localhost");
        endpoint.put("port", 50051);
        endpoint.put("transport", "TRANSPORT_PLAINTEXT");
        input.put("endpoint", "local");
        input.put("deadlineMs", 5000);
        return input;
    }
}
