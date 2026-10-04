package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerInterceptors;
import io.grpc.Metadata;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.ProtoFileDescriptorSupplier;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.ServerCalls;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class GitBackedReflectedAnyActionTest {

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

    @Test
    void reflectedDescriptorCommittedToGitReplaysIntoDynamicAnyInvocationAfterRestart() throws Exception {
        CompiledProtos protos = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("reflected/any/v1/service.proto", PROTO, "test").build());
        FileDescriptor file = protos.descriptorFor("reflected/any/v1/service.proto").orElseThrow();
        var protoExchange = file.findServiceByName("AnyService").findMethodByName("Exchange");
        var exchange = DynamicGrpcCalls.methodDescriptor(protoExchange);
        var grpcService = io.grpc.ServiceDescriptor.newBuilder("reflected.any.v1.AnyService")
                .setSchemaDescriptor((ProtoFileDescriptorSupplier) () -> file)
                .addMethod(exchange)
                .build();
        String serverName = "git-reflected-any-" + UUID.randomUUID();
        AtomicInteger reflectionCalls = new AtomicInteger();
        ServerInterceptor countReflection = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                reflectionCalls.incrementAndGet();
                return next.startCall(call, headers);
            }
        };
        Server server = InProcessServerBuilder.forName(serverName)
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
                .addService(ServerInterceptors.intercept(ProtoReflectionServiceV1.newInstance(),
                        countReflection))
                .build()
                .start();
        try {
            Path gitDir = directory.resolve("registry");
            Path profileDir = directory.resolve("workspace");
            ChannelFactory channels = (ignored, tls) -> InProcessChannelBuilder.forName(serverName).build();
            String fingerprint;
            int reflectedCallsAtRegistration;
            try (GitSchemaRegistryStore registry = GitSchemaRegistryStore.builder()
                    .repositoryDir(gitDir).build()) {
                var profiles = new FileSystemServiceProfileRepository(profileDir);
                ActionCatalog catalog = ServiceWorkspaceActions.register(
                        ActionCatalog.defaults(ActionContext.create()), profiles, registry, channels);
                catalog.execute("service-register", profile());
                fingerprint = profiles.find("closure-local").orElseThrow()
                        .getSchemaSource().getDescriptorFingerprint();
                assertThat(fingerprint).matches("[0-9a-f]{64}");
                assertThat(registry.descriptorSet(fingerprint)).isPresent();
                reflectedCallsAtRegistration = reflectionCalls.get();
                assertThat(reflectedCallsAtRegistration).isPositive();
            }

            Path committedDescriptor = gitDir.resolve("descriptors/sha256/" + fingerprint + ".pb");
            assertThat(Files.readAllBytes(committedDescriptor)).isNotEmpty();
            try (Git git = Git.open(gitDir.toFile())) {
                assertThat(git.log().call()).extracting(commit -> commit.getFullMessage().strip())
                        .containsExactly("Store descriptor " + fingerprint);
            }

            try (GitSchemaRegistryStore reopenedRegistry = GitSchemaRegistryStore.builder()
                    .repositoryDir(gitDir).build()) {
                var reopenedProfiles = new FileSystemServiceProfileRepository(profileDir);
                assertThat(reopenedRegistry.descriptorSet(fingerprint).orElseThrow().toByteArray())
                        .isEqualTo(Files.readAllBytes(committedDescriptor));
                ActionCatalog restarted = ServiceWorkspaceActions.register(
                        ActionCatalog.defaults(ActionContext.create()), reopenedProfiles,
                        reopenedRegistry, channels);
                Map<String, String> skipped = ReflectedServiceActions.registerStored(
                        restarted, reopenedProfiles, reopenedRegistry, channels);
                assertThat(skipped).isEmpty();
                assertThat(restarted.names()).contains("closure-local-exchange");

                ObjectNode request = MAPPER.createObjectNode();
                request.set("evidence", MAPPER.createObjectNode()
                        .put("@type", "type.googleapis.com/reflected.any.v1.Evidence")
                        .put("label", "git-replayed-payload"));
                ObjectNode result = restarted.execute("closure-local-exchange", request);
                assertThat(result.path("evidence").path("@type").asText())
                        .isEqualTo("type.googleapis.com/reflected.any.v1.Evidence");
                assertThat(result.path("evidence").path("label").asText())
                        .isEqualTo("git-replayed-payload");
                assertThat(reflectionCalls.get()).isEqualTo(reflectedCallsAtRegistration);
            }
        } finally {
            server.shutdownNow();
        }
    }

    private static ObjectNode profile() {
        ObjectNode input = MAPPER.createObjectNode();
        ObjectNode profile = input.putObject("profile");
        profile.put("name", "closure-local");
        profile.put("description", "Reflected Any Git replay test");
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
