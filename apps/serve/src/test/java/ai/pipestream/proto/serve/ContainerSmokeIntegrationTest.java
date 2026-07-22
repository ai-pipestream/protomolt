package ai.pipestream.proto.serve;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.grpc.invoke.ReflectionClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The serve Docker image as a real protomolt: the image is built from apps/serve/Dockerfile,
 * started with {@code --demo}, and every published surface answers over the mapped ports —
 * REST health, MCP on streamable HTTP, gRPC reflection driving a dynamic call, and the demo
 * registry. Waiting on the image's own HEALTHCHECK exercises the /dev/tcp probe, and a
 * healthy start proves the non-root user can read the chmod-normalized app directory.
 * The suite skips when Docker is unavailable.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class ContainerSmokeIntegrationTest {

    private static final String SERVICE = "ai.pipestream.protomolt.v1.ProtoMoltService";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    // The build context is exactly what CI ships: the Dockerfile plus the installDist
    // output it COPYs (the test task depends on installDist). Paths are relative to the
    // module directory, the test JVM's working directory under Gradle. During the
    // healthcheck's 15s start-period the engine probes at its default start-interval, so a
    // healthy start is detected in seconds; the timeout leaves room for the image build's
    // base-layer pull on a cold machine.
    @Container
    static final GenericContainer<?> SERVE = new GenericContainer<>(
            new ImageFromDockerfile()
                    .withFileFromPath("Dockerfile", Path.of("Dockerfile"))
                    .withFileFromPath("build/install", Path.of("build/install")))
            .withCommand("--demo")
            .withExposedPorts(8080, 9090, 8081)
            .waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(3)));

    private static String http(int containerPort) {
        return "http://" + SERVE.getHost() + ":" + SERVE.getMappedPort(containerPort);
    }

    private static HttpResponse<String> get(int containerPort, String path) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(http(containerPort) + path))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void healthAnswersOnTheRestPort() throws Exception {
        HttpResponse<String> response = get(8080, "/health");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("UP");
    }

    @Test
    void mcpInitializesOverStreamableHttp() throws Exception {
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(
                                URI.create(http(8080) + "/mcp"))
                        .header("content-type", "application/json")
                        .header("accept", "application/json, text/event-stream")
                        .POST(HttpRequest.BodyPublishers.ofString("""
                                {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                                  "protocolVersion":"2025-06-18","capabilities":{},
                                  "clientInfo":{"name":"smoke","version":"0"}}}
                                """))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode result = MAPPER.readTree(response.body()).path("result");
        assertThat(result.path("protocolVersion").asText()).isEqualTo("2025-06-18");
        assertThat(result.path("serverInfo").path("name").asText()).isEqualTo("protomolt");
    }

    @Test
    void grpcReflectionDrivesAListTypesCall() throws Exception {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(SERVE.getHost(), SERVE.getMappedPort(9090))
                .usePlaintext()
                .build();
        try {
            ReflectionClient.Result discovered = ReflectionClient.discover(channel, 10_000);
            assertThat(discovered.services()).contains(SERVICE);

            MethodDescriptor listTypes = findMethod(discovered.descriptorSet(), "ListTypes");
            DynamicMessage request = DynamicMessage.newBuilder(listTypes.getInputType())
                    .setField(listTypes.getInputType().findFieldByName("filter"), "demo.shop")
                    .build();
            List<DynamicMessage> responses = DynamicGrpcCalls.call(channel, listTypes, request,
                    CallOptions.DEFAULT.withDeadlineAfter(30, TimeUnit.SECONDS),
                    new Metadata(), 1);

            DynamicMessage response = responses.get(0);
            FieldDescriptor types = response.getDescriptorForType().findFieldByName("types");
            List<String> fullNames = new ArrayList<>();
            for (int i = 0; i < response.getRepeatedFieldCount(types); i++) {
                Message entry = (Message) response.getRepeatedField(types, i);
                fullNames.add((String) entry.getField(
                        entry.getDescriptorForType().findFieldByName("full_name")));
            }
            assertThat(fullNames).contains("demo.shop.v1.Order", "demo.shop.v1.Customer");
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void demoRegistryServesItsSubjectsAndChains() throws Exception {
        HttpResponse<String> subjects = get(8081, "/subjects");
        assertThat(subjects.statusCode()).isEqualTo(200);
        assertThat(subjects.body()).contains(DemoSchemas.SHOP_SUBJECT);

        HttpResponse<String> chains = get(8081, "/protomolt/chains");
        assertThat(chains.statusCode()).isEqualTo(200);
        assertThat(chains.body()).contains("compile-and-list");
    }

    /** Builds the reflected files in dependency order and resolves a ProtoMoltService method. */
    private static MethodDescriptor findMethod(FileDescriptorSet set, String method)
            throws DescriptorValidationException {
        Map<String, FileDescriptorProto> protos = new LinkedHashMap<>();
        for (FileDescriptorProto file : set.getFileList()) {
            protos.put(file.getName(), file);
        }
        Map<String, FileDescriptor> built = new HashMap<>();
        for (String name : protos.keySet()) {
            build(name, protos, built);
        }
        MethodDescriptor found = built.values().stream()
                .flatMap(file -> file.getServices().stream())
                .filter(service -> service.getFullName().equals(SERVICE))
                .findFirst()
                .orElseThrow(() -> new AssertionError(SERVICE + " is not in the reflected set"))
                .findMethodByName(method);
        assertThat(found).as(method).isNotNull();
        return found;
    }

    private static FileDescriptor build(String name, Map<String, FileDescriptorProto> protos,
                                        Map<String, FileDescriptor> built)
            throws DescriptorValidationException {
        FileDescriptor existing = built.get(name);
        if (existing != null) {
            return existing;
        }
        FileDescriptorProto proto = protos.get(name);
        FileDescriptor[] dependencies = new FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < dependencies.length; i++) {
            dependencies[i] = build(proto.getDependency(i), protos, built);
        }
        FileDescriptor descriptor = FileDescriptor.buildFrom(proto, dependencies);
        built.put(name, descriptor);
        return descriptor;
    }
}
