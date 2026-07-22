package ai.pipestream.proto.serve;

import ai.pipestream.proto.grpc.service.ProtoMoltServiceSchema;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * One {@code --api-token} guards every operational surface: gRPC calls, REST verbs, and the
 * MCP endpoint. Documentation surfaces (health, OpenAPI, Swagger UI) stay open, and the
 * OpenAPI document declares the scheme so Swagger UI's Authorize button works.
 */
class SecuredServeTest {

    private static final String TOKEN = "sekret-7";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ProtoMoltServe serve;
    private static HttpClient http;
    private static String base;

    @BeforeAll
    static void start() {
        serve = ProtoMoltServe.start(
                new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0, TOKEN));
        http = HttpClient.newHttpClient();
        base = "http://127.0.0.1:" + serve.httpPort();
    }

    @AfterAll
    static void stop() {
        serve.close();
    }

    private static HttpResponse<String> post(String path, String body, String... headers)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        for (int i = 0; i < headers.length; i += 2) {
            request.header(headers[i], headers[i + 1]);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static final String COMPILE_BODY = """
            {"sources": {"t.proto": "syntax = \\"proto3\\"; message T { int32 n = 1; }"}}
            """;

    @Test
    void restRefusesWithoutTheToken() throws Exception {
        HttpResponse<String> response = post("/grpc-json/ProtoMoltService/Compile", COMPILE_BODY);
        assertThat(response.statusCode()).isEqualTo(401);
    }

    @Test
    void restAcceptsTheToken() throws Exception {
        HttpResponse<String> response = post("/grpc-json/ProtoMoltService/Compile", COMPILE_BODY,
                "api_token", TOKEN);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(response.body()).path("ok").asBoolean()).isTrue();
    }

    @Test
    void mcpRefusesWithoutTheTokenAndAcceptsBothForms() throws Exception {
        String ping = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}";
        assertThat(post("/mcp", ping).statusCode()).isEqualTo(401);
        assertThat(post("/mcp", ping, "api_token", TOKEN).statusCode()).isEqualTo(200);
        assertThat(post("/mcp", ping, "authorization", "Bearer " + TOKEN).statusCode())
                .isEqualTo(200);
        assertThat(post("/mcp", ping, "api_token", "wrong").statusCode()).isEqualTo(401);
    }

    @Test
    void grpcRefusesWithoutTheTokenAndAcceptsMetadata() throws Exception {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("127.0.0.1", serve.grpcPort())
                .usePlaintext()
                .build();
        try {
            var method = ProtoMoltServiceSchema.service().findMethodByName("ListTypes");
            DynamicMessage request = DynamicMessage.newBuilder(method.getInputType()).build();

            StatusRuntimeException refused = catchThrowableOfType(StatusRuntimeException.class,
                    () -> DynamicGrpcCalls.call(channel, method, request,
                            CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS),
                            new Metadata(), 4));
            assertThat(refused.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);

            Metadata credentials = new Metadata();
            credentials.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), TOKEN);
            var responses = DynamicGrpcCalls.call(channel, method, request,
                    CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS),
                    credentials, 4);
            assertThat(responses).hasSize(1);
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void reflectionIsGuardedToo() {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("127.0.0.1", serve.grpcPort())
                .usePlaintext()
                .build();
        try {
            assertThatThrownBy(() -> ai.pipestream.proto.grpc.invoke.ReflectionClient
                    .discover(channel, 5_000))
                    .hasMessageContaining("UNAUTHENTICATED");
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void documentationSurfacesStayOpen() throws Exception {
        for (String path : new String[] {"/health", "/openapi.json", "/docs"}) {
            HttpResponse<String> response = http.send(
                    HttpRequest.newBuilder(URI.create(base + path)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).as(path).isEqualTo(200);
        }
    }

    @Test
    void openApiDeclaresTheSecurityScheme() throws Exception {
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create(base + "/openapi.json")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode document = MAPPER.readTree(response.body());
        assertThat(document.path("components").path("securitySchemes").isObject()).isTrue();
        assertThat(document.path("paths").path("/grpc-json/ProtoMoltService/Compile")
                .path("post").has("security")).isTrue();
    }
}
