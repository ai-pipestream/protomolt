package ai.pipestream.proto.server.jdk;

import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import ai.pipestream.proto.rest.ApiTokenRequirement;
import ai.pipestream.proto.rest.ProtoApiTokenValidator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.rest.ProtoRestMethod;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class JdkProtoRestServerTest {

    private JdkProtoRestServer server;
    private int port;
    private HttpClient client;

    @BeforeEach
    void setUp() {
        server = new JdkProtoRestServer(
                ProtoToolsServerConfig.defaults().withPort(0).withHost("127.0.0.1").withMaxRequestBytes(2048),
                new ProtoRestGateway(
                        newRegistry(),
                        new ProtobufJsonTranscoder(),
                        ProtoApiTokenValidator.sharedSecret("secret")));
        port = server.start();
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    static ProtoRestMethodRegistry newRegistry() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("EchoService", "Echo", request -> {
                    Struct in = (Struct) request;
                    String name = in.getFieldsOrDefault("name", Value.getDefaultInstance()).getStringValue();
                    return Struct.newBuilder()
                            .putFields("message", Value.newBuilder().setStringValue("hello " + name).build())
                            .build();
                })
                .requestType(Struct.class)
                .apiToken(ApiTokenRequirement.apiKeyHeader("api_token"))
                .summary("Echo")
                .httpMethods("GET", "POST", "PUT", "PATCH", "DELETE")
                .build());
        registry.register(ProtoRestMethod.builder("RestrictedService", "PostOnly",
                        request -> Struct.getDefaultInstance())
                .requestType(Struct.class)
                .httpMethods("POST")
                .build());
        registry.register(ProtoRestMethod.builder("BoomService", "Boom", request -> {
                    throw new RuntimeException("kaboom-secret-detail");
                })
                .requestType(Struct.class)
                .build());
        return registry;
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void healthOpenApiAndInvoke() throws Exception {
        assertThat(get("/health").statusCode()).isEqualTo(200);
        assertThat(get("/openapi.json").body()).contains("EchoService");

        HttpResponse<String> ok = client.send(
                HttpRequest.newBuilder(uri("/grpc-json/EchoService/Echo"))
                        .header("content-type", "application/json")
                        .header("api_token", "secret")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"jdk\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(ok.statusCode()).isEqualTo(200);
        assertThat(ok.body()).contains("hello jdk");
    }

    @Test
    void getAndDeleteInvokeWithEmptyJsonBody() throws Exception {
        HttpResponse<String> viaGet = client.send(
                HttpRequest.newBuilder(uri("/grpc-json/EchoService/Echo"))
                        .header("api_token", "secret")
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(viaGet.statusCode()).isEqualTo(200);
        assertThat(viaGet.body()).contains("hello ");

        HttpResponse<String> viaDelete = client.send(
                HttpRequest.newBuilder(uri("/grpc-json/EchoService/Echo"))
                        .header("api_token", "secret")
                        .DELETE()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(viaDelete.statusCode()).isEqualTo(200);
        assertThat(viaDelete.body()).contains("hello ");
    }

    @Test
    void undocumentedMethodStill405sWithAllowHeader() throws Exception {
        HttpResponse<String> options = client.send(
                HttpRequest.newBuilder(uri("/grpc-json/EchoService/Echo"))
                        .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(options.statusCode()).isEqualTo(405);
        assertThat(options.headers().firstValue("allow")).contains("GET, POST, PUT, PATCH, DELETE");
    }

    @Test
    void declaredHttpMethodsAreEnforcedWith405AndAllow() throws Exception {
        HttpResponse<String> viaGet = get("/grpc-json/RestrictedService/PostOnly");
        assertThat(viaGet.statusCode()).isEqualTo(405);
        assertThat(viaGet.headers().firstValue("allow")).contains("POST");

        HttpResponse<String> viaPost = client.send(
                HttpRequest.newBuilder(uri("/grpc-json/RestrictedService/PostOnly"))
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(viaPost.statusCode()).isEqualTo(200);
    }

    @Test
    void nonGetOnHealthIs405WithAllowGet() throws Exception {
        HttpResponse<String> res = client.send(
                HttpRequest.newBuilder(uri("/health"))
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).isEqualTo(405);
        assertThat(res.headers().firstValue("allow")).contains("GET");
    }

    @Test
    void healthAndOpenApiRequireExactPaths() throws Exception {
        assertThat(get("/healthzzz").statusCode()).isEqualTo(404);
        assertThat(get("/health/anything").statusCode()).isEqualTo(404);
        assertThat(get("/openapi.jsonX").statusCode()).isEqualTo(404);
    }

    @Test
    void trailingSlashOnInvokeRouteIs404() throws Exception {
        HttpResponse<String> res = client.send(
                HttpRequest.newBuilder(uri("/grpc-json/EchoService/Echo/"))
                        .header("api_token", "secret")
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).isEqualTo(404);
    }

    @Test
    void malformedPercentEncodingInQueryIs400() throws Exception {
        assertThat(rawStatusCode("GET /grpc-json/EchoService/Echo?x=%zz HTTP/1.1")).isEqualTo(400);
    }

    @Test
    void oversizedBodyIs413() throws Exception {
        HttpResponse<String> res = client.send(
                HttpRequest.newBuilder(uri("/grpc-json/EchoService/Echo"))
                        .header("api_token", "secret")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"name\":\"" + "x".repeat(8192) + "\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).isEqualTo(413);
    }

    @Test
    void serverErrorBodyIsGeneric() throws Exception {
        HttpResponse<String> res = client.send(
                HttpRequest.newBuilder(uri("/grpc-json/BoomService/Boom"))
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).isEqualTo(500);
        assertThat(res.body()).contains("Internal server error");
        assertThat(res.body()).doesNotContain("kaboom-secret-detail");
    }

    @Test
    void defaultGatewayFailsClosedForTokenProtectedMethods() throws Exception {
        try (JdkProtoRestServer failClosed = new JdkProtoRestServer(
                ProtoToolsServerConfig.defaults().withPort(0).withHost("127.0.0.1"),
                new ProtoRestGateway(newRegistry(), new ProtobufJsonTranscoder()))) {
            int failClosedPort = failClosed.start();
            HttpResponse<String> res = client.send(
                    HttpRequest.newBuilder(URI.create(
                                    "http://127.0.0.1:" + failClosedPort + "/grpc-json/EchoService/Echo"))
                            .header("api_token", "any-junk-token")
                            .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"x\"}"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(res.statusCode()).isEqualTo(401);
        }
    }

    private int rawStatusCode(String requestLine) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            OutputStream out = socket.getOutputStream();
            out.write((requestLine + "\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.flush();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
            String statusLine = reader.readLine();
            assertThat(statusLine).isNotNull();
            return Integer.parseInt(statusLine.split(" ")[1]);
        }
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + port + path);
    }
}
