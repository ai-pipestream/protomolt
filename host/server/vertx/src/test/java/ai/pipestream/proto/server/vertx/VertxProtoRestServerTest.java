package ai.pipestream.proto.server.vertx;

import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import ai.pipestream.proto.rest.ApiTokenRequirement;
import ai.pipestream.proto.rest.ProtoApiTokenValidator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.rest.ProtoRestMethod;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(VertxExtension.class)
class VertxProtoRestServerTest {

    private VertxProtoRestServer server;
    private int port;

    @BeforeEach
    void setUp() {
        server = new VertxProtoRestServer(
                ProtoToolsServerConfig.defaults().withPort(0).withHost("127.0.0.1").withMaxRequestBytes(2048),
                new ProtoRestGateway(
                        newRegistry(),
                        new ProtobufJsonTranscoder(),
                        ProtoApiTokenValidator.sharedSecret("secret")));
        port = server.start();
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
    void invokeViaVertx(Vertx vertx, VertxTestContext context) {
        WebClient client = WebClient.create(vertx);
        client.post(port, "127.0.0.1", "/grpc-json/EchoService/Echo")
                .putHeader("content-type", "application/json")
                .putHeader("api_token", "secret")
                .sendBuffer(Buffer.buffer("{\"name\":\"vertx\"}"))
                .onComplete(context.succeeding(response -> context.verify(() -> {
                    assertThat(response.statusCode()).isEqualTo(200);
                    assertThat(response.bodyAsString()).contains("hello vertx");
                    assertThat(server.engineId()).isEqualTo("vertx");
                    context.completeNow();
                })));
    }

    @Test
    void invokeViaGetWithoutBody(Vertx vertx, VertxTestContext context) {
        WebClient client = WebClient.create(vertx);
        client.get(port, "127.0.0.1", "/grpc-json/EchoService/Echo")
                .putHeader("api_token", "secret")
                .send()
                .onComplete(context.succeeding(response -> context.verify(() -> {
                    assertThat(response.statusCode()).isEqualTo(200);
                    assertThat(response.bodyAsString()).contains("hello ");
                    context.completeNow();
                })));
    }

    @Test
    void invokeViaDeleteWithoutBody(Vertx vertx, VertxTestContext context) {
        WebClient client = WebClient.create(vertx);
        client.delete(port, "127.0.0.1", "/grpc-json/EchoService/Echo")
                .putHeader("api_token", "secret")
                .send()
                .onComplete(context.succeeding(response -> context.verify(() -> {
                    assertThat(response.statusCode()).isEqualTo(200);
                    assertThat(response.bodyAsString()).contains("hello ");
                    context.completeNow();
                })));
    }

    @Test
    void doubleStartIsRejected() {
        assertThatThrownBy(server::start).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void undocumentedMethodStill405sWithAllowHeader() throws Exception {
        HttpResponse<String> options = httpClient().send(
                HttpRequest.newBuilder(uri("/grpc-json/EchoService/Echo"))
                        .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(options.statusCode()).isEqualTo(405);
        assertThat(options.headers().firstValue("allow")).contains("GET, POST, PUT, PATCH, DELETE");
    }

    @Test
    void declaredHttpMethodsAreEnforcedWith405AndAllow() throws Exception {
        HttpResponse<String> viaGet = httpClient().send(
                HttpRequest.newBuilder(uri("/grpc-json/RestrictedService/PostOnly")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(viaGet.statusCode()).isEqualTo(405);
        assertThat(viaGet.headers().firstValue("allow")).contains("POST");

        HttpResponse<String> viaPost = httpClient().send(
                HttpRequest.newBuilder(uri("/grpc-json/RestrictedService/PostOnly"))
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(viaPost.statusCode()).isEqualTo(200);
    }

    @Test
    void nonGetOnHealthIs405WithAllowGet() throws Exception {
        HttpResponse<String> res = httpClient().send(
                HttpRequest.newBuilder(uri("/health"))
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).isEqualTo(405);
        assertThat(res.headers().firstValue("allow")).contains("GET");
    }

    @Test
    void healthRequiresExactPathAndTrailingSlashIs404() throws Exception {
        HttpClient client = httpClient();
        assertThat(client.send(
                HttpRequest.newBuilder(uri("/healthzzz")).GET().build(),
                HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(404);
        assertThat(client.send(
                HttpRequest.newBuilder(uri("/grpc-json/EchoService/Echo/"))
                        .header("api_token", "secret")
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(404);
    }

    @Test
    void malformedPercentEncodingInQueryIs400() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            OutputStream out = socket.getOutputStream();
            out.write(("GET /grpc-json/EchoService/Echo?x=%zz HTTP/1.1\r\n"
                    + "Host: 127.0.0.1\r\napi_token: secret\r\nConnection: close\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.flush();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
            String statusLine = reader.readLine();
            assertThat(statusLine).isNotNull();
            assertThat(Integer.parseInt(statusLine.split(" ")[1])).isEqualTo(400);
        }
    }

    @Test
    void oversizedBodyIs413() throws Exception {
        HttpResponse<String> res = httpClient().send(
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
        HttpResponse<String> res = httpClient().send(
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
        VertxProtoRestServer failClosed = new VertxProtoRestServer(
                ProtoToolsServerConfig.defaults().withPort(0).withHost("127.0.0.1"),
                new ProtoRestGateway(newRegistry(), new ProtobufJsonTranscoder()));
        try {
            int failClosedPort = failClosed.start();
            HttpResponse<String> res = httpClient().send(
                    HttpRequest.newBuilder(URI.create(
                                    "http://127.0.0.1:" + failClosedPort + "/grpc-json/EchoService/Echo"))
                            .header("api_token", "any-junk-token")
                            .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"x\"}"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(res.statusCode()).isEqualTo(401);
        } finally {
            failClosed.close();
        }
    }

    private static HttpClient httpClient() {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + port + path);
    }
}
