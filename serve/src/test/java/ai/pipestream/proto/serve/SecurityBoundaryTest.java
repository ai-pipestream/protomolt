package ai.pipestream.proto.serve;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Token mode is one boundary around the whole process, not a partial one: the registry
 * listener honors the same secret and bind address as gRPC/REST/MCP, the console (which
 * cannot hold the secret in a browser) is explicitly disabled rather than half-working,
 * and no hand-rolled handler reads request bodies unbounded.
 */
class SecurityBoundaryTest {

    private static final String TOKEN = "boundary-sekret";

    private static ProtoMoltServe serve;
    private static HttpClient http;
    private static String base;
    private static String registryBase;

    @BeforeAll
    static void start() {
        // Demo mode mounts a registry in a temp directory, so every surface exists.
        serve = ProtoMoltServe.start(new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, null, 0, TOKEN, true));
        http = HttpClient.newHttpClient();
        base = "http://127.0.0.1:" + serve.httpPort();
        registryBase = "http://127.0.0.1:" + serve.registryPort();
    }

    @AfterAll
    static void stop() {
        serve.close();
        http.close();
    }

    private static HttpResponse<String> get(String url, String... headers) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url)).GET();
        for (int i = 0; i < headers.length; i += 2) {
            request.header(headers[i], headers[i + 1]);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void theRegistryHonorsTheSharedSecret() throws Exception {
        assertThat(get(registryBase + "/subjects").statusCode()).isEqualTo(401);
        assertThat(get(registryBase + "/subjects", "api_token", TOKEN).statusCode())
                .isEqualTo(200);
        assertThat(get(registryBase + "/health").statusCode()).isEqualTo(200);
    }

    @Test
    void theConsoleAndItsProxiesAreExplicitlyDisabled() throws Exception {
        for (String path : new String[]{"/console", "/console/",
                "/api/protomolt/subjects", "/api/serve/health"}) {
            HttpResponse<String> response = get(base + path);
            assertThat(response.statusCode()).as(path).isEqualTo(503);
            assertThat(response.body()).as(path).contains("disabled");
        }
    }

    @Test
    void mcpBodiesAreCapped() throws Exception {
        byte[] huge = new byte[17 * 1024 * 1024];
        java.util.Arrays.fill(huge, (byte) ' ');
        HttpRequest oversized = HttpRequest.newBuilder(URI.create(base + "/mcp"))
                .header("content-type", "application/json")
                .header("api_token", TOKEN)
                .POST(HttpRequest.BodyPublishers.ofByteArray(huge))
                .build();
        // The server answers 413 and closes the connection without draining the rest of the
        // body. Whether the client reads that response or hits the reset first is a TCP
        // race; both outcomes prove the cap held, because the upload was refused mid-stream.
        try {
            HttpResponse<String> response = http.send(oversized, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(413);
        } catch (IOException refusedMidStream) {
            // Expected on the reset path; fall through to the health check below.
        }
        // Either way the process must have shrugged the oversized body off, not died on it.
        HttpResponse<String> health = http.send(HttpRequest.newBuilder(
                        URI.create(base + "/health")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(health.statusCode()).isEqualTo(200);
    }

    @Test
    void mcpRejectsAnEmptyBatch() throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create(base + "/mcp"))
                .header("content-type", "application/json")
                .header("api_token", TOKEN)
                .POST(HttpRequest.BodyPublishers.ofString("[]"))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("-32600");
    }
}
