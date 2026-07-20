package ai.pipestream.proto.serve;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The console mount and its same-origin API bridges: static assets with SPA fallback at
 * /console, /api/protomolt onto the in-process registry, /api/serve back onto the verbs.
 */
class ConsoleMountTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    static Path registryDir;

    private static ProtoMoltServe serve;
    private static HttpClient http;
    private static String base;

    @BeforeAll
    static void start() throws Exception {
        serve = ProtoMoltServe.start(new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, Files.createDirectories(registryDir.resolve("git")), 0));
        http = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        base = "http://127.0.0.1:" + serve.httpPort();
    }

    @AfterAll
    static void stop() {
        serve.close();
    }

    private static HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void consoleServesIndexAndAssets() throws Exception {
        HttpResponse<String> index = get("/console/");
        assertThat(index.statusCode()).isEqualTo(200);
        assertThat(index.headers().firstValue("content-type")).contains("text/html; charset=utf-8");
        assertThat(index.body()).contains("console-test-marker");

        HttpResponse<String> asset = get("/console/assets/app-abc123.js");
        assertThat(asset.statusCode()).isEqualTo(200);
        assertThat(asset.headers().firstValue("content-type"))
                .contains("text/javascript; charset=utf-8");
        assertThat(asset.headers().firstValue("cache-control"))
                .contains("public, max-age=31536000, immutable");

        assertThat(get("/console/assets/missing.js").statusCode()).isEqualTo(404);
    }

    @Test
    void bareConsoleRedirectsAndRouterPathsFallBackToIndex() throws Exception {
        HttpResponse<String> bare = get("/console");
        assertThat(bare.statusCode()).isEqualTo(308);
        assertThat(bare.headers().firstValue("location")).contains("/console/");

        // The SPA router owns everything that is not a bundled file — including
        // subject paths with file extensions and encoded slashes.
        HttpResponse<String> route = get("/console/schema-registry/subjects");
        assertThat(route.statusCode()).isEqualTo(200);
        assertThat(route.body()).contains("console-test-marker");
        HttpResponse<String> subject =
                get("/console/schema-registry/subjects/demo%2Fshop%2Fv1%2Fshop.proto");
        assertThat(subject.statusCode()).isEqualTo(200);
        assertThat(subject.body()).contains("console-test-marker");
    }

    @Test
    void registryBridgeSpeaksTheConfluentProtocol() throws Exception {
        HttpResponse<String> subjects = get("/api/protomolt/subjects");
        assertThat(subjects.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(subjects.body()).isArray()).isTrue();
    }

    @Test
    void registryBridgePreservesEncodedSlashesInSubjects() throws Exception {
        HttpResponse<String> register = http.send(
                HttpRequest.newBuilder(URI.create(
                        base + "/api/protomolt/subjects/demo%2Fping.proto/versions"))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("""
                                {"schemaType": "PROTOBUF", "schema":
                                 "syntax = \\"proto3\\"; message Ping { int32 n = 1; }"}
                                """))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(register.statusCode()).isEqualTo(200);

        HttpResponse<String> versions =
                get("/api/protomolt/subjects/demo%2Fping.proto/versions");
        assertThat(versions.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(versions.body()).get(0).asInt()).isEqualTo(1);
    }

    @Test
    void serveBridgeReachesTheVerbs() throws Exception {
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create(base + "/api/serve/grpc-json/ProtoMoltService/Compile"))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("""
                                {"sources": {"t.proto":
                                  "syntax = \\"proto3\\"; message T { int32 n = 1; }"}}
                                """))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode result = MAPPER.readTree(response.body());
        assertThat(result.path("ok").asBoolean()).isTrue();
    }

    @Test
    void nestedBridgePathsAreRejected() throws Exception {
        assertThat(get("/api/serve/api/serve/grpc-json/x").statusCode()).isEqualTo(400);
    }

    /**
     * The JDK HTTP server matches a context by plain string prefix, so /api/servexyz is
     * dispatched to the /api/serve bridge. Without a segment-boundary check the remainder
     * has no leading slash and the forwarded URI is malformed.
     */
    @Test
    void pathsThatOnlyPrefixMatchTheBridgeAreNotForwarded() throws Exception {
        assertThat(get("/api/servexyz").statusCode()).isEqualTo(404);
        assertThat(get("/api/protomoltxyz/subjects").statusCode()).isEqualTo(404);
    }
}
