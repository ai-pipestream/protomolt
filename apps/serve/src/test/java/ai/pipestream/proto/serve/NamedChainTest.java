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
 * Named chains end to end through the one-process server: PUT gated by check-chain,
 * LIST/GET back out, and run-chain resolving the stored name — the chain here composes the
 * server's own verbs (Compile then ListTypes), so no second service is needed.
 */
class NamedChainTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    static Path registryDir;

    private static ProtoMoltServe serve;
    private static HttpClient http;
    private static String base;
    private static String registryBase;

    @BeforeAll
    static void start() throws Exception {
        serve = ProtoMoltServe.start(new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, Files.createDirectories(registryDir.resolve("git")), 0));
        http = HttpClient.newHttpClient();
        base = "http://127.0.0.1:" + serve.httpPort();
        registryBase = "http://127.0.0.1:" + serve.registryPort();
    }

    @AfterAll
    static void stop() {
        serve.close();
    }

    /** A chain over ProtoMoltService itself: compile inline sources, then list the types. */
    private static String chainJson() {
        String selfProto = ProtoMoltServiceJson();
        return """
                {"name": "compile-and-list",
                 "schema": {"sources": {"%s": %s}},
                 "inputType": "ai.pipestream.protomolt.v1.CompileRequest",
                 "steps": [
                   {"name": "compiled", "target": "127.0.0.1:%d",
                    "method": "ai.pipestream.protomolt.v1.ProtoMoltService/Compile",
                    "rules": ["sources = input.sources"]},
                   {"name": "types", "target": "127.0.0.1:%d",
                    "method": "ai.pipestream.protomolt.v1.ProtoMoltService/ListTypes",
                    "rules": ["schema.descriptor_set_base64 = compiled.descriptor_set_base64"]}
                 ]}
                """.formatted(
                ai.pipestream.proto.grpc.service.ProtoMoltServiceSchema.RESOURCE_PATH,
                selfProto, serve.grpcPort(), serve.grpcPort());
    }

    private static String ProtoMoltServiceJson() {
        try {
            return MAPPER.writeValueAsString(
                    ai.pipestream.proto.grpc.service.ProtoMoltServiceSchema.protoSource());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void putIsGatedListedAndRunByName() throws Exception {
        // A broken chain is rejected by the check-chain write gate with findings.
        String broken = chainJson().replace("compiled.descriptor_set_base64",
                "compiled.no_such_field");
        HttpResponse<String> rejected = http.send(
                HttpRequest.newBuilder(URI.create(registryBase + "/protomolt/chains/compile-and-list"))
                        .header("content-type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(broken)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(rejected.statusCode()).isEqualTo(422);
        assertThat(rejected.body()).contains("no_such_field");

        // The good chain stores, lists, and reads back.
        HttpResponse<String> stored = http.send(
                HttpRequest.newBuilder(URI.create(registryBase + "/protomolt/chains/compile-and-list"))
                        .header("content-type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(chainJson())).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(stored.statusCode()).isEqualTo(200);

        HttpResponse<String> listed = http.send(
                HttpRequest.newBuilder(URI.create(registryBase + "/protomolt/chains")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(MAPPER.readTree(listed.body()).get(0).asText()).isEqualTo("compile-and-list");

        // run-chain resolves the stored name and composes the server's own verbs.
        HttpResponse<String> run = http.send(
                HttpRequest.newBuilder(URI.create(base + "/grpc-json/ProtoMoltService/RunChain"))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("""
                                {"chainName": "compile-and-list",
                                 "input": {"sources": {"t.proto":
                                   "syntax = \\"proto3\\"; package t; message Thing { int32 n = 1; }"}}}
                                """)).build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode result = MAPPER.readTree(run.body());
        assertThat(result.path("ok").asBoolean())
                .as(run.body())
                .isTrue();
        assertThat(result.path("steps")).hasSize(2);
        assertThat(result.path("output").path("types").findValuesAsText("fullName"))
                .contains("t.Thing");

        // An unknown name answers cleanly.
        HttpResponse<String> missing = http.send(
                HttpRequest.newBuilder(URI.create(base + "/grpc-json/ProtoMoltService/RunChain"))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"chainName\": \"nope\", \"input\": {}}")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(MAPPER.readTree(missing.body()).path("error").asText())
                .contains("No stored chain named 'nope'");
    }
}
