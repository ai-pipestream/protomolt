package ai.pipestream.proto.acp.agent;

import ai.pipestream.proto.acp.AcpClient;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The agent as a real child process, driven over stdio exactly as an IDE drives it:
 * initialize, open a session, prompt with console lines, read the streamed session/update
 * chunks. The unary leg runs the local {@code compile} verb; the streaming leg points
 * {@code grpc-invoke} at a dynamic server declared inline here, shaped like the
 * samples module's DemoSearchService, and asserts each hit arrives as its own
 * chunk.
 */
// Forking a JVM and running protoc under a fully parallel build is slow; this bound only has
// to catch a genuine hang, so it sits above the client's own request timeout.
@Timeout(value = 4, unit = TimeUnit.MINUTES)
// Excluded from the default build because it forks a JVM per test and shells out to protoc,
// which is slower than anything the default test task runs. Run with
// ./gradlew :protomolt-acp-agent:acpProtocolTest.
@Tag("acp-protocol")
class ProtoMoltAcpAgentProcessTest {

    // Same shape as the samples module DemoSearchService, declared inline so the
    // test needs no generated stubs.
    private static final String PROTO = """
            syntax = "proto3";
            package demo.search.v1;
            service DemoSearch {
              rpc Search(SearchRequest) returns (stream SearchHit);
            }
            message SearchRequest {
              string query = 1;
              int32 hits = 2;
            }
            message SearchHit {
              string doc_id = 1;
              float score = 2;
              string text = 3;
            }
            """;

    private static final String[] TEXTS = {
        "approximate nearest neighbor search with HNSW graphs",
        "vector quantization for billion-scale indexes",
        "recall at high k: merging partial results from many shards",
    };

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Server searchServer;

    // Written by the client's notification listener, read by the test thread, so synchronized
    // rather than StringBuilder.
    private final StringBuffer messages = new StringBuffer();
    private final StringBuffer thoughts = new StringBuffer();

    @BeforeAll
    static void startSearchServer() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("demo/search/v1/demo_search.proto", PROTO, "test").build());
        FileDescriptor file = compiled.descriptorFor("demo/search/v1/demo_search.proto").orElseThrow();
        Descriptor hit = file.findMessageTypeByName("SearchHit");
        Descriptor searchRequest = file.findMessageTypeByName("SearchRequest");

        var search = DynamicGrpcCalls.methodDescriptor(
                file.findServiceByName("DemoSearch").findMethodByName("Search"));
        ServerServiceDefinition definition = ServerServiceDefinition
                .builder("demo.search.v1.DemoSearch")
                .addMethod(search, ServerCalls.asyncServerStreamingCall((request, out) -> {
                    int hits = (int) request.getField(searchRequest.findFieldByName("hits"));
                    for (int i = 0; i < hits; i++) {
                        out.onNext(DynamicMessage.newBuilder(hit)
                                .setField(hit.findFieldByName("doc_id"), "doc-" + (i + 1))
                                .setField(hit.findFieldByName("score"), 0.98f - 0.07f * i)
                                .setField(hit.findFieldByName("text"), TEXTS[i % TEXTS.length])
                                .build());
                    }
                    out.onCompleted();
                }))
                .build();
        searchServer = ServerBuilder.forPort(0).addService(definition).build().start();
    }

    @AfterAll
    static void stopSearchServer() {
        if (searchServer != null) {
            searchServer.shutdownNow();
        }
    }

    private AcpClient launchAgent() throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return AcpClient.launch(java, "-cp", System.getProperty("java.class.path"),
                        ProtoMoltAcpAgent.class.getName())
                // A real JVM plus a protoc-backed compile verb can be slow under a loaded
                // parallel build; the bound only has to catch a genuine hang.
                .withRequestTimeout(Duration.ofMinutes(3))
                .onSessionUpdate(params -> {
                    JsonNode update = params.path("update");
                    JsonNode content = update.path("content");
                    if (!"text".equals(content.path("type").asText())) {
                        return;
                    }
                    switch (update.path("sessionUpdate").asText()) {
                        case "agent_message_chunk" -> messages.append(content.path("text").asText());
                        case "agent_thought_chunk" -> thoughts.append(content.path("text").asText());
                        default -> {
                        }
                    }
                });
    }

    @Test
    void compileVerbRoundTripsThroughTheRealProcess() throws Exception {
        try (AcpClient client = launchAgent()) {
            JsonNode init = client.initialize();
            assertThat(init.path("protocolVersion").asInt()).isEqualTo(1);

            String sessionId = client.newSession("/workspace");
            assertThat(sessionId).isNotBlank();

            String line = "compile {\"sources\":{\"p/m.proto\":"
                    + "\"syntax = \\\"proto3\\\"; package p; message M { string id = 1; }\"}}";
            JsonNode response = client.prompt(sessionId, line);

            assertThat(response.path("stopReason").asText()).isEqualTo("end_turn");
            assertThat(thoughts.toString()).contains("running compile");
            assertThat(messages.toString()).contains("\"ok\" : true");
        }
    }

    @Test
    void streamingSearchChunksEachHitThroughTheRealProcess() throws Exception {
        try (AcpClient client = launchAgent()) {
            client.initialize();
            String sessionId = client.newSession("/workspace");

            ObjectNode invoke = MAPPER.createObjectNode();
            invoke.put("target", "localhost:" + searchServer.getPort());
            invoke.put("method", "demo.search.v1.DemoSearch/Search");
            invoke.putObject("schema").putObject("sources")
                    .put("demo/search/v1/demo_search.proto", PROTO);
            invoke.putObject("request").put("query", "nearest neighbor search").put("hits", 3);
            JsonNode response = client.prompt(sessionId,
                    "grpc-invoke " + MAPPER.writeValueAsString(invoke));

            assertThat(response.path("stopReason").asText()).isEqualTo("end_turn");
            assertThat(messages.toString())
                    .contains("\"docId\" : \"doc-1\"")
                    .contains("\"docId\" : \"doc-2\"")
                    .contains("\"docId\" : \"doc-3\"")
                    .contains("\"status\" : \"OK\"");
        }
    }
}
