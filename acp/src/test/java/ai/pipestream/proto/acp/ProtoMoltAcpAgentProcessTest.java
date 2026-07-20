package ai.pipestream.proto.acp;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.agentclientprotocol.sdk.client.AcpClient;
import com.agentclientprotocol.sdk.client.AcpSyncClient;
import com.agentclientprotocol.sdk.client.transport.AgentParameters;
import com.agentclientprotocol.sdk.client.transport.StdioAcpClientTransport;
import com.agentclientprotocol.sdk.spec.AcpSchema;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The agent as a real child process, driven over stdio exactly as an IDE drives it:
 * initialize, open a session, prompt with console lines, read the streamed session/update
 * chunks. The unary leg runs the local {@code compile} verb; the streaming leg points
 * {@code grpc-invoke} at a DemoSearch-shaped dynamic server (a local stand-in for the
 * samples module's demo service) and asserts each hit arrives as its own chunk.
 */
// Forking a JVM and running protoc under a fully parallel build is slow; this bound only has to
// catch a genuine hang, so it sits above the client's own request timeout rather than under it.
@Timeout(value = 4, unit = TimeUnit.MINUTES)
// Excluded from the default build for the same reason as
// ProtoMoltAcpAgentTest#catalogVerbsRunThroughTheProtocol: these drive the protoc-backed verbs
// through the SDK client, here across a real subprocess, and the client has been seen blocking
// indefinitely on a contended machine. Run with ./gradlew :protomolt-acp:acpProtocolTest.
@Tag("acp-protocol")
class ProtoMoltAcpAgentProcessTest {

    // Same shape as the samples module's DemoSearch service.
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

    // Written by the SDK's session-update thread, read by the test thread, so synchronized
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

    private AcpSyncClient launchAgent() {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        AgentParameters command = AgentParameters.builder(java)
                .args("-cp", System.getProperty("java.class.path"),
                        ProtoMoltAcpAgent.class.getName())
                .build();
        return AcpClient.sync(new StdioAcpClientTransport(command))
                // A real JVM plus a protoc-backed compile verb; see ProtoMoltAcpAgentTest for
                // why the SDK's 30s default is too tight under a loaded parallel build.
                .requestTimeout(Duration.ofMinutes(3))
                .sessionUpdateConsumer(notification -> {
                    if (notification.update() instanceof AcpSchema.AgentMessageChunk message
                            && message.content() instanceof AcpSchema.TextContent text) {
                        messages.append(text.text());
                    }
                    if (notification.update() instanceof AcpSchema.AgentThoughtChunk thought
                            && thought.content() instanceof AcpSchema.TextContent text) {
                        thoughts.append(text.text());
                    }
                })
                .build();
    }

    @Test
    void compileVerbRoundTripsThroughTheRealProcess() throws Exception {
        try (AcpSyncClient client = launchAgent()) {
            AcpSchema.InitializeResponse init = client.initialize();
            assertThat(init.protocolVersion()).isEqualTo(1);

            var session = client.newSession(new AcpSchema.NewSessionRequest("/workspace", List.of()));
            assertThat(session.sessionId()).isNotBlank();

            String line = "compile {\"sources\":{\"p/m.proto\":"
                    + "\"syntax = \\\"proto3\\\"; package p; message M { string id = 1; }\"}}";
            AcpSchema.PromptResponse response = client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(), List.of(new AcpSchema.TextContent(line))));

            assertThat(response.stopReason()).isEqualTo(AcpSchema.StopReason.END_TURN);
            assertThat(thoughts.toString()).contains("running compile");
            assertThat(messages.toString()).contains("\"ok\" : true");
        }
    }

    @Test
    void streamingSearchChunksEachHitThroughTheRealProcess() throws Exception {
        try (AcpSyncClient client = launchAgent()) {
            client.initialize();
            var session = client.newSession(new AcpSchema.NewSessionRequest("/workspace", List.of()));

            ObjectNode invoke = MAPPER.createObjectNode();
            invoke.put("target", "localhost:" + searchServer.getPort());
            invoke.put("method", "demo.search.v1.DemoSearch/Search");
            invoke.putObject("schema").putObject("sources")
                    .put("demo/search/v1/demo_search.proto", PROTO);
            invoke.putObject("request").put("query", "nearest neighbor search").put("hits", 3);
            AcpSchema.PromptResponse response = client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(),
                    List.of(new AcpSchema.TextContent("grpc-invoke " + MAPPER.writeValueAsString(invoke)))));

            assertThat(response.stopReason()).isEqualTo(AcpSchema.StopReason.END_TURN);
            assertThat(messages.toString())
                    .contains("\"docId\" : \"doc-1\"")
                    .contains("\"docId\" : \"doc-2\"")
                    .contains("\"docId\" : \"doc-3\"")
                    .contains("\"status\" : \"OK\"");
        }
    }
}
