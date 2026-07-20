package ai.pipestream.proto.samples;

import com.agentclientprotocol.sdk.client.AcpClient;
import com.agentclientprotocol.sdk.client.AcpSyncClient;
import com.agentclientprotocol.sdk.client.transport.AgentParameters;
import com.agentclientprotocol.sdk.client.transport.StdioAcpClientTransport;
import com.agentclientprotocol.sdk.spec.AcpSchema;

import java.util.List;

/**
 * Drives the protomolt ACP agent against the demo streaming search and prints each
 * session chunk with its arrival time, so streaming is visible in the terminal.
 * Expects the agent installed ({@code ./gradlew :protomolt-acp:installDist}) and the
 * demo server running ({@code ./gradlew :samples:runDemoSearch}).
 * Run with {@code ./gradlew :samples:runAcpStreamingDemo}.
 */
public final class AcpStreamingDemo {

    private static final String AGENT = "acp/build/install/protomolt-acp/bin/protomolt-acp";

    private static final String SCHEMA_TEXT = """
            syntax = "proto3";
            package demo.search.v1;
            service DemoSearch {
              rpc Search(SearchRequest) returns (stream SearchHit);
            }
            message SearchRequest {
              string query = 1;
              int32 hits = 2;
              int32 delay_ms = 3;
            }
            message SearchHit {
              string doc_id = 1;
              float score = 2;
              string text = 3;
            }
            """;

    public static void main(String[] args) throws Exception {
        long t0 = System.currentTimeMillis();
        var transport = new StdioAcpClientTransport(AgentParameters.builder(AGENT).build());
        try (AcpSyncClient client = AcpClient.sync(transport)
                .sessionUpdateConsumer(notification -> {
                    if (notification.update() instanceof AcpSchema.AgentMessageChunk message
                            && message.content() instanceof AcpSchema.TextContent text) {
                        long elapsed = (System.currentTimeMillis() - t0) / 100;
                        System.out.printf("[+%5.1fs] %s%n", elapsed / 10.0,
                                text.text().replace("\n", " "));
                    }
                })
                .build()) {
            client.initialize();
            var session = client.newSession(new AcpSchema.NewSessionRequest("/", List.of()));
            String line = "grpc-invoke {\"target\":\"localhost:9777\","
                    + "\"method\":\"demo.search.v1.DemoSearch/Search\","
                    + "\"schema\":{\"sources\":{\"demo.proto\":"
                    + toJsonString(SCHEMA_TEXT) + "}},"
                    + "\"request\":{\"query\":\"nearest neighbor search\",\"hits\":5,"
                    + "\"delayMs\":400}}";
            System.out.println("prompt: " + "grpc-invoke demo.search.v1.DemoSearch/Search"
                    + " (5 hits, 400ms apart)");
            client.prompt(new AcpSchema.PromptRequest(
                    session.sessionId(), List.of(new AcpSchema.TextContent(line))));
        }
    }

    private static String toJsonString(String text) {
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                default -> out.append(c);
            }
        }
        return out.append('"').toString();
    }

    private AcpStreamingDemo() {
    }
}
