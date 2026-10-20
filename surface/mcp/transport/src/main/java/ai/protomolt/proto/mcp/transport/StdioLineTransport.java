package ai.protomolt.proto.mcp.transport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Package-private newline-delimited JSON-RPC 2.0 stdio transport framing for {@link McpServer}.
 *
 * <p>Reads UTF-8 lines from {@code in}, emits {@link JsonRpc#PARSE_ERROR} responses on malformed
 * JSON without terminating the session, dispatches parsed messages through the supplied
 * {@link McpServer.Session}, synchronizes line writes to {@code out}, and awaits in-flight tool
 * completions at end of stream.
 */
final class StdioLineTransport {

    private static final long DRAIN_TIMEOUT_MILLIS = 30_000L;

    private StdioLineTransport() {
    }

    static void run(
            ObjectMapper mapper,
            McpServer.Session session,
            InputStream in,
            OutputStream out) throws IOException {
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        BufferedWriter writer =
                new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        Object writeLock = new Object();
        try (session) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode message;
                try {
                    message = mapper.readTree(line);
                } catch (JsonProcessingException e) {
                    synchronized (writeLock) {
                        write(writer, JsonRpc.error(
                                mapper, null, JsonRpc.PARSE_ERROR, "Parse error"));
                    }
                    continue;
                }
                session.dispatch(message, response -> response.ifPresent(value -> {
                    synchronized (writeLock) {
                        write(writer, value);
                    }
                }));
            }
            session.awaitInFlight(DRAIN_TIMEOUT_MILLIS);
        }
    }

    private static void write(BufferedWriter writer, ObjectNode response) {
        try {
            writer.write(response.toString());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
