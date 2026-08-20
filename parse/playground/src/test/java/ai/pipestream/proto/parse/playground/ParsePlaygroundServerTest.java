package ai.pipestream.proto.parse.playground;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.parse.text.TextParserService;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Test;

/**
 * Drives the playground over real HTTP against the embedded text parser and
 * pins the streaming transport: one JSON line per parse event, flushed as
 * emitted, final document present, page served.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ParsePlaygroundServerTest {

    static Server parser;
    static ParsePlaygroundServer playground;
    static HttpClient http;
    static URI base;

    @BeforeAll
    static void boot() throws Exception {
        String name = InProcessServerBuilder.generateName();
        parser = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new TextParserService())
                .build()
                .start();
        playground = new ParsePlaygroundServer(
                0,
                ParsePlaygroundServer.INPROCESS_TARGET_PREFIX + name,
                JsonFormat.TypeRegistry.newBuilder()
                        .add(ai.pipestream.proto.parse.document.v1.Document.getDescriptor())
                        .build());
        http = HttpClient.newHttpClient();
        base = URI.create("http://127.0.0.1:" + playground.port());
    }

    @AfterAll
    static void shutdown() {
        playground.close();
        parser.shutdownNow();
    }

    @Test
    @Order(1)
    void thePageServes() throws Exception {
        HttpResponse<String> page = http.send(
                HttpRequest.newBuilder(base.resolve("/")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(page.statusCode()).isEqualTo(200);
        assertThat(page.headers().firstValue("Content-Type")).contains("text/html; charset=utf-8");
        assertThat(page.body()).contains("Parser Playground").contains("/parse?filename=");
    }

    @Test
    @Order(2)
    void aParseStreamsOneJsonLinePerEventEndingWithTheDocument() throws Exception {
        HttpResponse<java.io.InputStream> response = http.send(
                HttpRequest.newBuilder(base.resolve(
                                "/parse?filename=demo.md&content_type=text/markdown"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "# Streaming Wins\n\nThe first paragraph.\n\nThe second paragraph."))
                        .build(),
                HttpResponse.BodyHandlers.ofInputStream());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type"))
                .contains("application/x-ndjson; charset=utf-8");

        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    lines.add(line);
                }
            }
        }
        // Progress, claims, exporting, document: several distinct events.
        assertThat(lines.size()).isGreaterThanOrEqualTo(3);
        assertThat(lines).anySatisfy(l -> assertThat(l).contains("\"progress\""));
        assertThat(lines).anySatisfy(l -> assertThat(l).contains("Streaming Wins"));
        // The final line carries the document with the Any-typed shape
        // rendered through the registry (fields visible, not base64).
        String last = lines.getLast();
        assertThat(last).contains("\"document\"");
        assertThat(last).contains("ai.pipestream.proto.parse.document.v1.Document");
        assertThat(last).contains("The first paragraph.");
    }

    @Test
    @Order(3)
    void aParserFailureStreamsAnErrorLineInsteadOfHanging() throws Exception {
        // A dead parser is the honest failure lever: the playground must
        // answer with an error line, not a hung response. Runs last, since
        // the shared parser stays down afterwards.
        parser.shutdownNow();
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(base.resolve("/parse?filename=x.txt"))
                        .POST(HttpRequest.BodyPublishers.ofString("payload"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"error\"");
    }
}
