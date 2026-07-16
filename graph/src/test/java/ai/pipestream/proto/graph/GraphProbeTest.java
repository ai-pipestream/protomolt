package ai.pipestream.proto.graph;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The probe's two testable halves: {@link GraphProbe#parse} (pure argument handling) and
 * {@link GraphProbe#run} driven against a JDK {@link HttpServer} standing in for Graph, so the
 * OK / PERMISSION DENIED / verbose-body reporting is verified without a real tenant.
 */
class GraphProbeTest {

    // ---- parse() ----

    @Test
    void parseReadsTheDelegatedLane() {
        GraphProbe.Options options = GraphProbe.parse(
                new String[]{"--tenant", "t1", "--client", "c1"});
        assertThat(options).isNotNull();
        assertThat(options.tenant()).isEqualTo("t1");
        assertThat(options.client()).isEqualTo("c1");
        assertThat(options.secret()).isNull();
        assertThat(options.appOnly()).isFalse();
        assertThat(options.verbose()).isFalse();
    }

    @Test
    void parseReadsTheAppOnlyLaneWhenASecretIsGiven() {
        GraphProbe.Options options = GraphProbe.parse(
                new String[]{"--client", "c1", "--secret", "shh", "--tenant", "t1"});
        assertThat(options).isNotNull();
        assertThat(options.secret()).isEqualTo("shh");
        assertThat(options.appOnly()).isTrue();
    }

    @Test
    void parseTakesTheVerboseFlagRegardlessOfPosition() {
        assertThat(GraphProbe.parse(new String[]{"--verbose", "--tenant", "t", "--client", "c"})
                .verbose()).isTrue();
        assertThat(GraphProbe.parse(new String[]{"--tenant", "t", "--client", "c", "--verbose"})
                .verbose()).isTrue();
    }

    @Test
    void parseReturnsNullWhenTenantOrClientIsMissing() {
        assertThat(GraphProbe.parse(new String[]{"--client", "c1"})).isNull();
        assertThat(GraphProbe.parse(new String[]{"--tenant", "t1"})).isNull();
        assertThat(GraphProbe.parse(new String[]{})).isNull();
    }

    @Test
    void parseTreatsATrailingValuelessFlagAsMissing() {
        // "--tenant" with nothing after it must not consume the array end and pass validation.
        assertThat(GraphProbe.parse(new String[]{"--client", "c1", "--tenant"})).isNull();
    }

    // ---- run() against a fake Graph ----

    private HttpServer server;
    private GraphClient graph;

    @BeforeEach
    void startHealthyTenant() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        register("/v1.0/me",
                "{\"displayName\":\"Ada Lovelace\","
                        + "\"userPrincipalName\":\"ada@contoso.onmicrosoft.com\"}");
        register("/v1.0/me/drive",
                "{\"driveType\":\"business\",\"quota\":{\"used\":2097152},\"id\":\"drive1\"}");
        register("/v1.0/drives/drive1/root/children", "{\"value\":[{\"name\":\"Reports\"}]}");
        register("/v1.0/sites",
                "{\"value\":[{\"webUrl\":\"https://contoso.sharepoint.com/sites/x\"}]}");
        register("/v1.0/external/connections", "{\"value\":[]}");
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        graph = new GraphClient(base + "/v1.0", () -> "test-token");
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void runReportsEverySurfaceOnAHealthyTenant() {
        String out = run(graph, true, false);
        assertThat(out).contains("Signed-in user (/me):").contains("OK - Ada Lovelace");
        assertThat(out).contains("OneDrive (/me/drive):").contains("business drive");
        assertThat(out).contains("OneDrive root listing:").contains("1 item(s)");
        assertThat(out).contains("SharePoint Online").contains("1 site(s)");
        assertThat(out).contains("Copilot connectors").contains("0 connection(s)");
    }

    @Test
    void runOmitsTheDelegatedUserLaneWhenAppOnly() {
        String out = run(graph, false, false);
        assertThat(out).doesNotContain("/me").doesNotContain("OneDrive");
        assertThat(out).contains("SharePoint Online").contains("Copilot connectors");
    }

    @Test
    void runPrintsPermissionDeniedAndTheRawBodyOnlyWhenVerbose() throws IOException {
        // A tenant whose OneDrive has not finished provisioning: 403 with an innerError that a
        // bare status code hides but the verbose body reveals.
        server.removeContext("/v1.0/me/drive");
        server.createContext("/v1.0/me/drive", exchange ->
                FakeGraphSupport.respond(exchange, 403, "{\"error\":{\"code\":\"notAllowed\","
                        + "\"message\":\"OneDrive is not provisioned for this user.\","
                        + "\"innerError\":{\"code\":\"provisioningError\"}}}"));

        String quiet = run(graph, true, false);
        assertThat(quiet).contains("PERMISSION DENIED").contains("(notAllowed)");
        assertThat(quiet).doesNotContain("body:").doesNotContain("provisioningError");

        String loud = run(graph, true, true);
        assertThat(loud).contains("PERMISSION DENIED");
        assertThat(loud).contains("body:").contains("provisioningError");
    }

    private static String run(GraphClient graph, boolean includeUserLane, boolean verbose) {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        GraphProbe.run(graph, includeUserLane, verbose,
                new PrintStream(captured, true, StandardCharsets.UTF_8));
        return captured.toString(StandardCharsets.UTF_8);
    }

    private void register(String path, String json) {
        server.createContext(path, exchange -> FakeGraphSupport.respond(exchange, 200, json));
    }
}
