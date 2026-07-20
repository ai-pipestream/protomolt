package ai.pipestream.proto.graph;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.pipestream.proto.graph.FakeGraphSupport.respond;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How {@link GraphAuth} behaves when the token endpoint answers with something other than a
 * well-formed token: an empty body, a gateway error page, a 2xx that carries no token at all.
 * Each of these once produced a {@link GraphAuth.Token} holding an empty access token, which
 * failed later as an opaque 401 on the first Graph call rather than at the sign-in that caused
 * it. The polling case guards the other side of that check — a non-2xx carrying an OAuth error
 * field is protocol, not failure, and must still reach the caller.
 */
class GraphAuthTest {

    private HttpServer server;

    private GraphAuth authAgainst(String path, int status, String body) throws IOException {
        return authAgainst(path, exchange -> respond(exchange, status, body));
    }

    private GraphAuth authAgainst(String path, com.sun.net.httpserver.HttpHandler handler)
            throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(path, handler);
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        return new GraphAuth(
                new GraphAuth.Config(base, "tenant-1", "client-1", "s3cret"),
                java.net.http.HttpClient.newHttpClient());
    }

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void emptyBodyOnServerErrorFailsInsteadOfYieldingAnEmptyToken() throws Exception {
        GraphAuth auth = authAgainst("/tenant-1/oauth2/v2.0/token", 500, "");

        assertThatThrownBy(auth::clientCredentials)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("HTTP 500");
    }

    @Test
    void nonJsonErrorPageFailsWithTheStatusAndAnExcerpt() throws Exception {
        GraphAuth auth = authAgainst("/tenant-1/oauth2/v2.0/token", 502,
                "<html><body>502 Bad Gateway</body></html>");

        assertThatThrownBy(auth::clientCredentials)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("HTTP 502")
                .hasMessageContaining("Bad Gateway");
    }

    @Test
    void successfulStatusCarryingNoTokenIsRejected() throws Exception {
        GraphAuth auth = authAgainst("/tenant-1/oauth2/v2.0/token", 200, "{}");

        assertThatThrownBy(auth::clientCredentials)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("neither an access_token nor an error field");
    }

    @Test
    void jsonArrayBodyIsRejectedRatherThanTreatedAsAToken() throws Exception {
        GraphAuth auth = authAgainst("/tenant-1/oauth2/v2.0/token", 200, "[]");

        assertThatThrownBy(auth::clientCredentials)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not a JSON object");
    }

    @Test
    void oauthErrorBodyKeepsItsDescriptiveMessage() throws Exception {
        GraphAuth auth = authAgainst("/tenant-1/oauth2/v2.0/token", 400,
                "{\"error\":\"invalid_client\",\"error_description\":\"secret expired\"}");

        assertThatThrownBy(auth::clientCredentials)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid_client")
                .hasMessageContaining("secret expired");
    }

    /**
     * Entra answers the device-code poll with {@code 400 authorization_pending} until the
     * operator approves. Treating a non-2xx as fatal would break sign-in, so the status check
     * must defer to the OAuth error field.
     */
    @Test
    void deviceCodePollTreatsHttp400PendingAsProtocolNotFailure() throws Exception {
        AtomicInteger polls = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/tenant-1/oauth2/v2.0/devicecode", exchange -> respond(exchange, 200,
                "{\"device_code\":\"dc-1\",\"user_code\":\"ABCD-1234\","
                        + "\"verification_uri\":\"https://microsoft.com/devicelogin\","
                        + "\"message\":\"go log in\",\"interval\":1,\"expires_in\":60}"));
        server.createContext("/tenant-1/oauth2/v2.0/token", exchange -> {
            // Drain the request so the exchange completes cleanly.
            exchange.getRequestBody().readAllBytes();
            if (polls.incrementAndGet() < 2) {
                respond(exchange, 400, "{\"error\":\"authorization_pending\"}");
            } else {
                respond(exchange, 200,
                        "{\"access_token\":\"user-token\",\"expires_in\":3600}");
            }
        });
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        // Config.delegated pins the production authority, so address the fake explicitly.
        GraphAuth auth = new GraphAuth(
                new GraphAuth.Config(base, "tenant-1", "client-1", null),
                java.net.http.HttpClient.newHttpClient());

        GraphAuth.Token token = auth.deviceCode("Files.Read", prompt ->
                assertThat(prompt.userCode()).isEqualTo("ABCD-1234"));

        assertThat(token.accessToken()).isEqualTo("user-token");
        assertThat(polls.get()).isEqualTo(2);
    }

    @Test
    void deviceCodeStartRejectsAnUnusableBody() throws Exception {
        GraphAuth auth = authAgainst("/tenant-1/oauth2/v2.0/devicecode", 503, "");

        assertThatThrownBy(() -> auth.deviceCode("Files.Read", prompt -> { }))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("HTTP 503");
    }

    @Test
    void refreshRejectsAnUnusableBody() throws Exception {
        GraphAuth auth = authAgainst("/tenant-1/oauth2/v2.0/token", 500, "");

        assertThatThrownBy(() -> auth.refresh("refresh-1", "Files.Read"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("HTTP 500");
    }

    @Test
    void bodyExcerptIsBoundedSoAHugeErrorPageDoesNotLandInTheMessage() throws Exception {
        String huge = "x".repeat(4096);
        GraphAuth auth = authAgainst("/tenant-1/oauth2/v2.0/token", 500, huge);

        assertThatThrownBy(auth::clientCredentials)
                .isInstanceOf(IOException.class)
                .satisfies(e -> assertThat(e.getMessage().length()).isLessThan(1024));
    }

    @Test
    void utf8ErrorBodySurvivesTheExcerpt() throws Exception {
        GraphAuth auth = authAgainst("/tenant-1/oauth2/v2.0/token", 500,
                new String("naïve — π".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));

        assertThatThrownBy(auth::clientCredentials)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("naïve");
    }
}
