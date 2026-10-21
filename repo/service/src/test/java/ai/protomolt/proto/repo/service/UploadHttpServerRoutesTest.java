package ai.protomolt.proto.repo.service;

import com.sun.net.httpserver.HttpHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Extra routes a host mounts on the repository HTTP port next to the built-in ones. */
class UploadHttpServerRoutesTest {

    private static final String TOKEN = "synthetic-routes-operator-key";

    @TempDir Path temp;

    private UploadHttpServer server;
    private final HttpClient client = HttpClient.newHttpClient();

    @AfterEach
    void stop() {
        if (server != null) {
            server.close();
        }
    }

    private static HttpHandler answering(String body) {
        return exchange -> {
            try (exchange) {
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(bytes);
                }
            }
        };
    }

    private static UploadHttpServer withRoutes(Map<String, HttpHandler> routes) {
        return new UploadHttpServer(null, null, TOKEN, null, RepositoryConsole.notConfigured(), routes);
    }

    private HttpResponse<String> get(String base, String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(base + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void anExtraRouteUnderTheConsoleWinsOverTheBundle() throws Exception {
        Path bundle = Files.createDirectories(temp.resolve("dist"));
        Files.writeString(bundle.resolve("index.html"), "<!doctype html>");
        server = new UploadHttpServer(null, null, TOKEN, null, RepositoryConsole.directory(bundle),
                Map.of("/console/session", answering("session route")));
        String base = "http://127.0.0.1:" + server.start(0);

        assertThat(get(base, "/console/session").body()).isEqualTo("session route");
        assertThat(get(base, "/console/session/login").body()).isEqualTo("session route");
        assertThat(get(base, "/console/").body()).isEqualTo("<!doctype html>");
    }

    @Test
    void theBuiltInRoutesStayAsTheyWere() throws Exception {
        server = withRoutes(Map.of("/v1/extra", answering("extra")));
        String base = "http://127.0.0.1:" + server.start(0);

        assertThat(get(base, "/v1/extra").body()).isEqualTo("extra");
        assertThat(get(base, "/console/").statusCode()).isEqualTo(503);
        assertThat(get(base, "/unmounted").statusCode()).isEqualTo(404);
    }

    @Test
    void routePathsThatWouldShadowOrEscapeAreRefused() {
        HttpHandler handler = answering("x");
        for (String path : new String[] {"relative", "/", UploadHttpServer.UPLOAD_PATH,
                UploadHttpServer.ARCHIVE_UPLOAD_PATH, RepositoryConsole.PATH,
                UploadHttpServer.CONNECT_PATH, UploadHttpServer.CONNECT_PATH + "/x.v1.Service/Call"}) {
            assertThatThrownBy(() -> withRoutes(Map.of(path, handler)))
                    .as(path)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(path);
        }
        assertThatThrownBy(() -> withRoutes(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void theConnectPrefixIsMatchedAsAPathNotAsAString() {
        server = withRoutes(Map.of("/connector", answering("not the gateway")));

        assertThat(server.start(0)).isPositive();
    }
}
