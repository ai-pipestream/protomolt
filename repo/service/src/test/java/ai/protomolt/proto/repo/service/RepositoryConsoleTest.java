package ai.protomolt.proto.repo.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The console route on the repository HTTP port, driven over real HTTP against a bundle
 * directory laid out the way {@code npm run build} in {@code apps/console} lays it out.
 */
class RepositoryConsoleTest {

    private static final String TOKEN = "synthetic-console-operator-key";
    private static final String INDEX = "<!doctype html><title>console</title>";
    private static final String SCRIPT = "console.log('repository')";
    private static final String SECRET = "outside the bundle";

    @TempDir Path temp;

    private Path bundle;
    private UploadHttpServer server;
    private String base;
    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeEach
    void bundle() throws IOException {
        bundle = Files.createDirectories(temp.resolve("dist"));
        Files.writeString(bundle.resolve("index.html"), INDEX);
        Files.createDirectories(bundle.resolve("assets"));
        Files.writeString(bundle.resolve("assets/index-3f2a.js"), SCRIPT);
        Files.writeString(temp.resolve("secret.txt"), SECRET);
    }

    @AfterEach
    void stop() {
        if (server != null) {
            server.close();
        }
    }

    private void serve(RepositoryConsole console) {
        server = new UploadHttpServer(null, null, TOKEN,
                UploadHttpServerCredentialTest.unimplementedArchive(), console);
        base = "http://127.0.0.1:" + server.start(0);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(base + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void servesTheBundleWithoutACredential() throws Exception {
        serve(RepositoryConsole.directory(bundle));

        HttpResponse<String> index = get("/console/");
        assertThat(index.statusCode()).isEqualTo(200);
        assertThat(index.body()).isEqualTo(INDEX);
        assertThat(index.headers().firstValue("Content-Type")).hasValue("text/html; charset=utf-8");
        assertThat(index.headers().firstValue("Cache-Control")).hasValue("no-cache");

        HttpResponse<String> script = get("/console/assets/index-3f2a.js");
        assertThat(script.statusCode()).isEqualTo(200);
        assertThat(script.body()).isEqualTo(SCRIPT);
        assertThat(script.headers().firstValue("Content-Type"))
                .hasValue("text/javascript; charset=utf-8");
        assertThat(script.headers().firstValue("Cache-Control"))
                .hasValue("public, max-age=31536000, immutable");
    }

    @Test
    void theUploadRoutesStillDemandTheCredential() throws Exception {
        serve(RepositoryConsole.directory(bundle));

        HttpResponse<String> upload = client.send(HttpRequest.newBuilder(
                        URI.create(base + UploadHttpServer.ARCHIVE_UPLOAD_PATH))
                .POST(HttpRequest.BodyPublishers.ofString("x")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(upload.statusCode()).isEqualTo(401);
    }

    @Test
    void theBareRouteRedirectsToTheBundleRoot() throws Exception {
        serve(RepositoryConsole.directory(bundle));

        HttpResponse<String> bare = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER).build()
                .send(HttpRequest.newBuilder(URI.create(base + "/console")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(bare.statusCode()).isEqualTo(308);
        assertThat(bare.headers().firstValue("Location")).hasValue("/console/");
    }

    @Test
    void routerPathsFallBackToTheIndexButMissingAssetsDoNot() throws Exception {
        serve(RepositoryConsole.directory(bundle));

        HttpResponse<String> routed = get("/console/repository/documents/doc-1");
        assertThat(routed.statusCode()).isEqualTo(200);
        assertThat(routed.body()).isEqualTo(INDEX);

        assertThat(get("/console/assets/gone-0000.js").statusCode()).isEqualTo(404);
        assertThat(get("/consoles").statusCode()).isEqualTo(404);
    }

    @Test
    void pathsOutsideTheBundleAreNeverServed() throws Exception {
        Files.createSymbolicLink(bundle.resolve("linked.txt"), temp.resolve("secret.txt"));
        Files.createSymbolicLink(bundle.resolve("assets/linked.js"), temp.resolve("secret.txt"));
        serve(RepositoryConsole.directory(bundle));

        assertThat(get("/console/%2e%2e/secret.txt").body()).isEqualTo(INDEX);
        assertThat(get("/console/linked.txt").body()).isEqualTo(INDEX);
        HttpResponse<String> linkedAsset = get("/console/assets/linked.js");
        assertThat(linkedAsset.statusCode()).isEqualTo(404);
        assertThat(linkedAsset.body()).doesNotContain(SECRET);
    }

    @Test
    void onlyReadsAreAllowed() throws Exception {
        serve(RepositoryConsole.directory(bundle));

        HttpResponse<String> post = client.send(HttpRequest.newBuilder(URI.create(base + "/console/"))
                .POST(HttpRequest.BodyPublishers.ofString("x")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(post.statusCode()).isEqualTo(405);
        assertThat(post.headers().firstValue("Allow")).hasValue("GET, HEAD");

        HttpResponse<String> head = client.send(HttpRequest.newBuilder(URI.create(base + "/console/"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(head.statusCode()).isEqualTo(200);
        assertThat(head.body()).isEmpty();
        assertThat(head.headers().firstValue("Content-Length"))
                .hasValue(Integer.toString(INDEX.getBytes(StandardCharsets.UTF_8).length));
    }

    @Test
    void anUnconfiguredHostSaysSoInsteadOfServingAnEmptyPage() throws Exception {
        serve(RepositoryConsole.notConfigured());

        HttpResponse<String> index = get("/console/");
        assertThat(index.statusCode()).isEqualTo(503);
        assertThat(index.body()).contains(RepositoryConsole.ENV_CONSOLE_DIR);
    }

    @Test
    void theEnvironmentNamesTheBundleOrNothing() {
        assertThat(RepositoryConsole.fromEnvironment(Map.of()).configured()).isFalse();
        assertThat(RepositoryConsole.fromEnvironment(
                Map.of(RepositoryConsole.ENV_CONSOLE_DIR, bundle.toString())).configured()).isTrue();

        Map<String, String> blank = new HashMap<>();
        blank.put(RepositoryConsole.ENV_CONSOLE_DIR, " ");
        assertThatThrownBy(() -> RepositoryConsole.fromEnvironment(blank))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(RepositoryConsole.ENV_CONSOLE_DIR);
        assertThatThrownBy(() -> RepositoryConsole.fromEnvironment(
                Map.of(RepositoryConsole.ENV_CONSOLE_DIR, temp.resolve("missing").toString())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(RepositoryConsole.ENV_CONSOLE_DIR)
                .hasMessageContaining("does not exist");
    }

    @Test
    void aDirectoryWithoutABuiltBundleIsRefused() throws IOException {
        Path empty = Files.createDirectories(temp.resolve("empty"));

        assertThatThrownBy(() -> RepositoryConsole.directory(empty))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("has no index.html");
    }
}
