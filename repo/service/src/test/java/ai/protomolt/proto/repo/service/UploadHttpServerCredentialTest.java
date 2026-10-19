package ai.protomolt.proto.repo.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Real HTTP credential boundary checks on the archive route, whose credential
 * enforcement is this server's own. The parts-based document route checks the
 * same credential inside the accession upload handler, covered there.
 */
class UploadHttpServerCredentialTest {

    private static final String TOKEN = "correct-horse-battery-staple";

    private UploadHttpServer server;
    private HttpClient client;
    private String base;

    @BeforeEach
    void start() {
        server = new UploadHttpServer(null, null, TOKEN, unimplementedArchive());
        base = "http://127.0.0.1:" + server.start(0);
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void stop() {
        client.close();
        server.close();
    }

    private HttpResponse<String> post(String path, String header, String value)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path))
                .POST(HttpRequest.BodyPublishers.ofInputStream(
                        () -> new ByteArrayInputStream(new byte[0])));
        if (header != null) {
            request.header(header, value);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void aRequestWithoutACredentialIsRefused() throws Exception {
        HttpResponse<String> response = post(UploadHttpServer.ARCHIVE_UPLOAD_PATH, null, null);

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("api_token");
    }

    @Test
    void aRequestWithTheWrongCredentialIsRefusedWithoutEchoingIt() throws Exception {
        HttpResponse<String> response =
                post(UploadHttpServer.ARCHIVE_UPLOAD_PATH, "api_token", "not-the-token");

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).doesNotContain("not-the-token");
    }

    @Test
    void theCredentialIsCheckedBeforeTheRouteIsMatched() throws Exception {
        // The handler is reached for any path under the registered prefix. An
        // unauthenticated probe of one must not learn whether it is a real route, so it
        // answers the same 401 as the route itself rather than 404.
        HttpResponse<String> response =
                post(UploadHttpServer.ARCHIVE_UPLOAD_PATH + "/probe", null, null);

        assertThat(response.statusCode()).isEqualTo(401);
    }

    @Test
    void bearerAuthorizationIsAccepted() throws Exception {
        // Authenticated requests reach archive validation before storage.
        HttpResponse<String> response =
                post(UploadHttpServer.ARCHIVE_UPLOAD_PATH, "Authorization", "Bearer " + TOKEN);

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("account_id is required");
    }

    @Test
    void missingOrBlankOperatorCredentialCannotOpenAListener() {
        for (String token : new String[]{null, "", "  "}) {
            assertThatThrownBy(() -> {
                try (var open = new UploadHttpServer(null, null, token, null)) { open.start(0); }
            }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("API token");
        }
    }

    /** Credential and validation failures fire before the archive port is ever called. */
    static ai.protomolt.proto.repo.spi.ArchiveRepository unimplementedArchive() {
        return (ai.protomolt.proto.repo.spi.ArchiveRepository) java.lang.reflect.Proxy.newProxyInstance(
                UploadHttpServerCredentialTest.class.getClassLoader(),
                new Class<?>[] { ai.protomolt.proto.repo.spi.ArchiveRepository.class },
                (proxy, method, args) -> { throw new UnsupportedOperationException(method.getName()); });
    }
}
