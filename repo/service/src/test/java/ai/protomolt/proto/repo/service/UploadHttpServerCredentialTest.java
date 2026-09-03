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

/**
 * The upload route writes documents into any account's drive, so a configured credential
 * has to be enforced before anything else the request asks for. Every assertion here
 * short-circuits ahead of the drive ledger, the blob store, and the intake save, so the
 * collaborators are {@code null}: a request that reached them would fail differently.
 */
class UploadHttpServerCredentialTest {

    private static final String TOKEN = "correct-horse-battery-staple";

    private UploadHttpServer server;
    private HttpClient client;
    private String base;

    @BeforeEach
    void start() {
        server = new UploadHttpServer(null, null, null, TOKEN);
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
        HttpResponse<String> response = post(UploadHttpServer.UPLOAD_PATH, null, null);

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("api_token");
    }

    @Test
    void aRequestWithTheWrongCredentialIsRefusedWithoutEchoingIt() throws Exception {
        HttpResponse<String> response =
                post(UploadHttpServer.UPLOAD_PATH, "api_token", "not-the-token");

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).doesNotContain("not-the-token");
    }

    @Test
    void theCredentialIsCheckedBeforeTheRouteIsMatched() throws Exception {
        // The handler is reached for any path under the registered prefix. An
        // unauthenticated probe of one must not learn whether it is a real route, so it
        // answers the same 401 as the route itself rather than 404.
        HttpResponse<String> response =
                post(UploadHttpServer.UPLOAD_PATH + "/probe", null, null);

        assertThat(response.statusCode()).isEqualTo(401);
    }

    @Test
    void bearerAuthorizationIsAccepted() throws Exception {
        // Past the credential check, so this reaches the null collaborators and fails
        // there. Anything other than 401 proves the credential was accepted.
        HttpResponse<String> response =
                post(UploadHttpServer.UPLOAD_PATH, "Authorization", "Bearer " + TOKEN);

        assertThat(response.statusCode()).isNotEqualTo(401);
    }

    @Test
    void anOpenServerServesWithoutACredential() throws Exception {
        try (UploadHttpServer open = new UploadHttpServer(null, null, null)) {
            String url = "http://127.0.0.1:" + open.start(0)
                    + UploadHttpServer.UPLOAD_PATH + "/probe";
            HttpResponse<String> response = client.send(
                    HttpRequest.newBuilder(URI.create(url))
                            .POST(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(404);
        }
    }
}
