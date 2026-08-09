package ai.pipestream.proto.repo.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Self-contained tests of the {@link UploadHttpServer} request contract that
 * is decided BEFORE any collaborator is touched: route matching, method
 * enforcement, required-identity validation, and the Content-Length rule.
 * Every assertion here short-circuits ahead of the drive ledger, the blob
 * store, and the intake save, so the collaborators are {@code null} — the
 * paths that genuinely reach them run against real storage in
 * {@code UploadHttpServerIT}.
 */
class UploadHttpServerContractTest {

    private UploadHttpServer server;
    private HttpClient client;
    private String url;

    @BeforeEach
    void start() {
        server = new UploadHttpServer(null, null, null);
        url = "http://127.0.0.1:" + server.start(0) + UploadHttpServer.UPLOAD_PATH;
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void stop() {
        client.close();
        server.close();
    }

    // ------------------------------------------------------------------ lifecycle

    @Test
    void portBeforeStartAndDoubleStartAreIllegalState() {
        UploadHttpServer fresh = new UploadHttpServer(null, null, null);
        assertThatThrownBy(fresh::port)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not started");
        fresh.start(0);
        try {
            assertThat(fresh.port()).isPositive();
            assertThatThrownBy(() -> fresh.start(0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already started");
        } finally {
            fresh.close();
        }
    }

    // ------------------------------------------------------------------ route and method

    @Test
    void nonPostMethodsAre405() throws Exception {
        for (String method : new String[]{"GET", "PUT", "DELETE"}) {
            HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(url))
                    .method(method, HttpRequest.BodyPublishers.noBody())
                    .build());
            assertThat(response.statusCode()).as(method).isEqualTo(405);
            assertThat(response.body()).contains("method not allowed");
        }
    }

    @Test
    void aRoutedButInexactPathIs404() throws Exception {
        // createContext prefix-matches; only the exact path is the route.
        HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(url + "/extra"))
                .POST(HttpRequest.BodyPublishers.ofString("x"))
                .build());
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).contains("unknown route");
    }

    // ------------------------------------------------------------------ identity validation

    @Test
    void eachMissingIdentityParameterIs400NamingIt() throws Exception {
        assertThat(post("").body()).contains("account_id is required");
        assertThat(post("account_id=a").body()).contains("datasource_id is required");
        assertThat(post("account_id=a&datasource_id=d").body()).contains("drive is required");
        assertThat(post("account_id=a&datasource_id=d&drive=x").body())
                .contains("filename is required");
    }

    @Test
    void identityValuesMayArriveAsHeaders() throws Exception {
        // account_id via header, the rest absent: the failure names
        // datasource_id, proving the header satisfied the account_id check.
        HttpResponse<String> response = send(HttpRequest.newBuilder(
                        URI.create(url + "?filename=f"))
                .header("x-account-id", "a")
                .POST(HttpRequest.BodyPublishers.ofString(""))
                .build());
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("datasource_id is required");
    }

    @Test
    void errorResponsesAreJson() throws Exception {
        HttpResponse<String> response = post("");
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.headers().firstValue("Content-Type")).contains("application/json");
        assertThat(response.body()).startsWith("{\"error\":");
    }

    // ------------------------------------------------------------------ Content-Length

    @Test
    void aChunkedBodyWithoutContentLengthIs411() throws Exception {
        // ofInputStream has an unknown length → the client sends chunked,
        // which the route rejects before a single byte is read.
        HttpResponse<String> response = send(HttpRequest.newBuilder(
                        URI.create(url + "?account_id=a&datasource_id=d&drive=x&filename=f"))
                .POST(HttpRequest.BodyPublishers.ofInputStream(
                        () -> new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8))))
                .build());
        assertThat(response.statusCode()).isEqualTo(411);
        assertThat(response.body()).contains("Content-Length");
    }

    // ------------------------------------------------------------------ helpers

    private HttpResponse<String> post(String query) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(url + (query.isEmpty() ? "" : "?" + query)))
                .POST(HttpRequest.BodyPublishers.ofString(""))
                .build());
    }

    private HttpResponse<String> send(HttpRequest request) throws Exception {
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
