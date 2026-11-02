package ai.protomolt.proto.repo.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Real HTTP boundary checks on the archive route, the surface this server still
 * owns. The parts-based document route's boundary checks belong to the accession
 * upload handler and are covered there; a committed upload runs against real
 * storage in {@code UploadHttpServerV2DelegationIT}.
 */
class UploadHttpServerContractTest {

    private UploadHttpServer server;
    private HttpClient client;
    private String url;

    @BeforeEach
    void start() {
        server = new UploadHttpServer(null, null, "synthetic-http-operator-key",
                UploadHttpServerCredentialTest.unimplementedArchive(),
                RepositoryConsole.notConfigured());
        url = "http://127.0.0.1:" + server.start(0) + UploadHttpServer.ARCHIVE_UPLOAD_PATH;
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
        UploadHttpServer fresh = new UploadHttpServer(null, null, "synthetic-http-operator-key", null,
                RepositoryConsole.notConfigured());
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
        assertThat(post("account_id=a").body()).contains("archive is required");
        assertThat(post("account_id=a&archive=ar").body()).contains("entry_id is required");
    }

    @Test
    void identityValuesMayArriveAsHeaders() throws Exception {
        // account_id via header, the rest absent: the failure names
        // archive, proving the header satisfied the account_id check.
        HttpResponse<String> response = send(HttpRequest.newBuilder(
                        URI.create(url + "?entry_id=e"))
                .header("x-account-id", "a")
                .POST(HttpRequest.BodyPublishers.ofString(""))
                .build());
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("archive is required");
    }

    @Test
    void errorResponsesAreProtobuf() throws Exception {
        var response = client.send(HttpRequest.newBuilder(URI.create(url))
                .header("api_token", "synthetic-http-operator-key")
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofByteArray());
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.headers().firstValue("Content-Type")).contains("application/protobuf");
        var status = com.google.rpc.Status.parseFrom(response.body());
        assertThat(status.getCode()).isEqualTo(io.grpc.Status.Code.INVALID_ARGUMENT.value());
        assertThat(status.getMessage()).contains("account_id is required");
    }

    // ------------------------------------------------------------------ Content-Length

    @Test
    void aChunkedBodyWithoutContentLengthIs411() throws Exception {
        // ofInputStream has an unknown length → the client sends chunked,
        // which the route rejects before a single byte is read.
        HttpResponse<String> response = send(HttpRequest.newBuilder(
                        URI.create(url + "?account_id=a&archive=ar&entry_id=e"))
                .POST(HttpRequest.BodyPublishers.ofInputStream(
                        () -> new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8))))
                .build());
        assertThat(response.statusCode()).isEqualTo(411);
        assertThat(response.body()).contains("Content-Length");
    }

    // ------------------------------------------------------------------ refusal delivery

    @Test
    void aRefusedClientFinishesSendingItsBodyAndReadsTheRefusal() throws Exception {
        // The route refuses from the headers alone. Closing the connection with
        // body bytes unread resets it under a client that is still sending, and the
        // JDK HttpClient then fails the exchange without the refusal. The server
        // must read the declared body before it closes.
        int bodyBytes = 16 * 1024 * 1024;
        try (Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(10_000);
            String head = "POST " + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                    + "?account_id=a&archive=ar&entry_id=e HTTP/1.1\r\n"
                    + "Host: 127.0.0.1\r\n"
                    + "Content-Type: application/octet-stream\r\n"
                    + "Content-Length: " + bodyBytes + "\r\n\r\n";
            OutputStream out = socket.getOutputStream();
            out.write(head.getBytes(StandardCharsets.US_ASCII));
            out.flush();
            AtomicReference<IOException> sendFailure = new AtomicReference<>();
            Thread sender = Thread.ofVirtual().start(() -> {
                byte[] chunk = new byte[64 * 1024];
                try {
                    for (int sent = 0; sent < bodyBytes; sent += chunk.length) {
                        out.write(chunk);
                    }
                    out.flush();
                } catch (IOException resetByServer) {
                    sendFailure.set(resetByServer);
                }
            });
            assertThat(sender.join(Duration.ofSeconds(10))).as("body sender finished").isTrue();
            assertThat(sendFailure.get()).as("the server reset the upload before reading it").isNull();

            byte[] statusLine = new byte["HTTP/1.1 401".length()];
            int read = socket.getInputStream().readNBytes(statusLine, 0, statusLine.length);
            assertThat(new String(statusLine, 0, read, StandardCharsets.US_ASCII)).isEqualTo("HTTP/1.1 401");
        }
    }

    // ------------------------------------------------------------------ helpers

    private HttpResponse<String> post(String query) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(url + (query.isEmpty() ? "" : "?" + query)))
                .POST(HttpRequest.BodyPublishers.ofString(""))
                .build());
    }

    private HttpResponse<String> send(HttpRequest request) throws Exception {
        return client.send(HttpRequest.newBuilder(request, (name, value) -> true)
                .header("api_token", "synthetic-http-operator-key").build(), HttpResponse.BodyHandlers.ofString());
    }
}
