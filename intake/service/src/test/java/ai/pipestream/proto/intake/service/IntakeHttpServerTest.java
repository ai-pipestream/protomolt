package ai.pipestream.proto.intake.service;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.intake.service.identity.ApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.IntakeScope;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import ai.pipestream.proto.repo.v1.DocIdDerivationMethod;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the HTTP intake lane's contract against the same recording fake repo
 * the gRPC contract tests use: the receipt vocabulary, the pinned
 * SaveDocument shape (identical to the gRPC raw lane), the auth split
 * (401 vs 500), the no-account-parameter rule, the scope 403s, the cap 413,
 * and the repo-status flattening.
 */
class IntakeHttpServerTest {

    static final String ACCOUNT = "acct-http";
    static final String KEY_UNRESTRICTED = "key-unrestricted";
    static final String KEY_NARROW = "key-narrow";
    static final String KEY_BOOM = "key-boom";

    static FakeDocumentService repo;
    static Server repoServer;
    static IntakeServices intake;
    static IntakeHttpServer http;
    static HttpClient client;
    static String base;

    @BeforeAll
    static void boot() throws Exception {
        repo = new FakeDocumentService();
        String repoName = InProcessServerBuilder.generateName();
        repoServer =
                InProcessServerBuilder.forName(repoName)
                        .directExecutor()
                        .addService(repo)
                        .build()
                        .start();
        InMemoryApiKeyIdentityResolver table =
                new InMemoryApiKeyIdentityResolver()
                        .register(KEY_UNRESTRICTED, IntakeScope.unrestricted(ACCOUNT))
                        .register(
                                KEY_NARROW,
                                new IntakeScope(
                                        ACCOUNT,
                                        Set.of("ds-allowed"),
                                        Set.of("intake"),
                                        Set.of("text/plain"),
                                        16L));
        // KEY_BOOM simulates the key store itself failing, which must never
        // be misreported as a bad key.
        ApiKeyIdentityResolver resolver =
                credential -> {
                    if (KEY_BOOM.equals(credential)) {
                        throw new IllegalStateException("key store down");
                    }
                    return table.resolve(credential);
                };
        intake =
                IntakeServices.build(
                        new IntakeServiceConfig(
                                0,
                                IntakeServiceConfig.INPROCESS_TARGET_PREFIX + repoName,
                                1024L),
                        resolver);
        http = intake.startHttp(0);
        client = HttpClient.newHttpClient();
        base = "http://localhost:" + http.port();
    }

    @AfterAll
    static void shutdown() {
        intake.close();
        repoServer.shutdownNow();
    }

    @BeforeEach
    void reset() {
        repo.saves.clear();
        repo.deduplicated = false;
        repo.failure = null;
    }

    @Test
    void happyPathAnswersTheReceiptAndIssuesThePinnedIntakeSave() throws Exception {
        byte[] payload = "the quick brown fox".getBytes();
        HttpResponse<String> response =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-http&filename=fox.txt",
                        payload,
                        "x-api-key", KEY_UNRESTRICTED,
                        "Content-Type", "text/plain");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).hasValue("application/json");
        // The receipt speaks proto field names, every field present.
        assertThat(response.body())
                .contains("\"doc_id\"", "\"node_id\"", "\"address\"", "\"drive\"",
                        "\"size_bytes\"", "\"sha256\"", "\"deduplicated\"");
        IngestDocumentResponse receipt = parseReceipt(response.body());

        byte[] digest = MessageDigest.getInstance("SHA-256").digest(payload);
        String expectedSha = HexFormat.of().formatHex(digest);
        String expectedDocId = UUID.nameUUIDFromBytes(digest).toString();
        assertThat(receipt.getDocId()).isEqualTo(expectedDocId);
        assertThat(receipt.getNodeId()).isNotBlank();
        assertThat(receipt.getSha256()).isEqualTo(expectedSha);
        assertThat(receipt.getSizeBytes()).isEqualTo(payload.length);
        assertThat(receipt.getDrive()).isEqualTo("intake");
        assertThat(receipt.getDeduplicated()).isFalse();
        assertThat(receipt.getAddress().getDocId()).isEqualTo(expectedDocId);
        assertThat(receipt.getAddress().getAccountId()).isEqualTo(ACCOUNT);
        assertThat(receipt.getAddress().getGraphId()).isEqualTo("intake:" + ACCOUNT);
        assertThat(receipt.getAddress().getGraphAddressId()).isEqualTo("ds-http");

        // The pinned save: identical to the gRPC raw lane's.
        assertThat(repo.saves).hasSize(1);
        SaveDocumentRequest save = repo.saves.getFirst();
        assertThat(save.getUseDatasourceId()).isTrue();
        assertThat(save.getGraphId()).isEqualTo("intake:" + ACCOUNT);
        assertThat(save.getDrive()).isEqualTo("intake");
        assertThat(save.getConnectorId()).isEqualTo("intake");
        assertThat(save.getWrittenBy().getModuleId()).isEqualTo("intake");
        Document saved = save.getDocument();
        assertThat(saved.getDocId()).isEqualTo(expectedDocId);
        assertThat(saved.getOwnership().getAccountId()).isEqualTo(ACCOUNT);
        assertThat(saved.getOwnership().getDatasourceId()).isEqualTo("ds-http");
        assertThat(saved.getOwnership().getConnectorId()).isEqualTo("intake");
        assertThat(saved.getBlobBag().getBlob().getData().toByteArray()).isEqualTo(payload);
        assertThat(saved.getBlobBag().getBlob().getFilename()).isEqualTo("fox.txt");
        assertThat(saved.getBlobBag().getBlob().getMimeType()).isEqualTo("text/plain");
        assertThat(saved.getBlobBag().getBlob().getChecksum()).isEqualTo(expectedSha);
        assertThat(saved.getSearchMetadata().getSourceMimeType()).isEqualTo("text/plain");
        assertThat(saved.getDocIdDerivation().getMethod())
                .isEqualTo(DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_CONTENT_HASH);
    }

    @Test
    void bearerAuthorizationHeaderAuthenticates() throws Exception {
        HttpResponse<String> response =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-http",
                        "bearer works".getBytes(),
                        "authorization", "Bearer " + KEY_UNRESTRICTED);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(parseReceipt(response.body()).getDocId()).isNotBlank();
    }

    @Test
    void headerTargetingWorksAndTheQueryParameterWins() throws Exception {
        HttpResponse<String> headerOnly =
                post(
                        IntakeHttpServer.UPLOAD_PATH,
                        "via header".getBytes(),
                        "x-api-key", KEY_UNRESTRICTED,
                        "x-datasource-id", "ds-header");
        assertThat(headerOnly.statusCode()).isEqualTo(200);
        assertThat(repo.saves.getFirst().getDocument().getOwnership().getDatasourceId())
                .isEqualTo("ds-header");

        repo.saves.clear();
        HttpResponse<String> both =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-query",
                        "query wins".getBytes(),
                        "x-api-key", KEY_UNRESTRICTED,
                        "x-datasource-id", "ds-header");
        assertThat(both.statusCode()).isEqualTo(200);
        assertThat(repo.saves.getFirst().getDocument().getOwnership().getDatasourceId())
                .isEqualTo("ds-query");
    }

    @Test
    void missingKeyIs401NamingTheHeaders() throws Exception {
        HttpResponse<String> response =
                post(IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-http", "x".getBytes());
        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("x-api-key").contains("authorization");
    }

    @Test
    void unknownKeyIs401() throws Exception {
        HttpResponse<String> response =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-http",
                        "x".getBytes(),
                        "x-api-key", "no-such-key");
        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("unknown API key");
    }

    @Test
    void keyStoreFailureIs500NeverABadKeyVerdict() throws Exception {
        HttpResponse<String> response =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-http",
                        "x".getBytes(),
                        "x-api-key", KEY_BOOM);
        assertThat(response.statusCode()).isEqualTo(500);
        assertThat(response.body()).contains("API key store failure");
    }

    @Test
    void missingDatasourceIs400NamingIt() throws Exception {
        HttpResponse<String> response =
                post(IntakeHttpServer.UPLOAD_PATH, "x".getBytes(), "x-api-key", KEY_UNRESTRICTED);
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("datasource_id");
    }

    @Test
    void accountIdentityParametersAre400TheAccountRidesTheKey() throws Exception {
        HttpResponse<String> queryParam =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-http&account_id=someone",
                        "x".getBytes(),
                        "x-api-key", KEY_UNRESTRICTED);
        assertThat(queryParam.statusCode()).isEqualTo(400);
        assertThat(queryParam.body()).contains("account identity rides the API key");

        HttpResponse<String> header =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-http",
                        "x".getBytes(),
                        "x-api-key", KEY_UNRESTRICTED,
                        "x-account-id", "someone");
        assertThat(header.statusCode()).isEqualTo(400);
        assertThat(repo.saves).isEmpty();
    }

    @Test
    void scopeViolationsAre403WithTheGrpcLaneMessages() throws Exception {
        HttpResponse<String> datasource =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-forbidden",
                        "x".getBytes(),
                        "x-api-key", KEY_NARROW,
                        "Content-Type", "text/plain");
        // JsonFormat escapes apostrophes as ', so pin apostrophe-free
        // fragments of the gRPC lanes' messages.
        assertThat(datasource.statusCode()).isEqualTo(403);
        assertThat(datasource.body()).contains("ds-forbidden").contains("outside the API key");

        HttpResponse<String> drive =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-allowed&drive=pipeline",
                        "x".getBytes(),
                        "x-api-key", KEY_NARROW,
                        "Content-Type", "text/plain");
        assertThat(drive.statusCode()).isEqualTo(403);
        assertThat(drive.body()).contains("drive").contains("pipeline");

        HttpResponse<String> mime =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-allowed",
                        "x".getBytes(),
                        "x-api-key", KEY_NARROW,
                        "Content-Type", "application/pdf");
        assertThat(mime.statusCode()).isEqualTo(403);
        assertThat(mime.body()).contains("content-type restrictions");
        assertThat(repo.saves).isEmpty();
    }

    @Test
    void payloadCapsAre413BeforeTheBodyIsBuffered() throws Exception {
        // Per-key cap (16 bytes on the narrow key).
        HttpResponse<String> perKey =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-allowed",
                        "x".repeat(17).getBytes(),
                        "x-api-key", KEY_NARROW,
                        "Content-Type", "text/plain");
        assertThat(perKey.statusCode()).isEqualTo(413);
        assertThat(perKey.body()).contains("exceeds the API key");

        // Service cap (1024 bytes in this rig) on an otherwise unrestricted key.
        HttpResponse<String> service =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-http",
                        "y".repeat(2048).getBytes(),
                        "x-api-key", KEY_UNRESTRICTED);
        assertThat(service.statusCode()).isEqualTo(413);
        assertThat(service.body()).contains("service cap");
        assertThat(repo.saves).isEmpty();
    }

    @Test
    void nonPostIs405() throws Exception {
        HttpResponse<String> response =
                client.send(
                        HttpRequest.newBuilder(URI.create(base + IntakeHttpServer.UPLOAD_PATH))
                                .header("x-api-key", KEY_UNRESTRICTED)
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(405);
    }

    @Test
    void chunkedBodyWithoutContentLengthIs411() throws Exception {
        byte[] payload = "no declared length".getBytes();
        HttpResponse<String> response =
                client.send(
                        HttpRequest.newBuilder(
                                        URI.create(
                                                base + IntakeHttpServer.UPLOAD_PATH
                                                        + "?datasource_id=ds-http"))
                                .header("x-api-key", KEY_UNRESTRICTED)
                                // An InputStream publisher has unknown length,
                                // so the client sends chunked with no
                                // Content-Length header.
                                .POST(
                                        HttpRequest.BodyPublishers.ofInputStream(
                                                () -> new ByteArrayInputStream(payload)))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(411);
        assertThat(response.body()).contains("Content-Length");
    }

    @Test
    void dedupeEchoesThroughTheReceipt() throws Exception {
        repo.deduplicated = true;
        HttpResponse<String> response =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-http",
                        "already stored".getBytes(),
                        "x-api-key", KEY_UNRESTRICTED);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(parseReceipt(response.body()).getDeduplicated()).isTrue();
    }

    @Test
    void repoNotFoundFlattensTo404() throws Exception {
        repo.failure =
                Status.NOT_FOUND
                        .withDescription("drive 'intake' not found for account '" + ACCOUNT + "'")
                        .asRuntimeException();
        HttpResponse<String> response =
                post(
                        IntakeHttpServer.UPLOAD_PATH + "?datasource_id=ds-http",
                        "x".getBytes(),
                        "x-api-key", KEY_UNRESTRICTED);
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).contains("not found");
    }

    // ------------------------------------------------------------------
    // helpers

    static HttpResponse<String> post(String pathAndQuery, byte[] body, String... headers)
            throws Exception {
        HttpRequest.Builder request =
                HttpRequest.newBuilder(URI.create(base + pathAndQuery))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        for (int i = 0; i < headers.length; i += 2) {
            request.header(headers[i], headers[i + 1]);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    static IngestDocumentResponse parseReceipt(String json) throws Exception {
        IngestDocumentResponse.Builder receipt = IngestDocumentResponse.newBuilder();
        JsonFormat.parser().merge(json, receipt);
        return receipt.build();
    }
}
