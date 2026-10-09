package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetDocumentRequest;
import ai.protomolt.proto.repo.v1.GetDocumentResponse;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration test of the streaming HTTP upload route against
 * REAL infrastructure: testcontainers PostgreSQL + LocalStack S3, the full
 * service stack booted through {@link RepoServices}, and a real JDK
 * {@code HttpServer} on an ephemeral port driven by {@code java.net.http}.
 *
 * <p>The semantics under test are the old repository-service's raw upload
 * path ({@code RawUploadDedupeTest}), ported: the body streams to object
 * storage without buffering, identical re-uploads dedupe by checksum and
 * answer with the existing coordinates, and the Content-Length contract is
 * enforced.
 */
@Testcontainers
class UploadHttpServerIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ACCOUNT = "acct-http";
    private static final String DATASOURCE = "ds-http";
    private static final String DRIVE = "upload";
    /** Multi-megabyte payload: proves the route streams, not buffers. */
    private static final int PAYLOAD_SIZE = 8 * 1024 * 1024;

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices services;
    static ManagedChannel channel;
    static DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    static UploadHttpServer http;
    static RepoServiceConfig config;
    static HttpClient client;
    static String uploadUrl;

    @BeforeAll
    static void boot() {
        config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-http-docs",
                0, null, null, null, null, 0, 0L);
        services = RepoServices.build(config.withManagedStorage(new ManagedStoragePolicy("http-test-v1", "test-realm", true)));
        services.startInProcess("it-http");
        http = services.startHttp(0, "synthetic-http-operator-key"); // ephemeral port
        channel = InProcessChannelBuilder.forName("it-http").build();
        documents = DocumentServiceGrpc.newBlockingStub(channel);
        DriveServiceGrpc.DriveServiceBlockingStub drives = DriveServiceGrpc.newBlockingStub(channel);
        drives.createDrive(CreateDriveRequest.newBuilder()
                .setName(DRIVE)
                .setAccountId(ACCOUNT)
                .build());
        client = HttpClient.newHttpClient();
        uploadUrl = "http://localhost:" + http.port() + UploadHttpServer.UPLOAD_PATH;
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    @Test void sameGenerationCannotBeReboundToAnotherStorageRealm() {
        assertThatThrownBy(() -> RepoServices.build(config.withManagedStorage(
                new ManagedStoragePolicy("http-test-v1", "different-realm", true))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("another physical profile");
    }

    @Test void disabledManagedStorageRefusesUploadsWithoutLegacyFallback() throws Exception {
        try (var disabled = RepoServices.build(config)) {
            var endpoint = disabled.startHttp(0, "synthetic-http-operator-key");
            var request = HttpRequest.newBuilder(URI.create("http://localhost:" + endpoint.port()
                    + UploadHttpServer.UPLOAD_PATH + "?account_id=" + ACCOUNT + "&datasource_id=" + DATASOURCE
                    + "&drive=" + DRIVE + "&filename=disabled.bin&doc_id=disabled-upload"))
                    .header("api_token", "synthetic-http-operator-key")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[] {1})).build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(503);
            assertThat(response.body()).contains("Managed document ingestion is not configured");
            assertThat(disabled.documentLedger().findByReference(ai.protomolt.proto.repo.v1.NodeAddress.newBuilder()
                    .setAccountId(ACCOUNT).setDocId("disabled-upload").setGraphId("intake:" + ACCOUNT)
                    .setGraphAddressId(DATASOURCE).build())).isEmpty();
        }
    }

    // ------------------------------------------------------------------ tests

    @Test
    void uploadStreamsMultiMbAndRoundTripsByteExact() throws Exception {
        String docId = "doc-http-" + UUID.randomUUID();
        String expectedSha = sha256OfPattern(PAYLOAD_SIZE);

        HttpResponse<String> response = client.send(uploadRequest(docId, PAYLOAD_SIZE,
                        patternPublisher(PAYLOAD_SIZE), null)
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode receipt = MAPPER.readTree(response.body());
        assertThat(receipt.get("doc_id").asText()).isEqualTo(docId);
        assertThat(receipt.get("deduplicated").asBoolean()).isFalse();
        assertThat(receipt.get("size_bytes").asLong()).isEqualTo(PAYLOAD_SIZE);
        assertThat(receipt.get("sha256").asText()).isEqualTo(expectedSha);
        String expectedKey = receipt.get("storage_ref").get("object_key").asText();
        assertThat(expectedKey).startsWith(DRIVE + "/blobs/.protomolt-managed/v1/");
        String nodeId = receipt.get("node_id").asText();
        assertThat(nodeId).isNotBlank();

        // GetDocument assembles the intake state: blob_bag carries the
        // storage_ref + checksum of the streamed body.
        GetDocumentResponse doc = documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId(nodeId).build());
        var blob = doc.getDocument().getBlobBag().getBlob();
        assertThat(doc.getDocument().getDocId()).isEqualTo(docId);
        assertThat(blob.getStorageRef().getDriveName()).isEqualTo(DRIVE);
        assertThat(blob.getStorageRef().getObjectKey()).isEqualTo(expectedKey);
        assertThat(blob.getChecksum()).isEqualTo(expectedSha);
        assertThat(blob.getSizeBytes()).isEqualTo(PAYLOAD_SIZE);
        assertThat(blob.getFilename()).isEqualTo("big.bin");
        assertThat(doc.getDocument().getOwnership().getAccountId()).isEqualTo(ACCOUNT);
        assertThat(doc.getDocument().getOwnership().getDatasourceId()).isEqualTo(DATASOURCE);

        // The manifest's BLOBS entry is PRESENT (the claim check landed).
        assertThat(doc.getManifest().getPartsList().stream()
                .filter(e -> e.getPart() == DocumentPart.DOCUMENT_PART_BLOBS)
                .map(PartManifestEntry::getState))
                .containsExactly(PartState.PART_STATE_PRESENT);

        // GetBlob returns the EXACT bytes that were streamed.
        var got = documents.getBlob(GetBlobRequest.newBuilder()
                .setStorageRef(ai.protomolt.proto.repo.v1.FileStorageReference.newBuilder()
                        .setDriveName(DRIVE)
                        .setObjectKey(expectedKey))
                .build());
        assertThat(got.getData().size()).isEqualTo(PAYLOAD_SIZE);
        assertThat(sha256(got.getData().toByteArray())).isEqualTo(expectedSha);
    }

    @Test
    void identicalReuploadDeduplicates() throws Exception {
        String docId = "doc-http-" + UUID.randomUUID();
        int size = 1024 * 1024;

        JsonNode first = MAPPER.readTree(client.send(
                uploadRequest(docId, size, patternPublisher(size), null).build(),
                HttpResponse.BodyHandlers.ofString()).body());
        assertThat(first.get("deduplicated").asBoolean()).isFalse();

        // Same logical doc, same bytes: deduplicated=true, same node_id —
        // the RawUploadDedupeTest semantics.
        JsonNode second = MAPPER.readTree(client.send(
                uploadRequest(docId, size, patternPublisher(size), null).build(),
                HttpResponse.BodyHandlers.ofString()).body());
        assertThat(second.get("deduplicated").asBoolean()).isTrue();
        assertThat(second.get("node_id").asText()).isEqualTo(first.get("node_id").asText());
        assertThat(second.get("sha256").asText()).isEqualTo(first.get("sha256").asText());

        // The row carries the re-processed marker.
        var row = services.documentLedger()
                .findByNodeId(UUID.fromString(first.get("node_id").asText())).orElseThrow();
        assertThat(row.reprocessCount).isEqualTo(1);
    }

    @Test
    void blankDocIdDerivesFromContentAndDeduplicates() throws Exception {
        int size = 512 * 1024;
        // No doc_id param: the server derives one from the content hash.
        HttpRequest.Builder template = HttpRequest.newBuilder(URI.create(uploadUrl
                        + "?account_id=" + ACCOUNT + "&datasource_id=" + DATASOURCE
                        + "&drive=" + DRIVE + "&filename=derived.bin"))
                .header("api_token", "synthetic-http-operator-key")
                .POST(patternPublisher(size));
        JsonNode first = MAPPER.readTree(client.send(template.build(),
                HttpResponse.BodyHandlers.ofString()).body());
        assertThat(first.get("doc_id").asText()).isNotBlank();
        assertThat(first.get("deduplicated").asBoolean()).isFalse();

        // Same bytes again: the derived doc_id is stable, so the re-upload
        // dedupes onto the same node.
        JsonNode second = MAPPER.readTree(client.send(template.build(),
                HttpResponse.BodyHandlers.ofString()).body());
        assertThat(second.get("doc_id").asText()).isEqualTo(first.get("doc_id").asText());
        assertThat(second.get("node_id").asText()).isEqualTo(first.get("node_id").asText());
        assertThat(second.get("deduplicated").asBoolean()).isTrue();
    }

    @Test
    void missingContentLengthIs411() throws Exception {
        // ofInputStream has an unknown length → the client sends chunked,
        // which the route rejects by contract.
        HttpRequest request = HttpRequest.newBuilder(URI.create(uploadUrl
                        + "?account_id=" + ACCOUNT + "&datasource_id=" + DATASOURCE
                        + "&drive=" + DRIVE + "&filename=x.bin"))
                .header("api_token", "synthetic-http-operator-key")
                .POST(HttpRequest.BodyPublishers.ofInputStream(
                        () -> patternStream(4096)))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(411);
    }

    @Test
    void missingAccountIdIs400NamingTheParam() throws Exception {
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(uploadUrl
                        + "?datasource_id=" + DATASOURCE + "&drive=" + DRIVE + "&filename=x.bin"))
                .header("api_token", "synthetic-http-operator-key")
                .POST(patternPublisher(128))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("account_id");
    }

    @Test
    void unknownDriveIs404() throws Exception {
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(uploadUrl
                        + "?account_id=" + ACCOUNT + "&datasource_id=" + DATASOURCE
                        + "&drive=no-such-drive&filename=x.bin"))
                .header("api_token", "synthetic-http-operator-key")
                .POST(patternPublisher(128))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void declaredChecksumMismatchIs400WithoutPublishingADocument() throws Exception {
        String docId = "doc-http-" + UUID.randomUUID();
        int size = 64 * 1024;
        HttpResponse<String> response = client.send(uploadRequest(docId, size,
                        patternPublisher(size), "0".repeat(64))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("X-Content-Sha256");

        // Failed candidates remain durable for cleanup; they are never published.
        assertThat(services.documentLedger().findByReference(
                ai.protomolt.proto.repo.v1.NodeAddress.newBuilder().setAccountId(ACCOUNT)
                        .setDocId(docId).setGraphAddressId(DATASOURCE).setGraphId("intake:" + ACCOUNT).build()))
                .isEmpty();
    }

    @Test
    void rejectedReplacementPreservesCommittedDocumentAndRawBytes() throws Exception {
        String docId = "doc-http-" + UUID.randomUUID();
        int originalSize = 4096;
        var accepted = client.send(uploadRequest(docId, originalSize,
                        patternPublisher(originalSize), null).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(accepted.statusCode()).isEqualTo(200);
        JsonNode receipt = MAPPER.readTree(accepted.body());
        String nodeId = receipt.get("node_id").asText();
        var before = documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId(nodeId).build());
        var originalRef = before.getDocument().getBlobBag().getBlob().getStorageRef();

        var rejected = client.send(uploadRequest(docId, 8192,
                        patternPublisher(8192), "0".repeat(64)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(rejected.statusCode()).isEqualTo(400);
        assertThat(rejected.body()).contains("X-Content-Sha256");
        var after = documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId(nodeId).build());
        assertThat(after.getDocument()).isEqualTo(before.getDocument());
        var bytes = documents.getBlob(GetBlobRequest.newBuilder()
                .setStorageRef(originalRef).build()).getData();
        assertThat(bytes.size()).isEqualTo(originalSize);
        assertThat(sha256(bytes.toByteArray())).isEqualTo(sha256OfPattern(originalSize));
    }

    @Test
    void acceptedReplacementDoesNotOverwritePreviousRawReference() throws Exception {
        String docId = "doc-http-" + UUID.randomUUID();
        var accepted = client.send(uploadRequest(docId, 4096,
                        patternPublisher(4096), null).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(accepted.statusCode()).isEqualTo(200);
        String nodeId = MAPPER.readTree(accepted.body()).get("node_id").asText();
        var before = documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId(nodeId).build());
        var originalRef = before.getDocument().getBlobBag().getBlob().getStorageRef();

        var replacement = client.send(uploadRequest(docId, 8192,
                        patternPublisher(8192), null).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(replacement.statusCode()).isEqualTo(200);
        var after = documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId(nodeId).build());
        var replacementRef = after.getDocument().getBlobBag().getBlob().getStorageRef();
        assertThat(replacementRef).isNotEqualTo(originalRef);
        // No raw-object GC runs in this fixture. Replacement must not overwrite
        // the old bytes; later reclamation of an unreferenced object is allowed.
        var originalBytes = documents.getBlob(GetBlobRequest.newBuilder()
                .setStorageRef(originalRef).build()).getData();
        assertThat(originalBytes.size()).isEqualTo(4096);
        assertThat(sha256(originalBytes.toByteArray())).isEqualTo(sha256OfPattern(4096));
        var replacementBytes = documents.getBlob(GetBlobRequest.newBuilder()
                .setStorageRef(replacementRef).build()).getData();
        assertThat(replacementBytes.size()).isEqualTo(8192);
        assertThat(sha256(replacementBytes.toByteArray())).isEqualTo(sha256OfPattern(8192));
    }

    // ------------------------------------------------------------- fixtures

    private static HttpRequest.Builder uploadRequest(String docId, long length,
            HttpRequest.BodyPublisher body, String declaredSha) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(uploadUrl
                        + "?account_id=" + ACCOUNT + "&datasource_id=" + DATASOURCE
                        + "&drive=" + DRIVE + "&filename=big.bin&doc_id=" + docId
                        + "&connector_id=conn-http&crawl_id=crawl-1"))
                .header("Content-Type", "application/octet-stream")
                .header("api_token", "synthetic-http-operator-key")
                .POST(body);
        if (declaredSha != null) {
            builder.header("X-Content-Sha256", declaredSha);
        }
        return builder;
    }

    /**
     * A streaming body publisher of KNOWN length over an InputStream:
     * {@code fromPublisher} sets Content-Length (the route's contract) while
     * the bytes are produced on demand — the client side of the no-buffering
     * upload.
     */
    private static HttpRequest.BodyPublisher patternPublisher(long size) {
        return HttpRequest.BodyPublishers.fromPublisher(
                HttpRequest.BodyPublishers.ofInputStream(() -> patternStream(size)), size);
    }

    /** A deterministic byte pattern generated on the fly (never materialized). */
    private static InputStream patternStream(long size) {
        return new InputStream() {
            private long pos;

            @Override
            public int read() {
                if (pos >= size) {
                    return -1;
                }
                return (int) ((pos++ * 31 + 7) & 0xFF);
            }

            @Override
            public int read(byte[] b, int off, int len) {
                if (pos >= size) {
                    return -1;
                }
                int n = (int) Math.min(len, size - pos);
                for (int i = 0; i < n; i++) {
                    b[off + i] = (byte) ((pos + i) * 31 + 7);
                }
                pos += n;
                return n;
            }
        };
    }

    private static String sha256OfPattern(long size) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[8192];
            try (InputStream in = patternStream(size)) {
                int read;
                while ((read = in.read(buf)) != -1) {
                    digest.update(buf, 0, read);
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String sha256(byte[] data) {
        try {
            return HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
