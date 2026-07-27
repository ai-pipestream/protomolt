package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.blob.DocumentIds;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.GetBlobRequest;
import ai.pipestream.proto.repo.v1.GetDocumentRequest;
import ai.pipestream.proto.repo.v1.GetDocumentResponse;
import ai.pipestream.proto.repo.v1.PartManifestEntry;
import ai.pipestream.proto.repo.v1.PartState;
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
@Testcontainers(disabledWithoutDocker = true)
class UploadHttpServerIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ACCOUNT = "acct-http";
    private static final String DATASOURCE = "ds-http";
    private static final String DRIVE = "upload";
    /** Multi-megabyte payload: proves the route streams, not buffers. */
    private static final int PAYLOAD_SIZE = 8 * 1024 * 1024;

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices services;
    static ManagedChannel channel;
    static DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    static UploadHttpServer http;
    static HttpClient client;
    static String uploadUrl;

    @BeforeAll
    static void boot() {
        RepoServiceConfig config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-http-docs",
                0, null, null, null, null, 0, 0L);
        services = RepoServices.build(config);
        services.startInProcess("it-http");
        http = services.startHttp(0); // ephemeral port
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
        String expectedKey = DRIVE + "/blobs/" + ACCOUNT + "/"
                + DocumentIds.blobId(docId, DATASOURCE, ACCOUNT) + ".bin";
        assertThat(receipt.get("storage_ref").get("object_key").asText()).isEqualTo(expectedKey);
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
                .setStorageRef(ai.pipestream.proto.repo.v1.FileStorageReference.newBuilder()
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
                .POST(patternPublisher(128))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void declaredChecksumMismatchIs400AndDeletesTheLandedObject() throws Exception {
        String docId = "doc-http-" + UUID.randomUUID();
        int size = 64 * 1024;
        HttpResponse<String> response = client.send(uploadRequest(docId, size,
                        patternPublisher(size), "0".repeat(64))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("X-Content-Sha256");

        // The mismatched landing was best-effort deleted, not left posing as
        // the document's body.
        String key = DRIVE + "/blobs/" + ACCOUNT + "/"
                + DocumentIds.blobId(docId, DATASOURCE, ACCOUNT) + ".bin";
        org.junit.jupiter.api.Assertions.assertThrows(
                io.grpc.StatusRuntimeException.class,
                () -> documents.getBlob(GetBlobRequest.newBuilder()
                        .setStorageRef(ai.pipestream.proto.repo.v1.FileStorageReference.newBuilder()
                                .setDriveName(DRIVE)
                                .setObjectKey(key))
                        .build()));
    }

    // ------------------------------------------------------------- fixtures

    private static HttpRequest.Builder uploadRequest(String docId, long length,
            HttpRequest.BodyPublisher body, String declaredSha) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(uploadUrl
                        + "?account_id=" + ACCOUNT + "&datasource_id=" + DATASOURCE
                        + "&drive=" + DRIVE + "&filename=big.bin&doc_id=" + docId
                        + "&connector_id=conn-http&crawl_id=crawl-1"))
                .header("Content-Type", "application/octet-stream")
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
