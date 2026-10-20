package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.accession.DocumentSaves;
import ai.protomolt.proto.repo.accession.DocumentUploads;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.catalog.CatalogConfig;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.spi.ArchiveRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.stack.BlobStoreStack;
import ai.protomolt.proto.repo.v2.StackRecord;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

/**
 * An {@link UploadHttpServer} built over the parts-based {@link DocumentUploads}
 * (Decision 4): the document route commits through the v2 {@link
 * ai.protomolt.proto.repo.accession.UploadHttpHandler} against a real catalog and
 * S3 stack, while the archive rendition route on the same server still reaches the
 * archive port, under one operator credential.
 */
@Testcontainers
class UploadHttpServerV2DelegationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    private static final String TOKEN = "synthetic-http-operator-key";
    private static final String BUCKET = "upload-v2-delegation";

    private final List<Map<String, Object>> archiveCalls = new CopyOnWriteArrayList<>();
    private S3Client s3;
    private PostgresCatalog catalog;
    private DocumentUploads uploads;
    private String prefix;
    private UploadHttpServer server;
    private HttpClient client;

    @BeforeEach void start() {
        s3 = S3Client.builder().endpointOverride(URI.create(S3.getEndpoint().toString()))
                .region(Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .httpClient(UrlConnectionHttpClient.create()).build();
        if (!s3.listBuckets().buckets().stream().anyMatch(bucket -> bucket.name().equals(BUCKET))) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
        catalog = new PostgresCatalog(new CatalogConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                POSTGRES.getPassword(), 4));
        prefix = "delegation/" + UUID.randomUUID();
        var stackId = catalog.registerStack(StackRecord.newBuilder().setStackId("stack-" + UUID.randomUUID())
                .setProvider("s3").setEndpoint(S3.getEndpoint().toString()).setNamespace(BUCKET + "/" + prefix).build());
        var drives = new DocumentSaves.Drives("default",
                Map.of("default", new BlobStoreStack(stackId, new S3BlobStore(s3), BUCKET, prefix)));
        var limits = DocumentSchemaAdmission.Limits.SERVICE_DEFAULTS;
        var saves = new DocumentSaves(catalog, drives, selection -> null, limits, bytes -> () -> {},
                Clock.systemUTC());
        uploads = new DocumentUploads(saves);
        server = new UploadHttpServer(uploads,
                () -> new RepositoryCaller("http-upload-host", true), TOKEN, recordingArchive());
        server.start(0);
        client = HttpClient.newHttpClient();
    }

    @AfterEach void stop() {
        if (server != null) server.close();
        if (catalog != null) catalog.close();
        if (s3 != null) s3.close();
    }

    @Test void theDocumentRouteCommitsThroughTheV2Handler() throws Exception {
        byte[] body = "parts-based upload".getBytes(StandardCharsets.UTF_8);
        String sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
        String account = "acct-" + UUID.randomUUID();
        String query = "?account_id=" + account + "&datasource_id=ds-1&drive=default&filename=note.txt"
                + "&operation_id=op-1&doc_id=doc-1";

        var first = post(UploadHttpServer.UPLOAD_PATH + query, TOKEN, body, sha256);
        assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
        // The v2 receipt: revision and replay flags, none of the v1 node_id/storage_ref shape.
        assertThat(first.body())
                .contains("\"operation_id\":\"op-1\"", "\"doc_id\":\"doc-1\"", "\"revision\":1",
                        "\"replayed\":false", "\"size_bytes\":" + body.length, "\"sha256\":\"" + sha256 + "\"")
                .doesNotContain("node_id", "storage_ref");
        assertThat(first.headers().firstValue("Content-Type")).hasValue("application/json; charset=utf-8");
        assertThat(s3.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).prefix(prefix).build())
                .contents()).as("bytes landed in the configured stack").isNotEmpty();

        var replay = post(UploadHttpServer.UPLOAD_PATH + query, TOKEN, body, sha256);
        assertThat(replay.statusCode()).as(replay.body()).isEqualTo(200);
        assertThat(replay.body()).contains("\"revision\":1", "\"replayed\":true");
        assertThat(archiveCalls).isEmpty();
    }

    @Test void theArchiveRouteStillReachesTheArchivePort() throws Exception {
        byte[] body = "frozen rendition".getBytes(StandardCharsets.UTF_8);
        var response = post(UploadHttpServer.ARCHIVE_UPLOAD_PATH
                + "?account_id=acct-1&archive=records&entry_id=entry-1&filename=r.bin", TOKEN, body, null);
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        assertThat(response.body()).contains("\"entry_uuid\":\"entry-uuid-1\"", "\"rendition\":\"original\"");
        assertThat(archiveCalls).singleElement().satisfies(call -> {
            assertThat(call.get("address")).isEqualTo(EntryAddress.newBuilder()
                    .setAccountId("acct-1").setArchive("records").setEntryId("entry-1").build());
            assertThat(call.get("body")).isEqualTo(body);
        });
    }

    @Test void bothRoutesDemandTheOneOperatorCredential() throws Exception {
        byte[] body = "x".getBytes(StandardCharsets.UTF_8);
        var document = post(UploadHttpServer.UPLOAD_PATH
                + "?account_id=a&datasource_id=d&drive=default&filename=f", "wrong-key", body, null);
        var archive = post(UploadHttpServer.ARCHIVE_UPLOAD_PATH
                + "?account_id=a&archive=b&entry_id=c", "wrong-key", body, null);
        assertThat(document.statusCode()).isEqualTo(401);
        assertThat(archive.statusCode()).isEqualTo(401);
        assertThat(archiveCalls).isEmpty();
    }

    @Test void theV2ConstructorRefusesMissingCollaboratorsByName() {
        // A null uploads mounts no document route (archive-only host); the other
        // collaborators are still validated by name.
        assertThatThrownBy(() -> new UploadHttpServer(uploads, null, TOKEN, null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("callers");
        assertThatThrownBy(() -> new UploadHttpServer(uploads, () -> null, " ", null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("API token");
    }

    private HttpResponse<String> post(String pathAndQuery, String token, byte[] body, String sha256)
            throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + pathAndQuery))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "text/plain");
        if (sha256 != null) request.header("X-Content-Sha256", sha256);
        return client.send(request.POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /** Records uploadStream calls; every other archive operation is outside this server's routes. */
    private ArchiveRepository recordingArchive() {
        return (ArchiveRepository) Proxy.newProxyInstance(ArchiveRepository.class.getClassLoader(),
                new Class<?>[] {ArchiveRepository.class}, (proxy, method, args) -> {
                    if (!method.getName().equals("uploadStream")) {
                        throw new UnsupportedOperationException(method.getName());
                    }
                    byte[] bytes = ((InputStream) args[9]).readAllBytes();
                    archiveCalls.add(Map.of("address", args[1], "body", bytes));
                    var descriptor = (RenditionDescriptor) args[2];
                    return new ArchiveRepository.UploadResult("entry-uuid-1", 1L, "sha", bytes.length,
                            "objects/" + descriptor.getName(), "root", false);
                });
    }
}
