package ai.pipestream.proto.acquire.s3;

import ai.pipestream.proto.acquire.pull.GrpcIntakeFeed;
import ai.pipestream.proto.acquire.pull.IntakeFeed;
import ai.pipestream.proto.acquire.pull.PullDocuments;
import ai.pipestream.proto.acquire.pull.PullReport;
import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.intake.service.IntakeServiceConfig;
import ai.pipestream.proto.intake.service.IntakeServices;
import ai.pipestream.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.IntakeScope;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.service.RepoServiceConfig;
import ai.pipestream.proto.repo.service.RepoServices;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.pipestream.proto.repo.v1.ListDocumentsRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The S3 connector against the real stack: a LocalStack source bucket pulled through the real
 * intake service into a real repo-service (Postgres + LocalStack). Proves stable identity (a
 * changed object replaces its own document), watermark incrementality, repository dedupe, the
 * cap, and the {@code pull-s3} verb's JSON round trip.
 */
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class S3PullIT {

    static final String ACCOUNT = "acct-s3-pull-it";
    static final String API_KEY = "s3-pull-it-key";
    static final String DATASOURCE = "ds-s3-pull";
    static final String SOURCE_BUCKET = "pull-source";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices repo;
    static IntakeServices intake;
    static ManagedChannel repoChannel;
    static S3Client sourceS3;
    static RecordingFeed feed;
    static S3Pull pull;

    static String watermark;

    @BeforeAll
    static void boot() throws Exception {
        RepoServiceConfig config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "s3-pull-it-docs",
                0,
                null, null, null, null, 0, 0L);
        repo = RepoServices.build(config);
        repo.startInProcess("s3-pull-it-repo");
        repoChannel = InProcessChannelBuilder.forName("s3-pull-it-repo").build();
        DriveServiceGrpc.newBlockingStub(repoChannel).createDrive(
                CreateDriveRequest.newBuilder()
                        .setName("intake")
                        .setAccountId(ACCOUNT)
                        .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                        .build());

        intake = IntakeServices.build(
                new IntakeServiceConfig(
                        0,
                        IntakeServiceConfig.INPROCESS_TARGET_PREFIX + "s3-pull-it-repo",
                        IntakeServiceConfig.DEFAULT_MAX_PAYLOAD_BYTES),
                new InMemoryApiKeyIdentityResolver()
                        .register(API_KEY, IntakeScope.unrestricted(ACCOUNT)));
        intake.startInProcess("s3-pull-it-intake");

        sourceS3 = S3Client.builder()
                .region(Region.of(LOCALSTACK.getRegion()))
                .endpointOverride(URI.create(LOCALSTACK.getEndpoint().toString()))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
        sourceS3.createBucket(b -> b.bucket(SOURCE_BUCKET));

        feed = new RecordingFeed(new GrpcIntakeFeed(
                GrpcIntakeFeed.INPROCESS_TARGET_PREFIX + "s3-pull-it-intake", API_KEY));
        pull = new S3Pull(sourceS3, feed);
    }

    @AfterAll
    static void shutdown() {
        feed.close();
        sourceS3.close();
        repoChannel.shutdownNow();
        intake.close();
        repo.close();
    }

    static void putObject(String key, String content, String contentType) {
        sourceS3.putObject(PutObjectRequest.builder()
                        .bucket(SOURCE_BUCKET).key(key).contentType(contentType).build(),
                RequestBody.fromString(content));
    }

    static Document fetch(String docId) {
        IngestDocumentResponse receipt = feed.receipts.get(docId);
        assertThat(receipt).as("no intake receipt recorded for " + docId).isNotNull();
        return DocumentServiceGrpc.newBlockingStub(repoChannel)
                .getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(receipt.getAddress())
                        .build())
                .getDocument();
    }

    static int totalDocuments() {
        return DocumentServiceGrpc.newBlockingStub(repoChannel)
                .listDocuments(ListDocumentsRequest.newBuilder()
                        .setDrive("intake").setAccountId(ACCOUNT).setLimit(100).build())
                .getTotalCount();
    }

    @Test
    @Order(1)
    void firstPullImportsTheBucketWithStableIdentity() {
        putObject("a.txt", "alpha", "text/plain");
        putObject("b/beta.txt", "beta", "text/plain");

        PullReport report = pull.pull(SOURCE_BUCKET, "", DATASOURCE, "", "", 0);
        assertThat(report.submitted()).isEqualTo(2);
        assertThat(report.deduplicated()).isZero();
        assertThat(report.failed()).isZero();
        assertThat(report.errors()).isEmpty();
        assertThat(report.watermark()).isNotBlank();
        watermark = report.watermark();

        String docId = PullDocuments.docId(
                S3Pull.CONNECTOR_ID, DATASOURCE, "s3://" + SOURCE_BUCKET + "/a.txt");
        Document stored = fetch(docId);
        assertThat(stored.getBlobBag().getBlob().getData().toStringUtf8()).isEqualTo("alpha");
        assertThat(stored.getBlobBag().getBlob().getMimeType()).isEqualTo("text/plain");
        assertThat(stored.getOwnership().getAccountId()).isEqualTo(ACCOUNT);
        assertThat(stored.getOwnership().getDatasourceId()).isEqualTo(DATASOURCE);
        assertThat(stored.getOwnership().getConnectorId()).isEqualTo(S3Pull.CONNECTOR_ID);
        assertThat(totalDocuments()).isEqualTo(2);
    }

    @Test
    @Order(2)
    void watermarkedRePullFindsNothingAndFullRePullDedupes() {
        PullReport incremental = pull.pull(SOURCE_BUCKET, "", DATASOURCE, "", watermark, 0);
        assertThat(incremental.submitted()).isZero();
        assertThat(incremental.deduplicated()).isZero();
        assertThat(incremental.watermark()).isEqualTo(watermark);

        PullReport full = pull.pull(SOURCE_BUCKET, "", DATASOURCE, "", "", 0);
        assertThat(full.submitted()).isZero();
        assertThat(full.deduplicated()).isEqualTo(2);
        assertThat(totalDocuments()).isEqualTo(2);
    }

    @Test
    @Order(3)
    void newObjectArrivesIncrementally() throws Exception {
        Thread.sleep(1100); // S3 last-modified has second granularity; move the clock
        putObject("z-new.txt", "gamma", "text/plain");

        PullReport report = pull.pull(SOURCE_BUCKET, "", DATASOURCE, "", watermark, 0);
        assertThat(report.submitted()).isEqualTo(1);
        assertThat(report.deduplicated()).isZero();
        assertThat(report.watermark()).isNotEqualTo(watermark);
        watermark = report.watermark();
        assertThat(totalDocuments()).isEqualTo(3);
    }

    @Test
    @Order(4)
    void changedObjectReplacesItsOwnDocumentInsteadOfDuplicating() throws Exception {
        Thread.sleep(1100);
        putObject("a.txt", "alpha revised", "text/plain");

        PullReport report = pull.pull(SOURCE_BUCKET, "", DATASOURCE, "", watermark, 0);
        assertThat(report.submitted()).isEqualTo(1);
        watermark = report.watermark();

        assertThat(totalDocuments())
                .as("the changed object must replace its own document, never add one")
                .isEqualTo(3);
        String docId = PullDocuments.docId(
                S3Pull.CONNECTOR_ID, DATASOURCE, "s3://" + SOURCE_BUCKET + "/a.txt");
        assertThat(fetch(docId).getBlobBag().getBlob().getData().toStringUtf8())
                .isEqualTo("alpha revised");
    }

    @Test
    @Order(5)
    void capLeavesTheRestForTheNextPull() throws Exception {
        Thread.sleep(1100);
        putObject("cap-1.txt", "one", "text/plain");
        putObject("cap-2.txt", "two", "text/plain");

        PullReport first = pull.pull(SOURCE_BUCKET, "", DATASOURCE, "", watermark, 1);
        assertThat(first.submitted()).isEqualTo(1);
        PullReport second = pull.pull(SOURCE_BUCKET, "", DATASOURCE, "", first.watermark(), 0);
        assertThat(second.submitted()).isEqualTo(1);
        watermark = second.watermark();
        assertThat(totalDocuments()).isEqualTo(5);
    }

    @Test
    @Order(6)
    void missingIdentityAndBadWatermarksAreRefusedByName() {
        assertThatThrownBy(() -> pull.pull(" ", "", DATASOURCE, "", "", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bucket");
        assertThatThrownBy(() -> pull.pull(SOURCE_BUCKET, "", "", "", "", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("datasourceId");
        assertThatThrownBy(() -> pull.pull(SOURCE_BUCKET, "", DATASOURCE, "", "junk", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("watermark");
    }

    @Test
    @Order(7)
    void theVerbRoundTripsJson() throws Exception {
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create())
                .register(new S3PullAction(pull));
        ObjectMapper json = new ObjectMapper();
        ObjectNode input = json.createObjectNode()
                .put("bucket", SOURCE_BUCKET)
                .put("prefix", "b/")
                .put("datasourceId", DATASOURCE);
        ObjectNode output = catalog.execute(S3PullAction.NAME, input);
        assertThat(output.get("deduplicated").asInt()).isEqualTo(1);
        assertThat(output.get("submitted").asInt()).isZero();
        assertThat(output.get("watermark").asText()).isNotBlank();
        assertThat(output.get("errors")).isEmpty();
    }

    /** Records intake receipts by doc id so assertions can fetch what was saved. */
    static final class RecordingFeed implements IntakeFeed {

        final Map<String, IngestDocumentResponse> receipts = new ConcurrentHashMap<>();
        private final IntakeFeed delegate;

        RecordingFeed(IntakeFeed delegate) {
            this.delegate = delegate;
        }

        @Override
        public IngestDocumentResponse submit(Document document, String datasourceId,
                                             String drive, Map<String, String> metadata) {
            IngestDocumentResponse receipt =
                    delegate.submit(document, datasourceId, drive, metadata);
            receipts.put(document.getDocId(), receipt);
            return receipt;
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
