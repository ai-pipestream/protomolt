package ai.pipestream.proto.acquire.jdbc;

import ai.pipestream.proto.acquire.pull.GrpcIntakeFeed;
import ai.pipestream.proto.acquire.pull.IntakeFeed;
import ai.pipestream.proto.acquire.pull.PullDocuments;
import ai.pipestream.proto.acquire.pull.PullReport;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The JDBC connector against the real stack: a Postgres source table pulled through the real
 * intake service into a real repo-service. Proves row-as-JSON documents with stable identity (an
 * updated row replaces its own document), placeholder-bound incremental pulls, the
 * contradiction and ordering refusals, and repository dedupe on re-pull.
 */
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JdbcPullIT {

    static final String ACCOUNT = "acct-jdbc-pull-it";
    static final String API_KEY = "jdbc-pull-it-key";
    static final String DATASOURCE = "ds-jdbc-pull";
    static final String FULL_QUERY =
            "SELECT id, title, updated_at FROM src_articles ORDER BY updated_at, id";
    static final String INCREMENTAL_QUERY =
            "SELECT id, title, updated_at FROM src_articles"
                    + " WHERE updated_at > ?::timestamptz ORDER BY updated_at, id";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices repo;
    static IntakeServices intake;
    static ManagedChannel repoChannel;
    static RecordingFeed feed;
    static JdbcPull pull;

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
                "jdbc-pull-it-docs",
                0,
                null, null, null, null, 0, 0L);
        repo = RepoServices.build(config);
        repo.startInProcess("jdbc-pull-it-repo");
        repoChannel = InProcessChannelBuilder.forName("jdbc-pull-it-repo").build();
        DriveServiceGrpc.newBlockingStub(repoChannel).createDrive(
                CreateDriveRequest.newBuilder()
                        .setName("intake")
                        .setAccountId(ACCOUNT)
                        .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                        .build());

        intake = IntakeServices.build(
                new IntakeServiceConfig(
                        0,
                        IntakeServiceConfig.INPROCESS_TARGET_PREFIX + "jdbc-pull-it-repo",
                        IntakeServiceConfig.DEFAULT_MAX_PAYLOAD_BYTES),
                new InMemoryApiKeyIdentityResolver()
                        .register(API_KEY, IntakeScope.unrestricted(ACCOUNT)));
        intake.startInProcess("jdbc-pull-it-intake");

        try (Connection connection = sourceConnection();
                Statement ddl = connection.createStatement()) {
            ddl.execute("CREATE TABLE src_articles ("
                    + " id bigint PRIMARY KEY,"
                    + " title text NOT NULL,"
                    + " updated_at timestamptz NOT NULL)");
            ddl.execute("INSERT INTO src_articles VALUES"
                    + " (1, 'first', '2026-01-01T10:00:00Z'),"
                    + " (2, 'second', '2026-01-02T10:00:00Z')");
        }

        feed = new RecordingFeed(new GrpcIntakeFeed(
                GrpcIntakeFeed.INPROCESS_TARGET_PREFIX + "jdbc-pull-it-intake", API_KEY));
        pull = new JdbcPull(JdbcPullIT::sourceConnection, feed);
    }

    static Connection sourceConnection() throws java.sql.SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    @AfterAll
    static void shutdown() {
        feed.close();
        repoChannel.shutdownNow();
        intake.close();
        repo.close();
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

    static JsonNode rowJson(String docId) throws Exception {
        return new ObjectMapper().readTree(
                fetch(docId).getBlobBag().getBlob().getData().toByteArray());
    }

    @Test
    @Order(1)
    void firstPullWrapsRowsAsStableIdentityJsonDocuments() throws Exception {
        PullReport report = pull.pull(FULL_QUERY, "id", "updated_at", DATASOURCE, "", "", 0);
        assertThat(report.submitted()).isEqualTo(2);
        assertThat(report.failed()).isZero();
        assertThat(report.watermark()).isNotBlank();
        watermark = report.watermark();

        String docId = PullDocuments.docId(JdbcPull.CONNECTOR_ID, DATASOURCE, "id=1");
        JsonNode row = rowJson(docId);
        assertThat(row.get("id").asLong()).isEqualTo(1);
        assertThat(row.get("title").asText()).isEqualTo("first");
        assertThat(row.get("updated_at").asText()).isNotBlank();
        Document stored = fetch(docId);
        assertThat(stored.getOwnership().getConnectorId()).isEqualTo(JdbcPull.CONNECTOR_ID);
        assertThat(stored.getBlobBag().getBlob().getMimeType()).isEqualTo("application/json");
        assertThat(totalDocuments()).isEqualTo(2);
    }

    @Test
    @Order(2)
    void incrementalPullBindsTheWatermarkAndFindsOnlyNewRows() throws Exception {
        PullReport empty = pull.pull(
                INCREMENTAL_QUERY, "id", "updated_at", DATASOURCE, "", watermark, 0);
        assertThat(empty.submitted()).isZero();
        assertThat(empty.watermark()).isEqualTo(watermark);

        try (Connection connection = sourceConnection();
                Statement dml = connection.createStatement()) {
            dml.execute("INSERT INTO src_articles VALUES (3, 'third', '2026-01-03T10:00:00Z')");
        }
        PullReport report = pull.pull(
                INCREMENTAL_QUERY, "id", "updated_at", DATASOURCE, "", watermark, 0);
        assertThat(report.submitted()).isEqualTo(1);
        watermark = report.watermark();
        assertThat(totalDocuments()).isEqualTo(3);
    }

    @Test
    @Order(3)
    void updatedRowReplacesItsOwnDocumentInsteadOfDuplicating() throws Exception {
        try (Connection connection = sourceConnection();
                Statement dml = connection.createStatement()) {
            dml.execute("UPDATE src_articles SET title = 'first revised',"
                    + " updated_at = '2026-01-04T10:00:00Z' WHERE id = 1");
        }
        PullReport report = pull.pull(
                INCREMENTAL_QUERY, "id", "updated_at", DATASOURCE, "", watermark, 0);
        assertThat(report.submitted()).isEqualTo(1);
        watermark = report.watermark();

        assertThat(totalDocuments())
                .as("the changed row must replace its own document, never add one")
                .isEqualTo(3);
        JsonNode row = rowJson(PullDocuments.docId(JdbcPull.CONNECTOR_ID, DATASOURCE, "id=1"));
        assertThat(row.get("title").asText()).isEqualTo("first revised");
    }

    @Test
    @Order(4)
    void unchangedRowsDedupeAtTheRepositoryOnFullRePull() {
        PullReport report = pull.pull(FULL_QUERY, "id", "updated_at", DATASOURCE, "", "", 0);
        assertThat(report.submitted()).isZero();
        assertThat(report.deduplicated()).isEqualTo(3);
        assertThat(totalDocuments()).isEqualTo(3);
    }

    @Test
    @Order(5)
    void placeholderAndWatermarkContradictionsAreRefusedByName() {
        assertThatThrownBy(() -> pull.pull(
                INCREMENTAL_QUERY, "id", "updated_at", DATASOURCE, "", "", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("placeholder but no watermark");
        assertThatThrownBy(() -> pull.pull(
                FULL_QUERY, "id", "updated_at", DATASOURCE, "", watermark, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no placeholder to bind");
    }

    @Test
    @Order(6)
    void unorderedQueriesAndUnknownColumnsAreRefusedByName() {
        assertThatThrownBy(() -> pull.pull(
                "SELECT id, title, updated_at FROM src_articles ORDER BY updated_at DESC",
                "id", "updated_at", DATASOURCE, "", "", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must order by updated_at ascending");
        assertThatThrownBy(() -> pull.pull(
                FULL_QUERY, "missing_column", "updated_at", DATASOURCE, "", "", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'missing_column' is not in the query's result set");
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
