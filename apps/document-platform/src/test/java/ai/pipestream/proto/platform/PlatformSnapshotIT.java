package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.metric.MetricServiceGrpc;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.service.RepoServiceConfig;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.search.door.RepoDocumentMapping;
import ai.pipestream.proto.search.v1.IndexDocumentRequest;
import ai.pipestream.proto.search.v1.SearchIndexServiceGrpc;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.SearchResponse;
import ai.pipestream.proto.search.v1.SearchServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The snapshot story at the platform level: a node with the
 * {@code DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_*} family set indexes a
 * document and shuts down; a second platform boot, mounting only the
 * read-only search role and the metrics role over a FRESH, EMPTY index
 * directory (no repo anywhere), restores the corpus from the bucket,
 * answers the same query and its aggregate, and mounts no write surface.
 */
@Testcontainers(disabledWithoutDocker = true)
class PlatformSnapshotIT {

    static final String ACCOUNT = "acct-snapshot";
    static final String SNAPSHOT_BUCKET = "search-snapshots";

    @Container
    static final PostgreSQLContainer REPO_DB = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    @TempDir
    static Path work;

    static RepoServiceConfig repoConfig() {
        return new RepoServiceConfig(
                0,
                new LedgerConfig(REPO_DB.getJdbcUrl(),
                        REPO_DB.getUsername(), REPO_DB.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "platform-snapshot-docs",
                0,
                null, null, null, null, 0, 0L);
    }

    @org.junit.jupiter.api.BeforeAll
    static void createSnapshotBucket() {
        // The bucket is infrastructure the operator provides; the store
        // never creates it.
        try (software.amazon.awssdk.services.s3.S3Client s3 =
                software.amazon.awssdk.services.s3.S3Client.builder()
                        .region(software.amazon.awssdk.regions.Region.of(
                                LOCALSTACK.getRegion()))
                        .endpointOverride(java.net.URI.create(
                                LOCALSTACK.getEndpoint().toString()))
                        .forcePathStyle(true)
                        .credentialsProvider(software.amazon.awssdk.auth.credentials
                                .StaticCredentialsProvider.create(
                                        software.amazon.awssdk.auth.credentials
                                                .AwsBasicCredentials.create(
                                                        LOCALSTACK.getAccessKey(),
                                                        LOCALSTACK.getSecretKey())))
                        .httpClientBuilder(software.amazon.awssdk.http.urlconnection
                                .UrlConnectionHttpClient.builder())
                        .build()) {
            s3.createBucket(b -> b.bucket(SNAPSHOT_BUCKET));
        }
    }

    static Map<String, String> snapshotEnvironment() {
        Map<String, String> environment = new HashMap<>();
        environment.put(SearchSnapshotConfig.ENV_BUCKET, SNAPSHOT_BUCKET);
        environment.put(SearchSnapshotConfig.ENV_REGION, LOCALSTACK.getRegion());
        environment.put(SearchSnapshotConfig.ENV_ENDPOINT,
                LOCALSTACK.getEndpoint().toString());
        environment.put(SearchSnapshotConfig.ENV_ACCESS_KEY, LOCALSTACK.getAccessKey());
        environment.put(SearchSnapshotConfig.ENV_SECRET_KEY, LOCALSTACK.getSecretKey());
        return environment;
    }

    static DocumentPlatformConfig config(List<String> roles, Path indexDir,
            Map<String, String> environment) {
        return new DocumentPlatformConfig(
                repoConfig(), null, null, 0, 0, 0, 0,
                null, null, null,
                60L, 1, 0, indexDir, 0, 0,
                roles, environment);
    }

    static void withChannel(int port, Consumer<ManagedChannel> body) {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress("127.0.0.1", port).usePlaintext().build();
        try {
            body.accept(channel);
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void aFreshIndexDirectoryRestoresFromTheBucketOnBoot() throws Exception {
        String docId = "doc-snapshot-1";
        NodeAddress address = NodeAddress.newBuilder()
                .setDocId(docId)
                .setGraphAddressId("ds-snapshot")
                .setAccountId(ACCOUNT)
                .setGraphId("intake:" + ACCOUNT)
                .build();
        try (DocumentPlatform writer = DocumentPlatform.start(
                config(List.of("repo", "search"), work.resolve("writer-index"),
                        snapshotEnvironment()), null)) {
            withChannel(writer.repoPort(), repo -> {
                DriveServiceGrpc.newBlockingStub(repo).createDrive(
                        CreateDriveRequest.newBuilder()
                                .setName("intake")
                                .setAccountId(ACCOUNT)
                                .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                                .build());
                DocumentServiceGrpc.newBlockingStub(repo).saveDocument(
                        SaveDocumentRequest.newBuilder()
                                .setDocument(Document.newBuilder()
                                        .setDocId(docId)
                                        .setOwnership(OwnershipContext.newBuilder()
                                                .setAccountId(ACCOUNT)
                                                .setDatasourceId("ds-snapshot"))
                                        .setSearchMetadata(SearchMetadata.newBuilder()
                                                .setTitle("Snapshot Corpus")
                                                .setBody("The bucket carries the index"
                                                        + " across boots.")))
                                .setDrive("intake")
                                .setConnectorId("snapshot-test")
                                .setUseDatasourceId(true)
                                .setGraphId("intake:" + ACCOUNT)
                                .build());
            });
            withChannel(writer.searchPort(), search ->
                    SearchIndexServiceGrpc.newBlockingStub(search).indexDocument(
                            IndexDocumentRequest.newBuilder()
                                    .setAddress(address)
                                    .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                    .build()));
        }

        // The remote metrics node: read-only search plus metrics over a
        // fresh, untouched directory, no repo role at all. The corpus must
        // come from S3.
        Map<String, String> readerEnvironment = snapshotEnvironment();
        readerEnvironment.put(DocumentPlatformConfig.ENV_SEARCH_READ_ONLY, "true");
        try (DocumentPlatform reader = DocumentPlatform.start(
                config(List.of("search", "metrics"), work.resolve("reader-index"),
                        readerEnvironment), null)) {
            withChannel(reader.searchPort(), search -> {
                SearchResponse hits = SearchServiceGrpc.newBlockingStub(search)
                        .search(SearchRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .setQuery("bucket carries")
                                .setK(3)
                                .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                                .build());
                assertThat(hits.getHitsList()).isNotEmpty();
                assertThat(hits.getHits(0).getDocId()).isEqualTo(docId);

                // No write surface on a reader.
                assertThatThrownBy(() -> SearchIndexServiceGrpc.newBlockingStub(search)
                        .indexDocument(IndexDocumentRequest.newBuilder()
                                .setAddress(address)
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                                assertThat(e.getStatus().getCode())
                                        .isEqualTo(Status.Code.UNIMPLEMENTED));
            });
            withChannel(reader.metricsPort(), metrics -> {
                QueryMetricsResponse counted = MetricServiceGrpc.newBlockingStub(metrics)
                        .queryMetrics(QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .addMeasures("documents")
                                .setLimit(10)
                                .build());
                assertThat(counted.getRows(0).getMeasuresMap())
                        .containsEntry("documents", 1.0);
            });
        }
    }
}
