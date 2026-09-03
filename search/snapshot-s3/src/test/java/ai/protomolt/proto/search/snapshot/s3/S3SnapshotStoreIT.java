package ai.protomolt.proto.search.snapshot.s3;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.search.service.IndexSnapshots;
import ai.protomolt.proto.search.service.LuceneSearchStore;
import ai.protomolt.proto.search.service.RepoDocumentMapping;
import ai.protomolt.proto.search.v1.SearchHit;
import ai.protomolt.proto.search.v1.SearchLane;
import ai.protomolt.proto.search.v1.SearchRequest;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The snapshot round trip over real S3 (LocalStack): a writer store
 * indexes and closes, a reader store on an empty directory restores from
 * the bucket and answers the same query. What the fake-store tests pin in
 * depth, this proves over the wire.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3SnapshotStoreIT {

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static final String BUCKET = "search-snapshots";

    @TempDir
    static Path work;

    static S3SnapshotStore blob;

    @BeforeAll
    static void boot() {
        S3Client s3 = S3Client.builder()
                .region(Region.of(LOCALSTACK.getRegion()))
                .endpointOverride(URI.create(LOCALSTACK.getEndpoint().toString()))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
        s3.createBucket(b -> b.bucket(BUCKET));
        blob = new S3SnapshotStore(s3, BUCKET, "search");
    }

    @Test
    void aWriterSnapshotsToTheBucketAndAReaderRestores() {
        try (LuceneSearchStore writer = new LuceneSearchStore(work.resolve("writer"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blob))) {
            writer.index(RepoDocumentMapping.SUBJECT, Document.newBuilder()
                    .setDocId("doc-s3")
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setTitle("Snapshot Treaty")
                            .setBody("the treaty crossed the bucket"))
                    .build());
        }

        try (LuceneSearchStore reader = new LuceneSearchStore(work.resolve("reader"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blob))) {
            assertThat(reader.search(RepoDocumentMapping.SUBJECT, SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("treaty bucket").setK(5)
                            .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                            .build()))
                    .extracting(SearchHit::getDocId)
                    .containsExactly("doc-s3");
        }
    }
}
