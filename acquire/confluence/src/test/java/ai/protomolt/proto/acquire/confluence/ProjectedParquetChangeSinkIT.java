package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.BlogPost;
import ai.protomolt.proto.acquire.confluence.v1.BlogPostContentStatus;
import ai.protomolt.proto.acquire.confluence.v1.Body;
import ai.protomolt.proto.acquire.confluence.v1.BodyFormat;
import ai.protomolt.proto.acquire.confluence.v1.BodyType;
import ai.protomolt.proto.acquire.confluence.v1.ChangeOperation;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceChange;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.protomolt.proto.acquire.confluence.v1.ContentStatus;
import ai.protomolt.proto.acquire.confluence.v1.Label;
import ai.protomolt.proto.acquire.confluence.v1.Page;
import ai.protomolt.proto.acquire.confluence.v1.Version;
import ai.protomolt.proto.emit.parquet.s3.S3Clients;
import com.google.protobuf.Timestamp;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The projected-row lane end to end on an S3-compatible store: a mix of page, blog
 * post and DELETE changes through {@link ProjectedParquetChangeSink} into a real
 * LocalStack bucket (the same Testcontainers stand-in
 * {@code ParquetS3LiveIntegrationTest} uses), the part files listed, fetched back over
 * the wire, and read with Parquet's own reader. The suite skips when Docker is
 * unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class ProjectedParquetChangeSinkIT {

    private static final String BUCKET = "protomolt-projected-parquet";
    private static final String PREFIX = "it-content-rows";

    @Container
    static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:4.13"))
            .withServices("s3");

    private static S3Client s3;

    @BeforeAll
    static void createBucket() {
        s3 = S3Clients.pathStyle(S3.getEndpoint().toString(), S3.getRegion(),
                S3.getAccessKey(), S3.getSecretKey());
        try {
            s3.createBucket(b -> b.bucket(BUCKET));
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException ignored) {
            // reruns share the bucket
        }
    }

    /** ParquetReader over a plain InputFile - no Hadoop filesystem anywhere in the test. */
    private static final class GroupBuilder extends ParquetReader.Builder<Group> {
        private GroupBuilder(InputFile file) {
            super(file);
        }

        @Override
        protected ReadSupport<Group> getReadSupport() {
            return new GroupReadSupport();
        }
    }

    private static List<Group> readKey(String key, Path dir, String name) throws Exception {
        byte[] fetched = s3.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
        Path fileOnDisk = dir.resolve(name);
        Files.write(fileOnDisk, fetched);
        List<Group> rows = new ArrayList<>();
        try (ParquetReader<Group> reader =
                     new GroupBuilder(new LocalInputFile(fileOnDisk)).build()) {
            for (Group row = reader.read(); row != null; row = reader.read()) {
                rows.add(row);
            }
        }
        return rows;
    }

    private static final String PAGE_BODY = "<p>storage body</p>";

    private static ConfluenceChange pageChange() {
        return ConfluenceChange.newBuilder()
                .setChangeId("it-change-page")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("it-page-1")
                        .setPage(Page.newBuilder()
                                .setId("it-page-1")
                                .setTitle("Integration Page")
                                .setSpaceId("it-space")
                                .setAuthorId("it-author")
                                .setStatus(ContentStatus.CONTENT_STATUS_CURRENT)
                                .setWebUrl("https://example.atlassian.net/wiki/pages/it-page-1")
                                .setCreatedAt(Timestamp.newBuilder().setSeconds(1_700_000_000))
                                .setVersion(Version.newBuilder().setNumber(4))
                                .setBody(Body.newBuilder().setStorage(BodyType.newBuilder()
                                        .setFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML)
                                        .setValue(PAGE_BODY)))
                                .addLabels(Label.newBuilder().setName("it-label"))))
                .build();
    }

    private static ConfluenceChange blogChange() {
        return ConfluenceChange.newBuilder()
                .setChangeId("it-change-blog")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("it-blog-1")
                        .setBlogPost(BlogPost.newBuilder()
                                .setId("it-blog-1")
                                .setTitle("Integration Blog")
                                .setSpaceId("it-space")
                                .setStatus(BlogPostContentStatus
                                        .BLOG_POST_CONTENT_STATUS_TRASHED)))
                .build();
    }

    private static ConfluenceChange deleteChange() {
        return ConfluenceChange.newBuilder()
                .setChangeId("it-change-delete")
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .setEntity(ConfluenceEntity.newBuilder().setEntityId("it-page-1"))
                .build();
    }

    @Test
    void projectsAndUploadsRowsOverTheWire(@TempDir Path dir) throws Exception {
        try (ProjectedParquetChangeSink sink =
                     new ProjectedParquetChangeSink(s3, BUCKET, PREFIX, 100)) {
            sink.emit(pageChange());
            sink.emit(blogChange());
            sink.emit(deleteChange());
        }

        List<S3Object> objects = s3.listObjectsV2(
                b -> b.bucket(BUCKET).prefix(PREFIX + "/")).contents();
        assertThat(objects).hasSize(1);
        String key = objects.getFirst().key();
        assertThat(key).matches(PREFIX + "/content/[^/]+-part-00000\\.parquet");

        List<Group> rows = readKey(key, dir, "rows.parquet");
        assertThat(rows).hasSize(3);

        Group page = rows.get(0);
        assertThat(page.getString("change_id", 0)).isEqualTo("it-change-page");
        assertThat(page.getString("operation", 0)).isEqualTo("CHANGE_OPERATION_UPSERT");
        assertThat(page.getString("content_id", 0)).isEqualTo("it-page-1");
        assertThat(page.getString("content_type", 0)).isEqualTo("page");
        assertThat(page.getString("title", 0)).isEqualTo("Integration Page");
        assertThat(page.getString("status", 0)).isEqualTo("current");
        assertThat(page.getLong("created_at", 0)).isEqualTo(1_700_000_000_000_000L);
        assertThat(page.getInteger("version_number", 0)).isEqualTo(4);
        assertThat(page.getInteger("body_length", 0)).isEqualTo(PAGE_BODY.length());
        Group labels = page.getGroup("label_names", 0);
        assertThat(labels.getGroup("list", 0).getString("element", 0)).isEqualTo("it-label");

        Group blog = rows.get(1);
        assertThat(blog.getString("content_id", 0)).isEqualTo("it-blog-1");
        assertThat(blog.getString("content_type", 0)).isEqualTo("blog_post");
        assertThat(blog.getString("title", 0)).isEqualTo("Integration Blog");
        // The blog enum numbers trashed as 4; the row still says "trashed".
        assertThat(blog.getString("status", 0)).isEqualTo("trashed");
        assertThat(blog.getFieldRepetitionCount("version_number")).isZero();

        Group delete = rows.get(2);
        assertThat(delete.getString("change_id", 0)).isEqualTo("it-change-delete");
        assertThat(delete.getString("operation", 0)).isEqualTo("CHANGE_OPERATION_DELETE");
        assertThat(delete.getFieldRepetitionCount("content_id")).isZero();
        assertThat(delete.getFieldRepetitionCount("title")).isZero();
        assertThat(delete.getString("content_type", 0)).isEmpty();
    }
}
