package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.BlogPost;
import ai.pipestream.proto.acquire.confluence.v1.BlogPostContentStatus;
import ai.pipestream.proto.acquire.confluence.v1.Body;
import ai.pipestream.proto.acquire.confluence.v1.BodyFormat;
import ai.pipestream.proto.acquire.confluence.v1.BodyType;
import ai.pipestream.proto.acquire.confluence.v1.ChangeOperation;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.proto.acquire.confluence.v1.ContentStatus;
import ai.pipestream.proto.acquire.confluence.v1.Label;
import ai.pipestream.proto.acquire.confluence.v1.Page;
import ai.pipestream.proto.acquire.confluence.v1.Version;
import com.google.protobuf.Timestamp;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Projection and batching semantics against a stub {@link S3Client} (a JDK proxy):
 * page and blog post changes project into the same flat row (candidate paths plus
 * CEL), a DELETE change yields an identity-only row with the content columns absent,
 * and the buffer flushes at the batch threshold and on close. Uploaded bytes are read
 * back with the same example Group reader {@code ParquetChangeSinkTest} uses.
 */
class ProjectedParquetChangeSinkTest {

    private record CapturedPut(String bucket, String key, byte[] bytes) {
    }

    /** An S3Client that records putObject calls; every other method is unsupported. */
    private static final class FakeS3 implements java.lang.reflect.InvocationHandler {
        final List<CapturedPut> puts = new ArrayList<>();
        boolean closed;

        S3Client client() {
            return (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(),
                    new Class<?>[]{S3Client.class}, this);
        }

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args)
                throws Throwable {
            return switch (method.getName()) {
                case "putObject" -> {
                    PutObjectRequest.Builder builder = PutObjectRequest.builder();
                    ((java.util.function.Consumer<PutObjectRequest.Builder>) args[0])
                            .accept(builder);
                    PutObjectRequest request = builder.build();
                    byte[] bytes;
                    try (var in = ((RequestBody) args[1]).contentStreamProvider().newStream()) {
                        bytes = in.readAllBytes();
                    }
                    puts.add(new CapturedPut(request.bucket(), request.key(), bytes));
                    yield PutObjectResponse.builder().eTag("fake-etag").build();
                }
                case "serviceName" -> "s3";
                case "close" -> {
                    closed = true;
                    yield null;
                }
                case "toString" -> "FakeS3";
                default -> throw new UnsupportedOperationException(method.getName());
            };
        }
    }

    private static final String PAGE_BODY = "<p>hello page</p>";
    private static final String BLOG_BODY = "<p>hello blog</p>";

    private static ConfluenceChange pageChange(int i) {
        return ConfluenceChange.newBuilder()
                .setChangeId("change-page-" + i)
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("page-" + i)
                        .setPage(Page.newBuilder()
                                .setId("page-" + i)
                                .setTitle("Page " + i)
                                .setSpaceId("space-1")
                                .setAuthorId("author-1")
                                .setStatus(ContentStatus.CONTENT_STATUS_CURRENT)
                                .setWebUrl("https://example.atlassian.net/wiki/pages/page-" + i)
                                .setCreatedAt(Timestamp.newBuilder()
                                        .setSeconds(1_700_000_000).setNanos(500_000_000))
                                .setVersion(Version.newBuilder().setNumber(3 + i))
                                .setBody(Body.newBuilder().setStorage(BodyType.newBuilder()
                                        .setFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML)
                                        .setValue(PAGE_BODY)))
                                .addLabels(Label.newBuilder().setName("alpha"))
                                .addLabels(Label.newBuilder().setName("beta"))))
                .build();
    }

    private static ConfluenceChange blogChange(int i) {
        return ConfluenceChange.newBuilder()
                .setChangeId("change-blog-" + i)
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("blog-" + i)
                        .setBlogPost(BlogPost.newBuilder()
                                .setId("blog-" + i)
                                .setTitle("Blog " + i)
                                .setSpaceId("space-2")
                                .setAuthorId("author-2")
                                .setStatus(BlogPostContentStatus.BLOG_POST_CONTENT_STATUS_DRAFT)
                                .setWebUrl("https://example.atlassian.net/wiki/blogs/blog-" + i)
                                .setCreatedAt(Timestamp.newBuilder().setSeconds(1_700_100_000))
                                .setVersion(Version.newBuilder().setNumber(7))
                                .setBody(Body.newBuilder().setStorage(BodyType.newBuilder()
                                        .setFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML)
                                        .setValue(BLOG_BODY)))
                                .addLabels(Label.newBuilder().setName("gamma"))))
                .build();
    }

    private static ConfluenceChange deleteChange(String id) {
        return ConfluenceChange.newBuilder()
                .setChangeId("change-delete-" + id)
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .setEntity(ConfluenceEntity.newBuilder().setEntityId(id))
                .build();
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

    private static List<Group> readAll(byte[] parquet, Path dir, String name) throws Exception {
        Path fileOnDisk = dir.resolve(name);
        Files.write(fileOnDisk, parquet);
        List<Group> rows = new ArrayList<>();
        try (ParquetReader<Group> reader =
                     new GroupBuilder(new LocalInputFile(fileOnDisk)).build()) {
            for (Group row = reader.read(); row != null; row = reader.read()) {
                rows.add(row);
            }
        }
        return rows;
    }

    /** The repeated string elements of a three-level LIST column. */
    private static List<String> strings(Group row, String field) {
        List<String> values = new ArrayList<>();
        if (row.getFieldRepetitionCount(field) == 0) {
            return values;
        }
        Group list = row.getGroup(field, 0);
        for (int i = 0; i < list.getFieldRepetitionCount("list"); i++) {
            values.add(list.getGroup("list", i).getString("element", 0));
        }
        return values;
    }

    @Test
    void pageUpsertProjectsEveryColumn(@TempDir Path dir) throws Exception {
        FakeS3 fake = new FakeS3();
        try (ProjectedParquetChangeSink sink =
                     new ProjectedParquetChangeSink(fake.client(), "lake-bucket", "lake", 500)) {
            sink.emit(pageChange(0));
        }
        assertThat(fake.puts).hasSize(1);
        CapturedPut put = fake.puts.getFirst();
        assertThat(put.bucket()).isEqualTo("lake-bucket");
        assertThat(put.key()).matches("lake/content/[^/]+-part-00000\\.parquet");

        List<Group> rows = readAll(put.bytes(), dir, "page.parquet");
        assertThat(rows).hasSize(1);
        Group row = rows.getFirst();
        assertThat(row.getString("change_id", 0)).isEqualTo("change-page-0");
        assertThat(row.getString("operation", 0)).isEqualTo("CHANGE_OPERATION_UPSERT");
        assertThat(row.getString("content_id", 0)).isEqualTo("page-0");
        assertThat(row.getString("content_type", 0)).isEqualTo("page");
        assertThat(row.getString("space_id", 0)).isEqualTo("space-1");
        assertThat(row.getString("title", 0)).isEqualTo("Page 0");
        assertThat(row.getString("status", 0)).isEqualTo("current");
        assertThat(row.getString("author_id", 0)).isEqualTo("author-1");
        assertThat(row.getString("web_url", 0))
                .isEqualTo("https://example.atlassian.net/wiki/pages/page-0");
        // Timestamps land as microsecond UTC columns.
        assertThat(row.getLong("created_at", 0)).isEqualTo(1_700_000_000_500_000L);
        assertThat(row.getInteger("version_number", 0)).isEqualTo(3);
        assertThat(strings(row, "label_names")).containsExactly("alpha", "beta");
        assertThat(row.getInteger("body_length", 0)).isEqualTo(PAGE_BODY.length());
    }

    @Test
    void blogPostUpsertFillsTheSameRowThroughTheFallbackPaths(@TempDir Path dir)
            throws Exception {
        FakeS3 fake = new FakeS3();
        try (ProjectedParquetChangeSink sink =
                     new ProjectedParquetChangeSink(fake.client(), "b", null, 500)) {
            sink.emit(blogChange(0));
        }
        List<Group> rows = readAll(fake.puts.getFirst().bytes(), dir, "blog.parquet");
        assertThat(rows).hasSize(1);
        Group row = rows.getFirst();
        assertThat(row.getString("change_id", 0)).isEqualTo("change-blog-0");
        assertThat(row.getString("content_id", 0)).isEqualTo("blog-0");
        // The blog arm fills the shared columns; content_type keeps them apart.
        assertThat(row.getString("content_type", 0)).isEqualTo("blog_post");
        assertThat(row.getString("space_id", 0)).isEqualTo("space-2");
        assertThat(row.getString("title", 0)).isEqualTo("Blog 0");
        assertThat(row.getString("status", 0)).isEqualTo("draft");
        assertThat(row.getString("author_id", 0)).isEqualTo("author-2");
        assertThat(row.getLong("created_at", 0)).isEqualTo(1_700_100_000_000_000L);
        assertThat(row.getInteger("version_number", 0)).isEqualTo(7);
        assertThat(strings(row, "label_names")).containsExactly("gamma");
        assertThat(row.getInteger("body_length", 0)).isEqualTo(BLOG_BODY.length());
        // The default prefix applies.
        assertThat(fake.puts.getFirst().key()).startsWith("confluence-content-rows/content/");
    }

    @Test
    void deleteChangeYieldsIdentityAndOperationWithAbsentContentColumns(@TempDir Path dir)
            throws Exception {
        FakeS3 fake = new FakeS3();
        try (ProjectedParquetChangeSink sink =
                     new ProjectedParquetChangeSink(fake.client(), "b", "lake", 500)) {
            sink.emit(deleteChange("page-9"));
        }
        List<Group> rows = readAll(fake.puts.getFirst().bytes(), dir, "delete.parquet");
        assertThat(rows).hasSize(1);
        Group row = rows.getFirst();
        assertThat(row.getString("change_id", 0)).isEqualTo("change-delete-page-9");
        assertThat(row.getString("operation", 0)).isEqualTo("CHANGE_OPERATION_DELETE");
        // The path-driven content columns stay absent (null) on the tombstone row.
        for (String pathColumn : new String[]{"content_id", "space_id", "title",
                "author_id", "web_url", "created_at", "version_number"}) {
            assertThat(row.getFieldRepetitionCount(pathColumn))
                    .as(pathColumn + " stays absent on a DELETE row")
                    .isZero();
        }
        // The CEL-derived columns cannot express absence; they carry zero values.
        assertThat(row.getString("content_type", 0)).isEmpty();
        assertThat(row.getString("status", 0)).isEmpty();
        assertThat(row.getInteger("body_length", 0)).isZero();
        assertThat(strings(row, "label_names")).isEmpty();
    }

    @Test
    void changeWithoutEntityArmProjectsWithoutWedging(@TempDir Path dir) throws Exception {
        FakeS3 fake = new FakeS3();
        try (ProjectedParquetChangeSink sink =
                     new ProjectedParquetChangeSink(fake.client(), "b", "lake", 500)) {
            sink.emit(ConfluenceChange.newBuilder()
                    .setChangeId("change-bare")
                    .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                    .build());
            sink.emit(pageChange(1));
        }
        // Both rows landed: the entity-less change did not stop the lane.
        assertThat(fake.puts).hasSize(1);
        List<Group> rows = readAll(fake.puts.getFirst().bytes(), dir, "mixed.parquet");
        assertThat(rows).hasSize(2);
        Group bare = rows.getFirst();
        assertThat(bare.getString("change_id", 0)).isEqualTo("change-bare");
        assertThat(bare.getFieldRepetitionCount("content_id")).isZero();
        assertThat(rows.get(1).getString("content_id", 0)).isEqualTo("page-1");
    }

    @Test
    void flushesAtBatchThresholdAndNumbersPartsInSequence(@TempDir Path dir)
            throws Exception {
        FakeS3 fake = new FakeS3();
        ProjectedParquetChangeSink sink =
                new ProjectedParquetChangeSink(fake.client(), "b", "lake", 3);

        sink.emit(pageChange(0));
        sink.emit(blogChange(0));
        assertThat(fake.puts).isEmpty();

        sink.emit(deleteChange("page-2"));
        assertThat(fake.puts).hasSize(1);
        assertThat(fake.puts.getFirst().key())
                .matches("lake/content/[^/]+-part-00000\\.parquet");
        assertThat(readAll(fake.puts.getFirst().bytes(), dir, "batch.parquet")).hasSize(3);

        // The remainder stays buffered until close, then lands as the next part.
        sink.emit(pageChange(1));
        assertThat(fake.puts).hasSize(1);
        sink.close();
        assertThat(fake.puts).hasSize(2);
        assertThat(fake.puts.get(1).key()).endsWith("-part-00001.parquet");
        assertThat(readAll(fake.puts.get(1).bytes(), dir, "rest.parquet")).hasSize(1);
        assertThat(fake.closed).isTrue();
    }

    @Test
    void snapshotFlushesThePendingBuffer(@TempDir Path dir) throws Exception {
        FakeS3 fake = new FakeS3();
        try (ProjectedParquetChangeSink sink =
                     new ProjectedParquetChangeSink(fake.client(), "b", "lake", 500)) {
            sink.emit(pageChange(0));
            sink.snapshot(ConfluenceSnapshot.newBuilder().setSnapshotId("sweep-1").build());
            assertThat(fake.puts).hasSize(1);
            List<Group> rows = readAll(fake.puts.getFirst().bytes(), dir, "snap.parquet");
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().getString("change_id", 0)).isEqualTo("change-page-0");
        }
    }

    @Test
    void fromEnvironmentActivatesOnlyWithABucket() {
        assertThat(ProjectedParquetChangeSink.fromEnvironment(Map.of())).isEmpty();
        assertThat(ProjectedParquetChangeSink.fromEnvironment(Map.of(
                ProjectedParquetChangeSink.ENV_ENDPOINT, "http://localhost:9000"))).isEmpty();
        try (ProjectedParquetChangeSink sink = ProjectedParquetChangeSink.fromEnvironment(
                Map.of(
                        ProjectedParquetChangeSink.ENV_BUCKET, "lake",
                        ProjectedParquetChangeSink.ENV_ENDPOINT, "http://localhost:9000",
                        ProjectedParquetChangeSink.ENV_ACCESS_KEY_ID, "rustfsadmin",
                        ProjectedParquetChangeSink.ENV_SECRET_ACCESS_KEY, "rustfsadmin"))
                .orElseThrow()) {
            assertThat(sink).isNotNull();
        }
    }
}
