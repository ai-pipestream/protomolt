package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.BlogPost;
import ai.pipestream.proto.acquire.confluence.v1.ChangeOperation;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.proto.acquire.confluence.v1.Page;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Batching semantics against a stub {@link S3Client} (a JDK proxy): changes accumulate
 * per entity kind, a full batch becomes one part file per kind, and the remainder
 * flushes on snapshot and on close. Uploaded bytes are read back with the same example
 * Group reader {@code ParquetEmitterTest} uses.
 */
class ParquetChangeSinkTest {

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

    private static ConfluenceChange pageChange(int i) {
        return ConfluenceChange.newBuilder()
                .setChangeId("page-change-" + i)
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("page-" + i)
                        .setPage(Page.newBuilder().setId("page-" + i).setTitle("Page " + i)))
                .build();
    }

    private static ConfluenceChange blogChange(int i) {
        return ConfluenceChange.newBuilder()
                .setChangeId("blog-change-" + i)
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("blog-" + i)
                        .setBlogPost(BlogPost.newBuilder().setId("blog-" + i)))
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

    @Test
    void flushesOnePartPerEntityTypeAtBatchThresholdAndRemainderOnClose(
            @TempDir Path dir) throws Exception {
        FakeS3 fake = new FakeS3();
        ParquetChangeSink sink = new ParquetChangeSink(fake.client(), "lake-bucket",
                "lake", 3);

        // Below the threshold for both kinds: nothing is uploaded.
        sink.emit(pageChange(0));
        sink.emit(blogChange(0));
        assertThat(fake.puts).isEmpty();

        sink.emit(pageChange(1));
        sink.emit(pageChange(2));
        sink.emit(blogChange(1));
        sink.emit(blogChange(2));

        assertThat(fake.puts).hasSize(2);
        Map<String, CapturedPut> byKey = new LinkedHashMap<>();
        fake.puts.forEach(p -> byKey.put(p.key(), p));
        assertThat(byKey.keySet()).allSatisfy(key -> assertThat(key)
                .matches("lake/(page|blog_post)/[^/]+-part-00000\\.parquet"));
        String pageKey = fake.puts.stream().map(CapturedPut::key)
                .filter(k -> k.contains("/page/")).findFirst().orElseThrow();
        String blogKey = fake.puts.stream().map(CapturedPut::key)
                .filter(k -> k.contains("/blog_post/")).findFirst().orElseThrow();
        assertThat(fake.puts).allSatisfy(p -> assertThat(p.bucket()).isEqualTo("lake-bucket"));

        List<Group> pageRows = readAll(byKey.get(pageKey).bytes(), dir, "pages.parquet");
        assertThat(pageRows).hasSize(3);
        assertThat(pageRows.getFirst().getString("change_id", 0)).isEqualTo("page-change-0");
        assertThat(pageRows.getFirst().getGroup("entity", 0)
                .getGroup("page", 0).getString("title", 0)).isEqualTo("Page 0");
        List<Group> blogRows = readAll(byKey.get(blogKey).bytes(), dir, "blogs.parquet");
        assertThat(blogRows).hasSize(3);

        // The remainder stays buffered until close.
        sink.emit(pageChange(3));
        sink.emit(pageChange(4));
        assertThat(fake.puts).hasSize(2);
        sink.close();

        assertThat(fake.puts).hasSize(3);
        CapturedPut remainder = fake.puts.get(2);
        assertThat(remainder.key()).contains("/page/").endsWith("-part-00001.parquet");
        List<Group> remainderRows = readAll(remainder.bytes(), dir, "rest.parquet");
        assertThat(remainderRows).hasSize(2);
        assertThat(remainderRows.getFirst().getString("change_id", 0))
                .isEqualTo("page-change-3");
        assertThat(fake.closed).isTrue();
    }

    @Test
    void snapshotFlushesPendingBuffersWithoutBeingExported(@TempDir Path dir)
            throws Exception {
        FakeS3 fake = new FakeS3();
        try (ParquetChangeSink sink = new ParquetChangeSink(fake.client(), "b", null, 500)) {
            sink.emit(pageChange(0));
            sink.snapshot(ConfluenceSnapshot.newBuilder()
                    .setSnapshotId("sweep-1").build());
            assertThat(fake.puts).hasSize(1);
            // The default prefix applies, and only the change is in the file.
            assertThat(fake.puts.getFirst().key())
                    .startsWith("confluence-changes/page/");
            List<Group> rows = readAll(fake.puts.getFirst().bytes(), dir, "snap.parquet");
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().getString("change_id", 0)).isEqualTo("page-change-0");
        }
    }

    @Test
    void fromEnvironmentActivatesOnlyWithABucket() {
        assertThat(ParquetChangeSink.fromEnvironment(Map.of())).isEmpty();
        assertThat(ParquetChangeSink.fromEnvironment(Map.of(
                ParquetChangeSink.ENV_ENDPOINT, "http://localhost:9000"))).isEmpty();
        try (ParquetChangeSink sink = ParquetChangeSink.fromEnvironment(Map.of(
                        ParquetChangeSink.ENV_BUCKET, "lake",
                        ParquetChangeSink.ENV_ENDPOINT, "http://localhost:9000",
                        ParquetChangeSink.ENV_ACCESS_KEY_ID, "rustfsadmin",
                        ParquetChangeSink.ENV_SECRET_ACCESS_KEY, "rustfsadmin"))
                .orElseThrow()) {
            assertThat(sink).isNotNull();
        }
    }
}
