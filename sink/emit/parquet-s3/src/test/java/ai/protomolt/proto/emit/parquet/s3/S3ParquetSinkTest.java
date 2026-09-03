package ai.protomolt.proto.emit.parquet.s3;

import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sink against a stub {@link S3Client} (a JDK proxy - the SDK interface is far too
 * wide to implement by hand): every {@code putObject} is captured, and the uploaded
 * bytes must round-trip as Parquet through the same example Group reader
 * {@code ParquetEmitterTest} uses.
 */
class S3ParquetSinkTest {

    private static final String PROTO = """
            syntax = "proto3";
            package pqs3.test;
            message Tick {
              string symbol = 1;
              double price = 2;
              int64 sequence = 3;
            }
            """;

    private record CapturedPut(String bucket, String key, String contentType, byte[] bytes) {
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
                    // The sink uses the Consumer<Builder> convenience overload; the
                    // proxy intercepts it before the interface default can build the
                    // request, so build it here.
                    PutObjectRequest.Builder builder = PutObjectRequest.builder();
                    ((java.util.function.Consumer<PutObjectRequest.Builder>) args[0])
                            .accept(builder);
                    PutObjectRequest request = builder.build();
                    byte[] bytes;
                    try (var in = ((RequestBody) args[1]).contentStreamProvider().newStream()) {
                        bytes = in.readAllBytes();
                    }
                    puts.add(new CapturedPut(request.bucket(), request.key(),
                            request.contentType(), bytes));
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

    private static FileDescriptor file() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("pqs3/test/tick.proto", PROTO, "test").build());
        return compiled.descriptorFor("pqs3/test/tick.proto").orElseThrow();
    }

    private static DynamicMessage tick(Descriptor type, int i) {
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("symbol"), "SYM" + i)
                .setField(type.findFieldByName("price"), 100.0 + i)
                .setField(type.findFieldByName("sequence"), (long) i)
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

    @Test
    void uploadsParquetBytesThatRoundTrip(@TempDir Path dir) throws Exception {
        Descriptor type = file().findMessageTypeByName("Tick");
        FakeS3 fake = new FakeS3();
        try (S3ParquetSink sink = new S3ParquetSink(fake.client(), "ticks-bucket")) {
            String key = sink.put("ticks/part-00000.parquet", type,
                    List.of(tick(type, 0), tick(type, 1)));
            assertThat(key).isEqualTo("ticks/part-00000.parquet");
            assertThat(sink.bucket()).isEqualTo("ticks-bucket");
        }
        assertThat(fake.closed).isTrue();

        assertThat(fake.puts).hasSize(1);
        CapturedPut put = fake.puts.getFirst();
        assertThat(put.bucket()).isEqualTo("ticks-bucket");
        assertThat(put.key()).isEqualTo("ticks/part-00000.parquet");
        assertThat(put.contentType()).isEqualTo(S3ParquetSink.PARQUET_CONTENT_TYPE);
        assertThat(new String(put.bytes(), 0, 4, java.nio.charset.StandardCharsets.US_ASCII))
                .isEqualTo("PAR1");

        Path fileOnDisk = dir.resolve("ticks.parquet");
        Files.write(fileOnDisk, put.bytes());
        try (ParquetReader<Group> reader =
                     new GroupBuilder(new LocalInputFile(fileOnDisk)).build()) {
            Group first = reader.read();
            assertThat(first.getString("symbol", 0)).isEqualTo("SYM0");
            assertThat(first.getDouble("price", 0)).isEqualTo(100.0);
            Group second = reader.read();
            assertThat(second.getLong("sequence", 0)).isEqualTo(1L);
            assertThat(reader.read()).isNull();
        }
    }

    @Test
    void putBytesUploadsVerbatim() {
        FakeS3 fake = new FakeS3();
        try (S3ParquetSink sink = new S3ParquetSink(fake.client(), "b")) {
            sink.putBytes("raw/file.parquet", new byte[]{1, 2, 3});
        }
        assertThat(fake.puts).hasSize(1);
        assertThat(fake.puts.getFirst().bytes()).containsExactly(1, 2, 3);
    }

    @Test
    void rejectsBadTargets() throws Exception {
        Descriptor type = file().findMessageTypeByName("Tick");
        FakeS3 fake = new FakeS3();
        try (S3ParquetSink sink = new S3ParquetSink(fake.client(), "b")) {
            assertThatThrownBy(() -> sink.put(" ", type, List.of()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> sink.put("/abs.parquet", type, List.of()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> sink.put("../up.parquet", type, List.of()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(fake.puts).isEmpty();
        assertThatThrownBy(() -> new S3ParquetSink(fake.client(), " "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
