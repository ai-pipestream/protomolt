package ai.pipestream.proto.emit.parquet.s3;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The lane end to end on an S3-compatible store: a Parquet file rendered in memory,
 * uploaded through {@link S3ParquetSink} to LocalStack (the same Testcontainers stand-in
 * {@code IcebergS3LiveIntegrationTest} uses for RustFS-class stores), fetched back over
 * the wire, and read with Parquet's own reader. The suite skips when Docker is
 * unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParquetS3LiveIntegrationTest {

    private static final String BUCKET = "protomolt-parquet";

    @Container
    static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:4.13"))
            .withServices("s3");

    private static final String PROTO = """
            syntax = "proto3";
            package pqs3.live;
            message Tick {
              string symbol = 1;
              double price = 2;
              Meta meta = 3;
            }
            message Meta { string source = 1; int64 sequence = 2; }
            """;

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

    @Test
    void uploadsAndReadsBackOverS3(@TempDir Path dir) throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("pqs3/live/tick.proto", PROTO, "test").build());
        FileDescriptor file = compiled.descriptorFor("pqs3/live/tick.proto").orElseThrow();
        Descriptor type = file.findMessageTypeByName("Tick");
        Descriptor meta = file.findMessageTypeByName("Meta");
        List<DynamicMessage> rows = List.of(
                DynamicMessage.newBuilder(type)
                        .setField(type.findFieldByName("symbol"), "SYM0")
                        .setField(type.findFieldByName("price"), 100.5)
                        .setField(type.findFieldByName("meta"), DynamicMessage.newBuilder(meta)
                                .setField(meta.findFieldByName("source"), "live")
                                .setField(meta.findFieldByName("sequence"), 7L)
                                .build())
                        .build(),
                DynamicMessage.newBuilder(type)
                        .setField(type.findFieldByName("symbol"), "SYM1")
                        .setField(type.findFieldByName("price"), 101.5)
                        .build());

        String key = "ticks/part-00000.parquet";
        S3ParquetSink sink = new S3ParquetSink(s3, BUCKET);
        sink.put(key, type, rows);

        byte[] fetched = s3.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
        sink.close();
        assertThat(new String(fetched, 0, 4, java.nio.charset.StandardCharsets.US_ASCII))
                .isEqualTo("PAR1");

        Path fileOnDisk = dir.resolve("fetched.parquet");
        Files.write(fileOnDisk, fetched);
        try (ParquetReader<Group> reader =
                     new GroupBuilder(new LocalInputFile(fileOnDisk)).build()) {
            Group first = reader.read();
            assertThat(first.getString("symbol", 0)).isEqualTo("SYM0");
            assertThat(first.getDouble("price", 0)).isEqualTo(100.5);
            assertThat(first.getGroup("meta", 0).getLong("sequence", 0)).isEqualTo(7L);
            Group second = reader.read();
            assertThat(second.getString("symbol", 0)).isEqualTo("SYM1");
            assertThat(reader.read()).isNull();
        }
    }
}
