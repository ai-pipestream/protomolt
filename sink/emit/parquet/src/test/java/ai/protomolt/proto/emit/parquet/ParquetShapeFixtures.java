package ai.protomolt.proto.emit.parquet;

import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared protobuf shapes, Parquet 1.18.2 golden resources and read-back helpers for the
 * Parquet shape tests. The goldens are byte-pinned to Parquet 1.18.2: a Parquet upgrade fails
 * the golden tests by design, and the five files under {@code golden/} are then regenerated
 * from the new writer output and reviewed as part of that upgrade.
 */
final class ParquetShapeFixtures {

    static final String GOLDEN_RESOURCE_PREFIX = "ai/protomolt/proto/emit/parquet/golden/";
    static final String GOLDEN_EMPTY_ZERO_ROWS = "empty-zero-rows-1.18.2.parquet";
    static final String GOLDEN_EMPTY_OPTIONAL_ROWS = "empty-optional-rows-1.18.2.parquet";
    static final String GOLDEN_RECURSIVE_JSON_AND_DAG = "recursive-json-and-dag-1.18.2.parquet";
    static final String GOLDEN_DOTTED_PATHS_PROTO = "dotted-paths-proto-1.18.2.parquet";
    static final String GOLDEN_DOTTED_COLUMN_HADOOP = "dotted-column-parquet-hadoop-1.18.2.parquet";

    static final String PARQUET_VERSION_PREFIX = "parquet-mr version 1.18.2";
    private static final String PARQUET_MAGIC = "PAR1";
    private static final String FOOTER_PROTO_MESSAGE_KEY = "protomolt.proto.message";
    private static final String FOOTER_WRITER_MODEL_KEY = "writer.model.name";
    private static final String FOOTER_WRITER_MODEL_VALUE = "protomolt";

    private static final String SHAPES_PROTO_PATH = "pq/golden/v1/shapes.proto";
    private static final String EXT_PROTO_PATH = "pq/golden/ext/v1/extensible.proto";

    private static final String SHAPES_PROTO = """
            syntax = "proto3";
            package pq.golden.v1;
            import "google/protobuf/empty.proto";
            import "google/protobuf/struct.proto";

            message EmptyShape {}

            message OptionalChild {
              optional string label = 1;
            }

            message OptionalShape {
              optional string note = 1;
              OptionalChild child = 2;
              repeated string tags = 3;
              map<string, int64> attrs = 4;
              google.protobuf.Struct extra = 5;
            }

            message WithEmptyChild {
              string id = 1;
              EmptyShape empty_child = 2;
            }

            message WithWellKnownEmpty {
              string id = 1;
              google.protobuf.Empty ack = 2;
            }

            message WithRepeatedEmpty {
              string id = 1;
              repeated EmptyShape items = 2;
            }

            message WithEmptyMapValue {
              string id = 1;
              map<string, EmptyShape> entries = 2;
            }

            message SelfNode {
              string name = 1;
              SelfNode next = 2;
            }

            message RepeatedTree {
              string label = 1;
              repeated RepeatedTree children = 2;
            }

            message MapGraph {
              map<string, MapGraph> edges = 1;
            }

            message Alpha {
              Beta beta = 1;
            }

            message Beta {
              Alpha alpha = 1;
            }

            message RecursiveEnvelope {
              string id = 1;
              Alpha root = 2;
            }

            message Endpoint {
              string host = 1;
              int32 port = 2;
            }

            message BoundedShape {
              string id = 1;
              Endpoint start = 2;
              Endpoint end = 3;
              google.protobuf.Struct object_json = 4;
              google.protobuf.Value dynamic_json = 5;
              google.protobuf.ListValue array_json = 6;
            }

            message Outer {
              message DottedRecord {
                string id = 1;
                Endpoint origin = 2;
                repeated string tags = 3;
                map<string, int64> metrics = 4;
                google.protobuf.Struct payload = 5;
              }
            }
            """;

    private static final String PROTO2_EXT_PROTO = """
            syntax = "proto2";
            package pq.golden.ext.v1;

            message ExtensibleRow {
              required string id = 1;
              optional int64 seq = 2;
              extensions 100 to 199;
            }

            extend ExtensibleRow {
              optional string dotted_ext = 100;
            }
            """;

    /** The compiled {@code pq.golden.v1} shapes file. */
    static final FileDescriptor SHAPES_FILE;
    /** The compiled proto2 {@code pq.golden.ext.v1} extension file. */
    static final FileDescriptor EXT_FILE;

    static {
        CompiledProtos compiled;
        try {
            compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                    .add(SHAPES_PROTO_PATH, SHAPES_PROTO, "test")
                    .add(EXT_PROTO_PATH, PROTO2_EXT_PROTO, "test")
                    .build());
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
        SHAPES_FILE = compiled.descriptorFor(SHAPES_PROTO_PATH).orElseThrow();
        EXT_FILE = compiled.descriptorFor(EXT_PROTO_PATH).orElseThrow();
    }

    private ParquetShapeFixtures() {
    }

    static Message parseJson(Descriptor messageType, String json) throws Exception {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageType);
        JsonFormat.parser().merge(json, builder);
        return builder.build();
    }

    static byte[] goldenBytes(String fileName) throws IOException {
        String resourcePath = GOLDEN_RESOURCE_PREFIX + fileName;
        try (InputStream in = ParquetShapeFixtures.class.getClassLoader()
                .getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("Missing golden test resource: " + resourcePath);
            }
            return in.readAllBytes();
        }
    }

    static void assertParquetEnvelopeAndFooter(byte[] parquet, String expectedProtoType,
                                                       long expectedRowCount) throws IOException {
        assertThat(parquet.length).isGreaterThanOrEqualTo(12);
        assertThat(new String(parquet, 0, 4, StandardCharsets.US_ASCII)).isEqualTo(PARQUET_MAGIC);
        assertThat(new String(parquet, parquet.length - 4, 4, StandardCharsets.US_ASCII))
                .isEqualTo(PARQUET_MAGIC);

        FileMetaData footer = readThriftFooter(parquet);
        assertThat(footer.getCreated_by()).startsWith(PARQUET_VERSION_PREFIX);
        assertThat(footer.getNum_rows()).isEqualTo(expectedRowCount);

        Map<String, String> kv = new LinkedHashMap<>();
        if (footer.getKey_value_metadata() != null) {
            for (KeyValue entry : footer.getKey_value_metadata()) {
                kv.put(entry.getKey(), entry.getValue());
            }
        }
        assertThat(kv)
                .containsEntry(FOOTER_PROTO_MESSAGE_KEY, expectedProtoType)
                .containsEntry(FOOTER_WRITER_MODEL_KEY, FOOTER_WRITER_MODEL_VALUE);
    }

    static FileMetaData readThriftFooter(byte[] parquet) throws IOException {
        int length = parquet.length;
        int footerLength = (parquet[length - 8] & 0xff)
                | ((parquet[length - 7] & 0xff) << 8)
                | ((parquet[length - 6] & 0xff) << 16)
                | ((parquet[length - 5] & 0xff) << 24);
        int footerStart = length - 8 - footerLength;
        try (ByteArrayInputStream in = new ByteArrayInputStream(
                parquet, footerStart, footerLength)) {
            return Util.readFileMetaData(in);
        }
    }

    static List<Group> readAllGroups(Path dir, String fileName, byte[] parquet)
            throws IOException {
        Path onDisk = dir.resolve(fileName);
        Files.write(onDisk, parquet);
        List<Group> groups = new ArrayList<>();
        try (ParquetReader<Group> reader = new GroupBuilder(new LocalInputFile(onDisk)).build()) {
            Group current;
            while ((current = reader.read()) != null) {
                groups.add(current);
            }
        }
        return groups;
    }

    private static final class GroupBuilder extends ParquetReader.Builder<Group> {
        private GroupBuilder(InputFile file) {
            super(file);
        }

        @Override
        protected ReadSupport<Group> getReadSupport() {
            return new GroupReadSupport();
        }
    }
}
