package ai.protomolt.proto.emit.parquet;

import ai.protomolt.proto.emit.Bundle;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins Parquet 1.18.2 output bytes and explicit schema refusals across the three corner shapes
 * sensitive to Parquet minor-version changes (#880, follow-up to #856):
 *
 * <ol>
 *   <li><b>Empty protobuf shapes</b>: top-level and nested zero-field message descriptors
 *       ({@code message EmptyShape {}} and {@code google.protobuf.Empty}) are explicitly refused,
 *       while zero-row files and all-optional / present-empty-submessage rows match stored
 *       Parquet 1.18.2 golden bytes.</li>
 *   <li><b>Recursive protobuf shapes</b>: direct, repeated, map-value, mutual, and non-root
 *       cycles are explicitly refused with exact cycle paths, while well-known recursive JSON
 *       types ({@code Struct}, {@code Value}, {@code ListValue}) and non-cyclic sibling DAG reuse
 *       match stored Parquet 1.18.2 golden bytes.</li>
 *   <li><b>Dotted column and path shapes</b>: dotted protobuf field identifiers and dotted
 *       projection selectors are explicitly refused, while packaged nested message names, dotted
 *       {@link ProtoParquetSchemas.FieldIdResolver} paths, dotted map/JSON keys, proto2 extension
 *       isolation, and {@code parquet-hadoop} 1.18.2's {@code [a.b]} vs {@code [a, b]} bloom
 *       filter separation match stored Parquet 1.18.2 golden bytes.</li>
 * </ol>
 */
class ParquetShapeGoldensTest {

    private static final String SHAPES_PROTO_PATH = "pq/golden/v1/shapes.proto";
    private static final String EXT_PROTO_PATH = "pq/golden/ext/v1/extensible.proto";
    private static final String GOLDEN_RESOURCE_PREFIX =
            "ai/protomolt/proto/emit/parquet/golden/";
    private static final String GOLDEN_EMPTY_ZERO_ROWS = "empty-zero-rows-1.18.2.parquet";
    private static final String GOLDEN_EMPTY_OPTIONAL_ROWS = "empty-optional-rows-1.18.2.parquet";
    private static final String GOLDEN_RECURSIVE_JSON_AND_DAG =
            "recursive-json-and-dag-1.18.2.parquet";
    private static final String GOLDEN_DOTTED_PATHS_PROTO = "dotted-paths-proto-1.18.2.parquet";
    private static final String GOLDEN_DOTTED_COLUMN_HADOOP =
            "dotted-column-parquet-hadoop-1.18.2.parquet";

    private static final String PARQUET_MAGIC = "PAR1";
    private static final String PARQUET_VERSION_PREFIX = "parquet-mr version 1.18.2";
    private static final String FOOTER_PROTO_MESSAGE_KEY = "protomolt.proto.message";
    private static final String FOOTER_WRITER_MODEL_KEY = "writer.model.name";
    private static final String FOOTER_WRITER_MODEL_VALUE = "protomolt";

    private static final String DOTTED_FIELD_NAME = "a.b";
    private static final String NESTED_GROUP_NAME = "a";
    private static final String NESTED_LEAF_NAME = "b";
    private static final String FLAT_DOT_VALUE = "flat-dot-value";
    private static final String NESTED_B_VALUE = "nested-b-value";
    private static final int BLOOM_FILTER_MAX_BYTES = 64;

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

    private static final MessageType DOTTED_COLLISION_SCHEMA = Types.buildMessage()
            .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
            .named(DOTTED_FIELD_NAME)
            .optionalGroup()
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                .named(NESTED_LEAF_NAME)
            .named(NESTED_GROUP_NAME)
            .named("pq.golden.v1.DottedCollision");

    private static FileDescriptor shapesFile;
    private static FileDescriptor extFile;

    @BeforeAll
    static void compileSchemas() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(SHAPES_PROTO_PATH, SHAPES_PROTO, "test")
                .add(EXT_PROTO_PATH, PROTO2_EXT_PROTO, "test")
                .build());
        shapesFile = compiled.descriptorFor(SHAPES_PROTO_PATH).orElseThrow();
        extFile = compiled.descriptorFor(EXT_PROTO_PATH).orElseThrow();
    }

    @Test
    void emptyMessageTypesAreExplicitlyRefusedAtRootAndNestedPositions(@TempDir Path dir)
            throws Exception {
        Descriptor emptyShape = shapesFile.findMessageTypeByName("EmptyShape");
        DynamicMessage emptyDynamic = DynamicMessage.getDefaultInstance(emptyShape);

        for (Descriptor rootEmpty : List.of(emptyShape, Empty.getDescriptor())) {
            String expectedName = rootEmpty.getFullName();
            assertThatThrownBy(() -> ProtoParquetSchemas.schema(rootEmpty))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Empty message type cannot become a columnar schema")
                    .hasMessageContaining(expectedName);
            assertThatThrownBy(() -> new ProtoParquetWriteSupport(rootEmpty))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(expectedName);
            assertThatThrownBy(() -> ParquetEmitter.toBytes(rootEmpty, List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(expectedName);
            assertThatThrownBy(() -> ParquetEmitter.bundle("empty.parquet", rootEmpty, List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(expectedName);
        }

        assertThatThrownBy(() -> ParquetEmitter.toBytes(
                emptyShape, List.of(emptyDynamic, emptyDynamic)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pq.golden.v1.EmptyShape");
        assertThatThrownBy(() -> ParquetEmitter.toBytes(
                Empty.getDescriptor(), List.of(Empty.getDefaultInstance())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("google.protobuf.Empty");

        assertNestedEmptyRefused("WithEmptyChild", "empty_child (pq.golden.v1.EmptyShape)");
        assertNestedEmptyRefused("WithWellKnownEmpty", "ack (google.protobuf.Empty)");
        assertNestedEmptyRefused("WithRepeatedEmpty", "items.element (pq.golden.v1.EmptyShape)");
        assertNestedEmptyRefused("WithEmptyMapValue", "entries.value (pq.golden.v1.EmptyShape)");

        // Projecting out the empty message field leaves a valid columnar schema.
        Descriptor withWellKnown = shapesFile.findMessageTypeByName("WithWellKnownEmpty");
        DynamicMessage validRow = DynamicMessage.newBuilder(withWellKnown)
                .setField(withWellKnown.findFieldByName("id"), "kept-id")
                .build();
        Bundle bundle = ParquetEmitter.bundle("projected.parquet", withWellKnown,
                List.of(validRow), ProtoParquetSchemas.FieldIdResolver.NONE,
                ParquetExportOptions.project(Set.of("id")));
        List<Group> rows = readAllGroups(dir, "projected-empty.parquet",
                bundle.file("projected.parquet"));
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().getString("id", 0)).isEqualTo("kept-id");
        assertThat(rows.getFirst().getType().containsField("ack")).isFalse();
    }

    @Test
    void zeroRowOptionalShapeMatchesPinnedParquet1182Golden(@TempDir Path dir) throws Exception {
        Descriptor optionalShape = shapesFile.findMessageTypeByName("OptionalShape");

        byte[] first = ParquetEmitter.toBytes(optionalShape, List.of());
        byte[] second = ParquetEmitter.toBytes(optionalShape, List.of());
        byte[] golden = goldenBytes(GOLDEN_EMPTY_ZERO_ROWS);

        assertThat(first).isEqualTo(second);
        assertThat(first).isEqualTo(golden);
        assertParquetEnvelopeAndFooter(first, "pq.golden.v1.OptionalShape", 0L);

        List<Group> rows = readAllGroups(dir, GOLDEN_EMPTY_ZERO_ROWS, first);
        assertThat(rows).isEmpty();
    }

    @Test
    void emptyOptionalInstancesAndPresentEmptySubmessageMatchPinnedParquet1182Golden(
            @TempDir Path dir) throws Exception {
        Descriptor optionalShape = shapesFile.findMessageTypeByName("OptionalShape");
        Descriptor optionalChild = shapesFile.findMessageTypeByName("OptionalChild");
        Descriptor structDesc = optionalShape.findFieldByName("extra").getMessageType();

        DynamicMessage allUnset = DynamicMessage.getDefaultInstance(optionalShape);
        DynamicMessage withPresentEmptySubmessage = DynamicMessage.newBuilder(optionalShape)
                .setField(optionalShape.findFieldByName("child"),
                        DynamicMessage.getDefaultInstance(optionalChild))
                .setField(optionalShape.findFieldByName("extra"), parseJson(structDesc, "{}"))
                .build();

        byte[] first = ParquetEmitter.toBytes(optionalShape,
                List.of(allUnset, withPresentEmptySubmessage));
        byte[] second = ParquetEmitter.toBytes(optionalShape,
                List.of(allUnset, withPresentEmptySubmessage));
        byte[] golden = goldenBytes(GOLDEN_EMPTY_OPTIONAL_ROWS);

        assertThat(first).isEqualTo(second);
        assertThat(first).isEqualTo(golden);
        assertParquetEnvelopeAndFooter(first, "pq.golden.v1.OptionalShape", 2L);

        List<Group> rows = readAllGroups(dir, GOLDEN_EMPTY_OPTIONAL_ROWS, first);
        assertThat(rows).hasSize(2);

        Group row0 = rows.get(0);
        assertThat(row0.getFieldRepetitionCount("note")).isZero();
        assertThat(row0.getFieldRepetitionCount("child")).isZero();
        assertThat(row0.getFieldRepetitionCount("tags")).isZero();
        assertThat(row0.getFieldRepetitionCount("attrs")).isZero();
        assertThat(row0.getFieldRepetitionCount("extra")).isZero();

        // Definition level 1 preserves a present empty child message distinct from row 0's null.
        Group row1 = rows.get(1);
        assertThat(row1.getFieldRepetitionCount("child")).isEqualTo(1);
        assertThat(row1.getGroup("child", 0).getFieldRepetitionCount("label")).isZero();
        assertThat(row1.getBinary("extra", 0).toStringUsingUTF8()).isEqualTo("{}");
    }

    @Test
    void recursiveMessageShapesAreExplicitlyRefusedWithExactCyclePaths(@TempDir Path dir)
            throws Exception {
        assertCycleRefused("SelfNode",
                "pq.golden.v1.SelfNode -> pq.golden.v1.SelfNode");
        assertCycleRefused("RepeatedTree",
                "pq.golden.v1.RepeatedTree -> pq.golden.v1.RepeatedTree");
        assertCycleRefused("MapGraph",
                "pq.golden.v1.MapGraph -> pq.golden.v1.MapGraph");
        assertCycleRefused("Alpha",
                "pq.golden.v1.Alpha -> pq.golden.v1.Beta -> pq.golden.v1.Alpha");
        assertCycleRefused("RecursiveEnvelope",
                "pq.golden.v1.RecursiveEnvelope -> pq.golden.v1.Alpha"
                        + " -> pq.golden.v1.Beta -> pq.golden.v1.Alpha");

        // Projecting out the recursive top-level branch emits the remaining non-recursive columns.
        Descriptor envelope = shapesFile.findMessageTypeByName("RecursiveEnvelope");
        DynamicMessage row = DynamicMessage.newBuilder(envelope)
                .setField(envelope.findFieldByName("id"), "env-1")
                .build();
        byte[] projected = ParquetEmitter.toBytes(envelope, List.of(row),
                ProtoParquetSchemas.FieldIdResolver.NONE,
                ParquetExportOptions.project(Set.of("id")));
        List<Group> rows = readAllGroups(dir, "projected-recursive.parquet", projected);
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().getString("id", 0)).isEqualTo("env-1");
        assertThat(rows.getFirst().getType().containsField("root")).isFalse();
    }

    @Test
    void wellKnownRecursiveJsonAndSiblingDagReuseMatchPinnedParquet1182Golden(@TempDir Path dir)
            throws Exception {
        Descriptor bounded = shapesFile.findMessageTypeByName("BoundedShape");
        Descriptor endpoint = shapesFile.findMessageTypeByName("Endpoint");
        DynamicMessage row = DynamicMessage.newBuilder(bounded)
                .setField(bounded.findFieldByName("id"), "dag-1")
                .setField(bounded.findFieldByName("start"), DynamicMessage.newBuilder(endpoint)
                        .setField(endpoint.findFieldByName("host"), "alpha.internal")
                        .setField(endpoint.findFieldByName("port"), 8080)
                        .build())
                .setField(bounded.findFieldByName("end"), DynamicMessage.newBuilder(endpoint)
                        .setField(endpoint.findFieldByName("host"), "omega.internal")
                        .setField(endpoint.findFieldByName("port"), 9090)
                        .build())
                .setField(bounded.findFieldByName("object_json"), parseJson(
                        bounded.findFieldByName("object_json").getMessageType(),
                        "{\"nested\":{\"depth\":2,\"items\":[1,\"two\",true]}}"))
                .setField(bounded.findFieldByName("dynamic_json"), parseJson(
                        bounded.findFieldByName("dynamic_json").getMessageType(),
                        "{\"branch\":[{\"leaf\":\"ok\"}]}"))
                .setField(bounded.findFieldByName("array_json"), parseJson(
                        bounded.findFieldByName("array_json").getMessageType(),
                        "[{\"k\":\"v\"},[1,2]]"))
                .build();

        byte[] first = ParquetEmitter.toBytes(bounded, List.of(row));
        byte[] second = ParquetEmitter.toBytes(bounded, List.of(row));
        byte[] golden = goldenBytes(GOLDEN_RECURSIVE_JSON_AND_DAG);

        assertThat(first).isEqualTo(second);
        assertThat(first).isEqualTo(golden);
        assertParquetEnvelopeAndFooter(first, "pq.golden.v1.BoundedShape", 1L);

        List<Group> rows = readAllGroups(dir, GOLDEN_RECURSIVE_JSON_AND_DAG, first);
        assertThat(rows).hasSize(1);
        Group actual = rows.getFirst();
        assertThat(actual.getString("id", 0)).isEqualTo("dag-1");
        assertThat(actual.getGroup("start", 0).getString("host", 0)).isEqualTo("alpha.internal");
        assertThat(actual.getGroup("start", 0).getInteger("port", 0)).isEqualTo(8080);
        assertThat(actual.getGroup("end", 0).getString("host", 0)).isEqualTo("omega.internal");
        assertThat(actual.getGroup("end", 0).getInteger("port", 0)).isEqualTo(9090);
        assertThat(actual.getBinary("object_json", 0).toStringUsingUTF8())
                .isEqualTo("{\"nested\":{\"depth\":2.0,\"items\":[1.0,\"two\",true]}}");
        assertThat(actual.getBinary("dynamic_json", 0).toStringUsingUTF8())
                .isEqualTo("{\"branch\":[{\"leaf\":\"ok\"}]}");
        assertThat(actual.getBinary("array_json", 0).toStringUsingUTF8())
                .isEqualTo("[{\"k\":\"v\"},[1.0,2.0]]");
    }

    @Test
    void dottedProtobufIdentifiersAndDottedProjectionsAreExplicitlyRefused() {
        FileDescriptorProto illegalFieldProto = FileDescriptorProto.newBuilder()
                .setName("illegal_dotted.proto")
                .setPackage("pq.golden.v1")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("IllegalDotted")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName(DOTTED_FIELD_NAME)
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        assertThatThrownBy(() -> FileDescriptor.buildFrom(illegalFieldProto, new FileDescriptor[0]))
                .isInstanceOf(DescriptorValidationException.class)
                .hasMessageContaining(DOTTED_FIELD_NAME)
                .hasMessageContaining("not a valid identifier");

        Descriptor dottedRecord = shapesFile.findMessageTypeByName("Outer")
                .findNestedTypeByName("DottedRecord");

        // Dotted path alone matches zero top-level columns.
        assertThatThrownBy(() -> ParquetEmitter.toBytes(dottedRecord, List.of(),
                ProtoParquetSchemas.FieldIdResolver.NONE,
                ParquetExportOptions.project(Set.of("origin.host"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Projection selected no columns")
                .hasMessageContaining("origin.host");

        // Dotted path mixed with a valid top-level column must be refused, never silently dropped.
        assertThatThrownBy(() -> ParquetEmitter.toBytes(dottedRecord, List.of(),
                ProtoParquetSchemas.FieldIdResolver.NONE,
                ParquetExportOptions.project(Set.of("id", "origin.host"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown top-level columns")
                .hasMessageContaining("origin.host");
    }

    @Test
    void dottedPathsMapKeysNestedTypeAndProto2ExtensionsMatchPinnedParquet1182Golden(
            @TempDir Path dir) throws Exception {
        Descriptor dotted = shapesFile.findMessageTypeByName("Outer")
                .findNestedTypeByName("DottedRecord");
        Descriptor endpoint = shapesFile.findMessageTypeByName("Endpoint");
        Descriptor metricsEntry = dotted.findFieldByName("metrics").getMessageType();

        ProtoParquetSchemas.FieldIdResolver dottedIds = path -> switch (path) {
            case "id" -> 1;
            case "origin" -> 2;
            case "origin.host" -> 3;
            case "origin.port" -> 4;
            case "tags" -> 5;
            case "tags.element" -> 6;
            case "metrics" -> 7;
            case "metrics.key" -> 8;
            case "metrics.value" -> 9;
            case "payload" -> 10;
            default -> null;
        };

        DynamicMessage dottedRow = DynamicMessage.newBuilder(dotted)
                .setField(dotted.findFieldByName("id"), "rec-1")
                .setField(dotted.findFieldByName("origin"), DynamicMessage.newBuilder(endpoint)
                        .setField(endpoint.findFieldByName("host"), "edge.rokkon.internal")
                        .setField(endpoint.findFieldByName("port"), 443)
                        .build())
                .addRepeatedField(dotted.findFieldByName("tags"), "v1.18.2")
                .addRepeatedField(dotted.findFieldByName("metrics"),
                        DynamicMessage.newBuilder(metricsEntry)
                                .setField(metricsEntry.findFieldByName("key"), "rpc.latency.ms")
                                .setField(metricsEntry.findFieldByName("value"), 42L)
                                .build())
                .addRepeatedField(dotted.findFieldByName("metrics"),
                        DynamicMessage.newBuilder(metricsEntry)
                                .setField(metricsEntry.findFieldByName("key"),
                                        "service.instance.id")
                                .setField(metricsEntry.findFieldByName("value"), 7L)
                                .build())
                .setField(dotted.findFieldByName("payload"), parseJson(
                        dotted.findFieldByName("payload").getMessageType(),
                        "{\"a.b\":\"dotted-json-key\"}"))
                .build();

        byte[] first = ParquetEmitter.toBytes(dotted, List.of(dottedRow), dottedIds);
        byte[] second = ParquetEmitter.toBytes(dotted, List.of(dottedRow), dottedIds);
        byte[] golden = goldenBytes(GOLDEN_DOTTED_PATHS_PROTO);

        assertThat(first).isEqualTo(second);
        assertThat(first).isEqualTo(golden);
        assertParquetEnvelopeAndFooter(first, "pq.golden.v1.Outer.DottedRecord", 1L);

        Path onDisk = dir.resolve(GOLDEN_DOTTED_PATHS_PROTO);
        Files.write(onDisk, first);
        try (ParquetFileReader fileReader = ParquetFileReader.open(new LocalInputFile(onDisk))) {
            MessageType storedSchema = fileReader.getFooter().getFileMetaData().getSchema();
            assertThat(storedSchema.getName()).isEqualTo("pq.golden.v1.Outer.DottedRecord");
            assertThat(storedSchema.getType("id").getId().intValue()).isEqualTo(1);
            GroupType originGroup = storedSchema.getType("origin").asGroupType();
            assertThat(originGroup.getId().intValue()).isEqualTo(2);
            assertThat(originGroup.getType("host").getId().intValue()).isEqualTo(3);
            assertThat(originGroup.getType("port").getId().intValue()).isEqualTo(4);
        }

        List<Group> rows = readAllGroups(dir, "dotted-readback.parquet", first);
        assertThat(rows).hasSize(1);
        Group row = rows.getFirst();
        assertThat(row.getString("id", 0)).isEqualTo("rec-1");
        assertThat(row.getGroup("origin", 0).getString("host", 0))
                .isEqualTo("edge.rokkon.internal");
        assertThat(row.getGroup("origin", 0).getInteger("port", 0)).isEqualTo(443);
        assertThat(row.getGroup("tags", 0).getGroup("list", 0).getString("element", 0))
                .isEqualTo("v1.18.2");
        Group metrics = row.getGroup("metrics", 0);
        assertThat(metrics.getGroup("key_value", 0).getString("key", 0))
                .isEqualTo("rpc.latency.ms");
        assertThat(metrics.getGroup("key_value", 0).getLong("value", 0)).isEqualTo(42L);
        assertThat(metrics.getGroup("key_value", 1).getString("key", 0))
                .isEqualTo("service.instance.id");
        assertThat(metrics.getGroup("key_value", 1).getLong("value", 0)).isEqualTo(7L);
        assertThat(row.getBinary("payload", 0).toStringUsingUTF8())
                .isEqualTo("{\"a.b\":\"dotted-json-key\"}");

        // Proto2 extension fields carry dotted full names (pq.golden.ext.v1.dotted_ext) in
        // upstream parquet-protobuf; ProtoParquetSchemas emits declared fields only.
        Descriptor extensible = extFile.findMessageTypeByName("ExtensibleRow");
        FieldDescriptor extension = extFile.findExtensionByName("dotted_ext");
        DynamicMessage withExtension = DynamicMessage.newBuilder(extensible)
                .setField(extensible.findFieldByName("id"), "ext-1")
                .setField(extensible.findFieldByName("seq"), 9L)
                .setField(extension, "ignored-extension-value")
                .build();
        MessageType extSchema = ProtoParquetSchemas.schema(extensible);
        assertThat(extSchema.getFields()).extracting(Type::getName).containsExactly("id", "seq");
        byte[] extBytes = ParquetEmitter.toBytes(extensible, List.of(withExtension));
        List<Group> extRows = readAllGroups(dir, "ext-readback.parquet", extBytes);
        assertThat(extRows).hasSize(1);
        assertThat(extRows.getFirst().getString("id", 0)).isEqualTo("ext-1");
        assertThat(extRows.getFirst().getLong("seq", 0)).isEqualTo(9L);
    }

    @Test
    void parquetHadoopDottedColumnAndNestedGroupCollisionMatchesPinned1182Golden(@TempDir Path dir)
            throws Exception {
        byte[] first = writeDottedCollisionParquet();
        byte[] second = writeDottedCollisionParquet();
        byte[] golden = goldenBytes(GOLDEN_DOTTED_COLUMN_HADOOP);

        assertThat(first).isEqualTo(second);
        assertThat(first).isEqualTo(golden);

        FileMetaData thriftFooter = readThriftFooter(first);
        assertThat(thriftFooter.getCreated_by()).startsWith(PARQUET_VERSION_PREFIX);
        assertThat(thriftFooter.getNum_rows()).isEqualTo(1L);

        List<Group> rows = readAllGroups(dir, GOLDEN_DOTTED_COLUMN_HADOOP, first);
        assertThat(rows).hasSize(1);
        Group row = rows.getFirst();
        assertThat(row.getString(DOTTED_FIELD_NAME, 0)).isEqualTo(FLAT_DOT_VALUE);
        assertThat(row.getGroup(NESTED_GROUP_NAME, 0).getString(NESTED_LEAF_NAME, 0))
                .isEqualTo(NESTED_B_VALUE);

        // Parquet 1.18.2 keys writer bloom filters by ColumnPath rather than toDotString(),
        // keeping [a.b] and [a, b] independent instead of overwriting [a.b]'s bloom filter.
        Path onDisk = dir.resolve("dotted-bloom-check.parquet");
        Files.write(onDisk, first);
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(onDisk))) {
            BlockMetaData block = reader.getFooter().getBlocks().getFirst();
            BloomFilterReader bloomReader = reader.getBloomFilterDataReader(block);
            Map<ColumnPath, ColumnChunkMetaData> byPath = new LinkedHashMap<>();
            for (ColumnChunkMetaData column : block.getColumns()) {
                byPath.put(column.getPath(), column);
            }

            ColumnChunkMetaData flatChunk = byPath.get(ColumnPath.get(DOTTED_FIELD_NAME));
            ColumnChunkMetaData nestedChunk = byPath.get(
                    ColumnPath.get(NESTED_GROUP_NAME, NESTED_LEAF_NAME));
            assertThat(flatChunk).isNotNull();
            assertThat(nestedChunk).isNotNull();

            BloomFilter flatBloom = bloomReader.readBloomFilter(flatChunk);
            BloomFilter nestedBloom = bloomReader.readBloomFilter(nestedChunk);
            assertThat(flatBloom).isNotNull();
            assertThat(nestedBloom).isNotNull();
            assertThat(flatBloom.findHash(flatBloom.hash(Binary.fromString(FLAT_DOT_VALUE))))
                    .isTrue();
            assertThat(flatBloom.findHash(flatBloom.hash(Binary.fromString(NESTED_B_VALUE))))
                    .isFalse();
            assertThat(nestedBloom.findHash(nestedBloom.hash(Binary.fromString(NESTED_B_VALUE))))
                    .isTrue();
            assertThat(nestedBloom.findHash(nestedBloom.hash(Binary.fromString(FLAT_DOT_VALUE))))
                    .isFalse();
        }
    }

    private static void assertNestedEmptyRefused(String messageName, String expectedTarget) {
        Descriptor descriptor = shapesFile.findMessageTypeByName(messageName);
        assertThatThrownBy(() -> ProtoParquetSchemas.schema(descriptor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Empty message type cannot become a columnar schema")
                .hasMessageContaining(expectedTarget);
        assertThatThrownBy(() -> ParquetEmitter.toBytes(descriptor, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedTarget);
    }

    private static void assertCycleRefused(String messageName, String expectedCycle) {
        Descriptor descriptor = shapesFile.findMessageTypeByName(messageName);
        assertThatThrownBy(() -> ProtoParquetSchemas.schema(descriptor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Recursive message type cannot become a columnar schema")
                .hasMessageContaining(expectedCycle);
        assertThatThrownBy(() -> ParquetEmitter.toBytes(descriptor, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedCycle);
    }

    private static byte[] writeDottedCollisionParquet() throws IOException {
        BufferOutputFile output = new BufferOutputFile();
        SimpleGroupFactory factory = new SimpleGroupFactory(DOTTED_COLLISION_SCHEMA);
        Group group = factory.newGroup();
        group.append(DOTTED_FIELD_NAME, FLAT_DOT_VALUE);
        group.addGroup(NESTED_GROUP_NAME).append(NESTED_LEAF_NAME, NESTED_B_VALUE);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(output)
                .withType(DOTTED_COLLISION_SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withCodecFactory(new HadoopFreeCodecs())
                .withBloomFilterEnabled(true)
                .withMaxBloomFilterBytes(BLOOM_FILTER_MAX_BYTES)
                .withConf(new PlainParquetConfiguration())
                .build()) {
            writer.write(group);
        }
        return output.bytes();
    }

    private static Message parseJson(Descriptor messageType, String json) throws Exception {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageType);
        JsonFormat.parser().merge(json, builder);
        return builder.build();
    }

    private static byte[] goldenBytes(String fileName) throws IOException {
        String resourcePath = GOLDEN_RESOURCE_PREFIX + fileName;
        try (InputStream in = ParquetShapeGoldensTest.class.getClassLoader()
                .getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("Missing golden test resource: " + resourcePath);
            }
            return in.readAllBytes();
        }
    }

    private static void assertParquetEnvelopeAndFooter(byte[] parquet, String expectedProtoType,
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

    private static FileMetaData readThriftFooter(byte[] parquet) throws IOException {
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

    private static List<Group> readAllGroups(Path dir, String fileName, byte[] parquet)
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

    private static final class BufferOutputFile implements OutputFile {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        private byte[] bytes() {
            return buffer.toByteArray();
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new PositionOutputStream() {
                private long position;

                @Override
                public long getPos() {
                    return position;
                }

                @Override
                public void write(int b) {
                    buffer.write(b);
                    position++;
                }

                @Override
                public void write(byte[] bytes, int offset, int length) {
                    buffer.write(bytes, offset, length);
                    position += length;
                }
            };
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            buffer.reset();
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }
}
