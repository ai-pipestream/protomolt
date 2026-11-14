package ai.protomolt.proto.emit.parquet;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.EXT_FILE;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.GOLDEN_DOTTED_PATHS_PROTO;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.GOLDEN_EMPTY_OPTIONAL_ROWS;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.GOLDEN_EMPTY_ZERO_ROWS;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.GOLDEN_RECURSIVE_JSON_AND_DAG;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.SHAPES_FILE;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.assertParquetEnvelopeAndFooter;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.goldenBytes;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.parseJson;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.readAllGroups;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins Parquet 1.18.2 output bytes for protobuf shapes sensitive to Parquet minor-version
 * changes: zero-row and all-optional files, well-known recursive JSON types ({@code Struct},
 * {@code Value}, {@code ListValue}) next to non-cyclic sibling reuse, and dotted field-id
 * paths, map keys, nested type names and proto2 extension isolation. Each case writes twice,
 * compares both runs with the stored golden, and reads the rows back.
 */
class ParquetShapeGoldensTest {

    @Test
    void zeroRowOptionalShapeMatchesPinnedParquet1182Golden(@TempDir Path dir) throws Exception {
        Descriptor optionalShape = SHAPES_FILE.findMessageTypeByName("OptionalShape");

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
        Descriptor optionalShape = SHAPES_FILE.findMessageTypeByName("OptionalShape");
        Descriptor optionalChild = SHAPES_FILE.findMessageTypeByName("OptionalChild");
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
    void wellKnownRecursiveJsonAndSiblingDagReuseMatchPinnedParquet1182Golden(@TempDir Path dir)
            throws Exception {
        Descriptor bounded = SHAPES_FILE.findMessageTypeByName("BoundedShape");
        Descriptor endpoint = SHAPES_FILE.findMessageTypeByName("Endpoint");
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
    void dottedPathsMapKeysNestedTypeAndProto2ExtensionsMatchPinnedParquet1182Golden(
            @TempDir Path dir) throws Exception {
        Descriptor dotted = SHAPES_FILE.findMessageTypeByName("Outer")
                .findNestedTypeByName("DottedRecord");
        Descriptor endpoint = SHAPES_FILE.findMessageTypeByName("Endpoint");
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
        Descriptor extensible = EXT_FILE.findMessageTypeByName("ExtensibleRow");
        FieldDescriptor extension = EXT_FILE.findExtensionByName("dotted_ext");
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
}
