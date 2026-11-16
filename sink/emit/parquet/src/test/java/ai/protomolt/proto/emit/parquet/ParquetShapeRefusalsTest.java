package ai.protomolt.proto.emit.parquet;

import ai.protomolt.proto.emit.Bundle;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.SHAPES_FILE;
import static ai.protomolt.proto.emit.parquet.ParquetShapeFixtures.readAllGroups;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Explicit schema refusals for shapes that cannot become a columnar Parquet schema: zero-field
 * message types at root and nested positions, recursive message types (with exact cycle
 * paths), and dotted protobuf identifiers or dotted projection selectors. Each case also checks
 * that projecting out the offending branch leaves a writable schema where one exists.
 */
class ParquetShapeRefusalsTest {

    private static final String DOTTED_FIELD_NAME = "a.b";

    @Test
    void emptyMessageTypesAreExplicitlyRefusedAtRootAndNestedPositions(@TempDir Path dir)
            throws Exception {
        Descriptor emptyShape = SHAPES_FILE.findMessageTypeByName("EmptyShape");
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
        Descriptor withWellKnown = SHAPES_FILE.findMessageTypeByName("WithWellKnownEmpty");
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
        Descriptor envelope = SHAPES_FILE.findMessageTypeByName("RecursiveEnvelope");
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

        Descriptor dottedRecord = SHAPES_FILE.findMessageTypeByName("Outer")
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

    private static void assertNestedEmptyRefused(String messageName, String expectedTarget) {
        Descriptor descriptor = SHAPES_FILE.findMessageTypeByName(messageName);
        assertThatThrownBy(() -> ProtoParquetSchemas.schema(descriptor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Empty message type cannot become a columnar schema")
                .hasMessageContaining(expectedTarget);
        assertThatThrownBy(() -> ParquetEmitter.toBytes(descriptor, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedTarget);
    }

    private static void assertCycleRefused(String messageName, String expectedCycle) {
        Descriptor descriptor = SHAPES_FILE.findMessageTypeByName(messageName);
        assertThatThrownBy(() -> ProtoParquetSchemas.schema(descriptor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Recursive message type cannot become a columnar schema")
                .hasMessageContaining(expectedCycle);
        assertThatThrownBy(() -> ParquetEmitter.toBytes(descriptor, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedCycle);
    }
}
