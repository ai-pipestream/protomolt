package ai.protomolt.proto.emit.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import ai.protomolt.proto.emit.renderer.RenderFailure;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Row-schema checks bound the work a descriptor can ask for, whatever the payload size. */
class ParquetInputChecksTest {

    /**
     * {@code levels} message types, each with two fields of the next type: an acyclic schema
     * whose column paths double at every level.
     */
    private static Descriptor doublingSchema(int levels) throws Exception {
        FileDescriptorProto.Builder file = FileDescriptorProto.newBuilder()
                .setName("doubling.proto")
                .setPackage("doubling")
                .setSyntax("proto3");
        for (int level = 0; level < levels; level++) {
            DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("L" + level);
            if (level == levels - 1) {
                message.addField(FieldDescriptorProto.newBuilder()
                        .setName("leaf").setNumber(1)
                        .setLabel(Label.LABEL_OPTIONAL).setType(Type.TYPE_STRING));
            } else {
                for (int n = 1; n <= 2; n++) {
                    message.addField(FieldDescriptorProto.newBuilder()
                            .setName("f" + n).setNumber(n)
                            .setLabel(Label.LABEL_OPTIONAL).setType(Type.TYPE_MESSAGE)
                            .setTypeName(".doubling.L" + (level + 1)));
                }
            }
            file.addMessageType(message);
        }
        return FileDescriptor.buildFrom(file.build(), new FileDescriptor[0])
                .findMessageTypeByName("L0");
    }

    @Test
    void schemaWhoseColumnPathsDoublePerLevelIsRefusedWithinTheFieldBudget() throws Exception {
        Descriptor root = doublingSchema(40);
        assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                assertThatThrownBy(() ->
                                ParquetInputChecks.validateRowSchemaAndColumns(root, Set.of()))
                        .isInstanceOfSatisfying(RenderFailure.class, refused -> {
                            assertThat(refused.kind()).isEqualTo(RenderFailure.Kind.LIMIT);
                            assertThat(refused.getMessage())
                                    .contains(String.valueOf(
                                            ParquetInputChecks.MAX_SCHEMA_FIELDS));
                        }));
    }

    /** {@code levels} message types in a line, each holding one field of the next. */
    private static Descriptor chainSchema(int levels) throws Exception {
        FileDescriptorProto.Builder file = FileDescriptorProto.newBuilder()
                .setName("chain.proto")
                .setPackage("chain")
                .setSyntax("proto3");
        for (int level = 0; level < levels; level++) {
            boolean last = level == levels - 1;
            FieldDescriptorProto.Builder field = FieldDescriptorProto.newBuilder()
                    .setName(last ? "leaf" : "next").setNumber(1)
                    .setLabel(Label.LABEL_OPTIONAL)
                    .setType(last ? Type.TYPE_STRING : Type.TYPE_MESSAGE);
            if (!last) {
                field.setTypeName(".chain.C" + (level + 1));
            }
            file.addMessageType(DescriptorProto.newBuilder().setName("C" + level).addField(field));
        }
        return FileDescriptor.buildFrom(file.build(), new FileDescriptor[0])
                .findMessageTypeByName("C0");
    }

    @Test
    void schemaNestedPastTheDepthLimitIsRefusedInsteadOfOverflowingTheStack() throws Exception {
        Descriptor root = chainSchema(3000);
        assertThatThrownBy(() -> ParquetInputChecks.validateRowSchemaAndColumns(root, Set.of()))
                .isInstanceOfSatisfying(RenderFailure.class, refused -> {
                    assertThat(refused.kind()).isEqualTo(RenderFailure.Kind.LIMIT);
                    assertThat(refused.getMessage())
                            .contains(String.valueOf(ParquetInputChecks.MAX_MESSAGE_WALK_DEPTH));
                });
    }

    @Test
    void schemaNestedInsideTheDepthLimitStillValidates() throws Exception {
        Descriptor root = chainSchema(ParquetInputChecks.MAX_MESSAGE_WALK_DEPTH);
        assertThatCode(() -> ParquetInputChecks.validateRowSchemaAndColumns(root, Set.of()))
                .doesNotThrowAnyException();
    }

    @Test
    void schemaInsideTheFieldBudgetStillValidates() throws Exception {
        Descriptor root = doublingSchema(8);
        assertThatCode(() -> ParquetInputChecks.validateRowSchemaAndColumns(root, Set.of()))
                .doesNotThrowAnyException();
    }

    @Test
    void compatibilityChecksSharedDescriptorPairsOnlyOnce() throws Exception {
        Descriptor expected = doublingSchema(40);
        Descriptor actual = doublingSchema(40);
        assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                assertThatCode(() -> ParquetInputChecks.verifyFieldsCompatible(expected, actual))
                        .doesNotThrowAnyException());
    }

    @Test
    void compatibilityWalkRefusesTooManyDistinctFieldsWithoutRecursion() throws Exception {
        Descriptor expected = chainSchema(ParquetInputChecks.MAX_SCHEMA_FIELDS + 1);
        Descriptor actual = chainSchema(ParquetInputChecks.MAX_SCHEMA_FIELDS + 1);
        assertThatThrownBy(() -> ParquetInputChecks.verifyFieldsCompatible(expected, actual))
                .isInstanceOfSatisfying(RenderFailure.class, failure -> {
                    assertThat(failure.kind()).isEqualTo(RenderFailure.Kind.LIMIT);
                    assertThat(failure.getMessage()).contains("Source compatibility check");
                });
    }

    @Test
    void compatibilityWalkTerminatesOnIndependentlyLinkedRecursiveDescriptors() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("recursive.proto").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Node")
                        .addField(FieldDescriptorProto.newBuilder().setName("next").setNumber(1)
                                .setLabel(Label.LABEL_OPTIONAL).setType(Type.TYPE_MESSAGE)
                                .setTypeName("Node")))
                .build();
        Descriptor expected = FileDescriptor.buildFrom(file, new FileDescriptor[0]).getMessageTypes().getFirst();
        Descriptor actual = FileDescriptor.buildFrom(file, new FileDescriptor[0]).getMessageTypes().getFirst();
        assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                assertThatCode(() -> ParquetInputChecks.verifyFieldsCompatible(expected, actual))
                        .doesNotThrowAnyException());
    }

}
