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

    @Test
    void schemaInsideTheFieldBudgetStillValidates() throws Exception {
        Descriptor root = doublingSchema(8);
        assertThatCode(() -> ParquetInputChecks.validateRowSchemaAndColumns(root, Set.of()))
                .doesNotThrowAnyException();
    }
}
