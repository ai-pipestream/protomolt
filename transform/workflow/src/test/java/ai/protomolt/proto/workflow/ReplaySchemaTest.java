package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowOutput;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Replay type resolution stays inside the recorded schema, nested types and dependencies included. */
class ReplaySchemaTest {

    private static final List<FileDescriptor> STRUCT = List.of(Struct.getDescriptor().getFile());

    @Test
    void topLevelAndNestedMessagesResolve() {
        assertThat(ReplaySchema.findMessage(STRUCT, "google.protobuf.Value"))
                .isEqualTo(Value.getDescriptor());
        assertThat(ReplaySchema.findMessage(STRUCT, "google.protobuf.Struct.FieldsEntry"))
                .isEqualTo(Struct.getDescriptor().findNestedTypeByName("FieldsEntry"));
    }

    @Test
    void aNameOutsideTheSchemaIsAbsentNotResolvedElsewhere() {
        assertThat(ReplaySchema.findMessage(STRUCT, "google.protobuf.Any")).isNull();
        assertThat(ReplaySchema.findMessage(List.of(), "google.protobuf.Value")).isNull();
    }

    @Test
    void missingOutputAndCollectTypesNameTheType() {
        Workflow workflow = Workflow.newBuilder()
                .setOutput(WorkflowOutput.newBuilder().setType("acme.Missing"))
                .build();
        WorkflowStep step = WorkflowStep.newBuilder()
                .setFanOut(FanOutSpec.newBuilder().setCollectType("acme.Gone"))
                .build();

        assertThatThrownBy(() -> ReplaySchema.outputType(STRUCT, workflow))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("output type acme.Missing not in the replay schema");
        assertThatThrownBy(() -> ReplaySchema.collectType(STRUCT, step))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("collect type acme.Gone not in the replay schema");
    }

    @Test
    void theGroundingRegistryWalksDependenciesOnce() throws Exception {
        FileDescriptor dependent = FileDescriptor.buildFrom(FileDescriptorProto.newBuilder()
                        .setName("dependent.proto")
                        .setPackage("acme")
                        .addDependency("google/protobuf/struct.proto")
                        .build(),
                new FileDescriptor[] {Struct.getDescriptor().getFile()});

        var registry = ReplaySchema.typeRegistry(
                List.of(dependent, Struct.getDescriptor().getFile()));

        assertThat(registry.find("google.protobuf.Struct")).isEqualTo(Struct.getDescriptor());
        assertThat(registry.find("google.protobuf.ListValue")).isNotNull();
    }

    @Test
    void theLazyGroundingRegistryBuildsOnceThenReusesTheSameInstance() {
        var supplier = ReplaySchema.lazyTypeRegistry(STRUCT);

        TypeRegistry built = supplier.get();

        assertThat(built.find("google.protobuf.Struct")).isEqualTo(Struct.getDescriptor());
        assertThat(supplier.get()).isSameAs(built);
        assertThat(supplier.get()).isSameAs(built);
    }
}
