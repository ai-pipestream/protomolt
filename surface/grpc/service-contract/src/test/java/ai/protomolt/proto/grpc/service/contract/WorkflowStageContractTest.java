package ai.protomolt.proto.grpc.service.contract;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A workflow reaches the verbs in one of two shapes, and which one a request takes is part
 * of its contract.
 *
 * <p>The authoring shape is what a caller writes: a name, a schema source to resolve step
 * types against, and a deadline in milliseconds. The durable shape is what the compiler
 * produced and a run actually executed: dependencies resolved, deadline a Duration. They
 * share field numbers for different meanings, so a request that names the wrong one does
 * not fail loudly; it reads one field as another.
 */
class WorkflowStageContractTest {

    private static final String DURABLE = "ai.protomolt.proto.grpc.workflow.v1.Workflow";
    private static final String AUTHORING =
            "ai.protomolt.proto.grpc.service.v1.CompiledWorkflow";

    private static String workflowFieldType(String message) {
        Descriptor descriptor = ProtoMoltServiceSchema.file().findMessageTypeByName(message);
        assertThat(descriptor).as("message %s", message).isNotNull();
        FieldDescriptor field = descriptor.findFieldByName("workflow");
        assertThat(field).as("%s.workflow", message).isNotNull();
        return field.getMessageType().getFullName();
    }

    @Test
    void verbsThatExecuteAWorkflowTakeTheAuthoringShape() {
        // These compile or run what the caller wrote, so they need the schema source.
        assertThat(workflowFieldType("RunWorkflowRequest")).isEqualTo(AUTHORING);
        assertThat(workflowFieldType("CheckWorkflowRequest")).isEqualTo(AUTHORING);
        assertThat(workflowFieldType("CompileWorkflowRequest")).isEqualTo(AUTHORING);
        assertThat(workflowFieldType("RecordWorkflowRunRequest")).isEqualTo(AUTHORING);
    }

    @Test
    void verbsThatActOnARecordedRunTakeTheDurableShape() {
        // These replay, publish or evaluate what a run already executed.
        assertThat(workflowFieldType("ReplayWorkflowRequest")).isEqualTo(DURABLE);
        assertThat(workflowFieldType("PromoteWorkflowRequest")).isEqualTo(DURABLE);
        assertThat(workflowFieldType("EvaluateWorkRecordRequest")).isEqualTo(DURABLE);
    }

    @Test
    void theTwoShapesDisagreeOnWhatTheirFieldNumbersMean() {
        // The reason mixing them up is dangerous rather than merely wrong: the same tag
        // carries a different field in each, so a mistyped request misreads rather than
        // failing to parse.
        Descriptor authoring =
                ProtoMoltServiceSchema.file().findMessageTypeByName("CompiledWorkflow");
        Descriptor durable = ProtoMoltServiceSchema.file().getDependencies().stream()
                .flatMap(file -> file.getMessageTypes().stream())
                .filter(message -> DURABLE.equals(message.getFullName()))
                .findFirst()
                .orElseGet(() -> ProtoMoltServiceSchema.file()
                        .findMessageTypeByName("ReplayWorkflowRequest")
                        .findFieldByName("workflow").getMessageType());

        assertThat(authoring.findFieldByNumber(2).getName()).isEqualTo("schema");
        assertThat(durable.findFieldByNumber(2).getName()).isEqualTo("description");
        assertThat(authoring.findFieldByNumber(4).getName()).isEqualTo("deadline_ms");
        assertThat(durable.findFieldByNumber(4).getName()).isEqualTo("dependencies");
    }
}
