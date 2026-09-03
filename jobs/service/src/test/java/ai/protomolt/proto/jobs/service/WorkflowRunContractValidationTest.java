package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.jobs.v1.WorkflowRunEvent;
import ai.protomolt.proto.jobs.v1.WorkflowRunRequest;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the jobs contract's own validate.v1 annotations compile and evaluate — the
 * dogfood gate. The protomolt serde enforces these rules on write, so a regression here
 * means a malformed event could reach the topic (or a good one be rejected).
 */
class WorkflowRunContractValidationTest {

    private static WorkflowRunEvent.Builder validEvent() {
        return WorkflowRunEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setJobId(UUID.randomUUID().toString())
                .setWorkflowName("court-decoration")
                .setType(WorkflowRunEvent.Type.TYPE_COMPLETED)
                .setAttempt(1)
                .setVerdict("3 steps, output court.v1.OpinionMetadata")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).build());
    }

    @Test
    void aWellFormedEventValidates() {
        ValidationResult result = ProtoValidator
                .forMessageType(WorkflowRunEvent.getDescriptor())
                .validate(validEvent().build());
        assertThat(result.valid()).as("violations: %s", result.violations()).isTrue();
    }

    @Test
    void aCompletedEventWithoutAVerdictFailsTheMessageCel() {
        ValidationResult result = ProtoValidator
                .forMessageType(WorkflowRunEvent.getDescriptor())
                .validate(validEvent().clearVerdict().build());
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.ruleId()).isEqualTo("completed-carries-verdict"));
    }

    @Test
    void aFailedEventWithoutAnErrorFailsTheMessageCel() {
        WorkflowRunEvent event = validEvent()
                .setType(WorkflowRunEvent.Type.TYPE_DEAD)
                .clearVerdict()
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(WorkflowRunEvent.getDescriptor())
                .validate(event);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.ruleId()).isEqualTo("terminal-carries-error"));
    }

    @Test
    void fieldRulesBiteOnTheEventIds() {
        WorkflowRunEvent event = validEvent()
                .setEventId("not-a-uuid")
                .setType(WorkflowRunEvent.Type.TYPE_UNSPECIFIED)
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(WorkflowRunEvent.getDescriptor())
                .validate(event);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).hasSizeGreaterThanOrEqualTo(2);
        assertThat(result.violations()).anySatisfy(v -> assertThat(v.path()).contains("event_id"));
        assertThat(result.violations()).anySatisfy(v -> assertThat(v.path()).contains("type"));
    }

    @Test
    void aWellFormedRequestValidates() {
        WorkflowRunRequest request = WorkflowRunRequest.newBuilder()
                .setJobId(UUID.randomUUID().toString())
                .setWorkflowName("court-decoration")
                .setInput(Struct.newBuilder().build())
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(WorkflowRunRequest.getDescriptor())
                .validate(request);
        assertThat(result.valid()).as("violations: %s", result.violations()).isTrue();
    }

    @Test
    void aRequestWithoutAWorkflowNameFails() {
        WorkflowRunRequest request = WorkflowRunRequest.newBuilder()
                .setJobId(UUID.randomUUID().toString())
                .setInput(Struct.newBuilder().build())
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(WorkflowRunRequest.getDescriptor())
                .validate(request);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).anySatisfy(v -> assertThat(v.path()).contains("workflow_name"));
    }
}
