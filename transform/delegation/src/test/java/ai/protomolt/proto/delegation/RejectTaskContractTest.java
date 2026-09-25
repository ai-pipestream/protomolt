package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.RejectTaskRequest;
import ai.protomolt.proto.delegation.v1.RejectTaskResponse;
import ai.protomolt.proto.validate.ProtoValidator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Runtime validation of the worker's decline request and its durable receipt. */
class RejectTaskContractTest {
    private static final String TASK_ID = "49e553aa-df4c-4f19-8c27-51655ecc0b53";

    private static RejectTaskRequest.Builder request() {
        return RejectTaskRequest.newBuilder()
                .setWorkerId("worker-kimi")
                .setTaskId(TASK_ID)
                .setAttempt(1)
                .setReason("Not equipped for this task");
    }

    private static boolean valid(RejectTaskRequest message) {
        return ProtoValidator.forMessageType(RejectTaskRequest.getDescriptor())
                .validate(message).valid();
    }

    @Test
    void requestAcceptsBothAttemptEndpoints() {
        assertThat(valid(request().setAttempt(1).build())).isTrue();
        assertThat(valid(request().setAttempt(1024).build())).isTrue();
    }

    @Test
    void requestRequiresWorkerTaskAttemptAndReasonWithTheirDeclaredShapes() {
        var validator = ProtoValidator.forMessageType(RejectTaskRequest.getDescriptor());
        assertThat(validator.validate(RejectTaskRequest.getDefaultInstance()).violations())
                .extracting(v -> v.path())
                .contains("worker_id", "task_id", "attempt", "reason");

        assertThat(validator.validate(request().setWorkerId("worker id").build()).violations())
                .anySatisfy(v -> assertThat(v.path()).isEqualTo("worker_id"));
        assertThat(validator.validate(request().setTaskId("not-a-uuid").build()).violations())
                .anySatisfy(v -> assertThat(v.path()).isEqualTo("task_id"));
        assertThat(validator.validate(request().setAttempt(0).build()).violations())
                .anySatisfy(v -> assertThat(v.path()).isEqualTo("attempt"));
        assertThat(validator.validate(request().setAttempt(1025).build()).violations())
                .anySatisfy(v -> assertThat(v.path()).isEqualTo("attempt"));
        assertThat(validator.validate(request().clearReason().build()).violations())
                .anySatisfy(v -> assertThat(v.path()).isEqualTo("reason"));
    }

    @Test
    void responseAcceptsBothAttemptEndpointsOnlyWhenSuccessIsTrue() {
        var validator = ProtoValidator.forMessageType(RejectTaskResponse.getDescriptor());
        for (int attempt : new int[] {1, 1024}) {
            var response = RejectTaskResponse.newBuilder()
                    .setOk(true).setTaskId(TASK_ID).setAttempt(attempt).build();
            assertThat(validator.validate(response).violations()).isEmpty();
        }

        var unsuccessful = RejectTaskResponse.newBuilder()
                .setOk(false).setTaskId(TASK_ID).setAttempt(1).build();
        assertThat(validator.validate(unsuccessful).violations())
                .anySatisfy(v -> assertThat(v.path()).isEqualTo("ok"));
    }
}
