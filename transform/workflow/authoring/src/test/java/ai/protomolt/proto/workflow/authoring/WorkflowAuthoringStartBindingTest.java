package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringRequest;
import com.google.protobuf.Duration;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthoringStartBindingTest {
    private static final TaskSpec SPEC = TaskSpec.newBuilder().setObjective("Author workflow")
            .addRequiredChecks(AcceptanceCheck.newBuilder().setName("compile")).build();
    private static final Duration LEASE = Duration.newBuilder().setSeconds(300).build();
    private static final StartWorkflowAuthoringRequest REQUEST = StartWorkflowAuthoringRequest.newBuilder()
            .setTaskId("aaaaaaaa-0000-4000-8000-000000000001").setWorkerId("author")
            .setTemplateSha256("b".repeat(64)).setObjective(SPEC.getObjective()).build();

    @Test void eachPersistedIdentityAndOfferedContentContributesToTheBinding() throws Exception {
        String original = hash(REQUEST, SPEC, LEASE);
        assertThat(original).matches("[0-9a-f]{64}");
        assertThat(hash(StartWorkflowAuthoringRequest.parseFrom(REQUEST.toByteArray()),
                TaskSpec.parseFrom(SPEC.toByteArray()), Duration.parseFrom(LEASE.toByteArray())))
                .isEqualTo(original);
        assertThat(hash(REQUEST.toBuilder().setWorkerId("other").build(), SPEC, LEASE)).isNotEqualTo(original);
        assertThat(hash(REQUEST.toBuilder().setTaskId("aaaaaaaa-0000-4000-8000-000000000002").build(),
                SPEC, LEASE)).isNotEqualTo(original);
        assertThat(hash(REQUEST.toBuilder().setTemplateSha256("c".repeat(64)).build(), SPEC, LEASE))
                .isNotEqualTo(original);
        assertThat(hash(REQUEST.toBuilder().setObjective("Different objective").build(),
                SPEC.toBuilder().setObjective("Different objective").build(), LEASE)).isNotEqualTo(original);
        assertThat(hash(REQUEST, SPEC.toBuilder().addConstraints("No remote writes").build(), LEASE))
                .isNotEqualTo(original);
        assertThat(hash(REQUEST, SPEC, LEASE.toBuilder().setSeconds(301).build())).isNotEqualTo(original);
    }

    @Test void invalidOrUnknownInputCannotAcquireAStartBinding() {
        assertThatThrownBy(() -> hash(REQUEST, SPEC.toBuilder().setObjective("Other").build(), LEASE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> hash(REQUEST, SPEC, Duration.getDefaultInstance()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> hash(REQUEST, SPEC, LEASE.toBuilder().setNanos(1).build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> hash(REQUEST, SPEC, LEASE.toBuilder().setSeconds(86_401).build()))
                .isInstanceOf(IllegalArgumentException.class);
        var unknown = UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        assertThatThrownBy(() -> hash(REQUEST.toBuilder().setUnknownFields(unknown).build(), SPEC, LEASE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> hash(REQUEST, SPEC.toBuilder().setUnknownFields(unknown).build(), LEASE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> hash(REQUEST, SPEC, LEASE.toBuilder().setUnknownFields(unknown).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static String hash(StartWorkflowAuthoringRequest request, TaskSpec spec, Duration lease) {
        return WorkflowAuthoringStartBinding.sha256(request, spec, lease);
    }
}
