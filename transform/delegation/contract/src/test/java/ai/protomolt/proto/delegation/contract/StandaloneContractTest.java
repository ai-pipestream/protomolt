package ai.protomolt.proto.delegation.contract;

import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Runs with only the worker contract dependency graph. */
class StandaloneContractTest {
    private DelegateRequest hello() {
        return DelegateRequest.newBuilder()
                .setFrameId("c3a8628b-b162-43dd-9b36-82a34a852769")
                .setSeq(1)
                .setSentAt(Timestamp.newBuilder().setSeconds(1_700_000_000L))
                .setHello(WorkerHello.newBuilder().setWorkerId("worker-1")
                        .setProtocolVersion(1).setProvider("test"))
                .build();
    }

    @Test
    void validatesFieldAndCrossFieldRulesWithoutCoordinator() {
        var validator = ProtoValidator.forMessageType(DelegateRequest.getDescriptor());
        assertThat(validator.validate(hello()).valid()).isTrue();
        var taskHello = hello().toBuilder()
                .setTaskId("0efb1c13-121a-4b14-a411-07d00f7938a0").build();
        assertThat(validator.validate(taskHello).violations())
                .anySatisfy(v -> assertThat(v.ruleId()).isEqualTo("hello-is-session-scoped"));
        assertThat(validator.validate(hello().toBuilder().setSeq(0).build()).valid()).isFalse();
    }

    @Test
    void preservesWireIdentity() {
        assertThat(DelegateRequest.getDescriptor().getFile().getName())
                .isEqualTo("ai/protomolt/proto/delegation/v1/delegation.proto");
        assertThat(Any.pack(hello()).getTypeUrl())
                .isEqualTo("type.googleapis.com/ai.protomolt.proto.delegation.v1.DelegateRequest");
    }

    @Test
    void optionalImplementationsAreAbsent() {
        for (String name : new String[] {
                "ai.protomolt.proto.delegation.InProcessDelegationCoordinator",
                "ai.protomolt.proto.delegation.repository.RepositoryServiceTranscriptRepository",
                "ai.protomolt.proto.repo.v1.DocumentServiceGrpc",
                "ai.protomolt.proto.receipt.WorkRecords"}) {
            assertThatThrownBy(() -> Class.forName(name)).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
