package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.AdmissionDecision;
import ai.pipestream.proto.delegation.v1.CheckEvidence;
import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import ai.pipestream.proto.delegation.v1.DelegateRequest;
import ai.pipestream.proto.delegation.v1.Lane;
import ai.pipestream.proto.delegation.v1.TaskSpec;
import ai.pipestream.proto.delegation.v1.TranscriptEntry;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.delegation.DelegationFixtures.commit;
import static ai.pipestream.proto.delegation.DelegationFixtures.evidence;
import static ai.pipestream.proto.delegation.DelegationFixtures.spec;
import static ai.pipestream.proto.delegation.DelegationFixtures.uuid;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the delegation contract's own validate.v1 annotations compile and evaluate:
 * the dogfood gate. The coordinator and worker adapters validate on write, so a
 * regression here means a malformed frame could cross the boundary.
 */
class DelegationContractValidationTest {

    private static DelegateRequest.Builder validHello() {
        return DelegateRequest.newBuilder()
                .setFrameId(uuid("frame-hello"))
                .setSeq(1)
                .setSentAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).build())
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-sol-1")
                        .setProtocolVersion(1)
                        .setProvider("sol"));
    }

    private static ValidationResult validate(DelegateRequest frame) {
        return ProtoValidator.forMessageType(DelegateRequest.getDescriptor())
                .validate(frame);
    }

    @Test
    void aWellFormedHelloFrameValidates() {
        ValidationResult result = validate(validHello().build());
        assertThat(result.valid()).as("violations: %s", result.violations()).isTrue();
    }

    @Test
    void aHelloCarryingATaskIdFailsTheMessageCel() {
        ValidationResult result = validate(validHello()
                .setTaskId(uuid("task"))
                .build());
        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).anySatisfy(
                v -> assertThat(v.ruleId()).isEqualTo("hello-is-session-scoped"));
    }

    @Test
    void aTaskFrameWithoutATaskIdFailsTheMessageCel() {
        ValidationResult result = validate(validHello()
                .clearHello()
                .setHeartbeat(ai.pipestream.proto.delegation.v1.Heartbeat.newBuilder()
                        .setAttempt(1))
                .build());
        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).anySatisfy(
                v -> assertThat(v.ruleId()).isEqualTo("hello-is-session-scoped"));
    }

    @Test
    void aNonUuidFrameIdFailsTheFieldRule() {
        ValidationResult result = validate(validHello()
                .setFrameId("not-a-uuid")
                .build());
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.path()).contains("frame_id"));
    }

    @Test
    void aSpecWithoutAcceptanceChecksFails() {
        ValidationResult result = ProtoValidator
                .forMessageType(TaskSpec.getDescriptor())
                .validate(TaskSpec.newBuilder().setObjective("do the thing").build());
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.path()).contains("required_checks"));
    }

    @Test
    void aCandidateWithoutOutputReferencesFailsTheMessageCel() {
        CompletionCandidate bare = CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("done, trust me")
                .addEvidence(evidence("compile"))
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(CompletionCandidate.getDescriptor())
                .validate(bare);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).anySatisfy(
                v -> assertThat(v.ruleId()).isEqualTo("candidate-references-output"));
    }

    @Test
    void aCandidateWithoutAnAttemptFailsTheFieldRule() {
        CompletionCandidate unbound = CompletionCandidate.newBuilder()
                .setRevision(1)
                .setSummary("cannot be attributed to a lease")
                .addEvidence(evidence("compile"))
                .addCommits(commit("unbound"))
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(CompletionCandidate.getDescriptor())
                .validate(unbound);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.path()).contains("attempt"));
    }

    @Test
    void evidenceWithAnUnspecifiedVerdictFailsTheEnumRule() {
        CheckEvidence unset = CheckEvidence.newBuilder()
                .setCheckName("compile")
                .setRanAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).build())
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(CheckEvidence.getDescriptor())
                .validate(unset);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.path()).contains("verdict"));
    }

    @Test
    void aRejectedAdmissionWithoutAReasonFailsTheMessageCel() {
        ValidationResult result = ProtoValidator
                .forMessageType(AdmissionDecision.getDescriptor())
                .validate(AdmissionDecision.newBuilder().setAdmitted(false).build());
        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).anySatisfy(
                v -> assertThat(v.ruleId()).isEqualTo("rejection-carries-reason"));
    }

    @Test
    void aTranscriptEntryWhoseLaneDisagreesWithItsFrameFailsTheMessageCel() {
        TranscriptEntry crossed = TranscriptEntry.newBuilder()
                .setLane(Lane.LANE_COORDINATOR)
                .setWorkerId("worker-sol-1")
                .setWorkerFrame(validHello().build())
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(TranscriptEntry.getDescriptor())
                .validate(crossed);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).anySatisfy(
                v -> assertThat(v.ruleId()).isEqualTo("lane-matches-frame"));
    }

    @Test
    void aWellFormedCandidateValidates() {
        CompletionCandidate candidate = CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("implemented and proven")
                .addEvidence(evidence("compile"))
                .addCommits(commit("ok"))
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(CompletionCandidate.getDescriptor())
                .validate(candidate);
        assertThat(result.valid()).as("violations: %s", result.violations()).isTrue();
    }

    @Test
    void aWellFormedSpecValidates() {
        ValidationResult result = ProtoValidator
                .forMessageType(TaskSpec.getDescriptor())
                .validate(spec("compile", "tests"));
        assertThat(result.valid()).as("violations: %s", result.violations()).isTrue();
    }
}
