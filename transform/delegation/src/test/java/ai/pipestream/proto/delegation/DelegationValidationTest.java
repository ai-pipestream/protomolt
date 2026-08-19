package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.AcceptanceCheck;
import ai.pipestream.proto.delegation.v1.AdmissionDecision;
import ai.pipestream.proto.delegation.v1.CheckEvidence;
import ai.pipestream.proto.delegation.v1.CheckVerdict;
import ai.pipestream.proto.delegation.v1.CommitReference;
import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import ai.pipestream.proto.delegation.v1.DelegateRequest;
import ai.pipestream.proto.delegation.v1.TaskSpec;
import ai.pipestream.proto.delegation.v1.TranscriptEntry;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.delegation.DelegationFixtures.commit;
import static ai.pipestream.proto.delegation.DelegationFixtures.evidence;
import static ai.pipestream.proto.delegation.DelegationFixtures.spec;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Structural validation of the delegation contract: the fail-fast, field-naming style
 * of the workflow and pipeline contracts, covering the invariants the reducer assumes.
 */
class DelegationValidationTest {

    @Test
    void aWellFormedHelloPasses() {
        assertThatCode(() -> DelegationValidation.validate(WorkerHello.newBuilder()
                .setWorkerId("worker-sol-1")
                .setProtocolVersion(1)
                .setProvider("sol")
                .build()))
                .doesNotThrowAnyException();
    }

    @Test
    void delegationNamesFollowTheSlugContract() {
        // The old hand-rolled pattern admitted uppercase; the delegation.proto
        // annotations declare slug, and the Java validator now agrees.
        assertThatThrownBy(() -> DelegationValidation.validate(WorkerHello.newBuilder()
                .setWorkerId("Worker-Sol-1")
                .setProtocolVersion(1)
                .setProvider("sol")
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("slug");
    }

    @Test
    void aHelloMustSpeakProtocolVersionOne() {
        assertThatThrownBy(() -> DelegationValidation.validate(WorkerHello.newBuilder()
                .setWorkerId("worker-sol-1")
                .setProtocolVersion(2)
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("protocol_version");
    }

    @Test
    void aSpecMustDeclareAtLeastOneAcceptanceCheck() {
        TaskSpec bare = TaskSpec.newBuilder()
                .setObjective("do the thing")
                .build();
        assertThatThrownBy(() -> DelegationValidation.validate(bare))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("required_checks")
                .hasMessageContaining("never sufficient");
    }

    @Test
    void duplicateCheckNamesAreRejected() {
        TaskSpec spec = TaskSpec.newBuilder()
                .setObjective("do the thing")
                .addRequiredChecks(AcceptanceCheck.newBuilder().setName("compile"))
                .addRequiredChecks(AcceptanceCheck.newBuilder().setName("compile"))
                .build();
        assertThatThrownBy(() -> DelegationValidation.validate(spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate required check name");
    }

    @Test
    void aCandidateMustReferenceACommitOrArtifact() {
        CompletionCandidate bare = CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("done, trust me")
                .addEvidence(evidence("compile"))
                .build();
        assertThatThrownBy(() -> DelegationValidation.validate(bare))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one commit or artifact");
    }

    @Test
    void aCandidateMustNameItsLeaseAttempt() {
        CompletionCandidate unbound = CompletionCandidate.newBuilder()
                .setRevision(1)
                .setSummary("cannot be attributed to a lease")
                .addEvidence(evidence("compile"))
                .addCommits(commit("unbound"))
                .build();
        assertThatThrownBy(() -> DelegationValidation.validate(unbound))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("completion.attempt");
    }

    @Test
    void aCandidateMustNotRepeatEvidenceForOneCheck() {
        CompletionCandidate doubled = CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("done twice over")
                .addEvidence(evidence("compile"))
                .addEvidence(evidence("compile"))
                .addCommits(commit("doubled"))
                .build();
        assertThatThrownBy(() -> DelegationValidation.validate(doubled))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate evidence");
    }

    @Test
    void evidenceMustCarryARealVerdict() {
        CheckEvidence unset = CheckEvidence.newBuilder()
                .setCheckName("compile")
                .setRanAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).build())
                .build();
        assertThatThrownBy(() -> DelegationValidation.validate(unset))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("verdict");
    }

    @Test
    void aCommitReferenceMustCarryAFullSha() {
        CommitReference short1 = CommitReference.newBuilder()
                .setRepository("repo")
                .setCommit("abc123")
                .build();
        assertThatThrownBy(() -> DelegationValidation.validate(short1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-1");
    }

    @Test
    void aRejectedAdmissionMustSayWhy() {
        assertThatThrownBy(() -> DelegationValidation.validate(
                AdmissionDecision.newBuilder().setAdmitted(false).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reason");
    }

    @Test
    void anAdmissionCarriesASessionIdExactlyWhenAdmitted() {
        assertThatCode(() -> DelegationValidation.validate(
                AdmissionDecision.newBuilder()
                        .setAdmitted(true)
                        .setSessionId(DelegationFixtures.uuid("session"))
                        .build()))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> DelegationValidation.validate(
                AdmissionDecision.newBuilder()
                        .setAdmitted(false)
                        .setReason("fleet full")
                        .setSessionId(DelegationFixtures.uuid("session"))
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("session_id");
    }

    @Test
    void aFrameMustCarryAUuidAndASequence() {
        DelegateRequest bad = DelegateRequest.newBuilder()
                .setFrameId("not-a-uuid")
                .setSeq(0)
                .setSentAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).build())
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-sol-1")
                        .setProtocolVersion(1))
                .build();
        assertThatThrownBy(() -> DelegationValidation.validate(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("frame_id");
    }

    @Test
    void aFrameTimestampMustBeInTheProtobufRange() {
        DelegateRequest bad = DelegateRequest.newBuilder()
                .setFrameId(DelegationFixtures.uuid("bad-time-frame"))
                .setSeq(1)
                .setSentAt(Timestamp.newBuilder().setSeconds(Long.MAX_VALUE))
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-sol-1")
                        .setProtocolVersion(1))
                .build();
        assertThatThrownBy(() -> DelegationValidation.validate(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("frame.sent_at");
    }

    @Test
    void aHelloFrameLeavesTheTaskEmpty() {
        DelegateRequest bad = DelegateRequest.newBuilder()
                .setFrameId(DelegationFixtures.uuid("frame"))
                .setTaskId(DelegationFixtures.uuid("task"))
                .setSeq(1)
                .setSentAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).build())
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-sol-1")
                        .setProtocolVersion(1))
                .build();
        assertThatThrownBy(() -> DelegationValidation.validate(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("session-scoped hello");
    }

    @Test
    void aTranscriptEntrysLaneMustMatchItsFrame() {
        TranscriptEntry crossed = TranscriptEntry.newBuilder()
                .setLane(ai.pipestream.proto.delegation.v1.Lane.LANE_WORKER)
                .setWorkerId("worker-sol-1")
                .setCoordinatorFrame(ai.pipestream.proto.delegation.v1.DelegateResponse
                        .newBuilder()
                        .setFrameId(DelegationFixtures.uuid("frame"))
                        .setSeq(1)
                        .setSentAt(Timestamp.newBuilder()
                                .setSeconds(1_700_000_000L).build())
                        .setAdmission(AdmissionDecision.newBuilder()
                                .setAdmitted(false)
                                .setReason("fleet full")))
                .build();
        assertThatThrownBy(() -> DelegationValidation.validate(crossed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("worker_frame");
    }

    @Test
    void aHappyPathTranscriptValidatesStructurally() {
        var transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(DelegationFixtures.WORKER)
                .admit(DelegationFixtures.WORKER)
                .offer(DelegationFixtures.TASK, DelegationFixtures.WORKER, 1,
                        spec("compile"))
                .accept(DelegationFixtures.TASK, DelegationFixtures.WORKER, 1)
                .candidate(DelegationFixtures.TASK, DelegationFixtures.WORKER, 1,
                        spec("compile"))
                .accepted(DelegationFixtures.TASK, DelegationFixtures.WORKER, 1,
                        "proven")
                .build();
        assertThatCode(() -> DelegationValidation.validate(transcript))
                .doesNotThrowAnyException();
    }
}
