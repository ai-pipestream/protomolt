package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.AcceptanceCheck;
import ai.pipestream.proto.delegation.v1.CheckEvidence;
import ai.pipestream.proto.delegation.v1.CheckVerdict;
import ai.pipestream.proto.delegation.v1.CompletionAccepted;
import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import ai.pipestream.proto.delegation.v1.Cancellation;
import ai.pipestream.proto.delegation.v1.DelegateRequest;
import ai.pipestream.proto.delegation.v1.DelegateResponse;
import ai.pipestream.proto.delegation.v1.Lane;
import ai.pipestream.proto.delegation.v1.RevisionRequested;
import ai.pipestream.proto.delegation.v1.TaskAccept;
import ai.pipestream.proto.delegation.v1.TaskOffer;
import ai.pipestream.proto.delegation.v1.TaskSpec;
import ai.pipestream.proto.delegation.v1.Transcript;
import ai.pipestream.proto.delegation.v1.TranscriptEntry;
import ai.pipestream.proto.grpc.workflow.v1.ArtifactReference;
import ai.pipestream.proto.receipt.CompletenessStatus;
import ai.pipestream.proto.receipt.KeyState;
import ai.pipestream.proto.receipt.RecordKeys;
import ai.pipestream.proto.receipt.RecordSigner;
import ai.pipestream.proto.receipt.RecordStep;
import ai.pipestream.proto.receipt.SignatureAlgorithm;
import ai.pipestream.proto.receipt.StepOutcome;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.TrustedIssuer;
import ai.pipestream.proto.receipt.TrustedKey;
import ai.pipestream.proto.receipt.Verification;
import ai.pipestream.proto.receipt.RecordVerifier;
import ai.pipestream.proto.receipt.WorkRecord;
import ai.pipestream.proto.receipt.WorkRecords;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The delegation projector: a task's transcript projects into a manifest that
 * signs and verifies like any other work record, under its own subject kind
 * and evidence policy. The strongest pin is the round trip — project, sign,
 * verify against a trust snapshot authorizing 'delegation-task' — because
 * that is the path a relying party actually walks.
 */
class DelegationRecordProjectorTest {

    private static final String TASK_ID = "6f9619ff-8b86-4d01-b42d-00cf4fc964ff";
    private static final String ISSUER = "records.protomolt.dev";
    private static final String KEY_ID = "key-delegation-test";
    private static final Timestamp ISSUED_AT = Timestamps.fromSeconds(1_750_000_000);

    private static final KeyPair KEYS = RecordKeys.generate();

    @Test
    void anAcceptedTaskProjectsSignsAndVerifiesAsACompleteRecord() {
        List<TranscriptEntry> entries = acceptedLifecycle();
        WorkRecord manifest = DelegationRecordProjector.project(TASK_ID, entries, issuance());

        assertThat(manifest.getSubject().getKind()).isEqualTo("delegation-task");
        assertThat(manifest.getSubject().getTaskId()).isEqualTo(TASK_ID);
        assertThat(manifest.getSubject().getWorkerId()).isEqualTo("kimi-worker");
        assertThat(manifest.getSubject().getSpecSha256())
                .isEqualTo(WorkRecords.fingerprint(spec()));
        assertThat(manifest.getCompleteness().getStatus())
                .isEqualTo(CompletenessStatus.COMPLETENESS_STATUS_COMPLETE);
        assertThat(manifest.getCompleteness().getPolicySha256())
                .isEqualTo(DelegationRecordProjector.policySha256());

        // The lifecycle milestones, with the recorded words as summaries.
        assertThat(manifest.getStepsList()).extracting(RecordStep::getName)
                .containsExactly("offer", "accept-attempt-1", "candidate-r1",
                        "revision-r1", "candidate-r2", "accepted");
        RecordStep revision = manifest.getSteps(3);
        assertThat(revision.getOutcome()).isEqualTo(StepOutcome.STEP_OUTCOME_FAILED);
        assertThat(revision.getSummary()).isEqualTo("tests cover the happy path only");
        assertThat(manifest.getSteps(5).getSummary())
                .isEqualTo("checks green and the diff is scoped");

        // The accepted candidate's artifact plus the transcript's own bytes.
        byte[] transcriptBytes = WorkRecords.deterministicBytes(
                Transcript.newBuilder().addAllEntries(entries).build());
        assertThat(manifest.getArtifactsList()).hasSize(2);
        assertThat(manifest.getArtifacts(0).getSha256()).isEqualTo("b".repeat(64));
        assertThat(manifest.getArtifacts(1).getSha256())
                .isEqualTo(WorkRecords.sha256Hex(transcriptBytes));
        assertThat(manifest.getArtifacts(1).getSizeBytes())
                .isEqualTo(transcriptBytes.length);

        // The round trip a relying party walks: sign, then verify offline.
        byte[] record = new RecordSigner(KEY_ID, KEYS.getPrivate())
                .sign(manifest).toByteArray();
        Verification verification = RecordVerifier.verify(record, trust());
        assertThat(verification.verified())
                .as(verification.checks().toString())
                .isTrue();
    }

    @Test
    void aCancelledTaskProjectsAsPartialWithItsReason() {
        List<TranscriptEntry> entries = List.of(
                coordinator(DelegateResponse.newBuilder()
                        .setOffer(TaskOffer.newBuilder().setSpec(spec()))),
                worker(DelegateRequest.newBuilder()
                        .setAccept(TaskAccept.newBuilder().setAttempt(1))),
                coordinator(DelegateResponse.newBuilder()
                        .setCancellation(Cancellation.newBuilder()
                                .setReason("superseded by a rescoped offer"))));
        WorkRecord manifest = DelegationRecordProjector.project(TASK_ID, entries, issuance());

        assertThat(manifest.getCompleteness().getStatus())
                .isEqualTo(CompletenessStatus.COMPLETENESS_STATUS_PARTIAL);
        assertThat(manifest.getCompleteness().getMissingReasonsList())
                .containsExactly("the task was cancelled before acceptance");
        assertThat(manifest.getStepsList()).extracting(RecordStep::getName)
                .containsExactly("offer", "accept-attempt-1", "cancelled");
        assertThat(manifest.getSteps(2).getSummary())
                .isEqualTo("superseded by a rescoped offer");
    }

    @Test
    void aTaskStillInFlightIsRefusedNotRecorded() {
        List<TranscriptEntry> inFlight = List.of(
                coordinator(DelegateResponse.newBuilder()
                        .setOffer(TaskOffer.newBuilder().setSpec(spec()))),
                worker(DelegateRequest.newBuilder()
                        .setAccept(TaskAccept.newBuilder().setAttempt(1))));
        assertThatThrownBy(() ->
                DelegationRecordProjector.project(TASK_ID, inFlight, issuance()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("still in flight");

        assertThatThrownBy(() ->
                DelegationRecordProjector.project(TASK_ID, List.of(), issuance()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no recorded offer");
    }

    private static DelegationRecordProjector.Issuance issuance() {
        return new DelegationRecordProjector.Issuance(
                "record-" + TASK_ID, ISSUER, KEY_ID, ISSUED_AT, "");
    }

    private static TrustSnapshot trust() {
        return TrustSnapshot.newBuilder()
                .addIssuers(TrustedIssuer.newBuilder()
                        .setIssuer(ISSUER)
                        .addKeys(TrustedKey.newBuilder()
                                .setKeyId(KEY_ID)
                                .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                                .setPublicKey(ByteString.copyFrom(
                                        RecordKeys.rawPublicKey(KEYS.getPublic())))
                                .setState(KeyState.KEY_STATE_ACTIVE))
                        .addSubjectKinds("delegation-task"))
                .build();
    }

    private static TaskSpec spec() {
        return TaskSpec.newBuilder()
                .setObjective("Prove the delegation receipt")
                .addRequiredChecks(AcceptanceCheck.newBuilder()
                        .setName("unit-tests")
                        .setDescription("focused tests pass"))
                .build();
    }

    private static List<TranscriptEntry> acceptedLifecycle() {
        return List.of(
                coordinator(DelegateResponse.newBuilder()
                        .setOffer(TaskOffer.newBuilder().setSpec(spec()))),
                worker(DelegateRequest.newBuilder()
                        .setAccept(TaskAccept.newBuilder().setAttempt(1))),
                worker(DelegateRequest.newBuilder().setCompletion(candidate(1))),
                coordinator(DelegateResponse.newBuilder()
                        .setRevisionRequested(RevisionRequested.newBuilder()
                                .setRevision(1)
                                .setFeedback("tests cover the happy path only"))),
                worker(DelegateRequest.newBuilder().setCompletion(candidate(2))),
                coordinator(DelegateResponse.newBuilder()
                        .setAccepted(CompletionAccepted.newBuilder()
                                .setRevision(2)
                                .setVerdict("checks green and the diff is scoped"))));
    }

    private static CompletionCandidate candidate(int revision) {
        return CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(revision)
                .setSummary("revision " + revision)
                .addEvidence(CheckEvidence.newBuilder()
                        .setCheckName("unit-tests")
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamps.fromSeconds(1_749_999_000)))
                .addArtifacts(ArtifactReference.newBuilder()
                        .setSha256("b".repeat(64))
                        .setMediaType("text/x-diff")
                        .setSizeBytes(512))
                .build();
    }

    private static TranscriptEntry coordinator(DelegateResponse.Builder frame) {
        return TranscriptEntry.newBuilder()
                .setLane(Lane.LANE_COORDINATOR)
                .setWorkerId("kimi-worker")
                .setCoordinatorFrame(frame)
                .build();
    }

    private static TranscriptEntry worker(DelegateRequest.Builder frame) {
        return TranscriptEntry.newBuilder()
                .setLane(Lane.LANE_WORKER)
                .setWorkerId("kimi-worker")
                .setWorkerFrame(frame)
                .build();
    }
}
