package ai.pipestream.proto.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.grpc.workflow.v1.ArtifactReference;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.grpc.workflow.v1.RunStatus;
import ai.pipestream.proto.grpc.workflow.v1.StepEvidence;
import ai.pipestream.proto.grpc.workflow.v1.StepStatus;
import ai.pipestream.proto.grpc.workflow.v1.StructuredGenerationEvidence;
import ai.pipestream.proto.inference.v1.Usage;
import ai.pipestream.proto.receipt.CompletenessStatus;
import ai.pipestream.proto.receipt.StepOutcome;
import ai.pipestream.proto.receipt.WorkRecord;
import ai.pipestream.proto.receipt.WorkRecords;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

class WorkRecordProjectorTest {

    private static final WorkRecordProjector.Issuance ISSUANCE =
            new WorkRecordProjector.Issuance("record-run-1", "records.protomolt.dev",
                    "key-2026", Timestamp.newBuilder().setSeconds(1750000000).build(), "");

    static RunEvidence.Builder evidence() {
        return RunEvidence.newBuilder()
                .setRunId("run-1")
                .setWorkflowName("embed-pipeline")
                .setWorkflowVersion("v1")
                .setWorkflowFingerprint(WorkRecords.sha256Hex("workflow".getBytes()))
                .setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setInputArtifact(reference("input"))
                .setOutputArtifact(reference("output"))
                .addSteps(StepEvidence.newBuilder()
                        .setStepName("tokenize")
                        .setMethod("workbench.test.Tokenizer/Tokenize")
                        .setStatus(StepStatus.STEP_STATUS_SUCCEEDED)
                        .setStartedAt(Timestamp.newBuilder().setSeconds(1749999990).build())
                        .setCompletedAt(Timestamp.newBuilder().setSeconds(1749999991).build())
                        .setRequestArtifact(reference("request"))
                        .setResponseArtifact(reference("response"))
                        .setSummary("tokenized"))
                .addSteps(StepEvidence.newBuilder()
                        .setStepName("summarize")
                        .setStatus(StepStatus.STEP_STATUS_SUCCEEDED)
                        .setStructured(StructuredGenerationEvidence.newBuilder()
                                .setModel("claude-sonnet-5")
                                .setModelVersion("20260501")
                                .setPromptFingerprint(WorkRecords.sha256Hex("p".getBytes()))
                                .setSchemaFingerprint(WorkRecords.sha256Hex("s".getBytes()))
                                .setValidationPassed(true)
                                .setTotalUsage(Usage.newBuilder()
                                        .setPromptTokens(120).setCompletionTokens(40))));
    }

    private static ArtifactReference reference(String content) {
        return ArtifactReference.newBuilder()
                .setSha256(WorkRecords.sha256Hex(content.getBytes()))
                .setMediaType("application/x-protobuf")
                .setSizeBytes(content.length())
                .setRedacted(true)
                .build();
    }

    @Test
    void aSucceededRunProjectsComplete() {
        WorkRecord manifest = WorkRecordProjector.project(evidence().build(), ISSUANCE);

        assertThat(manifest.getManifestVersion()).isEqualTo(WorkRecords.MANIFEST_VERSION);
        assertThat(manifest.getSubject().getKind())
                .isEqualTo(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN);
        assertThat(manifest.getSubject().getWorkflowName()).isEqualTo("embed-pipeline");
        assertThat(manifest.getSubject().getRunId()).isEqualTo("run-1");
        assertThat(manifest.getCompleteness().getStatus())
                .isEqualTo(CompletenessStatus.COMPLETENESS_STATUS_COMPLETE);
        assertThat(manifest.getCompleteness().getMissingReasonsList()).isEmpty();
        assertThat(manifest.getCompleteness().getPolicyId())
                .isEqualTo(WorkRecordProjector.POLICY_ID);
        assertThat(manifest.getCompleteness().getPolicySha256())
                .isEqualTo(WorkRecordProjector.policySha256());

        assertThat(manifest.getStepsCount()).isEqualTo(2);
        assertThat(manifest.getSteps(0).getMethod())
                .isEqualTo("workbench.test.Tokenizer/Tokenize");
        assertThat(manifest.getSteps(0).getOutcome())
                .isEqualTo(StepOutcome.STEP_OUTCOME_SUCCEEDED);
        assertThat(manifest.getSteps(0).getRequestArtifact().getSha256())
                .isEqualTo(WorkRecords.sha256Hex("request".getBytes()));
        assertThat(manifest.getSteps(0).getStartedAt().getSeconds())
                .isEqualTo(1749999990);
        assertThat(manifest.getSteps(1).getPromptTokens()).isEqualTo(120);
        assertThat(manifest.getSteps(1).getCompletionTokens()).isEqualTo(40);
        assertThat(manifest.getSteps(1).getModel()).isEqualTo("claude-sonnet-5");
        assertThat(manifest.getSteps(1).getMethod()).isEmpty();

        assertThat(manifest.getArtifactsCount()).isEqualTo(2);
        assertThat(manifest.getArtifacts(0).getSha256())
                .isEqualTo(WorkRecords.sha256Hex("input".getBytes()));
        assertThat(manifest.getArtifacts(1).getRedacted()).isTrue();
    }

    @Test
    void aFailedRunProjectsPartialWithItsFailure() {
        WorkRecord manifest = WorkRecordProjector.project(evidence()
                .setStatus(RunStatus.RUN_STATUS_FAILED)
                .setFailureSummary("step 'summarize' timed out")
                .build(), ISSUANCE);
        assertThat(manifest.getCompleteness().getStatus())
                .isEqualTo(CompletenessStatus.COMPLETENESS_STATUS_PARTIAL);
        assertThat(manifest.getCompleteness().getMissingReasons(0))
                .isEqualTo("the run failed: step 'summarize' timed out");
    }

    @Test
    void aFailedRunWithoutASummaryStillSaysWhy() {
        WorkRecord manifest = WorkRecordProjector.project(evidence()
                .setStatus(RunStatus.RUN_STATUS_FAILED)
                .build(), ISSUANCE);
        assertThat(manifest.getCompleteness().getMissingReasons(0))
                .isEqualTo("the run failed before completing");
    }

    @Test
    void aCancelledRunProjectsPartial() {
        WorkRecord manifest = WorkRecordProjector.project(evidence()
                .setStatus(RunStatus.RUN_STATUS_CANCELLED)
                .build(), ISSUANCE);
        assertThat(manifest.getCompleteness().getStatus())
                .isEqualTo(CompletenessStatus.COMPLETENESS_STATUS_PARTIAL);
        assertThat(manifest.getCompleteness().getMissingReasons(0))
                .isEqualTo("the run was cancelled before completing");
    }

    @Test
    void aLiveRunRefusesToProject() {
        assertThatThrownBy(() -> WorkRecordProjector.project(evidence()
                .setStatus(RunStatus.RUN_STATUS_RUNNING).build(), ISSUANCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only terminal run evidence");
    }

    @Test
    void everyTerminalStepStatusProjectsToItsOwnOutcome() {
        WorkRecord manifest = WorkRecordProjector.project(evidence()
                .setStatus(RunStatus.RUN_STATUS_FAILED)
                .setFailureSummary("step 'tokenize' failed")
                .setSteps(0, evidence().getSteps(0).toBuilder()
                        .setStatus(StepStatus.STEP_STATUS_FAILED))
                .setSteps(1, evidence().getSteps(1).toBuilder()
                        .setStatus(StepStatus.STEP_STATUS_SKIPPED))
                .addSteps(evidence().getSteps(1).toBuilder()
                        .setStepName("late")
                        .setStatus(StepStatus.STEP_STATUS_CANCELLED))
                .build(), ISSUANCE);
        assertThat(manifest.getSteps(0).getOutcome())
                .isEqualTo(StepOutcome.STEP_OUTCOME_FAILED);
        assertThat(manifest.getSteps(1).getOutcome())
                .isEqualTo(StepOutcome.STEP_OUTCOME_SKIPPED);
        assertThat(manifest.getSteps(2).getOutcome())
                .isEqualTo(StepOutcome.STEP_OUTCOME_CANCELLED);
    }

    @Test
    void aStepWithoutARecordedStatusRefuses() {
        assertThatThrownBy(() -> WorkRecordProjector.project(evidence()
                .setSteps(0, evidence().getSteps(0).toBuilder()
                        .setStatus(StepStatus.STEP_STATUS_UNSPECIFIED))
                .build(), ISSUANCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tokenize");
    }

    @Test
    void anOverlongSummaryIsBoundedWithoutSplittingASurrogatePair() {
        String summary = "a".repeat(4095) + "😀";
        WorkRecord manifest = WorkRecordProjector.project(evidence()
                .setSteps(0, evidence().getSteps(0).toBuilder().setSummary(summary))
                .build(), ISSUANCE);
        assertThat(manifest.getSteps(0).getSummary()).isEqualTo("a".repeat(4095));
    }

    @Test
    void thePriorLinkRidesTheIssuance() {
        String prior = WorkRecords.sha256Hex("prior".getBytes());
        WorkRecord manifest = WorkRecordProjector.project(evidence().build(),
                new WorkRecordProjector.Issuance("record-run-1b", "records.protomolt.dev",
                        "key-2026", Timestamp.newBuilder().setSeconds(1750000001).build(),
                        prior));
        assertThat(manifest.getPriorManifestSha256()).isEqualTo(prior);
    }
}
