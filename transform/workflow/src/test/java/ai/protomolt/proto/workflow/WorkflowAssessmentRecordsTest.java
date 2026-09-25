package ai.protomolt.proto.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.AssessmentDisposition;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowAssessment;
import ai.protomolt.proto.receipt.KeyState;
import ai.protomolt.proto.receipt.RecordKeys;
import ai.protomolt.proto.receipt.RecordSigner;
import ai.protomolt.proto.receipt.SignedWorkRecord;
import ai.protomolt.proto.receipt.SignatureAlgorithm;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.TrustedIssuer;
import ai.protomolt.proto.receipt.TrustedKey;
import ai.protomolt.proto.receipt.WorkRecord;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.inference.v1.ChoiceCriteria;
import ai.protomolt.proto.inference.v1.EvaluateRequest;
import ai.protomolt.proto.inference.v1.EvaluationBinding;
import ai.protomolt.proto.inference.v1.EvaluationQuestion;
import ai.protomolt.proto.inference.v1.EvaluationRubric;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.Timestamp;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkflowAssessmentRecordsTest {
    private static final byte[] SEED = HexFormat.of().parseHex(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
    private static final byte[] PUBLIC_KEY = HexFormat.of().parseHex(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
    private static final String ISSUER = "records.protomolt.dev";
    private static final String KEY = "key-2026";

    @TempDir Path temp;

    @Test
    void aFailedRunHasASeparatePartialAssessmentThatVerifiesOffline() throws Exception {
        FileSystemArtifactRepository artifacts = new FileSystemArtifactRepository(temp);
        var input = artifacts.save("input".getBytes(), "application/x-protobuf", true);
        var policy = artifacts.save("policy".getBytes(), "text/plain", false);
        RunEvidence run = RunEvidence.newBuilder().setRunId("run-1")
                .setWorkflowName("contact").setWorkflowFingerprint(digest("workflow"))
                .setStatus(RunStatus.RUN_STATUS_FAILED)
                .setStartedAt(Timestamp.newBuilder().setSeconds(100))
                .setCompletedAt(Timestamp.newBuilder().setSeconds(101))
                .addDependencies(ServiceDependency.newBuilder().setAlias("evaluator")
                        .setServiceProfile("evaluator").setEndpoint("local")
                        .setDescriptorFingerprint(digest("descriptor")))
                .setInputArtifact(input).setFailureSummary("generation failed").build();
        var runRef = artifacts.save(run.toByteArray(), "application/x-protobuf", false);
        RecordSigner signer = new RecordSigner(KEY, RecordKeys.privateKey(SEED));
        byte[] execution = signer.sign(WorkRecordProjector.project(run,
                issuance("execution"))).toByteArray();
        var executionRef = artifacts.save(execution, "application/x-protobuf", false);
        WorkflowAssessment assessment = WorkflowAssessment.newBuilder()
                .setRunId(run.getRunId()).setRunEvidenceSha256(runRef.getSha256())
                .setExecutionRecordSha256(executionRef.getSha256())
                .setInputSha256(input.getSha256()).setPolicySha256(policy.getSha256())
                .setDisposition(AssessmentDisposition.ASSESSMENT_DISPOSITION_FAILED)
                .setReason("generation-failed").build();

        WorkRecord manifest = WorkflowAssessmentRecords.project(run, assessment,
                artifacts, issuance("assessment"));
        assertThat(manifest.getStepsCount()).isZero();
        assertThat(manifest.getCompleteness().getPolicyId())
                .isEqualTo(WorkflowAssessmentRecords.POLICY_ID);
        assertThat(manifest.getCompleteness().getStatus().name()).contains("PARTIAL");
        byte[] signed = signer.sign(manifest).toByteArray();
        assertThat(WorkflowAssessmentRecords.verify(signed, trust(), run,
                assessment, artifacts).ok()).isTrue();

        byte[] badSignature = SignedWorkRecord.parseFrom(signed).toBuilder()
                .setSignatures(0, SignedWorkRecord.parseFrom(signed).getSignatures(0)
                        .toBuilder().setSignature(ByteString.copyFrom(new byte[64])))
                .build().toByteArray();
        ArtifactRepository unreadable = new ArtifactRepository() {
            @Override
            public ArtifactReference save(byte[] content, String mediaType, boolean redacted) {
                throw new AssertionError("untrusted record must not write artifacts");
            }

            @Override
            public Optional<StoredArtifact> find(String digest) {
                throw new AssertionError("untrusted record must not read artifacts");
            }
        };
        assertThat(WorkflowAssessmentRecords.verify(badSignature, trust(), run,
                assessment, unreadable).ok()).isFalse();

        Files.writeString(temp.resolve(policy.getSha256()), "changed");
        assertThat(WorkflowAssessmentRecords.verify(signed, trust(), run,
                assessment, artifacts).ok()).isFalse();
    }

    @Test
    void requestWithoutResponseStillNeedsAValidEnvelope() throws Exception {
        FileSystemArtifactRepository artifacts = new FileSystemArtifactRepository(temp);
        var input = artifacts.save("input".getBytes(), "application/x-protobuf", true);
        var output = artifacts.save("output".getBytes(), "application/x-protobuf", true);
        var policy = artifacts.save("policy".getBytes(), "text/plain", false);
        var descriptor = artifacts.save("descriptor".getBytes(), "application/x-protobuf", false);
        var configuration = artifacts.save("configuration".getBytes(),
                "application/octet-stream", false);
        var evidence = Any.pack(StringValue.of("evidence"));
        var evidenceRef = artifacts.save(evidence.toByteArray(), "application/x-protobuf", false);
        var rubric = EvaluationRubric.newBuilder().addQuestions(EvaluationQuestion.newBuilder()
                .setId("support").setInstruction("Supported?")
                .setChoice(ChoiceCriteria.newBuilder().addOptions("yes").addOptions("no")))
                .build();
        var rubricRef = artifacts.save(rubric.toByteArray(), "application/x-protobuf", false);
        RunEvidence run = RunEvidence.newBuilder().setRunId("run-1")
                .setWorkflowName("contact").setWorkflowFingerprint(digest("workflow"))
                .setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setStartedAt(Timestamp.newBuilder().setSeconds(100))
                .setCompletedAt(Timestamp.newBuilder().setSeconds(101))
                .addDependencies(ServiceDependency.newBuilder().setAlias("evaluator")
                        .setServiceProfile("evaluator").setEndpoint("local")
                        .setDescriptorFingerprint(digest("descriptor")))
                .setInputArtifact(input).setOutputArtifact(output).build();
        var runRef = artifacts.save(run.toByteArray(), "application/x-protobuf", false);
        byte[] execution = new RecordSigner(KEY, RecordKeys.privateKey(SEED))
                .sign(WorkRecordProjector.project(run, issuance("execution"))).toByteArray();
        var executionRef = artifacts.save(execution, "application/x-protobuf", false);
        var binding = EvaluationBinding.newBuilder().setRunId(run.getRunId())
                .setResultSha256(output.getSha256()).setDescriptorSha256(descriptor.getSha256())
                .setResultType("example.Result").setEvidenceSha256(evidenceRef.getSha256())
                .setProjectionSha256(descriptor.getSha256())
                .setRubricSha256(rubricRef.getSha256()).setPolicySha256(policy.getSha256());
        var request = EvaluateRequest.newBuilder()
                .setRequestId("123e4567-e89b-12d3-a456-426614174000")
                .setBinding(binding).setEvidence(evidence).setRubric(rubric)
                .setEvaluatorProfile("local.default").build();
        var assessment = WorkflowAssessment.newBuilder().setRunId(run.getRunId())
                .setRunEvidenceSha256(runRef.getSha256())
                .setExecutionRecordSha256(executionRef.getSha256())
                .setInputSha256(input.getSha256()).setPolicySha256(policy.getSha256())
                .setEvaluatorConfigurationSha256(configuration.getSha256())
                .setDisposition(AssessmentDisposition.ASSESSMENT_DISPOSITION_FAILED)
                .setReason("evaluator-unavailable").setRequest(request);
        assertThat(WorkflowAssessmentRecords.project(run, assessment.build(), artifacts,
                issuance("assessment"))).isNotNull();
        var unknown = UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        assertThatThrownBy(() -> WorkflowAssessmentRecords.project(run,
                assessment.setRequest(request.toBuilder().setUnknownFields(unknown)).build(),
                artifacts, issuance("assessment")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("request contains unknown fields");
    }

    @Test
    void aFailedRunCannotClaimAcceptance() throws Exception {
        FileSystemArtifactRepository artifacts = new FileSystemArtifactRepository(temp);
        var input = artifacts.save("input".getBytes(), "application/x-protobuf", true);
        var policy = artifacts.save("policy".getBytes(), "text/plain", false);
        RunEvidence run = RunEvidence.newBuilder().setRunId("run-1")
                .setWorkflowName("contact").setWorkflowFingerprint(digest("workflow"))
                .setStatus(RunStatus.RUN_STATUS_FAILED)
                .setStartedAt(Timestamp.newBuilder().setSeconds(100))
                .setCompletedAt(Timestamp.newBuilder().setSeconds(101))
                .addDependencies(ServiceDependency.newBuilder().setAlias("evaluator")
                        .setServiceProfile("evaluator").setEndpoint("local")
                        .setDescriptorFingerprint(digest("descriptor")))
                .setInputArtifact(input).build();
        var runRef = artifacts.save(run.toByteArray(), "application/x-protobuf", false);
        byte[] execution = new RecordSigner(KEY, RecordKeys.privateKey(SEED))
                .sign(WorkRecordProjector.project(run, issuance("execution"))).toByteArray();
        var executionRef = artifacts.save(execution, "application/x-protobuf", false);
        WorkflowAssessment assessment = WorkflowAssessment.newBuilder()
                .setRunId(run.getRunId()).setRunEvidenceSha256(runRef.getSha256())
                .setExecutionRecordSha256(executionRef.getSha256())
                .setInputSha256(input.getSha256()).setPolicySha256(policy.getSha256())
                .setDisposition(AssessmentDisposition.ASSESSMENT_DISPOSITION_ACCEPTED)
                .setReason("accepted").build();
        assertThatThrownBy(() -> WorkflowAssessmentRecords.project(run, assessment,
                artifacts, issuance("assessment"))).isInstanceOf(IllegalArgumentException.class);
    }

    private static String digest(String value) {
        return WorkRecords.sha256Hex(value.getBytes());
    }

    private static WorkRecordProjector.Issuance issuance(String id) {
        return new WorkRecordProjector.Issuance(id, ISSUER, KEY,
                Timestamp.newBuilder().setSeconds(1750000000).build(), "");
    }

    private static TrustSnapshot trust() {
        return TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder()
                .setIssuer(ISSUER).addKeys(TrustedKey.newBuilder().setKeyId(KEY)
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setPublicKey(ByteString.copyFrom(PUBLIC_KEY))
                        .setState(KeyState.KEY_STATE_ACTIVE))
                .addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)).build();
    }
}
