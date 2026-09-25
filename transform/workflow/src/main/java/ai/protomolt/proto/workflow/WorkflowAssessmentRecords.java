package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.AssessmentDisposition;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowAssessment;
import ai.protomolt.proto.inference.service.EvaluationResponses;
import ai.protomolt.proto.inference.v1.EvaluateRequest;
import ai.protomolt.proto.inference.v1.EvaluationBinding;
import ai.protomolt.proto.receipt.Completeness;
import ai.protomolt.proto.receipt.CompletenessStatus;
import ai.protomolt.proto.receipt.RecordArtifact;
import ai.protomolt.proto.receipt.RecordVerifier;
import ai.protomolt.proto.receipt.SignedWorkRecord;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.Verification;
import ai.protomolt.proto.receipt.WorkRecord;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A second, signed v1 record for a policy assessment of an immutable workflow run. */
public final class WorkflowAssessmentRecords {
    public static final String POLICY_ID = "workflow-assessment-evidence";
    public static final String POLICY_VERSION = "1";
    public static final String POLICY = """
            A complete workflow-assessment record includes the typed policy assessment, \
            exact stored run evidence, original signed execution record, run input and \
            output, and referenced evaluation artifacts: result, descriptor closure, \
            evidence, projection, rubric, policy, and evaluator configuration. The \
            response must match the request, and the run must have succeeded. Artifact \
            inclusion does not establish that a supplied descriptor, projection, rubric, \
            or policy was authoritative for the run; the caller checks that separately. A failed \
            or unevaluated run is partial. REVIEW is a recorded disposition, not \
            affirmative acceptance. The signature is the issuer's claim, not proof \
            that a model judgment or local policy decision is correct.""";

    private static final String ASSESSMENT_MEDIA_TYPE = "application/x-protobuf";

    private WorkflowAssessmentRecords() {
    }

    public static String policySha256() {
        return WorkRecords.sha256Hex(POLICY.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Projects an assessment into a distinct-policy v1 manifest. The caller signs it with
     * {@code RecordSigner}; this method has no key access. Every digest must resolve to
     * immutable, rehashed bytes before a manifest is issued.
     */
    public static WorkRecord project(RunEvidence run, WorkflowAssessment assessment,
                                     ArtifactRepository artifacts,
                                     WorkRecordProjector.Issuance issuance) throws IOException {
        Objects.requireNonNull(issuance, "issuance");
        validate(run, assessment, artifacts);
        byte[] assessmentBytes = assessment.toByteArray();
        if (assessmentBytes.length > 1024 * 1024) {
            throw new IllegalArgumentException("assessment exceeds 1 MiB");
        }
        ArtifactReference stored = artifacts.save(assessmentBytes, ASSESSMENT_MEDIA_TYPE, false);
        if (!stored.getSha256().equals(WorkRecords.sha256Hex(assessmentBytes))
                || !stored.equals(required(artifacts, stored.getSha256()).reference())
                || !Arrays.equals(assessmentBytes,
                        required(artifacts, stored.getSha256()).content())) {
            throw new IOException("stored assessment differs from its submitted bytes");
        }
        return projected(run, assessment, artifacts, issuance, stored);
    }

    /**
     * Verifies both signed records, all artifact bytes, and an exact reprojection. The
     * caller supplies the run and assessment; this checks their exact stored bytes and
     * internal correlation. It does not establish that a supplied schema or local
     * policy was authoritative, or that a model judgment was truthful.
     */
    public static Check verify(byte[] signedAssessmentRecord, TrustSnapshot trust,
                               RunEvidence run, WorkflowAssessment assessment,
                               ArtifactRepository artifacts) throws IOException {
        Objects.requireNonNull(signedAssessmentRecord, "signedAssessmentRecord");
        Objects.requireNonNull(trust, "trust");
        if (signedAssessmentRecord.length == 0
                || signedAssessmentRecord.length > WorkRecords.MAX_RECORD_BYTES) {
            return new Check(false, "assessment record exceeds v1 bounds");
        }
        try {
            // Authenticate the bounded manifest before following any of its artifact
            // references. This first pass explicitly makes no artifact-custody claim.
            Verification authenticated = RecordVerifier.verify(signedAssessmentRecord,
                    trust, null);
            if (!authenticated.verified()) {
                return new Check(false, "assessment record: "
                        + authenticated.refusal().detail());
            }
            validate(run, assessment, artifacts);
            WorkRecord manifest = authenticated.manifest();
            Map<String, byte[]> bytes = artifactBytes(manifest, artifacts);
            Verification verified = RecordVerifier.verify(signedAssessmentRecord, trust, bytes);
            if (!verified.verified()) {
                return new Check(false, "assessment record: " + verified.refusal().detail());
            }
            String assessmentDigest = WorkRecords.sha256Hex(assessment.toByteArray());
            ArtifactReference stored = required(artifacts, assessmentDigest).reference();
            if (!Arrays.equals(required(artifacts, assessmentDigest).content(),
                    assessment.toByteArray())) {
                return new Check(false, "stored assessment is not the supplied assessment");
            }
            WorkRecord expected = projected(run, assessment, artifacts,
                    new WorkRecordProjector.Issuance(manifest.getRecordId(),
                            manifest.getIssuer(), manifest.getKeyId(), manifest.getIssuedAt(),
                            manifest.getPriorManifestSha256()), stored);
            if (!Arrays.equals(WorkRecords.canonicalBytes(manifest),
                    WorkRecords.canonicalBytes(expected))) {
                return new Check(false, "assessment record differs from exact reprojection");
            }
            byte[] executionBytes = required(artifacts,
                    assessment.getExecutionRecordSha256()).content();
            Verification authenticatedExecution = RecordVerifier.verify(executionBytes,
                    trust, null);
            if (!authenticatedExecution.verified()) {
                return new Check(false, "execution record: "
                        + authenticatedExecution.refusal().detail());
            }
            WorkRecord originalExecution = authenticatedExecution.manifest();
            WorkRecord execution = WorkRecordProjector.project(run,
                    issuance(originalExecution));
            Verification executionVerified = RecordVerifier.verify(executionBytes, trust,
                    artifactBytes(execution, artifacts));
            if (!executionVerified.verified()) {
                return new Check(false, "execution record: " + executionVerified.refusal().detail());
            }
            if (!Arrays.equals(WorkRecords.canonicalBytes(execution),
                    WorkRecords.canonicalBytes(executionVerified.manifest()))) {
                return new Check(false, "execution record differs from the supplied run");
            }
            return new Check(true, "both signatures, artifacts, and projections verified");
        } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
            return new Check(false, e.getMessage());
        }
    }

    public record Check(boolean ok, String reason) { }

    private static WorkRecord projected(RunEvidence run, WorkflowAssessment assessment,
                                        ArtifactRepository artifacts,
                                        WorkRecordProjector.Issuance issuance,
                                        ArtifactReference assessmentRef) throws IOException {
        WorkRecord base = WorkRecordProjector.project(run, issuance);
        WorkRecord.Builder result = base.toBuilder().setCompleteness(completeness(run, assessment));
        result.addArtifacts(recordArtifact(assessmentRef));
        result.addArtifacts(recordArtifact(required(artifacts,
                assessment.getRunEvidenceSha256()).reference()));
        result.addArtifacts(recordArtifact(required(artifacts,
                assessment.getExecutionRecordSha256()).reference()));
        result.addArtifacts(recordArtifact(required(artifacts,
                assessment.getPolicySha256()).reference()));
        if (assessment.hasRequest()) {
            validateRequest(assessment.getRequest());
            EvaluationBinding binding = assessment.getRequest().getBinding();
            for (String digest : List.of(binding.getResultSha256(),
                    binding.getDescriptorSha256(), binding.getEvidenceSha256(),
                    binding.getProjectionSha256(), binding.getRubricSha256())) {
                result.addArtifacts(recordArtifact(required(artifacts, digest).reference()));
            }
            result.addArtifacts(recordArtifact(required(artifacts,
                    assessment.getEvaluatorConfigurationSha256()).reference()));
        }
        return result.build();
    }

    private static void validateRequest(EvaluateRequest request) {
        if (request.getSerializedSize() > 1024 * 1024) {
            throw new IllegalArgumentException("evaluation request exceeds 1 MiB");
        }
        checkUnknownFields(request, "request");
        ValidationResult result = ProtoValidator.forMessageType(request.getDescriptorForType())
                .validate(request);
        if (!result.valid()) {
            throw new IllegalArgumentException("evaluation request violates "
                    + result.violations().getFirst().path());
        }
    }

    private static void checkUnknownFields(Message message, String path) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException(path + " contains unknown fields");
        }
        for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            String child = path + "." + field.getKey().getName();
            if (field.getKey().isRepeated()) {
                int index = 0;
                for (Object item : (List<?>) field.getValue()) {
                    checkUnknownFields((Message) item, child + "[" + index++ + "]");
                }
            } else {
                checkUnknownFields((Message) field.getValue(), child);
            }
        }
    }

    private static Completeness completeness(RunEvidence run, WorkflowAssessment assessment) {
        Completeness.Builder completeness = Completeness.newBuilder()
                .setPolicyId(POLICY_ID).setPolicyVersion(POLICY_VERSION)
                .setPolicySha256(policySha256());
        if (run.getStatus() == RunStatus.RUN_STATUS_SUCCEEDED
                && assessment.hasRequest() && assessment.hasResponse()
                && assessment.getDisposition() != AssessmentDisposition.ASSESSMENT_DISPOSITION_FAILED) {
            return completeness.setStatus(CompletenessStatus.COMPLETENESS_STATUS_COMPLETE).build();
        }
        return completeness.setStatus(CompletenessStatus.COMPLETENESS_STATUS_PARTIAL)
                .addMissingReasons("run or evaluation did not complete: " + assessment.getReason())
                .build();
    }

    private static void validate(RunEvidence run, WorkflowAssessment assessment,
                                 ArtifactRepository artifacts) throws IOException {
        Objects.requireNonNull(run, "run");
        Objects.requireNonNull(assessment, "assessment");
        Objects.requireNonNull(artifacts, "artifacts");
        WorkflowValidation.validate(run);
        ValidationResult annotations = ProtoValidator.forMessageType(
                assessment.getDescriptorForType()).validate(assessment);
        if (!annotations.valid()) {
            throw new IllegalArgumentException("assessment violates "
                    + annotations.violations().getFirst().path());
        }
        if (!assessment.getRunId().equals(run.getRunId())) {
            throw new IllegalArgumentException("assessment run_id differs from run");
        }
        if (!Arrays.equals(required(artifacts, assessment.getRunEvidenceSha256()).content(),
                run.toByteArray())) {
            throw new IllegalArgumentException("stored run evidence differs from supplied run");
        }
        if (!run.hasInputArtifact() || !assessment.getInputSha256().equals(
                run.getInputArtifact().getSha256())) {
            throw new IllegalArgumentException("assessment input differs from run input");
        }
        requireReference(artifacts, run.getInputArtifact());
        if (run.hasOutputArtifact()) {
            requireReference(artifacts, run.getOutputArtifact());
        }
        for (var step : run.getStepsList()) {
            if (step.hasRequestArtifact()) requireReference(artifacts, step.getRequestArtifact());
            if (step.hasResponseArtifact()) requireReference(artifacts, step.getResponseArtifact());
        }
        WorkRecord execution = executionManifest(required(artifacts,
                assessment.getExecutionRecordSha256()).content());
        if (!Arrays.equals(WorkRecords.canonicalBytes(execution),
                WorkRecords.canonicalBytes(WorkRecordProjector.project(run, issuance(execution))))) {
            throw new IllegalArgumentException("execution record is not the projection of this run");
        }
        required(artifacts, assessment.getPolicySha256());
        if (assessment.hasRequest()) {
            EvaluationBinding binding = assessment.getRequest().getBinding();
            if (!binding.hasRunId() || !binding.getRunId().equals(run.getRunId())
                    || !binding.getResultSha256().equals(run.getOutputArtifact().getSha256())
                    || !binding.getPolicySha256().equals(assessment.getPolicySha256())) {
                throw new IllegalArgumentException("evaluation binding differs from run or policy");
            }
            for (String digest : List.of(binding.getDescriptorSha256(), binding.getEvidenceSha256(),
                    binding.getProjectionSha256(), binding.getRubricSha256(),
                    assessment.getEvaluatorConfigurationSha256())) {
                required(artifacts, digest);
            }
            if (!Arrays.equals(required(artifacts, binding.getEvidenceSha256()).content(),
                    assessment.getRequest().getEvidence().toByteArray())
                    || !Arrays.equals(required(artifacts, binding.getRubricSha256()).content(),
                            assessment.getRequest().getRubric().toByteArray())) {
                throw new IllegalArgumentException("evaluation evidence or rubric differs from stored bytes");
            }
        } else if (!assessment.getEvaluatorConfigurationSha256().isEmpty()) {
            throw new IllegalArgumentException("evaluator configuration without request");
        }
        if (assessment.hasResponse()) {
            EvaluationResponses.validate(assessment.getRequest(), assessment.getResponse());
        }
        if (assessment.getDisposition() == AssessmentDisposition.ASSESSMENT_DISPOSITION_ACCEPTED
                && (run.getStatus() != RunStatus.RUN_STATUS_SUCCEEDED
                    || !run.hasOutputArtifact() || !assessment.hasResponse())) {
            throw new IllegalArgumentException("acceptance requires a successful run and evaluation");
        }
        if (run.getStatus() != RunStatus.RUN_STATUS_SUCCEEDED
                && assessment.getDisposition() != AssessmentDisposition.ASSESSMENT_DISPOSITION_FAILED) {
            throw new IllegalArgumentException("failed run requires FAILED assessment");
        }
    }

    private static WorkRecord executionManifest(byte[] bytes) {
        if (bytes.length == 0 || bytes.length > WorkRecords.MAX_RECORD_BYTES) {
            throw new IllegalArgumentException("execution record exceeds v1 bounds");
        }
        try {
            return WorkRecord.parseFrom(SignedWorkRecord.parseFrom(bytes).getManifest());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("execution record is malformed", e);
        }
    }

    private static WorkRecordProjector.Issuance issuance(WorkRecord record) {
        return new WorkRecordProjector.Issuance(record.getRecordId(), record.getIssuer(),
                record.getKeyId(), record.getIssuedAt(), record.getPriorManifestSha256());
    }

    private static ArtifactRepository.StoredArtifact required(ArtifactRepository artifacts,
                                                              String digest) throws IOException {
        WorkflowValidation.validateFingerprint(digest, "artifact digest");
        ArtifactRepository.StoredArtifact artifact = artifacts.find(digest)
                .orElseThrow(() -> new IllegalArgumentException("missing artifact " + digest));
        if (!digest.equals(WorkRecords.sha256Hex(artifact.content()))) {
            throw new IllegalArgumentException("artifact digest differs from stored bytes");
        }
        return artifact;
    }

    private static void requireReference(ArtifactRepository artifacts, ArtifactReference reference)
            throws IOException {
        if (!required(artifacts, reference.getSha256()).reference().equals(reference)) {
            throw new IllegalArgumentException("stored artifact reference differs from run reference");
        }
    }

    private static RecordArtifact recordArtifact(ArtifactReference reference) {
        return RecordArtifact.newBuilder().setSha256(reference.getSha256())
                .setMediaType(reference.getMediaType()).setSizeBytes(reference.getSizeBytes())
                .setRedacted(reference.getRedacted()).build();
    }

    private static Map<String, byte[]> artifactBytes(WorkRecord record,
                                                      ArtifactRepository artifacts) throws IOException {
        Map<String, byte[]> bytes = new LinkedHashMap<>();
        List<RecordArtifact> references = new ArrayList<>(record.getArtifactsList());
        for (var step : record.getStepsList()) {
            if (step.hasRequestArtifact()) references.add(step.getRequestArtifact());
            if (step.hasResponseArtifact()) references.add(step.getResponseArtifact());
        }
        for (RecordArtifact reference : references) {
            ArtifactRepository.StoredArtifact stored = required(artifacts, reference.getSha256());
            if (!recordArtifact(stored.reference()).equals(reference)) {
                throw new IllegalArgumentException("record artifact reference differs from storage");
            }
            bytes.put(reference.getSha256(), stored.content());
        }
        return bytes;
    }
}
