package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.contract.DeliverableContracts;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.WorkflowAuthoringFixtures;
import ai.protomolt.proto.workflow.WorkflowAuthoringPreflight;
import ai.protomolt.proto.workflow.WorkflowAuthoringReceipt;
import ai.protomolt.proto.workflow.WorkflowReplay;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Example automatic reviewer for the authoring template, not a mounted API. */
public final class WorkflowAuthoringReviewer implements CandidateReviewer {
    public static final List<String> REQUIRED_CHECKS = List.of(
            "artifact_and_inline_workflow_match", "required_checks_independently_verified",
            "receipt_names_same_run");

    private final ArtifactReference policyReference;
    private final ArtifactRepository artifacts;
    private final RunEvidenceRepository runs;
    private final ActionContext actions;
    private final WorkflowRunner runner;
    private final TrustSnapshot trust;

    /** The operator selects the policy; the immutable task offer must carry the same reference. */
    public WorkflowAuthoringReviewer(ArtifactReference policyReference, ArtifactRepository artifacts,
            RunEvidenceRepository runs, ActionContext actions, WorkflowRunner runner, TrustSnapshot trust) {
        this.policyReference = Objects.requireNonNull(policyReference);
        this.artifacts = Objects.requireNonNull(artifacts);
        this.runs = Objects.requireNonNull(runs);
        this.actions = Objects.requireNonNull(actions);
        this.runner = Objects.requireNonNull(runner);
        this.trust = Objects.requireNonNull(trust);
    }

    /**
     * Independently verifies the candidate. No promotion or job submission occurs here:
     * the coordinator must first apply this decision to the still-current attempt/revision.
     * Storage/network infrastructure exceptions propagate and cannot yield acceptance.
     */
    @Override
    public ReviewDecision review(ReviewContext context) throws Exception {
        try {
            var prepared = prepare(context);
            int observed = verifyFixtures(prepared);
            var candidate = context.candidate();
            return ReviewDecision.accept("Verified " + observed + " caller fixtures; task="
                    + context.taskId() + "; attempt=" + candidate.getAttempt() + "; revision=" + candidate.getRevision()
                    + "; candidate=" + WorkRecords.sha256Hex(candidate.toByteArray())
                    + "; offer=" + WorkRecords.sha256Hex(context.spec().toByteArray())
                    + "; policy=" + policyReference.getSha256() + "; run=" + prepared.runId()
                    + "; manifest=" + prepared.manifest());
        } catch (IllegalArgumentException invalid) {
            return revise(invalid);
        } catch (WorkflowRunner.WorkflowExecutionException failure) {
            // A transport/deadline problem is not evidence that the authored mapping is wrong.
            if (failure.kind() == WorkflowRunner.FailureKind.GRPC
                    || failure.kind() == WorkflowRunner.FailureKind.DEADLINE) throw failure;
            return revise(failure);
        }
    }

    private static ReviewDecision revise(Exception invalid) {
        String message = String.valueOf(invalid.getMessage());
        return ReviewDecision.revise(message.substring(0, Math.min(message.length(), 2048)), REQUIRED_CHECKS);
    }

    record PreparedReview(WorkflowAuthoringDeliverable authored, WorkflowAuthoringPolicy policy,
            ArtifactReference policyReference, WorkflowAuthoringPreflight.Result admitted,
            String runId, String manifest) {}

    // Read-only verification can be repeated after authorization without invoking
    // external fixtures again. Only the trusted launch ledger permits that recovery.
    PreparedReview prepare(ReviewContext context) throws Exception {
        var spec = context.spec();
        var candidate = context.candidate();
        validate(spec);
        validate(candidate);
        if (!spec.hasContract() || !spec.getContract().getTypeName().equals(
                WorkflowAuthoringDeliverable.getDescriptor().getFullName())) {
            throw new IllegalArgumentException("task must offer the authoring deliverable contract");
        }
        var violations = DeliverableContracts.check(spec.getContract(), candidate.getResult());
        if (!violations.isEmpty()) throw new IllegalArgumentException("deliverable contract: " + violations);
        var authored = candidate.getResult().unpack(WorkflowAuthoringDeliverable.class);
        validate(authored);
        if (!spec.getContextList().contains(policyReference)) {
            throw new IllegalArgumentException("task offer does not pin the configured caller policy");
        }
        if (policyReference.getRedacted() || !policyReference.getMediaType().equals("application/x-protobuf")) {
            throw new IllegalArgumentException("caller policy must be an unredacted protobuf artifact");
        }
        var policy = WorkflowAuthoringPolicy.parseFrom(resolve(policyReference));
        validate(policy);
        var required = new HashSet<String>();
        spec.getRequiredChecksList().forEach(check -> {
            if (!required.add(check.getName())) throw new IllegalArgumentException("duplicate required check");
        });
        if (!required.equals(Set.copyOf(REQUIRED_CHECKS))) {
            throw new IllegalArgumentException("unsupported or missing authoring acceptance checks");
        }
        var deliverable = authored.getDeliverable();
        Map<String, CheckEvidence> enclosing = checks(candidate.getEvidenceList());
        if (!enclosing.equals(checks(deliverable.getChecksList())) || !enclosing.keySet().equals(required)) {
            throw new IllegalArgumentException("candidate and deliverable checks must match the offered checks");
        }
        // Reports are retained and checked for custody, but their PASS text never establishes success.
        var reportedReferences = new HashSet<ArtifactReference>(candidate.getArtifactsList());
        enclosing.values().forEach(check -> reportedReferences.addAll(check.getArtifactsList()));
        long total = 0;
        for (var ref : reportedReferences) {
            if (Long.compareUnsigned(ref.getSizeBytes(), 4 * 1024 * 1024) > 0) {
                throw new IllegalArgumentException("reported artifact exceeds 4 MiB");
            }
            total += ref.getSizeBytes();
            if (total > 64L * 1024 * 1024) throw new IllegalArgumentException("reported artifacts exceed 64 MiB");
            resolve(ref);
        }
        if (!candidate.getArtifactsList().contains(deliverable.getWorkflowArtifact())
                || !candidate.getArtifactsList().contains(authored.getExecutableSource())
                || !candidate.getArtifactsList().contains(deliverable.getReceipt())) {
            throw new IllegalArgumentException("candidate must reference its workflow, source and receipt");
        }
        var admitted = WorkflowAuthoringPreflight.verify(deliverable.getWorkflow(),
                deliverable.getWorkflowArtifact(), authored.getExecutableSource(), deliverable.getDescriptors(),
                new WorkflowAuthoringPreflight.Policy(policy.getDescriptors(),
                        policy.getPermittedCallsList().stream().map(call ->
                                new WorkflowAuthoringPreflight.PermittedCall(call.getTarget(),
                                        call.getMethod(), call.getTls())).toList()), actions, artifacts);
        var run = runs.find(deliverable.getRunId()).orElseThrow(() ->
                new IllegalArgumentException("recorded run is absent from the coordinator repository"));
        if (!run.getRunId().equals(deliverable.getRunId())) {
            throw new IllegalArgumentException("run repository returned a different run identity");
        }
        String manifest = WorkflowAuthoringReceipt.verify(deliverable.getReceipt(), run,
                WorkflowValidation.fingerprint(deliverable.getWorkflow()), trust, artifacts);
        var runArtifacts = new HashSet<>(List.of(run.getInputArtifact(), run.getOutputArtifact()));
        run.getStepsList().forEach(step -> {
            if (step.hasRequestArtifact()) runArtifacts.add(step.getRequestArtifact());
            if (step.hasResponseArtifact()) runArtifacts.add(step.getResponseArtifact());
        });
        if (!deliverable.getFixturesList().containsAll(List.of(run.getInputArtifact(), run.getOutputArtifact()))
                || !runArtifacts.containsAll(deliverable.getFixturesList())) {
            throw new IllegalArgumentException("recorded fixture references do not match the stored run");
        }
        var replay = WorkflowReplay.replay(deliverable.getWorkflow(), run, admitted.workflow().files(), artifacts);
        if (!replay.ok()) throw new IllegalArgumentException("stored run replay failed: " + replay.failure());
        return new PreparedReview(authored, policy, policyReference, admitted, run.getRunId(), manifest);
    }

    int verifyFixtures(PreparedReview prepared) throws Exception {
        return WorkflowAuthoringFixtures.execute(prepared.admitted(),
                prepared.authored().getAcceptanceFixturesList().stream().map(WorkflowAuthoringReviewer::fixture).toList(),
                prepared.policy().getFixturesList().stream().map(WorkflowAuthoringReviewer::fixture).toList(),
                artifacts, runner).size();
    }

    private static WorkflowAuthoringFixtures.Fixture fixture(WorkflowAcceptanceFixture fixture) {
        return new WorkflowAuthoringFixtures.Fixture(fixture.getName(), fixture.getInput(), fixture.getExpectedOutput());
    }

    private static Map<String, CheckEvidence> checks(List<CheckEvidence> evidence) {
        var result = new HashMap<String, CheckEvidence>();
        for (var check : evidence) {
            if (check.getVerdict() != CheckVerdict.CHECK_VERDICT_PASSED
                    || result.putIfAbsent(check.getCheckName(), check) != null) {
                throw new IllegalArgumentException("checks must be unique and report PASS");
            }
        }
        return result;
    }

    private byte[] resolve(ArtifactReference ref) throws IOException {
        WorkflowValidation.validate(ref);
        if (Long.compareUnsigned(ref.getSizeBytes(), 4 * 1024 * 1024) > 0) {
            throw new IllegalArgumentException("artifact exceeds 4 MiB");
        }
        var stored = artifacts.find(ref.getSha256()).orElseThrow(() ->
                new IllegalArgumentException("missing artifact " + ref.getSha256()));
        if (!stored.reference().equals(ref)) throw new IllegalArgumentException("artifact reference differs from storage");
        return stored.content();
    }

    private static void validate(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("unknown fields in authoring contract");
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                if (field.getKey().isRepeated()) {
                    for (Object nested : (List<?>) field.getValue()) validate((Message) nested);
                } else validate((Message) field.getValue());
            }
        }
        var result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        if (!result.valid()) throw new IllegalArgumentException("invalid authoring contract: " + result.violations());
    }
}
