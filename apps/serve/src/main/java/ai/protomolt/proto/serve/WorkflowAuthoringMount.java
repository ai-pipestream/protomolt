package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.TrustSnapshots;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.WorkflowRunner;
import ai.protomolt.proto.workflow.authoring.FileSystemWorkflowLaunchAuthorizationRepository;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthoringLauncher;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthoringOperations;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthoringReviewer;
import ai.protomolt.proto.workflow.authoring.WorkflowLaunchAuthorizationRepository;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/** Trusted, opt-in dependencies shared by automatic review and launch. */
final class WorkflowAuthoringMount {
    private static final int MAX_POLICY_BYTES = 4 * 1024 * 1024;

    private WorkflowAuthoringMount() {}

    static Prepared prepare(ProtoMoltServe.WorkflowAuthoringOptions options,
            ArtifactRepository artifacts, RunEvidenceRepository runs,
            WorkflowVersionRepository versions, WorkflowRunStore jobs,
            ActionContext actions, WorkflowRunner runner, int maxAttempts,
            Supplier<TrustSnapshot> trust) {
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(artifacts, "artifacts");
        Objects.requireNonNull(runs, "runs");
        Objects.requireNonNull(versions, "versions");
        Objects.requireNonNull(jobs, "jobs");
        Objects.requireNonNull(actions, "actions");
        Objects.requireNonNull(runner, "runner");
        Objects.requireNonNull(trust, "trust");
        requireTrust(trust);
        try {
            var stored = artifacts.find(options.policySha256()).orElseThrow(() ->
                    new IllegalStateException("workflow authoring policy artifact is absent"));
            ArtifactReference reference = stored.reference();
            WorkflowValidation.validate(reference);
            byte[] bytes = stored.content();
            if (!reference.getSha256().equals(options.policySha256())
                    || reference.getRedacted()
                    || !"application/x-protobuf".equals(reference.getMediaType())
                    || Long.compareUnsigned(reference.getSizeBytes(), MAX_POLICY_BYTES) > 0
                    || bytes.length > MAX_POLICY_BYTES) {
                throw new IllegalStateException("workflow authoring policy artifact is invalid");
            }
            WorkflowAuthoringPolicy policy = WorkflowAuthoringPolicy.parseFrom(bytes);
            rejectUnknown(policy);
            var result = ProtoValidator.forMessageType(policy.getDescriptorForType()).validate(policy);
            if (!result.valid()) {
                throw new IllegalStateException("workflow authoring policy violates its contract");
            }
            var ledger = new FileSystemWorkflowLaunchAuthorizationRepository(
                    options.authorizationDirectory());
            return new Prepared(reference, artifacts, runs, versions, jobs, actions,
                    runner, maxAttempts, trust, ledger);
        } catch (IOException e) {
            throw new IllegalStateException("workflow authoring storage is unavailable", e);
        }
    }

    private static TrustSnapshot requireTrust(Supplier<TrustSnapshot> source) {
        try {
            return TrustSnapshots.requireWellFormed(source.get());
        } catch (IllegalArgumentException invalid) {
            throw new IllegalStateException("workflow authoring receipt trust is unavailable", invalid);
        }
    }

    private static void rejectUnknown(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalStateException("workflow authoring policy has unknown fields");
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) rejectUnknown((Message) nested);
            } else {
                rejectUnknown((Message) field.getValue());
            }
        }
    }

    record Prepared(ArtifactReference policyReference, ArtifactRepository artifacts,
            RunEvidenceRepository runs, WorkflowVersionRepository versions,
            WorkflowRunStore jobs, ActionContext actions, WorkflowRunner runner,
            int maxAttempts, Supplier<TrustSnapshot> trust,
            WorkflowLaunchAuthorizationRepository ledger) {

        private WorkflowAuthoringReviewer reviewer() {
            // Resolve once for this review or launch. A later operation sees trust rotation.
            return new WorkflowAuthoringReviewer(policyReference, artifacts, runs,
                    actions, runner, requireTrust(trust));
        }

        CandidateReviewer selectingReviewer() {
            return context -> {
                if (!context.spec().hasContract() || !context.spec().getContract().getTypeName()
                        .equals(WorkflowAuthoringDeliverable.getDescriptor().getFullName())) {
                    return CandidateReviewer.ReviewDecision.pending();
                }
                return reviewer().review(context);
            };
        }

        WorkflowAuthoringOperations operations(TranscriptRepository transcripts) {
            Objects.requireNonNull(transcripts, "transcripts");
            return new WorkflowAuthoringOperations() {
                private WorkflowAuthoringLauncher launcher() {
                    return new WorkflowAuthoringLauncher(transcripts, reviewer(), ledger, versions,
                            artifacts, jobs, actions, maxAttempts);
                }

                @Override
                public ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate
                        acceptedCandidate(String taskId) throws Exception {
                    return launcher().acceptedCandidate(taskId);
                }

                @Override
                public ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult
                        launch(ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest request)
                        throws Exception {
                    return launcher().launch(request);
                }
            };
        }
    }
}
