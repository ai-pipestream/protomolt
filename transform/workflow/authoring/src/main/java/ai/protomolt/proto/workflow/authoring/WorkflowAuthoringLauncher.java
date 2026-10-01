package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchAuthorization;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** Accepted-workflow launch binding; no public endpoint is mounted. */
public final class WorkflowAuthoringLauncher implements WorkflowAuthoringOperations {
    private final TranscriptRepository transcripts;
    private final WorkflowAuthoringReviewer reviewer;
    private final WorkflowLaunchAuthorizationRepository authorizations;
    private final WorkflowVersionRepository versions;
    private final ArtifactRepository artifacts;
    private final WorkflowRunStore jobs;
    private final WorkflowRunSubmitter submitter;
    private final ActionContext actions;

    public WorkflowAuthoringLauncher(TranscriptRepository transcripts, WorkflowAuthoringReviewer reviewer,
            WorkflowLaunchAuthorizationRepository authorizations, WorkflowVersionRepository versions,
            ArtifactRepository artifacts, WorkflowRunStore jobs, ActionContext actions, int maxAttempts) {
        this.transcripts = Objects.requireNonNull(transcripts);
        this.reviewer = Objects.requireNonNull(reviewer);
        this.authorizations = Objects.requireNonNull(authorizations);
        this.versions = Objects.requireNonNull(versions);
        this.artifacts = Objects.requireNonNull(artifacts);
        this.jobs = Objects.requireNonNull(jobs);
        this.actions = Objects.requireNonNull(actions);
        this.submitter = new WorkflowRunSubmitter(jobs, null, maxAttempts);
    }

    /** Discover the server-computed selector; this performs no fixture calls or launch effects. */
    public WorkflowAcceptedCandidate acceptedCandidate(String taskId) {
        return WorkflowLaunchAcceptance.inspect(transcripts, taskId).identity();
    }

    /**
     * Authorize once, promote exact bytes, and queue the checked source under a stable UUID.
     * A persisted authorization survives later failure; retry resumes without live fixtures.
     * The injected ledger and transcript repository are trusted coordinator storage.
     */
    public WorkflowAuthoringLaunchResult launch(WorkflowAuthoringLaunchRequest supplied) throws Exception {
        WorkflowLaunchValidation.validate(supplied);
        var request = supplied.toBuilder().setLaunchId(UUID.fromString(supplied.getLaunchId()).toString()).build();
        var existing = authorizations.find(request.getLaunchId());
        if (existing.isPresent() && !existing.get().getRequest().equals(request)) {
            throw new WorkflowLaunchConflictException("launch UUID already belongs to a different intent");
        }
        var accepted = WorkflowLaunchAcceptance.inspect(transcripts, request.getAcceptance().getTaskId());
        if (!accepted.identity().equals(request.getAcceptance())) {
            throw new IllegalArgumentException("requested acceptance differs from the durable candidate");
        }
        var prepared = reviewer.prepare(accepted.context());
        byte[] inputBytes = resolve(request.getInput());
        var input = DynamicMessage.parseFrom(prepared.admitted().workflow().inputType(), inputBytes);
        WorkflowLaunchValidation.validate(input);
        var source = (ObjectNode) actions.objectMapper().readTree(prepared.admitted().sourceJson());
        var inputJson = actions.objectMapper().readTree(actions.transcoder().toJson(input));
        WorkflowRunRecord expected = new WorkflowRunRecord();
        expected.workflowName = prepared.admitted().workflow().name();
        expected.workflowDefinition = source.toString();
        expected.input = inputJson.toString();
        var presentJob = jobs.get(UUID.fromString(request.getLaunchId()));
        if (presentJob.isPresent()) {
            // Another launcher may have completed between our first ledger lookup
            // and this read. A job without its prior authorization cannot be adopted.
            if (existing.isEmpty()) existing = authorizations.find(request.getLaunchId());
            if (existing.isEmpty() || !existing.get().getRequest().equals(request)
                    || !WorkflowRunStore.sameSubmission(expected, presentJob.get())) {
                throw new WorkflowLaunchConflictException("launch UUID belongs to an unbound or different job");
            }
        }

        WorkflowAuthoringLaunchAuthorization authorization;
        if (existing.isPresent()) {
            authorization = existing.get();
            WorkflowLaunchValidation.validate(authorization);
            if (!authorization.getPolicy().equals(prepared.policyReference())
                    || !Arrays.equals(authorization.getAuthored().toByteArray(), accepted.authored().toByteArray())) {
                throw new IOException("stored authorization differs from accepted policy or deliverable");
            }
            WorkflowValidation.validate(authorization.getPromoted());
            if (!Arrays.equals(authorization.getPromoted().getWorkflow().toByteArray(),
                    accepted.authored().getDeliverable().getWorkflow().toByteArray())
                    || !authorization.getPromoted().getCreatedAt().equals(accepted.acceptedAt())) {
                throw new IOException("stored promotion envelope differs from acceptance");
            }
        } else {
            reviewer.verifyFixtures(prepared);
            var workflow = accepted.authored().getDeliverable().getWorkflow();
            var promoted = VersionedWorkflow.newBuilder().setWorkflow(workflow)
                    .setWorkflowFingerprint(WorkflowValidation.fingerprint(workflow))
                    .setVersion("accepted-" + WorkflowLaunchValidation.sha256(accepted.identity()))
                    .setCreatedAt(accepted.acceptedAt()).build();
            WorkflowValidation.validate(promoted);
            authorization = WorkflowAuthoringLaunchAuthorization.newBuilder().setRequest(request)
                    .setPolicy(prepared.policyReference()).setAuthored(accepted.authored())
                    .setPromoted(promoted).build();
            WorkflowLaunchValidation.validate(authorization);
            // A lost acceptance or changed transcript cannot authorize effects after fixtures.
            if (!WorkflowLaunchAcceptance.inspect(transcripts, request.getAcceptance().getTaskId())
                    .identity().equals(accepted.identity())) {
                throw new IllegalArgumentException("durable acceptance changed during verification");
            }
            var stored = authorizations.createOrMatch(authorization);
            if (!Arrays.equals(WorkflowLaunchValidation.deterministicBytes(stored),
                    WorkflowLaunchValidation.deterministicBytes(authorization))) {
                throw new IOException("authorization repository returned a different binding");
            }
            authorization = stored;
        }

        // Persist evidence before launch effects. A failure here can retry from the ledger.
        byte[] authorizationBytes = WorkflowLaunchValidation.deterministicBytes(authorization);
        var reference = artifacts.save(authorizationBytes, "application/x-protobuf", false);
        if (!Arrays.equals(resolve(reference), authorizationBytes)) {
            throw new IOException("authorization evidence differs from the keyed record");
        }
        var result = WorkflowAuthoringLaunchResult.newBuilder().setJobId(request.getLaunchId())
                .setAuthorization(reference).build();
        WorkflowLaunchValidation.validate(result);
        versions.save(authorization.getPromoted());
        var promoted = versions.find(authorization.getPromoted().getWorkflow().getName(),
                authorization.getPromoted().getVersion()).orElseThrow(() -> new IOException("promoted version missing"));
        if (!Arrays.equals(promoted.toByteArray(), authorization.getPromoted().toByteArray())) {
            throw new IOException("stored workflow version differs from authorized promotion");
        }
        var outcome = submitter.submit(source, null, inputJson, request.getLaunchId(), actions);
        if (outcome.conflict()) {
            throw new WorkflowLaunchConflictException("launch UUID belongs to a different workflow submission");
        }
        if (!outcome.ok()) throw new IllegalArgumentException("workflow submission failed: " + outcome.error());
        var job = jobs.get(UUID.fromString(request.getLaunchId())).orElseThrow(() ->
                new IOException("submitted job is absent"));
        if (!job.jobId.equals(UUID.fromString(request.getLaunchId()))
                || !WorkflowRunStore.sameSubmission(expected, job)) {
            throw new IOException("stored job differs from authorized source or input");
        }
        return result;
    }

    private byte[] resolve(ArtifactReference reference) throws IOException {
        WorkflowValidation.validate(reference);
        if (Long.compareUnsigned(reference.getSizeBytes(), WorkflowLaunchValidation.MAX_BYTES) > 0) {
            throw new IllegalArgumentException("launch artifact exceeds 4 MiB");
        }
        var stored = artifacts.find(reference.getSha256()).orElseThrow(() -> new IOException("launch artifact missing"));
        if (!stored.reference().equals(reference)) throw new IOException("launch artifact metadata differs");
        return stored.content();
    }
}
