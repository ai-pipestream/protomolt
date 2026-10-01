package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreException;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchAuthorization;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchJobState;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchJobStatus;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** Trusted, effect-free status projection. The ledger gates every other store read. */
public final class WorkflowLaunchStatusReader implements WorkflowLaunchStatusOperations {
    private final WorkflowLaunchAuthorizationRepository authorizations;
    private final TranscriptRepository transcripts;
    private final WorkflowAuthoringReviewer reviewer;
    private final ArtifactRepository artifacts;
    private final WorkflowRunStore jobs;
    private final ActionContext actions;

    public WorkflowLaunchStatusReader(WorkflowLaunchAuthorizationRepository authorizations,
            TranscriptRepository transcripts, WorkflowAuthoringReviewer reviewer,
            ArtifactRepository artifacts, WorkflowRunStore jobs, ActionContext actions) {
        this.authorizations = Objects.requireNonNull(authorizations);
        this.transcripts = Objects.requireNonNull(transcripts);
        this.reviewer = Objects.requireNonNull(reviewer);
        this.artifacts = Objects.requireNonNull(artifacts);
        this.jobs = Objects.requireNonNull(jobs);
        this.actions = Objects.requireNonNull(actions);
    }

    @Override
    public GetWorkflowLaunchStatusResponse get(GetWorkflowLaunchStatusRequest supplied)
            throws WorkflowLaunchStatusException {
        try {
            WorkflowLaunchValidation.validate(supplied);
        } catch (RuntimeException invalid) {
            throw failure(WorkflowLaunchStatusException.Kind.INVALID_INPUT,
                    "launch status request is invalid", invalid);
        }
        WorkflowAuthoringLaunchRequest intent;
        try {
            intent = supplied.getRequest().toBuilder()
                    .setLaunchId(UUID.fromString(supplied.getRequest().getLaunchId()).toString()).build();
        } catch (IllegalArgumentException invalid) {
            throw failure(WorkflowLaunchStatusException.Kind.INVALID_INPUT,
                    "launch status UUID is invalid", invalid);
        }
        try {
            // An absent keyed authorization is the only fact this operation may reveal.
            // In particular, never ask the job store about a caller-chosen UUID here.
            var found = authorizations.find(intent.getLaunchId());
            if (found.isEmpty()) return checked(GetWorkflowLaunchStatusResponse.newBuilder()
                    .setRequest(intent).setNotAuthorized(true).build());

            WorkflowAuthoringLaunchAuthorization authority = found.get();
            try {
                WorkflowLaunchValidation.validate(authority);
            } catch (RuntimeException corrupt) {
                throw failure(WorkflowLaunchStatusException.Kind.CORRUPT_EVIDENCE,
                        "stored launch authorization is invalid", corrupt);
            }
            if (!authority.getRequest().equals(intent)) {
                throw failure(WorkflowLaunchStatusException.Kind.CONFLICT,
                        "launch UUID belongs to a different intent", null);
            }
            WorkflowRunRecord expected = expectedSubmission(intent, authority);
            var job = jobs.get(UUID.fromString(intent.getLaunchId()));
            if (job.isEmpty()) return checked(GetWorkflowLaunchStatusResponse.newBuilder()
                    .setRequest(intent).setAuthorizedNotQueued(true).build());
            WorkflowRunRecord observed = job.get();
            if (!UUID.fromString(intent.getLaunchId()).equals(observed.jobId)
                    || !WorkflowRunStore.sameSubmission(expected, observed)) {
                throw failure(WorkflowLaunchStatusException.Kind.CORRUPT_EVIDENCE,
                        "stored job differs from authorized source or input", null);
            }
            return checked(GetWorkflowLaunchStatusResponse.newBuilder()
                    .setRequest(intent).setJob(project(observed)).build());
        } catch (WorkflowLaunchStatusException classified) {
            throw classified;
        } catch (WorkflowLaunchAuthorizationCorruptException corrupt) {
            throw failure(WorkflowLaunchStatusException.Kind.CORRUPT_EVIDENCE,
                    "stored launch authorization is corrupt", corrupt);
        } catch (IOException unavailable) {
            throw failure(deadline(unavailable) ? WorkflowLaunchStatusException.Kind.DEADLINE
                    : WorkflowLaunchStatusException.Kind.UNAVAILABLE,
                    "launch status storage is unavailable", unavailable);
        } catch (WorkflowRunStoreException unavailable) {
            throw failure(deadline(unavailable) ? WorkflowLaunchStatusException.Kind.DEADLINE
                    : WorkflowLaunchStatusException.Kind.UNAVAILABLE,
                    "launch job storage is unavailable", unavailable);
        } catch (RuntimeException invalidStore) {
            throw failure(deadline(invalidStore) ? WorkflowLaunchStatusException.Kind.DEADLINE
                    : invalidStore instanceof StatusRuntimeException
                            ? WorkflowLaunchStatusException.Kind.UNAVAILABLE
                            : WorkflowLaunchStatusException.Kind.CORRUPT_EVIDENCE,
                    "launch status evidence is unusable", invalidStore);
        } catch (Exception invalidEvidence) {
            throw failure(deadline(invalidEvidence) ? WorkflowLaunchStatusException.Kind.DEADLINE
                    : invalidEvidence instanceof StatusException
                            ? WorkflowLaunchStatusException.Kind.UNAVAILABLE
                            : WorkflowLaunchStatusException.Kind.CORRUPT_EVIDENCE,
                    "launch status evidence is invalid", invalidEvidence);
        }
    }

    private WorkflowRunRecord expectedSubmission(WorkflowAuthoringLaunchRequest intent,
            WorkflowAuthoringLaunchAuthorization authority) throws Exception {
        var accepted = WorkflowLaunchAcceptance.inspect(transcripts, intent.getAcceptance().getTaskId());
        if (!accepted.identity().equals(intent.getAcceptance())) {
            throw new IllegalArgumentException("accepted candidate differs from stored launch intent");
        }
        var prepared = reviewer.prepare(accepted.context()); // Stored evidence only; no fixture execution.
        if (!authority.getPolicy().equals(prepared.policyReference())
                || !Arrays.equals(authority.getAuthored().toByteArray(), accepted.authored().toByteArray())) {
            throw new IllegalArgumentException("stored authorization differs from accepted evidence");
        }
        WorkflowValidation.validate(authority.getPromoted());
        if (!Arrays.equals(authority.getPromoted().getWorkflow().toByteArray(),
                accepted.authored().getDeliverable().getWorkflow().toByteArray())
                || !authority.getPromoted().getCreatedAt().equals(accepted.acceptedAt())
                || !authority.getPromoted().getWorkflowFingerprint().equals(
                        WorkflowValidation.fingerprint(authority.getPromoted().getWorkflow()))
                || !authority.getPromoted().getVersion().equals(
                        "accepted-" + WorkflowLaunchValidation.sha256(accepted.identity()))) {
            throw new IllegalArgumentException("stored promotion differs from accepted workflow");
        }
        byte[] inputBytes = resolve(intent.getInput());
        var input = DynamicMessage.parseFrom(prepared.admitted().workflow().inputType(), inputBytes);
        WorkflowLaunchValidation.validate(input);
        var source = (ObjectNode) actions.objectMapper().readTree(prepared.admitted().sourceJson());
        var inputJson = actions.objectMapper().readTree(actions.transcoder().toJson(input));
        WorkflowRunRecord expected = new WorkflowRunRecord();
        expected.workflowName = prepared.admitted().workflow().name();
        expected.workflowDefinition = source.toString();
        expected.input = inputJson.toString();
        return expected;
    }

    private byte[] resolve(ArtifactReference reference) throws IOException {
        WorkflowValidation.validate(reference);
        if (reference.getRedacted() || !reference.getMediaType().equals("application/x-protobuf")
                || Long.compareUnsigned(reference.getSizeBytes(), WorkflowLaunchValidation.MAX_BYTES) > 0) {
            throw new IllegalArgumentException("launch input reference is invalid");
        }
        var stored = artifacts.find(reference.getSha256())
                .orElseThrow(() -> new IllegalArgumentException("launch input artifact is absent"));
        if (!stored.reference().equals(reference) || stored.content().length != reference.getSizeBytes()
                || !WorkRecords.sha256Hex(stored.content()).equals(reference.getSha256())) {
            throw new IllegalArgumentException("launch input artifact differs from its reference");
        }
        return stored.content();
    }

    private static WorkflowLaunchJobStatus project(WorkflowRunRecord job) {
        var builder = WorkflowLaunchJobStatus.newBuilder().setJobId(job.jobId.toString())
                .setAttempt(job.attempt).setMaxAttempts(job.maxAttempts)
                .setState(switch (job.status) {
                    case WorkflowRunRecord.STATUS_QUEUED -> WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_QUEUED;
                    case WorkflowRunRecord.STATUS_RUNNING -> WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_RUNNING;
                    case WorkflowRunRecord.STATUS_WAITING -> WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_WAITING;
                    case WorkflowRunRecord.STATUS_COMPLETED -> WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_COMPLETED;
                    case WorkflowRunRecord.STATUS_FAILED -> WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_FAILED;
                    case WorkflowRunRecord.STATUS_DEAD -> WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_DEAD;
                    default -> throw new IllegalArgumentException("unknown stored job state");
                }).setCreatedAt(timestamp(job.createdAt)).setUpdatedAt(timestamp(job.updatedAt));
        if (job.completedAt != null) builder.setCompletedAt(timestamp(job.completedAt));
        return builder.build();
    }

    private static Timestamp timestamp(Instant instant) {
        if (instant == null) throw new IllegalArgumentException("stored job timestamp is absent");
        Timestamp value = Timestamp.newBuilder().setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano()).build();
        if (!Timestamps.isValid(value)) throw new IllegalArgumentException("stored job timestamp is invalid");
        return value;
    }

    private static GetWorkflowLaunchStatusResponse checked(GetWorkflowLaunchStatusResponse response)
            throws WorkflowLaunchStatusException {
        try {
            WorkflowLaunchValidation.validate(response);
            return response;
        } catch (RuntimeException invalid) {
            throw failure(WorkflowLaunchStatusException.Kind.CORRUPT_EVIDENCE,
                    "launch status projection is invalid", invalid);
        }
    }

    private static WorkflowLaunchStatusException failure(WorkflowLaunchStatusException.Kind kind,
            String message, Throwable cause) {
        return new WorkflowLaunchStatusException(kind, message, cause);
    }

    private static boolean deadline(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof StatusRuntimeException status
                    && status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) return true;
            if (current instanceof StatusException status
                    && status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) return true;
            if (current instanceof java.io.InterruptedIOException
                    || current instanceof java.util.concurrent.TimeoutException) return true;
        }
        return false;
    }
}
