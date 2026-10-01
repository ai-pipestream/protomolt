package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.WorkerRegistrationConflictException;
import ai.protomolt.proto.delegation.DelegationReducer;
import ai.protomolt.proto.delegation.DelegationValidation;
import ai.protomolt.proto.delegation.DeliverableContracts;
import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.delegation.v1.*;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.workflow.authoring.v1.EnsureWorkflowAuthorRegistrationRequest;
import ai.protomolt.proto.workflow.authoring.v1.EnsureWorkflowAuthorRegistrationResponse;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import static ai.protomolt.proto.workflow.authoring.WorkflowPreparationException.Kind;

/** Author-scoped mutations over the existing coordinator and durable transcript. */
public final class WorkflowAuthorTaskMutations {
    private final DelegationBridge bridge;
    private final ArtifactReference policy;
    private final Clock clock;
    private final WorkflowAuthorTaskReader reader;

    public WorkflowAuthorTaskMutations(DelegationBridge bridge, ArtifactRepository artifacts,
            ArtifactReference policy, Clock clock) {
        this.bridge = Objects.requireNonNull(bridge);
        this.policy = Objects.requireNonNull(policy);
        this.clock = Objects.requireNonNull(clock);
        WorkflowPreparationValidation.validate(policy, 4 * 1024 * 1024);
        reader = new WorkflowAuthorTaskReader(new TranscriptRepository() {
            @Override public java.util.Optional<Transcript> load() {
                return java.util.Optional.of(bridge.coordinator().transcript());
            }
            @Override public void save(Transcript transcript) {
                throw new UnsupportedOperationException("Coordinator owns transcript writes");
            }
        }, Objects.requireNonNull(artifacts), policy, clock);
    }

    public RegisterWorkerResponse register(RegisterWorkerRequest request, Caller caller)
            throws WorkflowPreparationException {
        input(request);
        identity(request.getWorkerId(), caller);
        synchronized (bridge) {
            if (bridge.coordinator().workers().stream().anyMatch(worker ->
                    worker.workerId().equals(caller.name()) && worker.connected())) {
                throw failure(Kind.CONFLICT, "Worker is already connected");
            }
            try {
                var registration = bridge.registerWorker(WorkerHello.newBuilder()
                        .setWorkerId(caller.name()).setProtocolVersion(1)
                        .setProvider(request.getProvider()).setModel(request.getModel())
                        .setModelVersion(request.getModelVersion()).addAllCapabilities(request.getCapabilitiesList()).build());
                return RegisterWorkerResponse.newBuilder().setOk(true).setWorkerId(registration.workerId())
                        .setAdmitted(registration.admitted()).setSessionId(registration.sessionId())
                        .setReason(registration.reason()).build();
            } catch (RuntimeException unavailable) {
                throw new WorkflowPreparationException(Kind.UNAVAILABLE, "Worker registration failed", unavailable);
            }
        }
    }

    /** Exact bridge-owned registration replay, or conditional creation after restart. */
    public EnsureWorkflowAuthorRegistrationResponse ensureRegistration(EnsureWorkflowAuthorRegistrationRequest wrapped, Caller caller)
            throws WorkflowPreparationException {
        input(wrapped);
        var request = wrapped.getRegistration();
        identity(request.getWorkerId(), caller);
        synchronized (bridge) {
            try {
                var registration = bridge.ensureWorker(WorkerHello.newBuilder()
                        .setWorkerId(caller.name()).setProtocolVersion(1)
                        .setProvider(request.getProvider()).setModel(request.getModel())
                        .setModelVersion(request.getModelVersion())
                        .addAllCapabilities(request.getCapabilitiesList()).build());
                var result = RegisterWorkerResponse.newBuilder().setOk(true)
                        .setWorkerId(registration.workerId())
                        .setAdmitted(registration.admitted())
                        .setSessionId(registration.sessionId())
                        .setReason(registration.reason()).build();
                return EnsureWorkflowAuthorRegistrationResponse.newBuilder().setRegistration(result).build();
            } catch (WorkerRegistrationConflictException conflict) {
                throw new WorkflowPreparationException(Kind.CONFLICT,
                        "Worker identity is already connected", conflict);
            } catch (RuntimeException unavailable) {
                throw new WorkflowPreparationException(Kind.UNAVAILABLE,
                        "Worker registration is unavailable", unavailable);
            }
        }
    }

    public AcceptTaskResponse accept(AcceptTaskRequest request, Caller caller)
            throws WorkflowPreparationException {
        input(request);
        identity(request.getWorkerId(), caller);
        synchronized (bridge) {
            var snapshot = inspect(request.getTaskId(), request.getAttempt(), caller);
            if (accepted(snapshot, request.getAttempt())) return acceptance(request);
            live(snapshot, request.getAttempt(), DelegationReducer.Phase.OFFERED);
            verifiedContext(request.getTaskId(), request.getAttempt(), caller, snapshot);
            try {
                bridge.accept(caller.name(), request.getTaskId(), request.getAttempt());
            } catch (RuntimeException failure) {
                var after = inspect(request.getTaskId(), request.getAttempt(), caller);
                if (accepted(after, request.getAttempt())) return acceptance(request);
                live(after, request.getAttempt(), DelegationReducer.Phase.OFFERED);
                throw new WorkflowPreparationException(Kind.UNAVAILABLE, "Acceptance failed; inspect task and registration", failure);
            }
            var after = inspect(request.getTaskId(), request.getAttempt(), caller);
            if (!accepted(after, request.getAttempt())) throw failure(Kind.CORRUPT_EVIDENCE, "Acceptance was not persisted");
            return acceptance(request);
        }
    }

    public SubmitCandidateResponse submit(SubmitCandidateRequest request, Caller caller)
            throws WorkflowPreparationException {
        input(request);
        identity(request.getWorkerId(), caller);
        var candidate = request.getCandidate();
        WorkflowAuthoringDeliverable authored;
        try {
            if (!candidate.getResult().is(WorkflowAuthoringDeliverable.class)) throw new IllegalArgumentException();
            authored = candidate.getResult().unpack(WorkflowAuthoringDeliverable.class);
            WorkflowPreparationValidation.validate(authored, 4 * 1024 * 1024);
        } catch (Exception invalid) {
            throw new WorkflowPreparationException(Kind.INVALID_INPUT, "Invalid workflow deliverable", invalid);
        }
        synchronized (bridge) {
            var snapshot = inspect(request.getTaskId(), candidate.getAttempt(), caller);
            if (submitted(snapshot, candidate)) return submission(request);
            validateSubmissionFrame(request);
            live(snapshot, candidate.getAttempt(), DelegationReducer.Phase.LEASED);
            verifiedContext(request.getTaskId(), candidate.getAttempt(), caller, snapshot);
            if (candidate.getRevision() != snapshot.state().candidateRevision() + 1) {
                throw failure(Kind.INACTIVE, "Candidate revision is not current");
            }
            var spec = snapshot.offer().getCoordinatorFrame().getOffer().getSpec();
            Set<String> checks = new HashSet<>();
            for (CheckEvidence evidence : candidate.getEvidenceList()) {
                if (!checks.add(evidence.getCheckName()) || evidence.getVerdict() != CheckVerdict.CHECK_VERDICT_PASSED) {
                    throw failure(Kind.INVALID_INPUT, "Candidate checks must pass exactly once");
                }
            }
            if (!checks.equals(Set.copyOf(WorkflowAuthoringReviewer.REQUIRED_CHECKS))
                    || !DeliverableContracts.check(spec.getContract(), candidate.getResult()).isEmpty()) {
                throw failure(Kind.INVALID_INPUT, "Candidate does not satisfy the task contract");
            }
            var supplied = new java.util.HashMap<String, CheckEvidence>();
            candidate.getEvidenceList().forEach(check -> supplied.put(check.getCheckName(), check));
            var packed = new java.util.HashMap<String, CheckEvidence>();
            authored.getDeliverable().getChecksList().forEach(check -> packed.put(check.getCheckName(), check));
            if (packed.size() != authored.getDeliverable().getChecksCount() || !packed.equals(supplied)) {
                throw failure(Kind.INVALID_INPUT, "Candidate evidence differs from the deliverable checks");
            }
            try {
                bridge.submitCandidate(caller.name(), request.getTaskId(), candidate);
            } catch (RuntimeException failure) {
                var after = inspect(request.getTaskId(), candidate.getAttempt(), caller);
                if (submitted(after, candidate)) return submission(request);
                live(after, candidate.getAttempt(), DelegationReducer.Phase.LEASED);
                throw new WorkflowPreparationException(Kind.UNAVAILABLE, "Submission failed; inspect task and registration", failure);
            }
            var after = inspect(request.getTaskId(), candidate.getAttempt(), caller);
            if (!submitted(after, candidate)) throw failure(Kind.CORRUPT_EVIDENCE, "Candidate was not persisted");
            return submission(request);
        }
    }

    private record Snapshot(Transcript transcript, TranscriptEntry offer, DelegationReducer.TaskState state) {}

    private Snapshot inspect(String task, int attempt, Caller caller) throws WorkflowPreparationException {
        Transcript transcript;
        DelegationReducer.Result reduced;
        try {
            transcript = bridge.coordinator().transcript();
            WorkflowPreparationValidation.validate(transcript, 8 * 1024 * 1024);
            DelegationValidation.validate(transcript);
            reduced = new DelegationReducer().reduce(transcript);
            if (!reduced.clean()) throw new IllegalArgumentException("Invalid lifecycle");
        } catch (RuntimeException corrupt) {
            throw new WorkflowPreparationException(Kind.CORRUPT_EVIDENCE, "Coordinator transcript is invalid", corrupt);
        }
        TranscriptEntry offer = null;
        Set<TranscriptEntry> seen = new HashSet<>();
        for (var entry : transcript.getEntriesList()) {
            if (!seen.add(entry) || !entry.hasCoordinatorFrame()) continue;
            var frame = entry.getCoordinatorFrame();
            if (frame.getTaskId().equals(task) && frame.hasOffer() && frame.getOffer().getAttempt() == attempt) {
                if (offer != null) throw failure(Kind.CORRUPT_EVIDENCE, "Duplicate task offer");
                offer = entry;
            }
        }
        if (offer == null) throw failure(Kind.INACTIVE, "Task offer not found");
        if (!offer.getWorkerId().equals(caller.name())) throw failure(Kind.PERMISSION_DENIED, "Task belongs to another author");
        var spec = offer.getCoordinatorFrame().getOffer().getSpec();
        Set<String> checks = new HashSet<>();
        spec.getRequiredChecksList().forEach(check -> checks.add(check.getName()));
        if (!spec.hasContract() || !spec.getContract().getTypeName().equals(WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                || !spec.getContextList().contains(policy)
                || spec.getRequiredChecksCount() != WorkflowAuthoringReviewer.REQUIRED_CHECKS.size()
                || !checks.equals(Set.copyOf(WorkflowAuthoringReviewer.REQUIRED_CHECKS))) {
            throw failure(Kind.INACTIVE, "Task does not use the configured authoring contract");
        }
        return new Snapshot(transcript, offer, reduced.tasks().get(task));
    }

    private void live(Snapshot snapshot, int attempt, DelegationReducer.Phase phase) throws WorkflowPreparationException {
        var state = snapshot.state();
        if (state == null || state.attempt() != attempt || state.phase() != phase
                || (phase == DelegationReducer.Phase.LEASED && !state.holder().equals(snapshot.offer().getWorkerId()))) {
            throw failure(Kind.INACTIVE, "Task attempt is not active");
        }
        Timestamp expiry = snapshot.offer().getCoordinatorFrame().getOffer().getExpiresAt();
        Set<TranscriptEntry> seen = new HashSet<>();
        for (var entry : snapshot.transcript().getEntriesList()) {
            if (!seen.add(entry) || !entry.hasCoordinatorFrame()) continue;
            var frame = entry.getCoordinatorFrame();
            if (frame.getTaskId().equals(state.taskId()) && entry.getWorkerId().equals(snapshot.offer().getWorkerId())
                    && frame.hasRenewal() && frame.getRenewal().getAttempt() == attempt) expiry = frame.getRenewal().getExpiresAt();
        }
        var now = clock.instant();
        if (Timestamps.compare(expiry, Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build()) <= 0) {
            throw failure(Kind.INACTIVE, "Task lease has expired");
        }
        // Coordinator rechecks wall-clock expiry atomically before publication.
    }

    private void verifiedContext(String task, int attempt, Caller caller, Snapshot snapshot)
            throws WorkflowPreparationException {
        var context = reader.context(ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextRequest
                .newBuilder().setTaskId(task).setAttempt(attempt).build(), caller);
        if (!context.getOfferEntry().equals(snapshot.offer()) || !context.getPolicy().equals(policy)) {
            throw failure(Kind.INACTIVE, "Author context changed before mutation");
        }
    }

    private static boolean accepted(Snapshot snapshot, int attempt) {
        return snapshot.transcript().getEntriesList().stream().anyMatch(entry ->
                entry.getWorkerId().equals(snapshot.offer().getWorkerId()) && entry.hasWorkerFrame()
                        && entry.getWorkerFrame().getTaskId().equals(snapshot.offer().getCoordinatorFrame().getTaskId())
                        && entry.getWorkerFrame().hasAccept() && entry.getWorkerFrame().getAccept().getAttempt() == attempt);
    }

    private static boolean submitted(Snapshot snapshot, CompletionCandidate candidate) throws WorkflowPreparationException {
        for (var entry : snapshot.transcript().getEntriesList()) {
            if (!entry.hasWorkerFrame() || !entry.getWorkerId().equals(snapshot.offer().getWorkerId())) continue;
            var frame = entry.getWorkerFrame();
            if (!frame.getTaskId().equals(snapshot.offer().getCoordinatorFrame().getTaskId()) || !frame.hasCompletion()) continue;
            var prior = frame.getCompletion();
            if (prior.getAttempt() == candidate.getAttempt() && prior.getRevision() == candidate.getRevision()) {
                if (!prior.equals(candidate)) throw failure(Kind.CONFLICT, "Candidate revision has different content");
                return true;
            }
        }
        return false;
    }

    private static void validateSubmissionFrame(SubmitCandidateRequest request)
            throws WorkflowPreparationException {
        // Reserve the largest valid envelope the bridge can emit: a UUID frame
        // ID, positive int64 sequence, and maximum-width valid timestamp. This
        // preflight never allocates a stream sequence or publishes a frame.
        var frame = DelegateRequest.newBuilder()
                .setFrameId("00000000-0000-4000-8000-000000000000")
                .setTaskId(request.getTaskId()).setSeq(Long.MAX_VALUE)
                .setSentAt(Timestamp.newBuilder().setSeconds(-62135596800L).setNanos(999999999))
                .setCompletion(request.getCandidate()).build();
        try {
            DelegationValidation.validate(frame);
        } catch (IllegalArgumentException invalid) {
            throw new WorkflowPreparationException(Kind.INVALID_INPUT,
                    "Candidate exceeds delegation frame limits or violates its contract", invalid);
        }
    }

    private static void input(Message request) throws WorkflowPreparationException {
        try { WorkflowPreparationValidation.validate(request, 16 * 1024 * 1024); }
        catch (IllegalArgumentException invalid) { throw new WorkflowPreparationException(Kind.INVALID_INPUT, "Invalid author request", invalid); }
    }

    private static void identity(String worker, Caller caller) throws WorkflowPreparationException {
        if (caller == null || !caller.holds(Scopes.WORKFLOW_AUTHOR) || !worker.equals(caller.name())) {
            throw failure(Kind.PERMISSION_DENIED, "Authenticated author identity required");
        }
    }

    private static AcceptTaskResponse acceptance(AcceptTaskRequest request) {
        return AcceptTaskResponse.newBuilder().setOk(true).setTaskId(request.getTaskId()).setAttempt(request.getAttempt()).build();
    }

    private static SubmitCandidateResponse submission(SubmitCandidateRequest request) {
        return SubmitCandidateResponse.newBuilder().setOk(true).setTaskId(request.getTaskId())
                .setAttempt(request.getCandidate().getAttempt()).setRevision(request.getCandidate().getRevision()).build();
    }

    private static WorkflowPreparationException failure(Kind kind, String message) {
        return new WorkflowPreparationException(kind, message, null);
    }
}
