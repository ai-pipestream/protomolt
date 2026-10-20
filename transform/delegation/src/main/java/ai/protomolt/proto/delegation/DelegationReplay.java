package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.DelegationCoordinatorState.Session;
import ai.protomolt.proto.delegation.DelegationCoordinatorState.TaskRuntime;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator.Scope;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator.WorkerResumption;
import ai.protomolt.proto.delegation.contract.DelegationValidation;
import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;
import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rebuilds the coordinator's in-memory projection from a recorded transcript. Replay runs
 * once at coordinator construction, under the coordinator's monitor, before any live frame
 * is accepted; the live path and this replay driver share the same
 * {@link DelegationCoordinatorState} so a restored coordinator is indistinguishable from
 * one that recorded the transcript live.
 *
 * <p>Replay never publishes, delivers, or schedules anything: restored sessions are
 * disconnected placeholders, and lease timers are re-armed by the live path when a worker
 * reconnects. The transcript is validated and reduced clean before the first entry is
 * applied, so a corrupt store fails the coordinator construction instead of producing a
 * half-restored projection.
 */
final class DelegationReplay {

    /**
     * The response observer attached to sessions rebuilt by replay: restored events remain
     * available through the cursor-addressable feed, so nothing is delivered on restore.
     */
    private static final StreamObserver<DelegateResponse> DISCONNECTED_RESPONSES =
            new StreamObserver<>() {
                @Override
                public void onNext(DelegateResponse value) {
                    // Restored events remain available through the cursor-addressable feed.
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onCompleted() {
                }
            };

    private DelegationReplay() {
    }

    /**
     * Validates, reduces, and applies one stored transcript into {@code state}. The
     * reducer must certify the whole transcript before the first entry mutates the
     * projection; entry order is preserved exactly, and worker-frame payloads are
     * re-indexed for idempotent retry detection.
     */
    static void restore(DelegationReducer reducer, DelegationCoordinatorState state,
                        Transcript transcript) {
        DelegationValidation.validate(transcript);
        DelegationReducer.Result result = reducer.reduce(transcript);
        if (!result.clean()) {
            DelegationReducer.Finding finding = result.findings().getFirst();
            throw new IllegalArgumentException("stored transcript is invalid: "
                    + finding.kind() + ": " + finding.error());
        }
        for (TranscriptEntry entry : transcript.getEntriesList()) {
            state.record(entry);
            if (entry.getLane() == Lane.LANE_WORKER) {
                DelegateRequest frame = entry.getWorkerFrame();
                state.workerFrames.put(frame.getFrameId(), frame.toByteString());
            }
            restoreRuntime(state, entry);
        }
        // A restored session must already know its coordinator sequences: a verdict,
        // cancellation or message recorded before the worker reconnects continues
        // the scope instead of rewinding it.
        state.sessions.values().forEach(session -> restoreCoordinatorSequences(state, session));
    }

    /**
     * Rebuilds one worker's sender-side sequence state from the recorded transcript. The
     * transcript is the source of truth: a frame the coordinator never accepted is not in
     * it, so a replacement stream seeded from this resumption can neither replay a
     * recorded frame nor skip one.
     */
    static WorkerResumption workerResumption(List<TranscriptEntry> entries, String workerId) {
        Map<Scope, Long> sequences = new HashMap<>();
        Map<Scope, Integer> progress = new HashMap<>();
        Map<Scope, Integer> checkpoints = new HashMap<>();
        for (TranscriptEntry entry : entries) {
            if (entry.getLane() != Lane.LANE_WORKER
                    || !entry.getWorkerId().equals(workerId)) {
                continue;
            }
            DelegateRequest frame = entry.getWorkerFrame();
            Scope scope = new Scope(frame.getTaskId(), workerAttemptOf(frame));
            sequences.merge(scope, frame.getSeq(), Math::max);
            if (frame.hasProgress()) {
                progress.merge(scope, frame.getProgress().getProgressSeq(), Math::max);
            }
            if (frame.hasCheckpoint()) {
                checkpoints.merge(scope, frame.getCheckpoint().getCheckpointSeq(),
                        Math::max);
            }
        }
        return new WorkerResumption(sequences, progress, checkpoints);
    }

    /**
     * Seeds one session's coordinator-sequence high-water marks from the recorded
     * coordinator lane, so a reconnecting stream continues each scope's numbering.
     */
    static void restoreCoordinatorSequences(DelegationCoordinatorState state, Session session) {
        for (TranscriptEntry entry : state.entries) {
            if (entry.getLane() == Lane.LANE_COORDINATOR
                    && entry.getWorkerId().equals(session.workerId)) {
                DelegateResponse frame = entry.getCoordinatorFrame();
                session.restoreCoordinatorSeq(frame.getTaskId(), attemptOf(frame),
                        frame.getSeq());
            }
        }
    }

    private static void restoreRuntime(DelegationCoordinatorState state, TranscriptEntry entry) {
        if (entry.getLane() == Lane.LANE_WORKER) {
            restoreWorkerRuntime(state, entry.getWorkerId(), entry.getWorkerFrame());
        } else {
            restoreCoordinatorRuntime(state, entry.getWorkerId(), entry.getCoordinatorFrame());
        }
    }

    private static void restoreWorkerRuntime(DelegationCoordinatorState state, String workerId,
                                             DelegateRequest frame) {
        if (frame.hasHello()) {
            Session session = new Session(frame.getHello(), DISCONNECTED_RESPONSES);
            session.connected = false;
            state.sessions.put(workerId, session);
            return;
        }
        TaskRuntime task = state.requireRestoredTask(frame.getTaskId(), workerId);
        switch (frame.getPayloadCase()) {
            case ACCEPT -> task.phase = DelegationReducer.Phase.LEASED;
            case REJECT -> task.phase = DelegationReducer.Phase.REJECTED;
            case BLOCKED -> task.phase = DelegationReducer.Phase.BLOCKED;
            case FAILED -> task.phase = DelegationReducer.Phase.FAILED;
            case COMPLETION -> {
                task.phase = DelegationReducer.Phase.CANDIDATE;
                task.candidate = frame.getCompletion();
            }
            case HEARTBEAT, PROGRESS, CHECKPOINT, CANCELLED -> {
                // These frames do not independently change the reconstructed phase.
            }
            case TASK_MESSAGE -> {
                // Non-transitioning on the live path, so nothing to reconstruct:
                // the restored entry is already in the transcript and event feed.
            }
            case HELLO, PAYLOAD_NOT_SET -> throw new IllegalArgumentException(
                    "stored transcript contains an unexpected worker payload");
        }
    }

    private static void restoreCoordinatorRuntime(DelegationCoordinatorState state,
                                                  String workerId, DelegateResponse frame) {
        if (frame.hasAdmission()) {
            Session session = state.sessions.get(workerId);
            if (session == null) {
                throw new IllegalArgumentException(
                        "stored transcript admits a worker before its hello: " + workerId);
            }
            session.admitted = frame.getAdmission().getAdmitted();
            return;
        }
        String taskId = frame.getTaskId();
        if (frame.hasOffer()) {
            TaskOffer offer = frame.getOffer();
            TaskRuntime task = state.tasks.computeIfAbsent(taskId, TaskRuntime::new);
            task.workerId = workerId;
            task.attempt = offer.getAttempt();
            task.offer = offer;
            task.phase = DelegationReducer.Phase.OFFERED;
            task.expiry = DelegationTimes.toInstant(offer.getExpiresAt());
            task.leaseGeneration++;
            return;
        }
        TaskRuntime task = state.requireRestoredTask(taskId, workerId);
        switch (frame.getPayloadCase()) {
            case RENEWAL -> {
                task.expiry = DelegationTimes.toInstant(frame.getRenewal().getExpiresAt());
                task.leaseGeneration++;
            }
            case EXPIRED -> {
                task.phase = DelegationReducer.Phase.EXPIRED;
                task.leaseGeneration++;
            }
            case CANCELLATION -> {
                task.phase = DelegationReducer.Phase.CANCELLED;
                task.leaseGeneration++;
            }
            case REVISION_REQUESTED -> task.phase = DelegationReducer.Phase.LEASED;
            case ACCEPTED -> {
                task.phase = DelegationReducer.Phase.ACCEPTED;
                task.leaseGeneration++;
            }
            case REVIEW_STARTED, REVIEW_FAILED, REVIEW_DEFERRED, TASK_MESSAGE -> {
                // Non-transitioning on the live path, so nothing to reconstruct:
                // the restored entry is already in the transcript and event feed.
            }
            case ADMISSION, OFFER, PAYLOAD_NOT_SET -> throw new IllegalArgumentException(
                    "stored transcript contains an unexpected coordinator payload");
        }
    }

    /** The attempt a worker frame belongs to, for sequencing; mirrors the reducer. */
    static int workerAttemptOf(DelegateRequest frame) {
        return switch (frame.getPayloadCase()) {
            case ACCEPT -> frame.getAccept().getAttempt();
            case REJECT -> frame.getReject().getAttempt();
            case HEARTBEAT -> frame.getHeartbeat().getAttempt();
            case PROGRESS -> frame.getProgress().getAttempt();
            case CHECKPOINT -> frame.getCheckpoint().getAttempt();
            case BLOCKED -> frame.getBlocked().getAttempt();
            case FAILED -> frame.getFailed().getAttempt();
            case CANCELLED -> frame.getCancelled().getAttempt();
            case COMPLETION -> frame.getCompletion().getAttempt();
            // Hellos sequence on the session scope; task messages carry no
            // attempt and sequence in the task's attempt-0 scope.
            case HELLO, TASK_MESSAGE -> 0;
            default -> 0;
        };
    }

    /** The attempt a coordinator frame belongs to, for sequencing; mirrors the reducer. */
    static int attemptOf(DelegateResponse frame) {
        return switch (frame.getPayloadCase()) {
            case OFFER -> frame.getOffer().getAttempt();
            case RENEWAL -> frame.getRenewal().getAttempt();
            case EXPIRED -> frame.getExpired().getAttempt();
            case CANCELLATION -> frame.getCancellation().getAttempt();
            case REVISION_REQUESTED -> frame.getRevisionRequested().getAttempt();
            case ACCEPTED -> frame.getAccepted().getAttempt();
            case REVIEW_STARTED -> frame.getReviewStarted().getIdentity().getAttempt();
            case REVIEW_FAILED -> frame.getReviewFailed().getIdentity().getAttempt();
            case REVIEW_DEFERRED -> frame.getReviewDeferred().getIdentity().getAttempt();
            // Task messages carry no attempt; they sequence in the task's
            // attempt-0 scope, exactly as on the live path.
            case TASK_MESSAGE -> 0;
            case ADMISSION, PAYLOAD_NOT_SET -> 0;
        };
    }
}
