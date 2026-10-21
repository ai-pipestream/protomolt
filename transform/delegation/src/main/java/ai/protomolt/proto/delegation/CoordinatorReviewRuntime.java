package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.CandidateReviewer.ReviewContext;
import ai.protomolt.proto.delegation.CandidateReviewer.ReviewDecision;
import ai.protomolt.proto.delegation.DelegationCoordinatorState.TaskRuntime;
import ai.protomolt.proto.delegation.contract.DelegationValidation;
import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;
import ai.protomolt.proto.delegation.v1.CandidateReviewIdentity;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.ReviewDeferred;
import ai.protomolt.proto.delegation.v1.ReviewFailed;
import ai.protomolt.proto.delegation.v1.ReviewFailureCode;
import ai.protomolt.proto.delegation.v1.ReviewStarted;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;

import java.time.Clock;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * The coordinator's candidate-review runtime: review-start frame construction, review
 * identity lookup against the recorded transcript, asynchronous reviewer dispatch, review
 * deadline timers, and deadline/interruption/infrastructure failure recording.
 *
 * <p>The single monitor is preserved: every method either documents that its caller holds
 * the coordinator's {@code lock} (passed to the constructor) or synchronizes on that same
 * object itself. This class never creates a lock of its own. Frame emission and review
 * application stay with the coordinator behind {@link CoordinatorFrames}, so transcript
 * authority and reducer ordering are unchanged; this runtime decides <em>when</em> a
 * review frame is due, never how a frame becomes durable.
 */
final class CoordinatorReviewRuntime {

    private final Object lock;
    private final DelegationCoordinatorState state;
    private final CandidateReviewer reviewer;
    private final Clock clock;
    private final java.time.Duration reviewWindow;
    private final boolean scheduleReviewDeadlines;
    private final ExecutorService runtimeTasks;
    private final DelegationReducer reducer;
    private final CoordinatorFrames frames;

    /**
     * The last reduced transcript projection and the {@code state.transcriptVersion} it was
     * reduced at. The transcript is append-only through {@code state.record}, so a matching
     * version means the reduction is still exact; both fields are touched only under the
     * coordinator monitor.
     */
    private DelegationReducer.Result reduced;
    private long reducedVersion = -1;

    CoordinatorReviewRuntime(Object lock, DelegationCoordinatorState state,
            CandidateReviewer reviewer, Clock clock, java.time.Duration reviewWindow,
            boolean scheduleReviewDeadlines, ExecutorService runtimeTasks,
            DelegationReducer reducer, CoordinatorFrames frames) {
        this.lock = Objects.requireNonNull(lock, "lock");
        this.state = Objects.requireNonNull(state, "state");
        this.reviewer = Objects.requireNonNull(reviewer, "reviewer");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.reviewWindow = Objects.requireNonNull(reviewWindow, "reviewWindow");
        this.scheduleReviewDeadlines = scheduleReviewDeadlines;
        this.runtimeTasks = Objects.requireNonNull(runtimeTasks, "runtimeTasks");
        this.reducer = Objects.requireNonNull(reducer, "reducer");
        this.frames = Objects.requireNonNull(frames, "frames");
    }

    /** One pending reviewer invocation: the task under review and its recorded start. */
    record ReviewDispatch(String taskId, ReviewStarted started) {
    }

    /**
     * Builds the review-start payload for one candidate identity. Callers hold the
     * coordinator monitor. The deadline is the start instant plus the configured review
     * window; retry linkage fields appear exactly when a retry supplied them.
     */
    ReviewStarted newReviewStart(CandidateReviewIdentity identity,
            String previousInvocation, String retryId) {
        var startedAt = DelegationTimes.toTimestamp(clock.instant());
        ReviewStarted.Builder started = ReviewStarted.newBuilder()
                .setIdentity(identity).setStartedAt(startedAt)
                .setDeadline(DelegationTimes.toTimestamp(
                        DelegationTimes.toInstant(startedAt).plus(reviewWindow)));
        if (!previousInvocation.isEmpty()) started.setPreviousInvocationId(previousInvocation);
        if (!retryId.isEmpty()) started.setRetryId(retryId);
        return started.build();
    }

    /**
     * Builds and validates the coordinator frame carrying one review start. Callers hold
     * the coordinator monitor.
     */
    DelegateResponse reviewStartFrame(TaskRuntime task, long seq, ReviewStarted started) {
        DelegateResponse frame = DelegateResponse.newBuilder()
                .setFrameId(UUID.randomUUID().toString()).setTaskId(task.taskId)
                .setSeq(seq).setSentAt(started.getStartedAt())
                .setReviewStarted(started).build();
        DelegationValidation.validate(frame);
        return frame;
    }

    /** The transcript entry of the task's current offer. Callers hold the monitor. */
    TranscriptEntry selectedOfferEntry(TaskRuntime task) {
        for (int i = state.entries.size() - 1; i >= 0; i--) {
            TranscriptEntry entry = state.entries.get(i);
            if (entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasOffer()
                    && entry.getCoordinatorFrame().getTaskId().equals(task.taskId)
                    && entry.getCoordinatorFrame().getOffer().getAttempt() == task.attempt
                    && entry.getWorkerId().equals(task.workerId)) return entry;
        }
        throw new IllegalStateException("current task offer is missing");
    }

    /** The transcript entry of the task's current candidate. Callers hold the monitor. */
    TranscriptEntry selectedCandidateEntry(TaskRuntime task) {
        for (int i = state.entries.size() - 1; i >= 0; i--) {
            TranscriptEntry entry = state.entries.get(i);
            if (entry.hasWorkerFrame() && entry.getWorkerFrame().hasCompletion()
                    && entry.getWorkerFrame().getTaskId().equals(task.taskId)
                    && entry.getWorkerFrame().getCompletion().getAttempt() == task.attempt
                    && entry.getWorkerFrame().getCompletion().getRevision()
                            == task.candidate.getRevision()
                    && entry.getWorkerId().equals(task.workerId)) return entry;
        }
        throw new IllegalStateException("current task candidate is missing");
    }

    /**
     * The task's review state as reduced from the recorded transcript. Callers hold the
     * coordinator monitor.
     */
    DelegationReducer.ReviewState currentReview(TaskRuntime task) {
        DelegationReducer.Result result = reduction();
        if (!result.clean() || !result.tasks().containsKey(task.taskId)) {
            throw new IllegalStateException("current review projection is unavailable");
        }
        return result.tasks().get(task.taskId).review();
    }

    /**
     * The transcript reduction at the current transcript version: when no entry has been
     * appended since the last reduction the cached {@link DelegationReducer.Result} (an
     * immutable value) is reused instead of copying and re-reducing the full history.
     * Callers hold the coordinator monitor.
     */
    private DelegationReducer.Result reduction() {
        if (reduced == null || reducedVersion != state.transcriptVersion) {
            reduced = reducer.reduce(
                    Transcript.newBuilder().addAllEntries(state.entries).build());
            reducedVersion = state.transcriptVersion;
        }
        return reduced;
    }

    /**
     * Whether the task's open review is still the one {@code started} began, against the
     * live latches and the reduced transcript. Callers hold the coordinator monitor.
     */
    boolean isCurrentReview(TaskRuntime task, ReviewStarted started) {
        if (state.closed || state.publicationFailed || task == null
                || task.phase != DelegationReducer.Phase.CANDIDATE) return false;
        DelegationReducer.ReviewState review = currentReview(task);
        return review.status() == DelegationReducer.ReviewStatus.RUNNING
                && review.started().getIdentity().equals(started.getIdentity());
    }

    /**
     * Records a deadline failure when the running review's deadline has passed, and
     * reports whether the review was settled that way. Callers hold the monitor.
     */
    boolean settleExpiredReview(TaskRuntime task) {
        DelegationReducer.ReviewState review = currentReview(task);
        if (review.status() != DelegationReducer.ReviewStatus.RUNNING
                || DelegationTimes.toInstant(review.started().getDeadline())
                        .isAfter(clock.instant())) return false;
        recordReviewFailure(task, review.started(),
                ReviewFailureCode.REVIEW_FAILURE_CODE_DEADLINE);
        return true;
    }

    /** Emits the review-failed frame for one started invocation. Callers hold the monitor. */
    void recordReviewFailure(TaskRuntime task, ReviewStarted started, ReviewFailureCode code) {
        frames.emit(state.requireAdmittedWorker(task.workerId), task.taskId, task.attempt,
                DelegateResponse.newBuilder().setReviewFailed(
                        ReviewFailed.newBuilder().setIdentity(started.getIdentity())
                                .setCode(code)));
    }

    /**
     * Fails every review left running in the restored transcript as interrupted. Runs once
     * at coordinator construction, under the coordinator monitor, after replay.
     */
    void recordInterruptedReviews() {
        DelegationReducer.Result result = reduction();
        for (var snapshot : result.tasks().values()) {
            if (snapshot.phase() != DelegationReducer.Phase.CANDIDATE
                    || snapshot.review().status() != DelegationReducer.ReviewStatus.RUNNING)
                continue;
            TaskRuntime task = state.tasks.get(snapshot.taskId());
            recordReviewFailure(task, snapshot.review().started(),
                    ReviewFailureCode.REVIEW_FAILURE_CODE_INTERRUPTED);
        }
    }

    /**
     * Dispatches one started review to the configured reviewer on a runtime task and arms
     * its deadline timer. Called outside the coordinator monitor; every projection access
     * inside re-enters it. A reviewer callback that arrives after its invocation stopped
     * being current — deadline settled, manual verdict applied, or coordinator closed —
     * changes nothing.
     */
    void dispatchReview(ReviewDispatch dispatch) {
        scheduleReviewDeadline(dispatch);
        try {
            runtimeTasks.submit(() -> {
                ReviewContext context;
                synchronized (lock) {
                    TaskRuntime task = state.tasks.get(dispatch.taskId());
                    if (!isCurrentReview(task, dispatch.started())) return;
                    if (settleExpiredReview(task)) return;
                    context = new ReviewContext(task.taskId, task.workerId,
                            task.offer.getSpec(), task.candidate);
                }
                ReviewDecision decision;
                try {
                    decision = reviewer.review(context);
                } catch (Exception failure) {
                    synchronized (lock) {
                        TaskRuntime task = state.tasks.get(dispatch.taskId());
                        if (isCurrentReview(task, dispatch.started())) {
                            if (!settleExpiredReview(task)) {
                                recordReviewFailure(task, dispatch.started(),
                                        ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE);
                            }
                        }
                    }
                    return;
                }
                synchronized (lock) {
                    TaskRuntime task = state.tasks.get(dispatch.taskId());
                    if (!isCurrentReview(task, dispatch.started())) return;
                    if (settleExpiredReview(task)) return;
                    if (decision == null) {
                        recordReviewFailure(task, dispatch.started(),
                                ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE);
                    } else if (decision instanceof ReviewDecision.Pending) {
                        frames.emit(state.requireAdmittedWorker(task.workerId), task.taskId,
                                task.attempt,
                                DelegateResponse.newBuilder().setReviewDeferred(
                                        ReviewDeferred.newBuilder()
                                                .setIdentity(dispatch.started().getIdentity())));
                    } else {
                        try {
                            frames.applyReview(task, decision,
                                    dispatch.started().getIdentity().getInvocationId());
                        } catch (IllegalArgumentException invalidDecision) {
                            if (isCurrentReview(task, dispatch.started())) {
                                recordReviewFailure(task, dispatch.started(),
                                        ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE);
                            }
                        }
                    }
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException rejected) {
            synchronized (lock) {
                TaskRuntime task = state.tasks.get(dispatch.taskId());
                if (isCurrentReview(task, dispatch.started())) {
                    recordReviewFailure(task, dispatch.started(),
                            ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE);
                }
            }
        }
    }

    /**
     * Arms the deadline timer for one started review: a runtime task sleeps until the
     * recorded deadline and settles the review as a deadline failure when it is still the
     * current one. Called outside the coordinator monitor; the timer re-enters it.
     */
    void scheduleReviewDeadline(ReviewDispatch dispatch) {
        if (!scheduleReviewDeadlines) return;
        try {
            runtimeTasks.submit(() -> {
                try {
                    while (true) {
                        java.time.Duration delay = java.time.Duration.between(clock.instant(),
                                DelegationTimes.toInstant(dispatch.started().getDeadline()));
                        if (!delay.isNegative() && !delay.isZero()) Thread.sleep(delay);
                        synchronized (lock) {
                            TaskRuntime task = state.tasks.get(dispatch.taskId());
                            if (!isCurrentReview(task, dispatch.started())) return;
                            if (settleExpiredReview(task)) return;
                        }
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException rejected) {
            synchronized (lock) {
                TaskRuntime task = state.tasks.get(dispatch.taskId());
                if (isCurrentReview(task, dispatch.started())) {
                    recordReviewFailure(task, dispatch.started(),
                            ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE);
                }
            }
        }
    }
}
