package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.Checkpoint;
import ai.protomolt.proto.delegation.v1.CheckpointReference;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The offline delegation lifecycle checker: a pure, in-process state reducer over a
 * recorded {@link Transcript}. It performs no I/O, trusts no clock, and never repairs
 * input: every frame is validated, deduplicated, sequenced, and applied to the task
 * state machine in recorded order, and every deviation is reported as a
 * {@link Finding} that names the task, the frame, the kind of problem, and the precise
 * detail.
 *
 * <p>The lifecycle it enforces, per task:</p>
 *
 * <pre>
 *   (new) --offer(attempt n)--&gt; OFFERED --accept--&gt; LEASED
 *     OFFERED --reject / cancel--&gt; REJECTED / CANCELLED (attempt terminal)
 *     LEASED  --heartbeat, renewal, progress, checkpoint (loops, monotonic)
 *     LEASED  --blocked / failed / expired / cancel--&gt; attempt terminal
 *     LEASED  --completion candidate(revision r)--&gt; CANDIDATE
 *     CANDIDATE --revision requested(r)--&gt; LEASED (next candidate must be r+1)
 *     CANDIDATE --expired / cancel--&gt; attempt terminal
 *     CANDIDATE --accepted(r)--&gt; ACCEPTED (task terminal: nothing may follow)
 *   Any attempt-terminal phase --offer(attempt n+1)--&gt; OFFERED (re-offer or
 *   reassignment; resume_from must name a checkpoint the task actually recorded)
 * </pre>
 *
 * <p>Idempotency and ordering: a frame redelivered with identical bytes replays
 * silently; the same frame id with changed bytes is a conflicting duplicate. Each
 * (worker stream, lane, task, attempt) scope sequences from 1; a gap or rewind is a
 * finding (the reducer still advances its expectation past a gap so one hole does not
 * cascade into noise, but the finding stands). Cancellation is terminal the moment the
 * coordinator emits it: a completion candidate that arrives afterwards races the
 * cancellation and loses.</p>
 *
 * <p>Evidence: a completion candidate must prove every required acceptance check of the
 * current offer's spec ran, with a passing verdict, exactly once per check. Missing,
 * unknown, duplicated, or failed-check evidence is a finding; a candidate with no
 * commit or artifact reference does not pass structural validation at all.</p>
 */
public final class DelegationReducer {

    /**
     * One problem found in the transcript. {@code taskId} is empty for session-scope
     * findings; {@code kind} is one of {@code envelope}, {@code session},
     * {@code sequence}, {@code duplicate}, {@code transition}, {@code lease},
     * {@code progress}, {@code checkpoint}, {@code revision}, {@code evidence}, or
     * {@code terminal}.
     */
    public record Finding(String taskId, String frameId, String kind, String error) {
    }

    /** The lifecycle phase of one task after the frames seen so far. */
    public enum Phase {
        /** An offer is open; no lease yet. */
        OFFERED,
        /** The worker declined the open offer. */
        REJECTED,
        /** One worker holds the attempt's lease. */
        LEASED,
        /** A completion candidate is under review. */
        CANDIDATE,
        /** The worker reported it cannot proceed. */
        BLOCKED,
        /** The worker gave up. */
        FAILED,
        /** The coordinator cancelled the attempt. */
        CANCELLED,
        /** The coordinator declared the lease expired. */
        EXPIRED,
        /** The coordinator accepted a candidate; terminal for the task. */
        ACCEPTED
    }

    /** An immutable snapshot of one task's reduced state. */
    public record TaskState(String taskId, Phase phase, int attempt, String holder,
                            int candidateRevision, int lastProgressSeq,
                            int lastCheckpointSeq) {
    }

    /** The reduction outcome: every finding, plus the final state of every task. */
    public record Result(List<Finding> findings, Map<String, TaskState> tasks) {

        /**
         * Returns whether the transcript reduced without a single finding.
         *
         * @return true when the transcript is a legal delegation recording
         */
        public boolean clean() {
            return findings.isEmpty();
        }
    }

    /** Mutable per-task reduction state. */
    private static final class TaskTrack {
        private final String taskId;
        private Phase phase;
        private int lastOfferedAttempt;
        private int attempt;
        private String offeree = "";
        private String holder = "";
        private TaskSpec spec;
        private Timestamp leaseExpiry;
        private int candidateRevision;
        private int expectedRevision = 1;
        private int lastProgressSeq;
        private int lastCheckpointSeq;
        private final Map<String, String> checkpoints = new HashMap<>();

        private TaskTrack(String taskId) {
            this.taskId = taskId;
        }

        private boolean attemptTerminal() {
            return phase == Phase.REJECTED || phase == Phase.BLOCKED
                    || phase == Phase.FAILED || phase == Phase.CANCELLED
                    || phase == Phase.EXPIRED;
        }

        private TaskState snapshot() {
            return new TaskState(taskId, phase, attempt, holder, candidateRevision,
                    lastProgressSeq, lastCheckpointSeq);
        }
    }

    /** One worker session's admission state. */
    private static final class Session {
        private boolean admitted;
    }

    /**
     * Reduces {@code transcript} in recorded order.
     *
     * @param transcript the recorded frames; never reordered or filtered
     * @return every finding in transcript order plus the final task states
     */
    public Result reduce(Transcript transcript) {
        Objects.requireNonNull(transcript, "transcript");
        List<Finding> findings = new ArrayList<>();
        Map<String, Session> sessions = new HashMap<>();
        Map<String, TaskTrack> tasks = new LinkedHashMap<>();
        Map<String, ByteString> seen = new HashMap<>();
        Map<String, Long> expectedSeq = new HashMap<>();
        for (TranscriptEntry entry : transcript.getEntriesList()) {
            reduce(entry, sessions, tasks, seen, expectedSeq, findings);
        }
        Map<String, TaskState> states = new LinkedHashMap<>();
        tasks.forEach((taskId, track) -> states.put(taskId, track.snapshot()));
        return new Result(List.copyOf(findings), Map.copyOf(states));
    }

    private static void reduce(TranscriptEntry entry, Map<String, Session> sessions,
                               Map<String, TaskTrack> tasks,
                               Map<String, ByteString> seen,
                               Map<String, Long> expectedSeq, List<Finding> findings) {
        // Structural validation first: a malformed frame cannot drive state.
        try {
            DelegationValidation.validate(entry);
        } catch (IllegalArgumentException e) {
            findings.add(new Finding(frameTaskId(entry), frameId(entry), "envelope",
                    e.getMessage()));
            return;
        }
        String frameId = frameId(entry);
        ByteString bytes = entry.toByteString();

        // Idempotent redelivery: identical bytes under a known frame id replay
        // silently; changed bytes under a known id are a conflicting duplicate and
        // never drive state.
        ByteString previous = seen.get(frameId);
        if (previous != null) {
            if (!previous.equals(bytes)) {
                findings.add(new Finding(frameTaskId(entry), frameId, "duplicate",
                        "frame id was already recorded with a different payload; the"
                                + " conflicting duplicate is ignored"));
            }
            return;
        }
        seen.put(frameId, bytes);

        boolean workerLane = entry.getLane() == Lane.LANE_WORKER;
        DelegateRequest request = workerLane ? entry.getWorkerFrame() : null;
        DelegateResponse response = workerLane ? null : entry.getCoordinatorFrame();
        String taskId = frameTaskId(entry);
        TaskTrack task = taskId.isEmpty() ? null : tasks.get(taskId);
        int attempt = attemptOf(request, response);

        // Replay-safe ordering inside the worker stream's (lane, task, attempt) scope.
        String seqKey = entry.getWorkerId() + "\n" + entry.getLane() + "\n"
                + taskId + "\n" + attempt;
        long seq = workerLane ? request.getSeq() : response.getSeq();
        long expected = expectedSeq.getOrDefault(seqKey, 1L);
        if (seq < expected) {
            findings.add(new Finding(taskId, frameId, "sequence",
                    "frame seq " + seq + " rewinds the scope's next expected seq "
                            + expected + "; replayed or reordered delivery"));
        } else if (seq > expected) {
            findings.add(new Finding(taskId, frameId, "sequence",
                    "frame seq " + seq + " skips expected seq " + expected
                            + "; the intervening frames were lost or never sent"));
        }
        expectedSeq.put(seqKey, Math.max(expected, seq) + 1);

        if (workerLane) {
            reduceWorker(entry.getWorkerId(), request, task, sessions, tasks,
                    findings);
        } else {
            reduceCoordinator(entry.getWorkerId(), response, task, sessions, tasks,
                    findings);
        }
    }

    private static void reduceWorker(String workerId, DelegateRequest frame,
                                     TaskTrack task, Map<String, Session> sessions,
                                     Map<String, TaskTrack> tasks,
                                     List<Finding> findings) {
        String taskId = frame.getTaskId();
        String frameId = frame.getFrameId();
        switch (frame.getPayloadCase()) {
            case HELLO -> {
                if (!frame.getHello().getWorkerId().equals(workerId)) {
                    findings.add(new Finding("", frameId, "session",
                            "hello advertises worker '" + frame.getHello().getWorkerId()
                                    + "' but the stream belongs to '" + workerId + "'"));
                    return;
                }
                // A repeat hello is a reconnect: the session re-keys and requires a
                // fresh admission before any task frame.
                sessions.put(workerId, new Session());
            }
            default -> {
                Session session = sessions.get(workerId);
                if (session == null || !session.admitted) {
                    findings.add(new Finding(taskId, frameId, "session",
                            "worker '" + workerId + "' sent a task frame before being"
                                    + " admitted"));
                    return;
                }
                if (task == null) {
                    findings.add(new Finding(taskId, frameId, "transition",
                            "frame names a task that was never offered"));
                    return;
                }
                reduceWorkerTask(workerId, frame, task, findings);
            }
        }
    }

    private static void reduceWorkerTask(String workerId, DelegateRequest frame,
                                         TaskTrack task, List<Finding> findings) {
        String taskId = frame.getTaskId();
        String frameId = frame.getFrameId();
        if (task.phase == Phase.ACCEPTED) {
            findings.add(new Finding(taskId, frameId, "terminal",
                    "the task is accepted; no worker frame may follow"));
            return;
        }
        switch (frame.getPayloadCase()) {
            case ACCEPT -> {
                int attempt = frame.getAccept().getAttempt();
                if (task.phase != Phase.OFFERED || attempt != task.attempt) {
                    findings.add(new Finding(taskId, frameId, "transition",
                            "accept of attempt " + attempt + " but the task is "
                                    + task.phase + " on attempt " + task.attempt));
                    return;
                }
                if (!workerId.equals(task.offeree)) {
                    findings.add(new Finding(taskId, frameId, "lease",
                            "worker '" + workerId + "' accepted an offer addressed to '"
                                    + task.offeree + "'"));
                    return;
                }
                task.phase = Phase.LEASED;
                task.holder = workerId;
                task.expectedRevision = 1;
                task.candidateRevision = 0;
                task.lastProgressSeq = 0;
                task.lastCheckpointSeq = 0;
            }
            case REJECT -> {
                int attempt = frame.getReject().getAttempt();
                if (task.phase != Phase.OFFERED || attempt != task.attempt) {
                    findings.add(new Finding(taskId, frameId, "transition",
                            "reject of attempt " + attempt + " but the task is "
                                    + task.phase + " on attempt " + task.attempt));
                    return;
                }
                if (!workerId.equals(task.offeree)) {
                    findings.add(new Finding(taskId, frameId, "lease",
                            "worker '" + workerId + "' rejected an offer addressed to '"
                                    + task.offeree + "'"));
                    return;
                }
                task.phase = Phase.REJECTED;
            }
            case HEARTBEAT -> {
                int attempt = frame.getHeartbeat().getAttempt();
                if (!leasedTo(task, workerId, attempt)) {
                    findings.add(new Finding(taskId, frameId, "lease",
                            leaseProblem(task, workerId, attempt, "heartbeat")));
                }
            }
            case PROGRESS -> {
                int attempt = frame.getProgress().getAttempt();
                if (!leasedTo(task, workerId, attempt)) {
                    findings.add(new Finding(taskId, frameId, "lease",
                            leaseProblem(task, workerId, attempt, "progress")));
                    return;
                }
                if (task.phase != Phase.LEASED) {
                    findings.add(new Finding(taskId, frameId, "transition",
                            "progress while the task is " + task.phase
                                    + "; the worker is not actively working"));
                    return;
                }
                int seq = frame.getProgress().getProgressSeq();
                if (seq <= task.lastProgressSeq) {
                    findings.add(new Finding(taskId, frameId, "progress",
                            "progress_seq " + seq + " does not advance past "
                                    + task.lastProgressSeq + "; progress is monotonic"));
                } else if (seq > task.lastProgressSeq + 1) {
                    findings.add(new Finding(taskId, frameId, "progress",
                            "progress_seq " + seq + " skips "
                                    + (task.lastProgressSeq + 1)));
                }
                task.lastProgressSeq = Math.max(task.lastProgressSeq, seq);
            }
            case CHECKPOINT -> {
                Checkpoint checkpoint = frame.getCheckpoint();
                if (!leasedTo(task, workerId, checkpoint.getAttempt())) {
                    findings.add(new Finding(taskId, frameId, "lease",
                            leaseProblem(task, workerId, checkpoint.getAttempt(),
                                    "checkpoint")));
                    return;
                }
                if (task.phase != Phase.LEASED) {
                    findings.add(new Finding(taskId, frameId, "transition",
                            "checkpoint while the task is " + task.phase));
                    return;
                }
                int seq = checkpoint.getCheckpointSeq();
                if (seq <= task.lastCheckpointSeq) {
                    findings.add(new Finding(taskId, frameId, "checkpoint",
                            "checkpoint_seq " + seq + " regresses from "
                                    + task.lastCheckpointSeq
                                    + "; checkpoints never rewind"));
                    return;
                }
                if (seq > task.lastCheckpointSeq + 1) {
                    findings.add(new Finding(taskId, frameId, "checkpoint",
                            "checkpoint_seq " + seq + " skips "
                                    + (task.lastCheckpointSeq + 1)));
                }
                task.lastCheckpointSeq = seq;
                task.checkpoints.put(checkpoint.getAttempt() + ":" + seq,
                        checkpoint.getResumeToken());
            }
            case BLOCKED -> {
                int attempt = frame.getBlocked().getAttempt();
                if (!leasedTo(task, workerId, attempt) || task.phase != Phase.LEASED) {
                    findings.add(new Finding(taskId, frameId, "transition",
                            "blocked report of attempt " + attempt
                                    + " but the task is " + task.phase
                                    + " on attempt " + task.attempt + " held by '"
                                    + task.holder + "'"));
                    return;
                }
                task.phase = Phase.BLOCKED;
            }
            case FAILED -> {
                int attempt = frame.getFailed().getAttempt();
                if (!leasedTo(task, workerId, attempt)
                        || (task.phase != Phase.LEASED
                        && task.phase != Phase.CANDIDATE)) {
                    findings.add(new Finding(taskId, frameId, "transition",
                            "failure report of attempt " + attempt
                                    + " but the task is " + task.phase
                                    + " on attempt " + task.attempt + " held by '"
                                    + task.holder + "'"));
                    return;
                }
                task.phase = Phase.FAILED;
            }
            case CANCELLED -> {
                int attempt = frame.getCancelled().getAttempt();
                if (task.phase != Phase.CANCELLED || attempt != task.attempt) {
                    findings.add(new Finding(taskId, frameId, "transition",
                            "cancelled notice of attempt " + attempt
                                    + " but no cancellation of that attempt is in"
                                    + " flight (task is " + task.phase + ")"));
                }
            }
            case COMPLETION -> {
                reduceCompletion(workerId, frame, task, findings);
            }
            case TASK_MESSAGE -> {
                // Non-transitioning by contract: recorded and sequenced like any
                // frame, but it never moves the lifecycle. The named sender must
                // be the worker whose stream carried the frame.
                if (!frame.getTaskMessage().getSender().equals(workerId)) {
                    findings.add(new Finding(taskId, frameId, "session",
                            "task message names sender '"
                                    + frame.getTaskMessage().getSender()
                                    + "' but the stream belongs to '" + workerId + "'"));
                }
            }
            default -> findings.add(new Finding(taskId, frameId, "transition",
                    "unexpected worker payload " + frame.getPayloadCase()));
        }
    }

    private static void reduceCompletion(String workerId, DelegateRequest frame,
                                         TaskTrack task, List<Finding> findings) {
        String taskId = frame.getTaskId();
        String frameId = frame.getFrameId();
        CompletionCandidate candidate = frame.getCompletion();
        if (task.phase == Phase.CANCELLED) {
            findings.add(new Finding(taskId, frameId, "terminal",
                    "completion candidate arrived after the coordinator cancelled"
                            + " attempt " + task.attempt
                            + "; the candidate races the cancellation and loses"));
            return;
        }
        if (task.attemptTerminal()) {
            findings.add(new Finding(taskId, frameId, "terminal",
                    "completion candidate but attempt " + task.attempt + " is already "
                            + task.phase));
            return;
        }
        if (task.phase != Phase.LEASED) {
            findings.add(new Finding(taskId, frameId, "transition",
                    "completion candidate while the task is " + task.phase
                            + "; only a leased worker can submit one"));
            return;
        }
        if (!workerId.equals(task.holder)) {
            findings.add(new Finding(taskId, frameId, "lease",
                    "completion candidate from worker '" + workerId
                            + "' but the lease is held by '" + task.holder + "'"));
            return;
        }
        if (candidate.getAttempt() != task.attempt) {
            findings.add(new Finding(taskId, frameId, "lease",
                    "completion candidate for stale attempt "
                            + candidate.getAttempt() + "; the current attempt is "
                            + task.attempt));
            return;
        }
        if (candidate.getRevision() != task.expectedRevision) {
            findings.add(new Finding(taskId, frameId, "revision",
                    "candidate revision " + candidate.getRevision()
                            + " but the coordinator awaits revision "
                            + task.expectedRevision
                            + "; stale or skipped revisions are not reviewable"));
            return;
        }
        if (!verifyEvidence(candidate, task, frameId, findings)) {
            return;
        }
        task.phase = Phase.CANDIDATE;
        task.candidateRevision = candidate.getRevision();
    }

    private static boolean verifyEvidence(CompletionCandidate candidate, TaskTrack task,
                                          String frameId, List<Finding> findings) {
        int initialFindings = findings.size();
        Set<String> required = new LinkedHashSet<>();
        if (task.spec != null) {
            task.spec.getRequiredChecksList()
                    .forEach(check -> required.add(check.getName()));
        }
        Set<String> evidenced = new LinkedHashSet<>();
        for (CheckEvidence evidence : candidate.getEvidenceList()) {
            evidenced.add(evidence.getCheckName());
            if (!required.contains(evidence.getCheckName())) {
                findings.add(new Finding(task.taskId, frameId, "evidence",
                        "evidence names check '" + evidence.getCheckName()
                                + "' which the offer's spec does not require"));
            }
            if (evidence.getVerdict() != CheckVerdict.CHECK_VERDICT_PASSED) {
                findings.add(new Finding(task.taskId, frameId, "evidence",
                        "evidence for check '" + evidence.getCheckName()
                                + "' reports " + evidence.getVerdict()
                                + "; a candidate must prove passing checks"));
            }
        }
        Set<String> missing = new LinkedHashSet<>(required);
        missing.removeAll(evidenced);
        if (!missing.isEmpty()) {
            findings.add(new Finding(task.taskId, frameId, "evidence",
                    "the candidate carries no evidence for required checks "
                            + missing + "; every required check must be proven"));
        }
        return findings.size() == initialFindings;
    }

    private static void reduceCoordinator(String workerId, DelegateResponse frame,
                                          TaskTrack task,
                                          Map<String, Session> sessions,
                                          Map<String, TaskTrack> tasks,
                                          List<Finding> findings) {
        String taskId = frame.getTaskId();
        String frameId = frame.getFrameId();
        switch (frame.getPayloadCase()) {
            case ADMISSION -> {
                Session session = sessions.get(workerId);
                if (session == null) {
                    findings.add(new Finding("", frameId, "session",
                            "admission for worker '" + workerId
                                    + "' that never said hello"));
                    return;
                }
                session.admitted = frame.getAdmission().getAdmitted();
            }
            case OFFER -> {
                reduceOffer(workerId, frame, task, sessions, tasks, findings);
            }
            default -> {
                if (task == null) {
                    findings.add(new Finding(taskId, frameId, "transition",
                            "frame names a task that was never offered"));
                    return;
                }
                if (task.phase == Phase.ACCEPTED) {
                    findings.add(new Finding(taskId, frameId, "terminal",
                            "the task is accepted; no coordinator frame may follow"));
                    return;
                }
                reduceCoordinatorTask(workerId, frame, task, findings);
            }
        }
    }

    private static void reduceOffer(String workerId, DelegateResponse frame,
                                    TaskTrack task, Map<String, Session> sessions,
                                    Map<String, TaskTrack> tasks,
                                    List<Finding> findings) {
        String taskId = frame.getTaskId();
        String frameId = frame.getFrameId();
        TaskOffer offer = frame.getOffer();
        Session session = sessions.get(workerId);
        if (session == null || !session.admitted) {
            findings.add(new Finding(taskId, frameId, "session",
                    "offer addressed to worker '" + workerId
                            + "' that was never admitted"));
            return;
        }
        if (task == null) {
            task = new TaskTrack(taskId);
            tasks.put(taskId, task);
        }
        if (task.phase == Phase.ACCEPTED) {
            findings.add(new Finding(taskId, frameId, "terminal",
                    "the task is accepted; re-offering it is meaningless"));
            return;
        }
        if (task.phase != null && !task.attemptTerminal()) {
            findings.add(new Finding(taskId, frameId, "transition",
                    "offer while the task is " + task.phase + " on attempt "
                            + task.attempt
                            + "; end the open attempt before re-offering"));
            return;
        }
        if (offer.getAttempt() != task.lastOfferedAttempt + 1) {
            findings.add(new Finding(taskId, frameId, "transition",
                    "offer attempt " + offer.getAttempt() + " but the last offer was"
                            + " attempt " + task.lastOfferedAttempt
                            + "; attempts increment by exactly one"));
            return;
        }
        if (offer.hasResumeFrom() && !resolvesTo(task, offer.getResumeFrom(),
                offer.getAttempt(), findings, frameId)) {
            return;
        }
        task.phase = Phase.OFFERED;
        task.attempt = offer.getAttempt();
        task.lastOfferedAttempt = offer.getAttempt();
        task.offeree = workerId;
        task.holder = "";
        task.spec = offer.getSpec();
        task.leaseExpiry = offer.getExpiresAt();
    }

    private static boolean resolvesTo(TaskTrack task, CheckpointReference reference,
                                      int offerAttempt, List<Finding> findings,
                                      String frameId) {
        if (reference.getAttempt() >= offerAttempt) {
            findings.add(new Finding(task.taskId, frameId, "checkpoint",
                    "resume_from points at attempt " + reference.getAttempt()
                            + " which is not before the offered attempt "
                            + offerAttempt));
            return false;
        }
        String token = task.checkpoints.get(
                reference.getAttempt() + ":" + reference.getCheckpointSeq());
        if (token == null) {
            findings.add(new Finding(task.taskId, frameId, "checkpoint",
                    "resume_from names checkpoint " + reference.getAttempt() + ":"
                            + reference.getCheckpointSeq()
                            + " which the task never recorded"));
            return false;
        }
        if (!token.equals(reference.getResumeToken())) {
            findings.add(new Finding(task.taskId, frameId, "checkpoint",
                    "resume_from token does not match the recorded checkpoint "
                            + reference.getAttempt() + ":"
                            + reference.getCheckpointSeq()));
            return false;
        }
        return true;
    }

    private static void reduceCoordinatorTask(String workerId, DelegateResponse frame,
                                              TaskTrack task, List<Finding> findings) {
        String taskId = frame.getTaskId();
        String frameId = frame.getFrameId();
        switch (frame.getPayloadCase()) {
            case RENEWAL -> {
                int attempt = frame.getRenewal().getAttempt();
                if ((task.phase != Phase.LEASED && task.phase != Phase.CANDIDATE)
                        || attempt != task.attempt) {
                    findings.add(new Finding(taskId, frameId, "lease",
                            "renewal of attempt " + attempt + " but the task is "
                                    + task.phase + " on attempt " + task.attempt));
                    return;
                }
                Timestamp expiry = frame.getRenewal().getExpiresAt();
                if (task.leaseExpiry != null
                        && Timestamps.compare(expiry, task.leaseExpiry) <= 0) {
                    findings.add(new Finding(taskId, frameId, "lease",
                            "renewal expires_at " + Timestamps.toString(expiry)
                                    + " does not advance past the declared expiry "
                                    + Timestamps.toString(task.leaseExpiry)));
                    return;
                }
                task.leaseExpiry = expiry;
            }
            case EXPIRED -> {
                int attempt = frame.getExpired().getAttempt();
                if ((task.phase != Phase.LEASED && task.phase != Phase.CANDIDATE)
                        || attempt != task.attempt) {
                    findings.add(new Finding(taskId, frameId, "lease",
                            "expiry of attempt " + attempt + " but the task is "
                                    + task.phase + " on attempt " + task.attempt
                                    + "; there is no live lease to expire"));
                    return;
                }
                task.phase = Phase.EXPIRED;
            }
            case CANCELLATION -> {
                int attempt = frame.getCancellation().getAttempt();
                if (task.phase != Phase.OFFERED && task.phase != Phase.LEASED
                        && task.phase != Phase.CANDIDATE) {
                    findings.add(new Finding(taskId, frameId, "transition",
                            "cancellation but the task is " + task.phase
                                    + "; there is no open attempt to cancel"));
                    return;
                }
                if (attempt != task.attempt) {
                    findings.add(new Finding(taskId, frameId, "transition",
                            "cancellation of attempt " + attempt
                                    + " but the open attempt is " + task.attempt));
                    return;
                }
                task.phase = Phase.CANCELLED;
            }
            case REVISION_REQUESTED -> {
                int attempt = frame.getRevisionRequested().getAttempt();
                int revision = frame.getRevisionRequested().getRevision();
                if (attempt != task.attempt || task.phase != Phase.CANDIDATE
                        || revision != task.candidateRevision) {
                    findings.add(new Finding(taskId, frameId, "revision",
                            "revision request for attempt " + attempt
                                    + " revision " + revision
                                    + " but the task is " + task.phase
                                    + " on attempt " + task.attempt
                                    + " with open candidate revision "
                                    + task.candidateRevision));
                    return;
                }
                task.phase = Phase.LEASED;
                task.expectedRevision = revision + 1;
            }
            case ACCEPTED -> {
                int attempt = frame.getAccepted().getAttempt();
                int revision = frame.getAccepted().getRevision();
                if (attempt != task.attempt || task.phase != Phase.CANDIDATE
                        || revision != task.candidateRevision) {
                    findings.add(new Finding(taskId, frameId, "revision",
                            "acceptance of attempt " + attempt + " revision " + revision
                                    + " but the task is " + task.phase
                                    + " on attempt " + task.attempt
                                    + " with open candidate revision " + task.candidateRevision));
                    return;
                }
                task.phase = Phase.ACCEPTED;
            }
            case TASK_MESSAGE -> {
                // Non-transitioning by contract: recorded and sequenced like any
                // frame, but it never moves the lifecycle. The coordinator is the
                // only sender on this lane, and the message must address the
                // worker whose stream carried it.
                if (!frame.getTaskMessage().getSender().equals(DelegationValidation.COORDINATOR)
                        || !frame.getTaskMessage().getRecipient().equals(workerId)) {
                    findings.add(new Finding(taskId, frameId, "session",
                            "coordinator task message must name sender 'coordinator'"
                                    + " and recipient '" + workerId + "'"));
                }
            }
            default -> findings.add(new Finding(taskId, frameId, "transition",
                    "unexpected coordinator payload " + frame.getPayloadCase()));
        }
    }

    /** Whether the worker currently holds the lease for the given attempt. */
    private static boolean leasedTo(TaskTrack task, String workerId, int attempt) {
        return (task.phase == Phase.LEASED || task.phase == Phase.CANDIDATE)
                && attempt == task.attempt && workerId.equals(task.holder);
    }

    private static String leaseProblem(TaskTrack task, String workerId, int attempt,
                                       String payload) {
        if (task.phase == Phase.CANCELLED) {
            return payload + " for attempt " + attempt
                    + " arrived after the coordinator cancelled it";
        }
        if (task.attemptTerminal()) {
            return payload + " for attempt " + attempt + " but that attempt is "
                    + task.phase;
        }
        if (attempt != task.attempt) {
            return payload + " for stale attempt " + attempt
                    + "; the current attempt is " + task.attempt;
        }
        return payload + " from worker '" + workerId
                + "' but the lease is held by '" + task.holder + "'";
    }

    private static String frameId(TranscriptEntry entry) {
        return entry.getLane() == Lane.LANE_WORKER
                ? entry.getWorkerFrame().getFrameId()
                : entry.getCoordinatorFrame().getFrameId();
    }

    private static String frameTaskId(TranscriptEntry entry) {
        if (entry.getLane() == Lane.LANE_WORKER) {
            return entry.hasWorkerFrame() ? entry.getWorkerFrame().getTaskId() : "";
        }
        return entry.hasCoordinatorFrame()
                ? entry.getCoordinatorFrame().getTaskId() : "";
    }

    /** The attempt a frame belongs to, for sequencing; 0 on the session scope. */
    private static int attemptOf(DelegateRequest request, DelegateResponse response) {
        if (request != null) {
            return switch (request.getPayloadCase()) {
                case HELLO -> 0;
                case ACCEPT -> request.getAccept().getAttempt();
                case REJECT -> request.getReject().getAttempt();
                case HEARTBEAT -> request.getHeartbeat().getAttempt();
                case PROGRESS -> request.getProgress().getAttempt();
                case CHECKPOINT -> request.getCheckpoint().getAttempt();
                case BLOCKED -> request.getBlocked().getAttempt();
                case FAILED -> request.getFailed().getAttempt();
                case CANCELLED -> request.getCancelled().getAttempt();
                case COMPLETION -> request.getCompletion().getAttempt();
                // Task messages carry no attempt; they sequence in the task's
                // attempt-0 scope.
                case TASK_MESSAGE -> 0;
                default -> 0;
            };
        }
        return switch (response.getPayloadCase()) {
            case ADMISSION -> 0;
            case OFFER -> response.getOffer().getAttempt();
            case RENEWAL -> response.getRenewal().getAttempt();
            case EXPIRED -> response.getExpired().getAttempt();
            case CANCELLATION -> response.getCancellation().getAttempt();
            case REVISION_REQUESTED -> response.getRevisionRequested().getAttempt();
            case ACCEPTED -> response.getAccepted().getAttempt();
            case TASK_MESSAGE -> 0;
            default -> 0;
        };
    }
}
