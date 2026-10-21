package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;
import ai.protomolt.proto.delegation.v1.AdmissionDecision;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The coordinator's mutable in-memory projection: worker sessions, task runtimes, the
 * deduplicated worker-frame payloads, the recorded transcript entries, and the
 * cursor-addressable event feed derived from them, plus the closed and
 * publication-failed latches.
 *
 * <p>This class owns no synchronization of its own. Every read and write must happen
 * while holding the owning {@link InProcessDelegationCoordinator}'s monitor, so the
 * coordinator remains a single-monitor state machine across live operation and replay.
 * The wire transcript stays the source of truth; this projection is rebuilt from it by
 * {@link DelegationReplay} and checked by {@link DelegationReducer} after every accepted
 * frame.
 */
final class DelegationCoordinatorState {

    final Map<String, Session> sessions = new LinkedHashMap<>();
    final Map<String, TaskRuntime> tasks = new LinkedHashMap<>();
    final Map<String, ByteString> workerFrames = new HashMap<>();
    final List<TranscriptEntry> entries = new ArrayList<>();
    final List<InProcessDelegationCoordinator.Event> events = new ArrayList<>();
    long cursor;
    boolean closed;
    boolean publicationFailed;

    /**
     * Monotonic version of {@link #entries}, bumped by every {@link #record} call. Readers
     * that derive a projection from the full transcript (such as the review runtime's
     * reduction cache) compare versions instead of re-deriving when nothing was appended.
     */
    long transcriptVersion;

    /**
     * Records one committed entry and its cursor-addressable event. Call only after the
     * transcript carrying the entry has been validated, reduced clean, and persisted.
     */
    void record(TranscriptEntry entry) {
        entries.add(entry);
        transcriptVersion++;
        events.add(new InProcessDelegationCoordinator.Event(++cursor, entry));
    }

    /** Fails when the coordinator is closed or its transcript publication has failed. */
    void requireOpen() {
        if (closed) {
            throw new IllegalStateException("coordinator is closed");
        }
        if (publicationFailed) {
            throw new IllegalStateException("transcript publication is unavailable");
        }
    }

    /**
     * The session of an admitted worker whether or not its stream is live. Coordinator
     * frames that settle an attempt (verdicts, cancellations, expiries, messages) are
     * recorded and sequenced against this session; a worker whose stream is down
     * reads them from the durable event feed when it reconnects, so a review does
     * not wait on the worker's connection.
     */
    Session requireAdmittedWorker(String workerId) {
        Session session = sessions.get(workerId);
        if (session == null || !session.admitted) {
            throw new IllegalStateException("worker is not admitted: " + workerId);
        }
        return session;
    }

    /** The session of an admitted worker whose stream must also be live. */
    Session requireAdmittedSession(String workerId) {
        Session session = requireAdmittedWorker(workerId);
        if (!session.connected) {
            throw new IllegalStateException(
                    "worker is not admitted and connected: " + workerId);
        }
        return session;
    }

    /** The named task's runtime, or {@link IllegalArgumentException} when unknown. */
    TaskRuntime requireTask(String taskId) {
        TaskRuntime task = tasks.get(taskId);
        if (task == null) {
            throw new IllegalArgumentException("unknown task: " + taskId);
        }
        return task;
    }

    /** During replay, the named task must already exist and belong to this worker. */
    TaskRuntime requireRestoredTask(String taskId, String workerId) {
        TaskRuntime task = tasks.get(taskId);
        if (task == null || !workerId.equals(task.workerId)) {
            throw new IllegalArgumentException(
                    "stored transcript references a task before its offer: " + taskId);
        }
        return task;
    }

    /** One worker stream's registration and sender-side coordinator sequence state. */
    static final class Session {
        final String workerId;
        final WorkerHello hello;
        final StreamObserver<DelegateResponse> responses;
        final Map<String, Long> coordinatorSequences = new HashMap<>();
        boolean admitted;
        boolean connected = true;
        AdmissionDecision pendingAdmission;

        Session(WorkerHello hello, StreamObserver<DelegateResponse> responses) {
            this.workerId = hello.getWorkerId();
            this.hello = hello;
            this.responses = responses;
        }

        long peekNextCoordinatorSeq(String taskId, int attempt) {
            String key = taskId + "\n" + attempt;
            return coordinatorSequences.getOrDefault(key, 0L) + 1L;
        }

        void commitCoordinatorSeq(String taskId, int attempt, long seq) {
            String key = taskId + "\n" + attempt;
            coordinatorSequences.put(key, seq);
        }

        void restoreCoordinatorSeq(String taskId, int attempt, long seq) {
            String key = taskId + "\n" + attempt;
            coordinatorSequences.merge(key, seq, Math::max);
        }
    }

    /** One task's live attempt projection: owner, offer, phase, lease, and candidate. */
    static final class TaskRuntime {
        final String taskId;
        String workerId;
        int attempt;
        TaskOffer offer;
        DelegationReducer.Phase phase;
        Instant expiry;
        long leaseGeneration;
        CompletionCandidate candidate;

        TaskRuntime(String taskId) {
            this.taskId = taskId;
        }

        boolean attemptTerminal() {
            return phase == DelegationReducer.Phase.REJECTED
                    || phase == DelegationReducer.Phase.BLOCKED
                    || phase == DelegationReducer.Phase.FAILED
                    || phase == DelegationReducer.Phase.CANCELLED
                    || phase == DelegationReducer.Phase.EXPIRED;
        }
    }
}
