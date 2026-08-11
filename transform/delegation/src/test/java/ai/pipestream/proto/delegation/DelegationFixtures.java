package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.AcceptanceCheck;
import ai.pipestream.proto.delegation.v1.AdmissionDecision;
import ai.pipestream.proto.delegation.v1.BlockedReport;
import ai.pipestream.proto.delegation.v1.CancelledNotice;
import ai.pipestream.proto.delegation.v1.Cancellation;
import ai.pipestream.proto.delegation.v1.CheckEvidence;
import ai.pipestream.proto.delegation.v1.CheckVerdict;
import ai.pipestream.proto.delegation.v1.Checkpoint;
import ai.pipestream.proto.delegation.v1.CheckpointReference;
import ai.pipestream.proto.delegation.v1.CommitReference;
import ai.pipestream.proto.delegation.v1.CompletionAccepted;
import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import ai.pipestream.proto.delegation.v1.DelegateRequest;
import ai.pipestream.proto.delegation.v1.DelegateResponse;
import ai.pipestream.proto.delegation.v1.FailureReport;
import ai.pipestream.proto.delegation.v1.Heartbeat;
import ai.pipestream.proto.delegation.v1.Lane;
import ai.pipestream.proto.delegation.v1.LeaseExpired;
import ai.pipestream.proto.delegation.v1.LeaseRenewal;
import ai.pipestream.proto.delegation.v1.ProgressEvent;
import ai.pipestream.proto.delegation.v1.RevisionRequested;
import ai.pipestream.proto.delegation.v1.TaskAccept;
import ai.pipestream.proto.delegation.v1.TaskOffer;
import ai.pipestream.proto.delegation.v1.TaskReject;
import ai.pipestream.proto.delegation.v1.TaskSpec;
import ai.pipestream.proto.delegation.v1.Transcript;
import ai.pipestream.proto.delegation.v1.TranscriptEntry;
import ai.pipestream.proto.delegation.v1.WorkerCapability;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import ai.pipestream.proto.grpc.recipe.v1.ArtifactReference;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Builders for delegation transcripts and their payloads. The {@link TranscriptBuilder}
 * allocates fresh frame ids and assigns each frame the next sequence of its (lane, task,
 * attempt) scope exactly the way a well-behaved stream would, so tests read as the
 * scenario they describe; the raw append hooks exist for the dishonest scenarios.
 */
final class DelegationFixtures {

    static final String WORKER = "worker-sol-1";
    static final String SECOND_WORKER = "worker-kimi-1";
    static final String TASK = uuid("task-1");

    private DelegationFixtures() {
    }

    /** A deterministic uuid derived from a test-local seed. */
    static String uuid(String seed) {
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
    }

    /** A deterministic 64-hex fingerprint derived from a test-local seed. */
    static String fingerprint(String seed) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(seed.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A task spec with one scope entry, one constraint, and the named checks. */
    static TaskSpec spec(String... checks) {
        TaskSpec.Builder spec = TaskSpec.newBuilder()
                .setObjective("Implement the bounded change and prove it")
                .addAllowedScope("transform/delegation/**")
                .addConstraints("in-process tests only");
        for (String check : checks) {
            spec.addRequiredChecks(AcceptanceCheck.newBuilder()
                    .setName(check)
                    .setDescription("the " + check + " check passes"));
        }
        return spec.build();
    }

    /** Passing evidence for one check. */
    static CheckEvidence evidence(String check) {
        return CheckEvidence.newBuilder()
                .setCheckName(check)
                .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                .setRanAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).build())
                .setDetail(check + " ran clean")
                .build();
    }

    /** A commit reference with a deterministic full SHA-1. */
    static CommitReference commit(String seed) {
        return CommitReference.newBuilder()
                .setRepository("git.rokkon.com/ai-pipestream/protomolt")
                .setCommit(fingerprint(seed).substring(0, 40))
                .setSubject("delegation: " + seed)
                .build();
    }

    /** A content-addressed artifact reference. */
    static ArtifactReference artifact(String seed) {
        return ArtifactReference.newBuilder()
                .setSha256(fingerprint(seed))
                .setMediaType("text/plain")
                .setSizeBytes(128)
                .setRedacted(true)
                .build();
    }

    /**
     * Assembles a transcript entry by entry, allocating frame ids and per-scope
     * sequences the way a real pair of well-behaved peers would.
     */
    static final class TranscriptBuilder {
        private final List<TranscriptEntry> entries = new ArrayList<>();
        private final Map<String, Long> sequences = new HashMap<>();
        private final Map<String, Integer> currentAttempt = new HashMap<>();
        private int frameCounter;
        private long tick = 1_700_000_000L;

        TranscriptBuilder hello(String worker) {
            return worker(worker, "", 0, DelegateRequest.newBuilder()
                    .setHello(WorkerHello.newBuilder()
                            .setWorkerId(worker)
                            .setProtocolVersion(1)
                            .setProvider("sol")
                            .setModel("sol-large")
                            .addCapabilities(WorkerCapability.newBuilder()
                                    .setName("java-build"))));
        }

        TranscriptBuilder admit(String worker) {
            return coordinator(worker, "", 0, DelegateResponse.newBuilder()
                    .setAdmission(AdmissionDecision.newBuilder()
                            .setAdmitted(true)
                            .setSessionId(uuid("session-" + worker + "-" + frameCounter))));
        }

        TranscriptBuilder offer(String task, String worker, int attempt,
                                TaskSpec spec) {
            currentAttempt.put(task, attempt);
            return coordinator(worker, task, attempt, DelegateResponse.newBuilder()
                    .setOffer(TaskOffer.newBuilder()
                            .setAttempt(attempt)
                            .setSpec(spec)
                            .setLeaseDuration(Duration.newBuilder().setSeconds(300))
                            .setExpiresAt(nextStamp())));
        }

        TranscriptBuilder offerResuming(String task, String worker, int attempt,
                                        TaskSpec spec, CheckpointReference resumeFrom) {
            currentAttempt.put(task, attempt);
            return coordinator(worker, task, attempt, DelegateResponse.newBuilder()
                    .setOffer(TaskOffer.newBuilder()
                            .setAttempt(attempt)
                            .setSpec(spec)
                            .setLeaseDuration(Duration.newBuilder().setSeconds(300))
                            .setExpiresAt(nextStamp())
                            .setResumeFrom(resumeFrom)));
        }

        static CheckpointReference resumeFrom(int attempt, int checkpointSeq,
                                              String token) {
            return CheckpointReference.newBuilder()
                    .setAttempt(attempt)
                    .setCheckpointSeq(checkpointSeq)
                    .setResumeToken(token)
                    .build();
        }

        TranscriptBuilder accept(String task, String worker, int attempt) {
            return worker(worker, task, attempt, DelegateRequest.newBuilder()
                    .setAccept(TaskAccept.newBuilder().setAttempt(attempt)));
        }

        TranscriptBuilder reject(String task, String worker, int attempt,
                                 String reason) {
            return worker(worker, task, attempt, DelegateRequest.newBuilder()
                    .setReject(TaskReject.newBuilder()
                            .setAttempt(attempt)
                            .setReason(reason)
                            .setRetryable(true)));
        }

        TranscriptBuilder heartbeat(String task, String worker, int attempt) {
            return worker(worker, task, attempt, DelegateRequest.newBuilder()
                    .setHeartbeat(Heartbeat.newBuilder().setAttempt(attempt)));
        }

        TranscriptBuilder renew(String task, String worker, int attempt) {
            return coordinator(worker, task, attempt, DelegateResponse.newBuilder()
                    .setRenewal(LeaseRenewal.newBuilder()
                            .setAttempt(attempt)
                            .setExpiresAt(nextStampPlus(600))));
        }

        TranscriptBuilder expire(String task, String worker, int attempt) {
            return coordinator(worker, task, attempt, DelegateResponse.newBuilder()
                    .setExpired(LeaseExpired.newBuilder()
                            .setAttempt(attempt)
                            .setReason("no heartbeat within the lease")));
        }

        TranscriptBuilder progress(String task, String worker, int attempt,
                                   int progressSeq, String message) {
            return worker(worker, task, attempt, DelegateRequest.newBuilder()
                    .setProgress(ProgressEvent.newBuilder()
                            .setAttempt(attempt)
                            .setProgressSeq(progressSeq)
                            .setMessage(message)));
        }

        TranscriptBuilder checkpoint(String task, String worker, int attempt,
                                     int checkpointSeq, String token) {
            return worker(worker, task, attempt, DelegateRequest.newBuilder()
                    .setCheckpoint(Checkpoint.newBuilder()
                            .setAttempt(attempt)
                            .setCheckpointSeq(checkpointSeq)
                            .setResumeToken(token)));
        }

        TranscriptBuilder blocked(String task, String worker, int attempt,
                                  String reason) {
            return worker(worker, task, attempt, DelegateRequest.newBuilder()
                    .setBlocked(BlockedReport.newBuilder()
                            .setAttempt(attempt)
                            .setReason(reason)));
        }

        TranscriptBuilder failed(String task, String worker, int attempt,
                                 String error) {
            return worker(worker, task, attempt, DelegateRequest.newBuilder()
                    .setFailed(FailureReport.newBuilder()
                            .setAttempt(attempt)
                            .setError(error)));
        }

        TranscriptBuilder cancelledNotice(String task, String worker, int attempt) {
            return worker(worker, task, attempt, DelegateRequest.newBuilder()
                    .setCancelled(CancelledNotice.newBuilder().setAttempt(attempt)));
        }

        TranscriptBuilder cancel(String task, String worker, int attempt,
                                 String reason) {
            return coordinator(worker, task, attempt, DelegateResponse.newBuilder()
                    .setCancellation(Cancellation.newBuilder()
                            .setAttempt(attempt)
                            .setReason(reason)));
        }

        TranscriptBuilder candidate(String task, String worker, int revision,
                                    TaskSpec spec) {
            CompletionCandidate.Builder candidate = CompletionCandidate.newBuilder()
                    .setRevision(revision)
                    .setSummary("the bounded change is implemented and proven")
                    .addCommits(commit("task-output-" + revision));
            spec.getRequiredChecksList()
                    .forEach(check -> candidate.addEvidence(evidence(check.getName())));
            return worker(worker, task, currentAttempt.getOrDefault(task, 1),
                    DelegateRequest.newBuilder().setCompletion(candidate));
        }

        TranscriptBuilder candidateWith(String task, String worker, int revision,
                                        CompletionCandidate candidate) {
            return worker(worker, task, currentAttempt.getOrDefault(task, 1),
                    DelegateRequest.newBuilder().setCompletion(candidate));
        }

        TranscriptBuilder revisionRequested(String task, String worker, int revision,
                                            String feedback) {
            return coordinator(worker, task, currentAttempt.getOrDefault(task, 1),
                    DelegateResponse.newBuilder()
                            .setRevisionRequested(RevisionRequested.newBuilder()
                                    .setRevision(revision)
                                    .setFeedback(feedback)));
        }

        TranscriptBuilder accepted(String task, String worker, int revision,
                                   String verdict) {
            return coordinator(worker, task, currentAttempt.getOrDefault(task, 1),
                    DelegateResponse.newBuilder()
                            .setAccepted(CompletionAccepted.newBuilder()
                                    .setRevision(revision)
                                    .setVerdict(verdict)));
        }

        /** Appends an already-built entry unchanged (duplicate and ordering tests). */
        TranscriptBuilder append(TranscriptEntry entry) {
            entries.add(entry);
            return this;
        }

        /** The last appended entry, for building duplicates and conflicts. */
        TranscriptEntry lastEntry() {
            return entries.get(entries.size() - 1);
        }

        Transcript build() {
            return Transcript.newBuilder().addAllEntries(entries).build();
        }

        private TranscriptBuilder worker(String worker, String task, int attempt,
                                         DelegateRequest.Builder payload) {
            DelegateRequest frame = payload
                    .setFrameId(uuid("frame-" + (++frameCounter)))
                    .setTaskId(task)
                    .setSeq(nextSeq(Lane.LANE_WORKER, task, attempt))
                    .setSentAt(nextStamp())
                    .build();
            entries.add(TranscriptEntry.newBuilder()
                    .setLane(Lane.LANE_WORKER)
                    .setWorkerId(worker)
                    .setWorkerFrame(frame)
                    .build());
            return this;
        }

        private TranscriptBuilder coordinator(String worker, String task, int attempt,
                                              DelegateResponse.Builder payload) {
            DelegateResponse frame = payload
                    .setFrameId(uuid("frame-" + (++frameCounter)))
                    .setTaskId(task)
                    .setSeq(nextSeq(Lane.LANE_COORDINATOR, task, attempt))
                    .setSentAt(nextStamp())
                    .build();
            entries.add(TranscriptEntry.newBuilder()
                    .setLane(Lane.LANE_COORDINATOR)
                    .setWorkerId(worker)
                    .setCoordinatorFrame(frame)
                    .build());
            return this;
        }

        private long nextSeq(Lane lane, String task, int attempt) {
            String key = lane + "\n" + task + "\n" + attempt;
            long next = sequences.getOrDefault(key, 1L);
            sequences.put(key, next + 1);
            return next;
        }

        private Timestamp nextStamp() {
            return Timestamp.newBuilder().setSeconds(++tick).build();
        }

        private Timestamp nextStampPlus(long seconds) {
            return Timestamp.newBuilder().setSeconds(tick + seconds).build();
        }
    }
}
