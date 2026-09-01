package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.ClusterDirectory.ApplyOutcome;
import ai.pipestream.proto.mesh.cluster.ClusterEventRepository.StoredDirectory;
import ai.pipestream.proto.mesh.cluster.v1.CapacityAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.mesh.cluster.v1.ClusterSnapshot;
import ai.pipestream.proto.mesh.cluster.v1.NodeAdvertisement;
import ai.pipestream.proto.mesh.cluster.v1.NodePresence;
import ai.pipestream.proto.mesh.cluster.v1.NodeRecord;
import ai.pipestream.proto.mesh.cluster.v1.PresenceState;
import ai.pipestream.proto.mesh.cluster.v1.ProcessorAdvertisement;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Seeded, deterministic model fuzz of {@link PersistentClusterDirectory} over an in-memory
 * repository. The generator reads the live directory to build frames it believes valid
 * (fresh sequences, matching epochs, lease windows anchored on the fake clock) and mixes in
 * deliberately stale frames it expects to be refused, repository outages, and restarts.
 * Three invariants are the point:
 *
 * <ul>
 *   <li><b>The durable record always rebuilds.</b> After every operation a fresh facade is
 *       constructed over the same repository. If that throws, the log or checkpoint the
 *       directory just wrote cannot be restored, which is exactly what the next durable
 *       mutation would trip over.</li>
 *   <li><b>Between restarts the facade is a plain directory.</b> A reference
 *       {@link ClusterDirectory} receives the same operations in lockstep. Because
 *       {@link ClusterDirectory#adoptSoftState} carries every soft record across a rebuild,
 *       the two must agree on every outcome, every rejection, every sweep, and the complete
 *       snapshot, presence and lease windows included. Any daylight between them is a soft
 *       record the rebuild lost or a durable one it invented. A mutation whose save failed
 *       is withheld from the reference, so the same comparison proves a failed write leaks
 *       nothing.</li>
 *   <li><b>A restart preserves everything durable.</b> The restored directory holds the
 *       same nodes, processors, and capacities as the one that wrote the record, differing
 *       at most in the fields a lease refresh moves and in presence, which the class
 *       documents as soft. Fencing tombstones must survive too: a frame for an absent
 *       identity at its last epoch is refused before and after every fold and restart. After
 *       the restart the reference is rebuilt from the repository and lockstep resumes.</li>
 * </ul>
 *
 * <p>Any failure is shrunk to a minimal operation script and printed with its seed so the
 * reproduction can be read straight from the test output. {@code -Dcluster.fuzz.seeds=N}
 * and {@code -Dcluster.fuzz.ops=N} widen the default matrix for a longer local run.
 */
class PersistentClusterDirectoryRestartFuzzTest {

    private static final long[] SEEDS = LongStream.rangeClosed(1,
            Integer.getInteger("cluster.fuzz.seeds", 2)).toArray();
    private static final int OPS_PER_RUN = Integer.getInteger("cluster.fuzz.ops", 200);
    private static final int[] RETAINED_EVENTS = {1, 2, 3, 5};
    private static final int SHRINK_RUN_BUDGET = 600;
    private static final int SNAPSHOT_CONTRACT_EVERY = 8;
    private static final List<String> NODE_IDS = List.of("node-0", "node-1", "node-2", "node-3");
    private static final List<String> PROCESSOR_IDS =
            List.of("proc-0", "proc-1", "proc-2", "proc-3", "proc-4", "proc-5");
    private static final Duration[] TTLS =
            {Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofSeconds(120)};
    private static final Duration[] LEASES =
            {Duration.ofSeconds(10), Duration.ofSeconds(45), Duration.ofSeconds(300)};
    private static final String OUTAGE = "repository unavailable (injected outage)";

    /**
     * Only durable traffic for nodes and processors (new incarnations and substantive
     * changes) plus heartbeats and capacity refreshes. Isolates everything that is not the
     * soft-lease-refresh path.
     */
    @Test
    void durableTrafficAlwaysRebuildsAndSurvivesRestart() {
        runMatrix(false);
    }

    /**
     * The full menu, including same-epoch refresh-only re-advertisements of nodes and
     * processors that live only in memory until a fold or an expiry event captures them.
     */
    @Test
    void softLeaseRefreshTrafficAlwaysRebuildsAndSurvivesRestart() {
        runMatrix(true);
    }

    private static void runMatrix(boolean softLeases) {
        Map<String, String> distinct = new LinkedHashMap<>();
        int failingRuns = 0;
        for (long seed : SEEDS) {
            for (int retained : RETAINED_EVENTS) {
                Outcome outcome = fuzzOnce(seed, retained, softLeases);
                if (outcome == null) {
                    continue;
                }
                failingRuns++;
                String signature = outcome.failure().signature();
                if (!distinct.containsKey(signature)) {
                    List<Op> minimal = shrink(outcome.script(), signature, retained);
                    distinct.put(signature, describeFailure(seed, retained, softLeases,
                            outcome, minimal));
                }
            }
        }
        if (distinct.isEmpty()) {
            return;
        }
        StringBuilder report = new StringBuilder();
        report.append("PersistentClusterDirectory fuzz (softLeases=").append(softLeases)
                .append("): ").append(failingRuns).append(" of ")
                .append(SEEDS.length * RETAINED_EVENTS.length).append(" runs failed, ")
                .append(distinct.size()).append(" distinct failure(s)\n");
        distinct.values().forEach(report::append);
        System.out.println(report);
        fail(report.toString());
    }

    /** Runs one seeded script to completion; null when every invariant held. */
    private static Outcome fuzzOnce(long seed, int retained, boolean softLeases) {
        Generator generator = new Generator(new Random(seed), softLeases);
        Session session = new Session(retained);
        List<Op> script = new ArrayList<>();
        for (int index = 0; index < OPS_PER_RUN; index++) {
            Op op = generator.next(session);
            script.add(op);
            Failure failure = session.apply(op, index);
            if (failure != null) {
                return new Outcome(List.copyOf(script), failure);
            }
            generator.observe(op, session.lastFrameApplied);
        }
        return null;
    }

    private record Outcome(List<Op> script, Failure failure) {
    }

    // ------------------------------------------------------------------ shrinking

    /** ddmin over the concrete script, keeping only removals that reproduce the signature. */
    private static List<Op> shrink(List<Op> script, String signature, int retained) {
        List<Op> current = new ArrayList<>(script);
        int budget = SHRINK_RUN_BUDGET;
        int granularity = 2;
        while (current.size() >= 2 && budget > 0) {
            int chunk = Math.max(1, current.size() / granularity);
            boolean reduced = false;
            for (int start = 0; start < current.size() && budget > 0; start += chunk) {
                List<Op> candidate = new ArrayList<>(current.subList(0, start));
                candidate.addAll(current.subList(Math.min(start + chunk, current.size()),
                        current.size()));
                budget--;
                if (reproduces(candidate, signature, retained)) {
                    current = candidate;
                    granularity = Math.max(granularity - 1, 2);
                    reduced = true;
                    break;
                }
            }
            if (!reduced) {
                if (chunk == 1) {
                    break;
                }
                granularity = Math.min(granularity * 2, current.size());
            }
        }
        return current;
    }

    private static boolean reproduces(List<Op> script, String signature, int retained) {
        Session session = new Session(retained);
        for (int index = 0; index < script.size(); index++) {
            Failure failure = session.apply(script.get(index), index);
            if (failure == null) {
                continue;
            }
            if (failure.signature().equals(signature)) {
                return true;
            }
            // A shrunk script routinely turns a believed-valid frame stale (its
            // registration was removed); that is a generator artefact, not a different
            // failure, so the op is skipped and the search continues.
            if (failure.kind() == Kind.UNEXPECTED_REJECTION
                    || failure.kind() == Kind.MISSING_REJECTION) {
                continue;
            }
            return false;
        }
        return false;
    }

    private static String describeFailure(long seed, int retained, boolean softLeases,
                                          Outcome outcome, List<Op> minimal) {
        Failure failure = outcome.failure();
        StringBuilder out = new StringBuilder();
        out.append("\n--- failure: ").append(failure.kind()).append(" ---\n");
        out.append("seed=").append(seed).append(" retainedEvents=").append(retained)
                .append(" softLeases=").append(softLeases)
                .append(" firstFailingOp=#").append(failure.opIndex() + 1)
                .append(" of ").append(outcome.script().size())
                .append(" (").append(failure.stage()).append(")\n");
        out.append("exception: ").append(failure.cause().getClass().getSimpleName())
                .append(": ").append(failure.cause().getMessage()).append('\n');
        StackTraceElement[] frames = failure.cause().getStackTrace();
        for (int i = 0; i < Math.min(6, frames.length); i++) {
            out.append("    at ").append(frames[i]).append('\n');
        }
        out.append("minimal script (").append(minimal.size()).append(" ops, shrunk from ")
                .append(outcome.script().size()).append("):\n");
        Instant now = ClusterFixtures.T0;
        for (int i = 0; i < minimal.size(); i++) {
            Op op = minimal.get(i);
            if (op instanceof Advance advance) {
                now = now.plus(advance.by());
            }
            out.append(String.format("  %3d. %s%s%n", i + 1, op.describe(),
                    op instanceof Advance ? "  (now=" + rel(now) + ")" : ""));
        }
        return out.toString();
    }

    // ------------------------------------------------------------------ the session

    private enum Kind {
        /** The directory's own durable record (log or checkpoint) failed to rebuild. */
        DURABLE_REBUILD,
        /** The facade and the lockstep reference directory disagree. */
        MODEL_DIVERGENCE,
        /** A restarted directory disagrees with the one that wrote the record. */
        RESTART_DIVERGENCE,
        /** The live snapshot no longer satisfies the snapshot contract. */
        SNAPSHOT_INVALID,
        /** A frame the generator believed valid was refused (by both directories). */
        UNEXPECTED_REJECTION,
        /** A frame the generator built to be stale was accepted. */
        MISSING_REJECTION,
        /** Anything else that escaped. */
        OTHER
    }

    private record Failure(Kind kind, int opIndex, String stage, Throwable cause) {
        String signature() {
            String message = String.valueOf(cause.getMessage());
            int newline = message.indexOf('\n');
            if (newline >= 0) {
                message = message.substring(0, newline);
            }
            return kind + "|" + cause.getClass().getSimpleName() + "|"
                    + message.replaceAll("\\d+", "#");
        }
    }

    /** The in-memory repository with a one-shot outage that fails the next durable save. */
    private static final class OutageRepository implements ClusterEventRepository {
        private final InMemoryClusterEventRepository backing =
                new InMemoryClusterEventRepository();
        boolean failNextSave;

        @Override
        public Optional<StoredDirectory> load(ClusterDescriptor cluster) {
            return backing.load(cluster);
        }

        @Override
        public void save(ClusterDescriptor cluster, StoredDirectory directory) {
            if (failNextSave) {
                failNextSave = false;
                throw new IllegalStateException(OUTAGE);
            }
            backing.save(cluster, directory);
        }
    }

    /**
     * One live facade over one repository and one fake clock, restartable in place, with a
     * plain {@link ClusterDirectory} receiving every operation in lockstep as the model.
     */
    private static final class Session {
        final ClusterDescriptor cluster = ClusterFixtures.cluster();
        final ClusterFixtures.MutableClock clock = new ClusterFixtures.MutableClock(
                ClusterFixtures.T0);
        final OutageRepository repository = new OutageRepository();
        final int retained;
        PersistentClusterDirectory live;
        ClusterDirectory reference;
        /** Whether the live facade accepted the most recent frame (not refused, no outage). */
        boolean lastFrameApplied;

        Session(int retained) {
            this.retained = retained;
            this.live = new PersistentClusterDirectory(cluster, clock, repository, retained);
            this.reference = new ClusterDirectory(cluster, clock);
        }

        Instant now() {
            return clock.instant();
        }

        Failure apply(Op op, int index) {
            lastFrameApplied = false;
            Failure failure;
            try {
                failure = switch (op) {
                    case Advance advance -> {
                        clock.advance(advance.by());
                        yield null;
                    }
                    case Outage outage -> {
                        repository.failNextSave = true;
                        yield null;
                    }
                    case Sweep sweep -> sweep(index);
                    case Restart restart -> restart(index);
                    case RegisterNode node -> frame(node, index,
                            d -> d.register(node.advertisement()),
                            d -> d.register(node.advertisement()));
                    case Heartbeat heartbeat -> frame(heartbeat, index,
                            d -> d.heartbeat(heartbeat.presence()),
                            d -> d.heartbeat(heartbeat.presence()));
                    case RegisterProcessor processor -> frame(processor, index,
                            d -> d.registerProcessor(processor.advertisement()),
                            d -> d.registerProcessor(processor.advertisement()));
                    case UpdateCapacity capacity -> frame(capacity, index,
                            d -> d.updateCapacity(capacity.snapshot()),
                            d -> d.updateCapacity(capacity.snapshot()));
                };
            } catch (AssertionError e) {
                return new Failure(Kind.MODEL_DIVERGENCE, index, "apply", e);
            } catch (RuntimeException e) {
                return new Failure(isDurableRebuild(e) ? Kind.DURABLE_REBUILD : Kind.OTHER,
                        index, "apply", e);
            }
            if (failure != null) {
                return failure;
            }
            return postConditions(op, index);
        }

        /**
         * Applies one frame to both directories and demands the same verdict: both accept
         * with the same outcome, or both refuse with the same message. A frame whose save
         * hit the injected outage is withheld from the reference: the facade promises the
         * failed write changed nothing, and the snapshot comparison afterwards holds it to
         * that.
         */
        private Failure frame(Frame op, int index,
                              Function<PersistentClusterDirectory, ApplyOutcome> onLive,
                              Function<ClusterDirectory, ApplyOutcome> onReference) {
            ApplyOutcome liveOutcome = null;
            IllegalArgumentException liveRejection = null;
            try {
                liveOutcome = onLive.apply(live);
            } catch (IllegalArgumentException e) {
                if (isDurableRebuild(e)) {
                    return new Failure(Kind.DURABLE_REBUILD, index, "apply", e);
                }
                liveRejection = e;
            } catch (IllegalStateException e) {
                if (!OUTAGE.equals(e.getMessage())) {
                    throw e;
                }
                return null;
            }
            lastFrameApplied = liveRejection == null;
            ApplyOutcome referenceOutcome = null;
            IllegalArgumentException referenceRejection = null;
            try {
                referenceOutcome = onReference.apply(reference);
            } catch (IllegalArgumentException e) {
                referenceRejection = e;
            }
            if ((liveRejection == null) != (referenceRejection == null)) {
                return new Failure(Kind.MODEL_DIVERGENCE, index, "apply", new AssertionError(
                        "verdicts differ: persistent -> " + verdict(liveOutcome, liveRejection)
                                + "; reference -> "
                                + verdict(referenceOutcome, referenceRejection)));
            }
            if (liveRejection != null) {
                if (!liveRejection.getMessage().equals(referenceRejection.getMessage())) {
                    return new Failure(Kind.MODEL_DIVERGENCE, index, "apply", new AssertionError(
                            "rejections differ: persistent -> " + liveRejection.getMessage()
                                    + "; reference -> " + referenceRejection.getMessage()));
                }
                return op.expectReject() ? null
                        : new Failure(Kind.UNEXPECTED_REJECTION, index, "apply", liveRejection);
            }
            if (op.expectReject()) {
                return new Failure(Kind.MISSING_REJECTION, index, "apply",
                        new AssertionError("stale frame was accepted as " + liveOutcome + ": "
                                + op.describe()));
            }
            if (liveOutcome != referenceOutcome) {
                return new Failure(Kind.MODEL_DIVERGENCE, index, "apply", new AssertionError(
                        "outcomes differ: persistent -> " + liveOutcome + "; reference -> "
                                + referenceOutcome));
            }
            return null;
        }

        private static String verdict(ApplyOutcome outcome, IllegalArgumentException rejection) {
            return rejection == null ? String.valueOf(outcome)
                    : "rejected (" + rejection.getMessage() + ")";
        }

        private Failure sweep(int index) {
            List<ClusterEvent> liveEvents;
            try {
                liveEvents = live.sweep();
            } catch (IllegalStateException e) {
                if (!OUTAGE.equals(e.getMessage())) {
                    throw e;
                }
                return null;
            }
            List<ClusterEvent> referenceEvents = reference.sweep();
            if (!liveEvents.equals(referenceEvents)) {
                return new Failure(Kind.MODEL_DIVERGENCE, index, "sweep", new AssertionError(
                        "sweep events differ: persistent -> " + summarize(liveEvents)
                                + "; reference -> " + summarize(referenceEvents)));
            }
            return null;
        }

        private static String summarize(List<ClusterEvent> events) {
            return events.stream()
                    .map(e -> e.getType().name().replace("CLUSTER_EVENT_TYPE_", "")
                            + "@" + e.getSeq() + "(" + e.getNodeId()
                            + (e.getProcessorId().isEmpty() ? "" : "/" + e.getProcessorId())
                            + ")")
                    .toList().toString();
        }

        private Failure restart(int index) {
            PersistentClusterDirectory before = live;
            PersistentClusterDirectory after =
                    new PersistentClusterDirectory(cluster, clock, repository, retained);
            try {
                assertThat(after.eventLog()).as("retained event log after restart")
                        .isEqualTo(before.eventLog());
                assertThat(after.checkpoint()).as("checkpoint after restart")
                        .isEqualTo(before.checkpoint());
                assertThat(durable(after.snapshot())).as("durable projection after restart")
                        .isEqualTo(durable(before.snapshot()));
            } catch (AssertionError e) {
                return new Failure(Kind.RESTART_DIVERGENCE, index, "restart", e);
            }
            live = after;
            reference = rebuildReference();
            return null;
        }

        /** The reference after a restart: exactly what the repository holds, nothing soft. */
        private ClusterDirectory rebuildReference() {
            StoredDirectory stored = repository.load(cluster)
                    .orElseGet(() -> StoredDirectory.of(List.of()));
            return stored.compacted()
                    ? ClusterDirectory.restore(cluster, stored.checkpoint(), stored.events(),
                            clock)
                    : ClusterDirectory.replay(cluster, stored.events(), clock);
        }

        private Failure postConditions(Op op, int index) {
            // Eager form of "the next mutation must not trip over the record we just wrote":
            // rebuild from the repository now, exactly as mutate() and a restart would.
            try {
                new PersistentClusterDirectory(cluster, clock, repository, retained);
            } catch (RuntimeException e) {
                return new Failure(Kind.DURABLE_REBUILD, index, "post-op rebuild probe", e);
            }
            ClusterSnapshot snapshot = live.snapshot();
            if (index % SNAPSHOT_CONTRACT_EVERY == 0 || op instanceof Restart) {
                try {
                    ClusterValidation.validate(snapshot);
                } catch (RuntimeException e) {
                    return new Failure(Kind.SNAPSHOT_INVALID, index, "post-op snapshot", e);
                }
            }
            try {
                assertThat(snapshot).as("snapshot vs lockstep reference")
                        .isEqualTo(reference.snapshot());
                assertThat(live.eligibleProcessors("", "", "")).as("eligibility vs reference")
                        .isEqualTo(reference.eligibleProcessors("", "", ""));
            } catch (AssertionError e) {
                return new Failure(Kind.MODEL_DIVERGENCE, index, "post-op snapshot", e);
            }
            return null;
        }

        /**
         * Strips a snapshot down to what the class promises survives a restart: identities,
         * epochs, and substantive content. Presence is soft; the refresh-only fields
         * ({@code advertised_at}/{@code ttl}/{@code seq} on nodes,
         * {@code advertised_at}/{@code lease_expires_at}/{@code seq} on processors,
         * {@code observed_at}/{@code seq} on capacity) are soft when they moved without an
         * event.
         */
        private static ClusterSnapshot durable(ClusterSnapshot snapshot) {
            ClusterSnapshot.Builder builder = snapshot.toBuilder()
                    .clearCapturedAt()
                    .clearFingerprint()
                    .clearNodes()
                    .clearProcessors()
                    .clearCapacities();
            for (NodeRecord record : snapshot.getNodesList()) {
                NodeRecord.Builder node = NodeRecord.newBuilder()
                        .setAdvertisement(record.getAdvertisement().toBuilder()
                                .clearAdvertisedAt().clearTtl().clearSeq());
                if (record.hasCapacity()) {
                    node.setCapacity(durable(record.getCapacity()));
                }
                builder.addNodes(node);
            }
            for (ProcessorAdvertisement processor : snapshot.getProcessorsList()) {
                builder.addProcessors(processor.toBuilder()
                        .clearAdvertisedAt().clearLeaseExpiresAt().clearSeq());
            }
            for (CapacityAdvertisement capacity : snapshot.getCapacitiesList()) {
                builder.addCapacities(durable(capacity));
            }
            return builder.build();
        }

        private static CapacityAdvertisement durable(CapacityAdvertisement capacity) {
            return capacity.toBuilder().clearObservedAt().clearSeq().build();
        }

        private static boolean isDurableRebuild(Throwable e) {
            for (Throwable t = e; t != null; t = t.getCause()) {
                for (StackTraceElement frame : t.getStackTrace()) {
                    String method = frame.getMethodName();
                    boolean directory = frame.getClassName().endsWith(".ClusterDirectory");
                    if (directory && (method.startsWith("replay") || method.equals("restore")
                            || method.equals("applyReplay"))) {
                        return true;
                    }
                    if (frame.getClassName().endsWith(".ClusterValidation")
                            && method.equals("validateEventLog")) {
                        return true;
                    }
                    if (frame.getClassName().endsWith(".PersistentClusterDirectory")
                            && method.equals("<init>")) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    // ------------------------------------------------------------------ operations

    private sealed interface Op permits Advance, Outage, Sweep, Restart, Frame {
        String describe();
    }

    private sealed interface Frame extends Op permits RegisterNode, Heartbeat,
            RegisterProcessor, UpdateCapacity {
        boolean expectReject();
    }

    private record Advance(Duration by) implements Op {
        @Override
        public String describe() {
            return "advance +" + by.toSeconds() + "s";
        }
    }

    private record Outage() implements Op {
        @Override
        public String describe() {
            return "OUTAGE (the next durable save throws; soft-only mutations still succeed)";
        }
    }

    private record Sweep() implements Op {
        @Override
        public String describe() {
            return "sweep";
        }
    }

    private record Restart() implements Op {
        @Override
        public String describe() {
            return "RESTART (new PersistentClusterDirectory over the same repository)";
        }
    }

    private record RegisterNode(NodeAdvertisement advertisement, boolean expectReject)
            implements Frame {
        @Override
        public String describe() {
            return "register " + advertisement.getNodeId()
                    + " epoch=" + advertisement.getEpoch()
                    + " seq=" + advertisement.getSeq()
                    + " caps=" + advertisement.getCapabilitiesList()
                    + " ttl=" + advertisement.getTtl().getSeconds() + "s"
                    + " advertisedAt=" + rel(advertisement.getAdvertisedAt())
                    + suffix(expectReject);
        }
    }

    private record Heartbeat(NodePresence presence, boolean expectReject) implements Frame {
        @Override
        public String describe() {
            return "heartbeat " + presence.getNodeId()
                    + " nodeEpoch=" + presence.getNodeEpoch()
                    + " heartbeatSeq=" + presence.getHeartbeatSeq()
                    + " state=" + presence.getState().name().replace("PRESENCE_STATE_", "")
                    + " ttl=" + presence.getTtl().getSeconds() + "s"
                    + " at=" + rel(presence.getLastHeartbeatAt())
                    + " expiresAt=" + rel(presence.getExpiresAt())
                    + suffix(expectReject);
        }
    }

    private record RegisterProcessor(ProcessorAdvertisement advertisement, boolean expectReject)
            implements Frame {
        @Override
        public String describe() {
            return "registerProcessor " + advertisement.getProcessorId()
                    + " on " + advertisement.getNodeId()
                    + " nodeEpoch=" + advertisement.getNodeEpoch()
                    + " leaseEpoch=" + advertisement.getLeaseEpoch()
                    + " seq=" + advertisement.getSeq()
                    + " caps=" + advertisement.getCapabilitiesList()
                    + " advertisedAt=" + rel(advertisement.getAdvertisedAt())
                    + " leaseExpiresAt=" + rel(advertisement.getLeaseExpiresAt())
                    + suffix(expectReject);
        }
    }

    private record UpdateCapacity(CapacityAdvertisement snapshot, boolean expectReject)
            implements Frame {
        @Override
        public String describe() {
            return "updateCapacity " + snapshot.getNodeId()
                    + (snapshot.getProcessorId().isEmpty() ? "" : "/" + snapshot.getProcessorId())
                    + " sourceEpoch=" + snapshot.getSourceEpoch()
                    + " seq=" + snapshot.getSeq()
                    + " inFlight=" + snapshot.getInFlight()
                    + " observedAt=" + rel(snapshot.getObservedAt())
                    + suffix(expectReject);
        }
    }

    private static String suffix(boolean expectReject) {
        return expectReject ? "   [stale: expect IllegalArgumentException]" : "";
    }

    private static String rel(Timestamp timestamp) {
        return rel(Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()));
    }

    private static String rel(Instant instant) {
        return "T0+" + Duration.between(ClusterFixtures.T0, instant).toSeconds() + "s";
    }

    // ------------------------------------------------------------------ the generator

    /**
     * Builds the next operation from the live directory's own view, so every believed-valid
     * frame carries the sequence, epoch, and window the directory should accept. Only the
     * highest epoch ever issued per identity is remembered across the run: a fresh
     * incarnation after expiry must clear the fencing tombstone the directory keeps and
     * does not expose, and a stale probe at that epoch with sequence one must hit it.
     */
    private static final class Generator {
        private final Random rng;
        private final boolean softLeases;
        private final Map<String, Long> maxNodeEpoch = new LinkedHashMap<>();
        private final Map<String, Long> maxLeaseEpoch = new LinkedHashMap<>();

        Generator(Random rng, boolean softLeases) {
            this.rng = rng;
            this.softLeases = softLeases;
        }

        /**
         * Learns an identity's epoch only from a frame the live directory accepted, so a
         * refused or outage-failed incarnation never becomes a phantom tombstone the stale
         * probes would wrongly expect to be honoured.
         */
        void observe(Op op, boolean applied) {
            if (!applied) {
                return;
            }
            if (op instanceof RegisterNode node) {
                maxNodeEpoch.merge(node.advertisement().getNodeId(),
                        node.advertisement().getEpoch(), Math::max);
            } else if (op instanceof RegisterProcessor processor) {
                maxLeaseEpoch.merge(processor.advertisement().getProcessorId(),
                        processor.advertisement().getLeaseEpoch(), Math::max);
            }
        }

        Op next(Session session) {
            int roll = rng.nextInt(100);
            if (roll < 14) {
                return registerNode(session);
            }
            if (roll < 27) {
                return heartbeat(session);
            }
            if (roll < 42) {
                return registerProcessor(session);
            }
            if (roll < 55) {
                return capacity(session);
            }
            if (roll < 70) {
                return advance();
            }
            if (roll < 80) {
                return new Sweep();
            }
            if (roll < 86) {
                return new Restart();
            }
            if (roll < 90) {
                return new Outage();
            }
            return stale(session);
        }

        private Advance advance() {
            return new Advance(rng.nextInt(10) < 6
                    ? Duration.ofSeconds(1 + rng.nextInt(10))
                    : Duration.ofSeconds(30 + rng.nextInt(371)));
        }

        private List<NodeAdvertisement> registeredNodes(Session session) {
            return session.live.snapshot().getNodesList().stream()
                    .map(NodeRecord::getAdvertisement).toList();
        }

        private <T> T pick(List<T> values) {
            return values.get(rng.nextInt(values.size()));
        }

        private Duration ttl() {
            return TTLS[rng.nextInt(TTLS.length)];
        }

        private Duration lease() {
            return LEASES[rng.nextInt(LEASES.length)];
        }

        private long nextNodeEpoch(String nodeId, NodeAdvertisement existing) {
            long max = maxNodeEpoch.getOrDefault(nodeId, 0L);
            if (existing != null) {
                max = Math.max(max, existing.getEpoch());
            }
            return max + 1;
        }

        private long nextLeaseEpoch(String processorId, ProcessorAdvertisement existing) {
            long max = maxLeaseEpoch.getOrDefault(processorId, 0L);
            if (existing != null) {
                max = Math.max(max, existing.getLeaseEpoch());
            }
            return max + 1;
        }

        private Op registerNode(Session session) {
            String nodeId = pick(NODE_IDS);
            NodeAdvertisement existing = session.live.node(nodeId).orElse(null);
            Timestamp now = ClusterFixtures.ts(session.now());
            if (existing != null) {
                maxNodeEpoch.merge(nodeId, existing.getEpoch(), Math::max);
            }
            int roll = rng.nextInt(10);
            if (existing == null || roll < 2) {
                return new RegisterNode(ClusterFixtures.nodeBuilder(nodeId,
                                nextNodeEpoch(nodeId, existing), 1)
                        .setAdvertisedAt(now)
                        .setTtl(proto(ttl()))
                        .build(), false);
            }
            NodeAdvertisement.Builder fresh = existing.toBuilder()
                    .setSeq(existing.getSeq() + 1)
                    .setAdvertisedAt(now)
                    .setTtl(proto(ttl()));
            if (softLeases && roll < 7) {
                return new RegisterNode(fresh.build(), false);
            }
            return new RegisterNode(toggleCapability(fresh, "gpu"), false);
        }

        private static NodeAdvertisement toggleCapability(NodeAdvertisement.Builder builder,
                                                          String capability) {
            List<String> caps = new ArrayList<>(builder.getCapabilitiesList());
            if (!caps.remove(capability)) {
                caps.add(capability);
            }
            return builder.clearCapabilities().addAllCapabilities(caps).build();
        }

        private static ProcessorAdvertisement toggleCapability(
                ProcessorAdvertisement.Builder builder, String capability) {
            List<String> caps = new ArrayList<>(builder.getCapabilitiesList());
            if (!caps.remove(capability)) {
                caps.add(capability);
            }
            return builder.clearCapabilities().addAllCapabilities(caps).build();
        }

        private Op heartbeat(Session session) {
            List<NodeAdvertisement> nodes = registeredNodes(session);
            if (nodes.isEmpty()) {
                return registerNode(session);
            }
            NodeAdvertisement node = pick(nodes);
            NodePresence current = session.live.presence(node.getNodeId()).orElseThrow();
            PresenceState state = rng.nextInt(10) == 0
                    ? PresenceState.PRESENCE_STATE_GONE
                    : PresenceState.PRESENCE_STATE_ACTIVE;
            return new Heartbeat(presence(session, node, current.getHeartbeatSeq() + 1, ttl())
                    .toBuilder().setState(state).build(), false);
        }

        private static NodePresence presence(Session session, NodeAdvertisement node,
                                             long heartbeatSeq, Duration ttl) {
            Instant now = session.now();
            return ClusterFixtures.presenceBuilder(node.getNodeId(), heartbeatSeq)
                    .setNodeEpoch(node.getEpoch())
                    .setLastHeartbeatAt(ClusterFixtures.ts(now))
                    .setTtl(proto(ttl))
                    .setExpiresAt(ClusterFixtures.ts(now.plus(ttl)))
                    .build();
        }

        private Op registerProcessor(Session session) {
            List<NodeAdvertisement> nodes = registeredNodes(session);
            if (nodes.isEmpty()) {
                return registerNode(session);
            }
            String processorId = pick(PROCESSOR_IDS);
            ProcessorAdvertisement existing = session.live.processor(processorId).orElse(null);
            Instant now = session.now();
            if (existing != null) {
                maxLeaseEpoch.merge(processorId, existing.getLeaseEpoch(), Math::max);
            }
            int roll = rng.nextInt(10);
            if (existing == null || roll < 2) {
                // A fresh lease, possibly on another node: the only way a processor may move.
                NodeAdvertisement host = pick(nodes);
                return new RegisterProcessor(
                        ClusterFixtures.processorBuilder(processorId, host.getNodeId())
                                .setNodeEpoch(host.getEpoch())
                                .setLeaseEpoch(nextLeaseEpoch(processorId, existing))
                                .setSeq(1)
                                .setAdvertisedAt(ClusterFixtures.ts(now))
                                .setLeaseExpiresAt(ClusterFixtures.ts(now.plus(lease())))
                                .build(), false);
            }
            NodeAdvertisement host = session.live.node(existing.getNodeId()).orElseThrow();
            ProcessorAdvertisement.Builder fresh = existing.toBuilder()
                    .setNodeEpoch(host.getEpoch())
                    .setSeq(existing.getSeq() + 1)
                    .setAdvertisedAt(ClusterFixtures.ts(now))
                    .setLeaseExpiresAt(ClusterFixtures.ts(now.plus(lease())));
            if (softLeases && roll < 7) {
                return new RegisterProcessor(fresh.build(), false);
            }
            return new RegisterProcessor(toggleCapability(fresh, "llm-embed"), false);
        }

        private Op capacity(Session session) {
            List<NodeAdvertisement> nodes = registeredNodes(session);
            if (nodes.isEmpty()) {
                return registerNode(session);
            }
            List<ProcessorAdvertisement> processors = session.live.snapshot().getProcessorsList();
            Instant now = session.now();
            CapacityAdvertisement existing;
            CapacityAdvertisement.Builder builder;
            if (processors.isEmpty() || rng.nextBoolean()) {
                NodeAdvertisement node = pick(nodes);
                existing = session.live.nodeCapacity(node.getNodeId()).orElse(null);
                builder = ClusterFixtures.capacityBuilder(node.getNodeId(), 1)
                        .setSourceEpoch(node.getEpoch());
            } else {
                ProcessorAdvertisement processor = pick(processors);
                existing = session.live.processorCapacity(processor.getNodeId(),
                        processor.getProcessorId()).orElse(null);
                builder = ClusterFixtures.capacityBuilder(processor.getNodeId(), 1)
                        .setProcessorId(processor.getProcessorId())
                        .setSourceEpoch(processor.getLeaseEpoch());
            }
            builder.setObservedAt(ClusterFixtures.ts(now));
            if (existing == null) {
                return new UpdateCapacity(builder.setInFlight(rng.nextInt(17)).build(), false);
            }
            builder = existing.toBuilder().setSeq(existing.getSeq() + 1)
                    .setObservedAt(ClusterFixtures.ts(now));
            if (rng.nextBoolean()) {
                return new UpdateCapacity(builder.build(), false);
            }
            int inFlight = (existing.getInFlight() + 1 + rng.nextInt(16)) % 17;
            return new UpdateCapacity(builder.setInFlight(inFlight).build(), false);
        }

        /**
         * A frame the directory must refuse: stale position, wrong epoch, unknown host, or
         * an absent identity knocking at an epoch its fencing tombstone still covers.
         */
        private Op stale(Session session) {
            List<NodeAdvertisement> nodes = registeredNodes(session);
            Instant now = session.now();
            if (nodes.isEmpty()) {
                return new Heartbeat(ClusterFixtures.presenceBuilder("node-9", 1)
                        .setLastHeartbeatAt(ClusterFixtures.ts(now))
                        .setExpiresAt(ClusterFixtures.ts(now.plusSeconds(30)))
                        .build(), true);
            }
            NodeAdvertisement node = pick(nodes);
            List<ProcessorAdvertisement> processors = session.live.snapshot().getProcessorsList();
            switch (rng.nextInt(8)) {
                case 0 -> {
                    // Same heartbeat sequence, different window: a changed record that does
                    // not advance.
                    NodePresence current = session.live.presence(node.getNodeId()).orElseThrow();
                    Duration ttl = Duration.ofSeconds(current.getTtl().getSeconds() + 1);
                    return new Heartbeat(presence(session, node, current.getHeartbeatSeq(), ttl),
                            true);
                }
                case 1 -> {
                    // Heartbeat from a superseded incarnation, or one that never existed.
                    NodePresence current = session.live.presence(node.getNodeId()).orElseThrow();
                    NodeAdvertisement other = node.toBuilder()
                            .setEpoch(node.getEpoch() > 1 ? node.getEpoch() - 1
                                    : node.getEpoch() + 1)
                            .build();
                    return new Heartbeat(presence(session, other,
                            current.getHeartbeatSeq() + 1, ttl()), true);
                }
                case 2 -> {
                    // Changed content at the registered (epoch, seq): conflicting update.
                    return new RegisterNode(toggleCapability(node.toBuilder()
                            .setAdvertisedAt(ClusterFixtures.ts(now)), "tpu"), true);
                }
                case 3 -> {
                    // Processor for a node nobody registered.
                    return new RegisterProcessor(
                            ClusterFixtures.processorBuilder("proc-9", "node-9")
                                    .setAdvertisedAt(ClusterFixtures.ts(now))
                                    .setLeaseExpiresAt(ClusterFixtures.ts(now.plusSeconds(60)))
                                    .build(), true);
                }
                case 4 -> {
                    if (processors.isEmpty()) {
                        return stale(session);
                    }
                    // Changed content at the registered (lease_epoch, seq).
                    ProcessorAdvertisement processor = pick(processors);
                    return new RegisterProcessor(toggleCapability(processor.toBuilder()
                            .setAdvertisedAt(ClusterFixtures.ts(now))
                            .setLeaseExpiresAt(ClusterFixtures.ts(now.plusSeconds(60))),
                            "llm-rerank"), true);
                }
                case 5 -> {
                    // A swept node knocking at its last epoch with sequence one: every
                    // tombstone at that epoch carries a sequence of at least one, so this
                    // must be refused for as long as the tombstone survives folds and
                    // restarts.
                    List<String> absent = NODE_IDS.stream()
                            .filter(id -> maxNodeEpoch.containsKey(id)
                                    && session.live.node(id).isEmpty())
                            .toList();
                    if (absent.isEmpty()) {
                        return stale(session);
                    }
                    String nodeId = pick(absent);
                    return new RegisterNode(ClusterFixtures.nodeBuilder(nodeId,
                                    maxNodeEpoch.get(nodeId), 1)
                            .setAdvertisedAt(ClusterFixtures.ts(now))
                            .build(), true);
                }
                case 6 -> {
                    // Same probe for a processor whose lease lapsed or was cascaded away.
                    List<String> absent = PROCESSOR_IDS.stream()
                            .filter(id -> maxLeaseEpoch.containsKey(id)
                                    && session.live.processor(id).isEmpty())
                            .toList();
                    if (absent.isEmpty()) {
                        return stale(session);
                    }
                    String processorId = pick(absent);
                    return new RegisterProcessor(
                            ClusterFixtures.processorBuilder(processorId, node.getNodeId())
                                    .setNodeEpoch(node.getEpoch())
                                    .setLeaseEpoch(maxLeaseEpoch.get(processorId))
                                    .setSeq(1)
                                    .setAdvertisedAt(ClusterFixtures.ts(now))
                                    .setLeaseExpiresAt(ClusterFixtures.ts(now.plusSeconds(60)))
                                    .build(), true);
                }
                default -> {
                    // Capacity from the wrong source epoch.
                    return new UpdateCapacity(ClusterFixtures.capacityBuilder(node.getNodeId(), 1)
                            .setSourceEpoch(node.getEpoch() + 1)
                            .setObservedAt(ClusterFixtures.ts(now))
                            .build(), true);
                }
            }
        }

        private static com.google.protobuf.Duration proto(Duration duration) {
            return com.google.protobuf.Duration.newBuilder()
                    .setSeconds(duration.getSeconds())
                    .setNanos(duration.getNano())
                    .build();
        }
    }
}
