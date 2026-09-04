package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

/** Owns worker stream session fences and claim ownership across bounded reconnects. */
public final class WorkerSessionRegistry {

    private final Map<String, State> bySession = new LinkedHashMap<>();
    private final Map<String, State> byWorker = new LinkedHashMap<>();

    /** Opens a fresh session or resumes the exact disconnected session named by the hello. */
    public synchronized OpenResult open(
            WorkerHello hello, Duration reconnectGrace, Instant now) {
        Objects.requireNonNull(hello, "hello");
        Objects.requireNonNull(reconnectGrace, "reconnectGrace");
        Objects.requireNonNull(now, "now");
        if (reconnectGrace.isNegative()) {
            throw new IllegalArgumentException("reconnectGrace must not be negative");
        }

        List<ClaimRef> releases = expire(now);
        if (!hello.getResumeSessionId().isBlank()) {
            State resumable = bySession.get(hello.getResumeSessionId());
            if (resumable == null) {
                return OpenResult.refused("resume session is absent or expired", releases);
            }
            if (resumable.connected) {
                return OpenResult.refused("resume session is already connected", releases);
            }
            if (!resumable.workerId.equals(hello.getWorkerId())
                    || !resumable.nodeId.equals(hello.getNodeId())
                    || resumable.nodeIncarnationEpoch != hello.getNodeIncarnationEpoch()) {
                return OpenResult.refused(
                        "resume session does not match worker incarnation", releases);
            }
            if (!resumable.hello.getContractsList().equals(hello.getContractsList())
                    || !resumable.hello.getProcessorLeasesList()
                    .equals(hello.getProcessorLeasesList())) {
                return OpenResult.refused("resume session contract or lease drift", releases);
            }
            if (resumable.disconnectedAt == null
                    || now.isAfter(resumable.disconnectedAt.plus(resumable.reconnectGrace))) {
                remove(resumable);
                releases.addAll(resumable.claims.values());
                return OpenResult.refused("resume session exceeded disconnect grace", releases);
            }
            resumable.connected = true;
            resumable.disconnectedAt = null;
            return OpenResult.admitted(resumable.sessionId, true,
                    resumable.claims, releases);
        }

        State current = byWorker.get(hello.getWorkerId());
        if (current != null) {
            if (current.connected) {
                return OpenResult.refused(
                        "worker_id already has a connected session", releases);
            }
            if (hello.getNodeIncarnationEpoch() <= current.nodeIncarnationEpoch) {
                return OpenResult.refused(
                        "disconnected worker must resume or use a newer incarnation", releases);
            }
            remove(current);
            releases.addAll(current.claims.values());
        }

        String sessionId = UUID.randomUUID().toString();
        State created = new State(sessionId, hello, reconnectGrace);
        bySession.put(sessionId, created);
        byWorker.put(created.workerId, created);
        return OpenResult.admitted(sessionId, false, Map.of(), releases);
    }

    /** Records one channel claim under the live stream/session fence. */
    public synchronized void claimed(String sessionId, DeliveryClaim claim) {
        State state = requireConnected(sessionId);
        if (!claim.getWorkerId().equals(state.workerId)) {
            throw new IllegalArgumentException("claim worker does not match session");
        }
        ClaimRef next = ClaimRef.from(state.sessionId, state.workerId, claim);
        ClaimRef previous = state.claims.putIfAbsent(next.deliveryId(), next);
        if (previous != null && !previous.equals(next)) {
            throw new IllegalArgumentException(
                    "session already owns a different fence for delivery " + next.deliveryId());
        }
    }

    /** Requires and removes the exact claim completed under a live session. */
    public synchronized ClaimRef finished(
            String sessionId, String deliveryId, String leaseToken) {
        State state = requireConnected(sessionId);
        ClaimRef claim = state.claims.get(deliveryId);
        if (claim == null || !claim.leaseToken().equals(leaseToken)) {
            throw new IllegalArgumentException(
                    "worker outcome has no matching live claim");
        }
        state.claims.remove(deliveryId);
        return claim;
    }

    /** Finds the connected session that can receive cancellation for one live claim. */
    public synchronized Optional<ClaimRoute> routeCancellation(String deliveryId) {
        for (State state : bySession.values()) {
            ClaimRef claim = state.claims.get(deliveryId);
            if (state.connected && claim != null) {
                return Optional.of(new ClaimRoute(state.sessionId, claim));
            }
        }
        return Optional.empty();
    }

    /** Marks a stream disconnected and returns claims that cannot be retained. */
    public synchronized List<ClaimRef> disconnect(String sessionId, Instant now) {
        State state = bySession.get(sessionId);
        if (state == null || !state.connected) {
            return List.of();
        }
        state.connected = false;
        state.disconnectedAt = Objects.requireNonNull(now, "now");
        if (!state.reconnectGrace.isZero()) {
            List<ClaimRef> unsafe = state.claims.values().stream()
                    .filter(claim -> !claim.claim().getWork().getContract()
                            .getGuarantees().getIdempotentInvocation())
                    .toList();
            unsafe.forEach(claim -> state.claims.remove(claim.deliveryId()));
            return unsafe;
        }
        remove(state);
        return List.copyOf(state.claims.values());
    }

    /** Releases disconnected sessions whose advertised resume grace elapsed. */
    public synchronized List<ClaimRef> expire(Instant now) {
        Objects.requireNonNull(now, "now");
        List<ClaimRef> releases = new ArrayList<>();
        for (State state : List.copyOf(bySession.values())) {
            if (!state.connected && state.disconnectedAt != null
                    && !now.isBefore(state.disconnectedAt.plus(state.reconnectGrace))) {
                remove(state);
                releases.addAll(state.claims.values());
            }
        }
        return releases;
    }

    /** Drops claim references that no longer match the channel's active lease. */
    public synchronized void retainClaims(Predicate<ClaimRef> live) {
        Objects.requireNonNull(live, "live");
        bySession.values().forEach(state ->
                state.claims.values().removeIf(claim -> !live.test(claim)));
    }

    public synchronized Optional<SessionView> session(String sessionId) {
        State state = bySession.get(sessionId);
        return state == null ? Optional.empty() : Optional.of(state.view());
    }

    public synchronized int connectedCount() {
        return (int) bySession.values().stream().filter(state -> state.connected).count();
    }

    private State requireConnected(String sessionId) {
        State state = bySession.get(sessionId);
        if (state == null || !state.connected) {
            throw new IllegalArgumentException("worker session is absent or disconnected");
        }
        return state;
    }

    private void remove(State state) {
        bySession.remove(state.sessionId, state);
        byWorker.remove(state.workerId, state);
    }

    public record ClaimRef(
            String sessionId,
            String workerId,
            String deliveryId,
            String leaseToken,
            String processorId,
            DeliveryClaim claim) {
        private static ClaimRef from(
                String sessionId, String workerId, DeliveryClaim claim) {
            return new ClaimRef(sessionId, workerId,
                    claim.getWork().getDeliveryId(), claim.getLeaseToken(),
                    claim.getWork().getContract().getProcessorId(), claim);
        }
    }

    public record ClaimRoute(String sessionId, ClaimRef claim) {
    }

    public record SessionView(
            String sessionId,
            String workerId,
            String nodeId,
            long nodeIncarnationEpoch,
            boolean connected,
            Map<String, ClaimRef> claims) {
        public SessionView {
            claims = Map.copyOf(claims);
        }
    }

    public record OpenResult(
            boolean admitted,
            String reason,
            String sessionId,
            boolean resumed,
            Map<String, ClaimRef> claims,
            List<ClaimRef> releases) {
        public OpenResult {
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(sessionId, "sessionId");
            claims = Map.copyOf(claims);
            releases = List.copyOf(releases);
        }

        private static OpenResult admitted(
                String sessionId,
                boolean resumed,
                Map<String, ClaimRef> claims,
                List<ClaimRef> releases) {
            return new OpenResult(true, resumed ? "resumed" : "admitted",
                    sessionId, resumed, claims, releases);
        }

        private static OpenResult refused(String reason, List<ClaimRef> releases) {
            return new OpenResult(false, reason, "", false, Map.of(), releases);
        }
    }

    private static final class State {
        private final String sessionId;
        private final String workerId;
        private final String nodeId;
        private final long nodeIncarnationEpoch;
        private final WorkerHello hello;
        private final Duration reconnectGrace;
        private final Map<String, ClaimRef> claims = new LinkedHashMap<>();
        private boolean connected = true;
        private Instant disconnectedAt;

        private State(String sessionId, WorkerHello hello, Duration reconnectGrace) {
            this.sessionId = sessionId;
            this.workerId = hello.getWorkerId();
            this.nodeId = hello.getNodeId();
            this.nodeIncarnationEpoch = hello.getNodeIncarnationEpoch();
            this.hello = hello;
            this.reconnectGrace = reconnectGrace;
        }

        private SessionView view() {
            return new SessionView(sessionId, workerId, nodeId,
                    nodeIncarnationEpoch, connected, claims);
        }
    }
}
