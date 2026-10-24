package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.DelegationCoordinatorState.Session;
import ai.protomolt.proto.delegation.DelegationCoordinatorState.TaskRuntime;
import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.LeaseExpired;
import ai.protomolt.proto.delegation.v1.LeaseRenewal;
import com.google.protobuf.util.Durations;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The coordinator's lease clock: renewals, re-arming after a revision request, expiry
 * timers, and deadline sweeps. The lease clock covers the worker's obligation only: a
 * submitted candidate waits for its review without a deadline, so a task in the candidate
 * phase never expires here, and a revision request re-arms the lease when it hands the
 * task back.
 *
 * <p>The single lock is preserved: the mutating methods are called with the
 * coordinator's {@code lock} (passed to the constructor) already held, and the expiry
 * timer task acquires that same lock. Frame emission stays with the coordinator
 * behind {@link CoordinatorFrames}; this schedule decides <em>when</em> a lease frame is
 * due, never how a frame becomes durable.
 */
final class CoordinatorLeaseSchedule {

    private final ReentrantLock lock;
    private final DelegationCoordinatorState state;
    private final Clock clock;
    private final boolean scheduleLeaseExpiries;
    private final ExecutorService runtimeTasks;
    private final CoordinatorFrames frames;

    CoordinatorLeaseSchedule(ReentrantLock lock, DelegationCoordinatorState state, Clock clock,
            boolean scheduleLeaseExpiries, ExecutorService runtimeTasks,
            CoordinatorFrames frames) {
        this.lock = Objects.requireNonNull(lock, "lock");
        this.state = Objects.requireNonNull(state, "state");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduleLeaseExpiries = scheduleLeaseExpiries;
        this.runtimeTasks = Objects.requireNonNull(runtimeTasks, "runtimeTasks");
        this.frames = Objects.requireNonNull(frames, "frames");
    }

    /**
     * Renews a leased task for one full offered lease duration past its recorded expiry.
     * Callers hold the coordinator monitor.
     */
    void renew(TaskRuntime task) {
        java.time.Duration lease = java.time.Duration.ofMillis(
                Durations.toMillis(task.offer.getLeaseDuration()));
        Instant next = task.expiry.plus(lease);
        LeaseRenewal renewal = LeaseRenewal.newBuilder()
                .setAttempt(task.attempt)
                .setExpiresAt(DelegationTimes.toTimestamp(next))
                .build();
        frames.emit(state.requireAdmittedWorker(task.workerId), task.taskId, task.attempt,
                DelegateResponse.newBuilder().setRenewal(renewal), () -> {
                    task.expiry = next;
                    task.leaseGeneration++;
                });
        scheduleExpiry(task.taskId, task.attempt, task.leaseGeneration, next);
    }

    /**
     * Re-arms the lease when a revision request hands the task back to the worker:
     * the worker gets a full lease duration from now unless the recorded expiry is
     * already later, and the expiry timer is scheduled either way (a task restored
     * in the candidate phase has none). Callers hold the coordinator monitor.
     */
    void rearmLease(TaskRuntime task, Session session) {
        java.time.Duration lease = java.time.Duration.ofMillis(
                Durations.toMillis(task.offer.getLeaseDuration()));
        Instant next = clock.instant().plus(lease);
        if (next.isAfter(task.expiry)) {
            LeaseRenewal renewal = LeaseRenewal.newBuilder()
                    .setAttempt(task.attempt)
                    .setExpiresAt(DelegationTimes.toTimestamp(next))
                    .build();
            frames.emit(session, task.taskId, task.attempt,
                    DelegateResponse.newBuilder().setRenewal(renewal), () -> {
                        task.expiry = next;
                        task.leaseGeneration++;
                    });
        }
        scheduleExpiry(task.taskId, task.attempt, task.leaseGeneration, task.expiry);
    }

    /**
     * Expires every live lease whose declared expiry is at or before {@code now}, and
     * returns how many leases expired. Callers hold the coordinator monitor.
     */
    int expireLeases(Instant now) {
        int expired = 0;
        for (TaskRuntime task : state.tasks.values()) {
            if (task.phase == DelegationReducer.Phase.LEASED
                    && !task.expiry.isAfter(now)) {
                expire(task, "lease deadline elapsed");
                expired++;
            }
        }
        return expired;
    }

    /**
     * Emits the lease-expired frame for one task and settles its attempt. Callers hold
     * the coordinator monitor.
     */
    void expire(TaskRuntime task, String reason) {
        LeaseExpired expired = LeaseExpired.newBuilder()
                .setAttempt(task.attempt)
                .setReason(reason)
                .build();
        frames.emit(state.requireAdmittedWorker(task.workerId), task.taskId, task.attempt,
                DelegateResponse.newBuilder().setExpired(expired), () -> {
                    task.phase = DelegationReducer.Phase.EXPIRED;
                    task.leaseGeneration++;
                });
    }

    /**
     * Re-arms expiry timers for one session's restored leases: a lease that elapsed while
     * the coordinator was offline expires immediately; a live one gets its timer back.
     * Callers hold the coordinator monitor.
     */
    void resumeRestoredLeases(Session session) {
        for (TaskRuntime task : state.tasks.values()) {
            if (!task.workerId.equals(session.workerId)
                    || task.phase != DelegationReducer.Phase.LEASED) {
                continue;
            }
            if (!task.expiry.isAfter(clock.instant())) {
                expire(task, "lease deadline elapsed while the coordinator was offline");
            } else {
                scheduleExpiry(task.taskId, task.attempt, task.leaseGeneration, task.expiry);
            }
        }
    }

    /**
     * Arms the expiry timer for one lease generation: a runtime task sleeps until the
     * expiry and expires the lease only when the task is still on that attempt, that
     * generation, and still leased. The generation counter makes a stale timer a no-op.
     */
    void scheduleExpiry(String taskId, int attempt, long generation, Instant expiry) {
        if (!scheduleLeaseExpiries) return;
        runtimeTasks.submit(() -> {
            try {
                java.time.Duration delay = java.time.Duration.between(
                        clock.instant(), expiry);
                if (!delay.isNegative() && !delay.isZero()) {
                    Thread.sleep(delay);
                }
                lock.lock();
                try {
                    TaskRuntime task = state.tasks.get(taskId);
                    if (!state.closed && task != null && task.attempt == attempt
                            && task.leaseGeneration == generation
                            && !task.expiry.isAfter(clock.instant())
                            && task.phase == DelegationReducer.Phase.LEASED) {
                        expire(task, "lease deadline elapsed");
                    }
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
}
