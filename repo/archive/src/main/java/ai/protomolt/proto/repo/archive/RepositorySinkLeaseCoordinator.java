package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import io.grpc.Context;
import io.grpc.Status;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates durable lease ownership, scheduled heartbeat renewals, cooperative cross-instance
 * cancellation propagation, and VirtualThread interrupt-safe {@link RepositorySinkJobStore} access
 * for a single {@link RepositorySink} instance.
 */
final class RepositorySinkLeaseCoordinator implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(RepositorySinkLeaseCoordinator.class);
    private static final long MAX_CROSS_INSTANCE_POLL_MILLIS = 25L;
    private static final long MIN_CROSS_INSTANCE_POLL_MILLIS = 5L;

    record JobKey(String accountId, String operationId) {}

    record InFlightRegistration(InFlightJob tracker, boolean newlyRegistered) {}

    static final class InFlightJob {
        final CompletableFuture<RepositorySinkJob> completion = new CompletableFuture<>();
        final AtomicBoolean cancelled = new AtomicBoolean();
        final AtomicBoolean shuttingDown = new AtomicBoolean();
        final AtomicBoolean leaseLost = new AtomicBoolean();
        final AtomicBoolean enteringCommit = new AtomicBoolean();
        volatile String cancelReason = "";
        volatile Thread runnerThread;

        void cancel(String reason) {
            if (reason != null && !reason.isBlank() && this.cancelReason.isBlank()) {
                this.cancelReason = reason.trim();
            }
            cancelled.set(true);
            interruptIfPreCommit();
        }

        void markShuttingDown() {
            shuttingDown.set(true);
            interruptIfPreCommit();
        }

        void markLeaseLost() {
            leaseLost.set(true);
            interruptIfPreCommit();
        }

        private void interruptIfPreCommit() {
            if (!enteringCommit.get()) {
                Thread thread = runnerThread;
                if (thread != null) {
                    thread.interrupt();
                }
            }
        }

        boolean shouldAbortWork() {
            return cancelled.get() || shuttingDown.get() || leaseLost.get();
        }

        String reasonOrDefault() {
            return cancelReason.isBlank() ? "Cancelled" : cancelReason;
        }
    }

    private final String instanceId;
    private final RepositorySinkJobStore store;
    private final Supplier<RepositorySink.LeaseConfig> leaseConfig;
    private final Clock clock;
    private final ScheduledExecutorService heartbeatScheduler;
    private final ConcurrentHashMap<JobKey, InFlightJob> inFlight = new ConcurrentHashMap<>();

    RepositorySinkLeaseCoordinator(
            RepositorySinkJobStore store,
            Supplier<RepositorySink.LeaseConfig> leaseConfig,
            Clock clock) {
        this(UUID.randomUUID().toString(), store, leaseConfig, clock);
    }

    RepositorySinkLeaseCoordinator(
            String instanceId,
            RepositorySinkJobStore store,
            Supplier<RepositorySink.LeaseConfig> leaseConfig,
            Clock clock) {
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId");
        if (instanceId.isBlank()) {
            throw new IllegalArgumentException("instanceId must not be blank");
        }
        this.store = Objects.requireNonNull(store, "store");
        this.leaseConfig = Objects.requireNonNull(leaseConfig, "leaseConfig");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("repo-sink-lease-", 0).factory());
    }

    String instanceId() {
        return instanceId;
    }

    RepositorySink.LeaseConfig config() {
        return Objects.requireNonNull(leaseConfig.get(), "leaseConfig");
    }

    Instant nextExpiration() {
        return clock.instant().plus(config().leaseDuration());
    }

    RepositorySinkJobStore.JobLease newInitialLease() {
        return new RepositorySinkJobStore.JobLease(instanceId, 1L, nextExpiration());
    }

    InFlightJob getInFlight(JobKey key) {
        return inFlight.get(key);
    }

    boolean isInFlight(JobKey key) {
        return inFlight.containsKey(key);
    }

    InFlightRegistration registerInFlight(JobKey key) {
        InFlightJob created = new InFlightJob();
        InFlightJob existing = inFlight.putIfAbsent(key, created);
        return existing == null
                ? new InFlightRegistration(created, true)
                : new InFlightRegistration(existing, false);
    }

    ScheduledFuture<?> startHeartbeat(JobKey key, long leaseEpoch, InFlightJob tracker) {
        long intervalMillis = Math.max(1L, config().heartbeatInterval().toMillis());
        return heartbeatScheduler.scheduleAtFixedRate(
                () -> heartbeatTick(key, leaseEpoch, tracker),
                intervalMillis,
                intervalMillis,
                TimeUnit.MILLISECONDS);
    }

    void unregisterInFlight(
            JobKey key, InFlightJob tracker, ScheduledFuture<?> heartbeat) {
        if (heartbeat != null) {
            heartbeat.cancel(false);
        }
        inFlight.remove(key, tracker);
    }

    private void heartbeatTick(JobKey key, long leaseEpoch, InFlightJob tracker) {
        if (tracker.completion.isDone() || tracker.leaseLost.get() || tracker.shuttingDown.get()) {
            return;
        }
        try {
            Optional<RepositorySinkJobStore.LeaseRenewal> renewed = renewLease(
                    key.accountId(), key.operationId(), leaseEpoch);
            if (renewed.isEmpty()) {
                Optional<RepositorySinkJobStore.StoredJob> latest =
                        find(key.accountId(), key.operationId());
                if (latest.isEmpty()
                        || !latest.get().lease().matches(instanceId, leaseEpoch)
                        || isTerminal(latest.get().job().getState())) {
                    tracker.markLeaseLost();
                }
                return;
            }
            if (renewed.get().cancelRequested()) {
                tracker.cancel(renewed.get().cancelReason());
            }
        } catch (RuntimeException transientStoreError) {
            LOG.debug(
                    "Transient repository sink heartbeat renewal failure for {}/{}",
                    key.accountId(),
                    key.operationId(),
                    transientStoreError);
        }
    }

    Optional<RepositorySinkJobStore.StoredJob> find(String accountId, String operationId) {
        return withInterruptCleared(() -> store.find(accountId, operationId));
    }

    RepositorySinkJobStore.StoredJob insertOrGet(RepositorySinkJobStore.StoredJob candidate) {
        return withInterruptCleared(() -> store.insertOrGet(candidate));
    }

    Optional<RepositorySinkJobStore.LeaseRenewal> renewLease(
            String accountId, String operationId, long leaseEpoch) {
        return withInterruptCleared(() -> store.renewLease(
                accountId, operationId, instanceId, leaseEpoch, nextExpiration()));
    }

    RepositorySinkJobStore.PreCommitDecision prepareCommit(
            RepositorySinkJobStore.StoredJob preCommit, long leaseEpoch) {
        return withInterruptCleared(
                () -> store.prepareCommit(preCommit, instanceId, leaseEpoch));
    }

    boolean transitionOwned(RepositorySinkJobStore.StoredJob updated, long leaseEpoch) {
        return withInterruptCleared(
                () -> store.transitionOwned(updated, instanceId, leaseEpoch));
    }

    boolean reconcileTerminalCommit(
            RepositorySinkJobStore.StoredJob expected, RepositorySinkJob committed) {
        return withInterruptCleared(() -> store.reconcileTerminalCommit(expected, committed));
    }

    Optional<RepositorySinkJobStore.StoredJob> requestCancel(
            String accountId, String operationId, String reason) {
        return withInterruptCleared(() -> store.requestCancel(accountId, operationId, reason));
    }

    Optional<RepositorySinkJobStore.StoredJob> tryClaimExpiredOrUnresolved(
            String accountId, String operationId, long expectedLeaseEpoch) {
        Instant now = clock.instant();
        Instant expiresAt = now.plus(config().leaseDuration());
        return withInterruptCleared(() -> store.tryClaimExpiredOrUnresolved(
                accountId, operationId, expectedLeaseEpoch, instanceId, now, expiresAt));
    }

    Optional<RepositorySinkJobStore.StoredJob> tryClaimRetryableFailure(
            String accountId,
            String operationId,
            long expectedLeaseEpoch,
            RepositorySinkJob requeuedJob) {
        return withInterruptCleared(() -> store.tryClaimRetryableFailure(
                accountId, operationId, expectedLeaseEpoch, requeuedJob, instanceId, nextExpiration()));
    }

    /**
     * Polls {@code store.find} boundedly until the job reaches a terminal state, its lease expires,
     * or {@code maxWait} elapses in wall-clock time.
     */
    RepositorySinkJobStore.StoredJob awaitRemoteOwnerProgress(
            RepositorySinkJobStore.StoredJob initial, Duration maxWait) {
        long waitNanos = Math.max(0L, maxWait.toNanos());
        if (waitNanos == 0L) {
            return find(initial.job().getAccountId(), initial.job().getOperationId())
                    .orElse(initial);
        }
        long deadlineNanos = System.nanoTime() + waitNanos;
        long pollMillis = Math.clamp(
                config().heartbeatInterval().toMillis() / 2L,
                MIN_CROSS_INSTANCE_POLL_MILLIS,
                MAX_CROSS_INSTANCE_POLL_MILLIS);
        RepositorySinkJobStore.StoredJob current = initial;
        while (System.nanoTime() < deadlineNanos) {
            if (Context.current().isCancelled() || Thread.currentThread().isInterrupted()) {
                throw Status.CANCELLED
                        .withDescription("Cancelled while awaiting peer repository sink owner")
                        .asRuntimeException();
            }
            Optional<RepositorySinkJobStore.StoredJob> refreshed =
                    find(current.job().getAccountId(), current.job().getOperationId());
            if (refreshed.isEmpty()) {
                return current;
            }
            current = refreshed.get();
            if (isTerminal(current.job().getState())
                    || current.job().getState()
                            == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_UNRESOLVED
                    || !current.lease().isActiveAt(clock.instant())) {
                return current;
            }
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
            if (remainingMillis <= 0L) {
                break;
            }
            try {
                Thread.sleep(Math.min(pollMillis, remainingMillis));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw Status.CANCELLED
                        .withDescription("Interrupted while awaiting peer repository sink owner")
                        .withCause(interrupted)
                        .asRuntimeException();
            }
        }
        return find(current.job().getAccountId(), current.job().getOperationId()).orElse(current);
    }

    @Override
    public void close() {
        for (InFlightJob active : inFlight.values()) {
            active.markShuttingDown();
        }
        heartbeatScheduler.shutdownNow();
    }

    private static <T> T withInterruptCleared(Supplier<T> action) {
        boolean restoreInterrupt = Thread.interrupted();
        try {
            return action.get();
        } finally {
            if (restoreInterrupt) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static boolean isTerminal(RepositorySinkJobState state) {
        return state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED
                || state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED
                || state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_CANCELLED;
    }
}
