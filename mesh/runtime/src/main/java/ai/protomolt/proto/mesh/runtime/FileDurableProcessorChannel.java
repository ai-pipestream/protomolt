package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.v1.ChannelRecord;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterRecord;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterReplayStatus;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterStatusChanged;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryAttemptRecord;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorOutcome;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorOutcomeKind;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;
import ai.protomolt.proto.mesh.runtime.v1.SettlementEffect;
import ai.protomolt.proto.mesh.runtime.v1.RetryStrategy;
import ai.protomolt.proto.mesh.runtime.v1.WorkClaimed;
import ai.protomolt.proto.mesh.runtime.v1.WorkCompleted;
import ai.protomolt.proto.mesh.runtime.v1.WorkEnqueued;
import ai.protomolt.proto.mesh.runtime.v1.WorkFailed;
import ai.protomolt.proto.mesh.runtime.v1.WorkReleased;
import ai.protomolt.proto.mesh.runtime.v1.WorkSettled;
import ai.protomolt.proto.mesh.runtime.v1.TypedPayload;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * File-backed protobuf processor channel.
 *
 * <p>The WAL starts with an eight-byte format marker. Each record is a big-endian length,
 * exact {@link ChannelRecord} bytes, and CRC32C. Every append is forced before in-memory state
 * advances. Recovery truncates only an incomplete final frame left by a torn append; a complete
 * frame with a bad checksum or an invalid transition is refused.</p>
 */
public class FileDurableProcessorChannel implements DurableProcessorChannel {

    private final DescriptorRegistry descriptors;
    private final Clock clock;
    private final ProcessorChannelJournal journal;
    private final Map<String, WorkState> states = new LinkedHashMap<>();
    private final Map<String, DeadLetterRecord> deadLetters = new LinkedHashMap<>();
    private final Map<String, NavigableMap<Long, String>> deadLettersByNamespace =
            new LinkedHashMap<>();
    private final List<ChannelRecord> log = new ArrayList<>();
    private boolean closed;

    public FileDurableProcessorChannel(Path path, DescriptorRegistry descriptors) {
        this(path, descriptors, Clock.systemUTC());
    }

    public FileDurableProcessorChannel(
            Path path, DescriptorRegistry descriptors, Clock clock) {
        this(new FileProcessorChannelJournal(Objects.requireNonNull(path, "path")),
                descriptors, clock);
    }

    FileDurableProcessorChannel(
            ProcessorChannelJournal journal,
            DescriptorRegistry descriptors,
            Clock clock) {
        this.journal = Objects.requireNonNull(journal, "journal");
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.clock = Objects.requireNonNull(clock, "clock");
        try {
            initializeAndReplay();
        } catch (RuntimeException e) {
            journal.close();
            throw e;
        }
    }

    @Override
    public synchronized DeliveryView enqueue(ProcessorWork work) {
        requireOpen();
        RemoteValidation.work(work, descriptors);
        if (!RemoteValidation.instant(work.getDeadline()).isAfter(clock.instant())) {
            throw new IllegalArgumentException(
                    "processor work deadline must be in the future");
        }
        WorkState existing = states.get(work.getDeliveryId());
        if (existing != null) {
            if (!existing.work.equals(work)) {
                throw new IllegalArgumentException("conflicting work already uses delivery_id "
                        + work.getDeliveryId());
            }
            return existing.view();
        }
        append(ChannelRecord.newBuilder()
                .setEnqueued(WorkEnqueued.newBuilder().setWork(work)));
        return states.get(work.getDeliveryId()).view();
    }

    @Override
    public synchronized List<DeliveryClaim> claim(
            String workerId,
            Collection<ProcessorContract> contracts,
            int permits,
            Duration leaseDuration,
            Instant now) {
        Map<String, Integer> processorPermits = new LinkedHashMap<>();
        contracts.forEach(contract -> processorPermits.put(
                contract.getProcessorId(), permits));
        return claim(workerId, contracts, processorPermits, permits, leaseDuration, now);
    }

    @Override
    public synchronized List<DeliveryClaim> claim(
            String workerId,
            Collection<ProcessorContract> contracts,
            Map<String, Integer> processorPermits,
            int permits,
            Duration leaseDuration,
            Instant now) {
        requireOpen();
        RemoteValidation.workerId(workerId);
        Objects.requireNonNull(contracts, "contracts");
        Objects.requireNonNull(processorPermits, "processorPermits");
        contracts.forEach(contract -> RemoteValidation.contract(contract, descriptors));
        processorPermits.forEach((processorId, capacity) -> {
            if (processorId == null || processorId.isBlank()
                    || capacity == null || capacity < 0 || capacity > 100_000) {
                throw new IllegalArgumentException(
                        "processor permits require a processor id and a value from 0 to 100000");
            }
        });
        if (permits < 1 || permits > 100_000) {
            throw new IllegalArgumentException("permits must be between 1 and 100000");
        }
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        Objects.requireNonNull(now, "now");
        expire(now);
        List<DeliveryClaim> claimed = new ArrayList<>();
        Map<String, Integer> remaining = new LinkedHashMap<>(processorPermits);
        for (WorkState state : states.values()) {
            if (claimed.size() == permits) {
                break;
            }
            String processorId = state.work.getContract().getProcessorId();
            if (state.status != DeliveryState.PENDING
                    || state.retryNotBefore.isAfter(now)
                    || remaining.getOrDefault(processorId, 0) <= 0
                    || !RemoteValidation.supports(contracts, state.work.getContract())) {
                continue;
            }
            if (state.attempt >= state.work.getMaxAttempts()) {
                appendFailed(state, "attempts-exhausted",
                        "delivery exhausted max_attempts before another claim", "", now);
                continue;
            }
            Instant workDeadline = RemoteValidation.instant(state.work.getDeadline());
            Instant leaseExpiry = now.plus(leaseDuration);
            if (workDeadline.isBefore(leaseExpiry)) {
                leaseExpiry = workDeadline;
            }
            DeliveryClaim claim = DeliveryClaim.newBuilder()
                    .setWork(state.work)
                    .setWorkerId(workerId)
                    .setLeaseToken(UUID.randomUUID().toString())
                    .setAttempt(state.attempt + 1)
                    .setLeaseExpiresAt(RemoteValidation.timestamp(leaseExpiry))
                    .build();
            append(ChannelRecord.newBuilder()
                    .setClaimed(WorkClaimed.newBuilder()
                            .setClaim(claim)
                            .setClaimedAt(RemoteValidation.timestamp(now))));
            claimed.add(claim);
            remaining.computeIfPresent(processorId, (ignored, available) -> available - 1);
        }
        return List.copyOf(claimed);
    }

    @Override
    public synchronized void complete(
            String workerId, ProcessorCompletion completion, Instant now) {
        requireOpen();
        RemoteValidation.annotations(completion);
        RemoteValidation.uuid(completion.getCompletionId(), "completion_id");
        WorkState existing = requireState(completion.getDeliveryId());
        if (existing.lastCompletionId.equals(completion.getCompletionId())) {
            if (existing.completion != null
                    && existing.completion.workerId().equals(workerId)
                    && existing.completion.completion().equals(completion)) {
                return;
            }
            throw new IllegalArgumentException(
                    "completion-id-conflict: completion_id already names different bytes");
        }
        WorkState state = requireClaim(workerId, completion.getDeliveryId(),
                completion.getLeaseToken(), now);
        validateCompletion(state.work.getContract(), completion);
        append(ChannelRecord.newBuilder()
                .setCompleted(WorkCompleted.newBuilder()
                        .setWorkerId(workerId)
                        .setCompletion(completion)
                        .setCompletedAt(RemoteValidation.timestamp(now))));
    }

    @Override
    public synchronized void fail(
            String workerId, ProcessorFailure failure, Instant now) {
        requireOpen();
        RemoteValidation.failure(failure);
        WorkState existing = requireState(failure.getDeliveryId());
        if (existing.lastCompletionId.equals(failure.getCompletionId())) {
            if (existing.lastFailure != null && existing.lastFailure.equals(failure)) {
                return;
            }
            throw new IllegalArgumentException(
                    "completion-id-conflict: completion_id already names different bytes");
        }
        WorkState state = requireClaim(workerId, failure.getDeliveryId(),
                failure.getLeaseToken(), now);
        String code = bounded(failure.getCode(), 128, "remote-processor-failed");
        String message = bounded(failure.getMessage(), 8_192, "remote processor failed");
        boolean release = failure.getOutcome().getSettlementEffect()
                == SettlementEffect.SETTLEMENT_EFFECT_RELEASE
                && (failure.getOutcome().getKind()
                == ProcessorOutcomeKind.PROCESSOR_OUTCOME_KIND_RETRYABLE
                || failure.getOutcome().getKind()
                == ProcessorOutcomeKind.PROCESSOR_OUTCOME_KIND_CANCELLED)
                && state.attempt < attemptsCeiling(state, failure.getOutcome())
                && RemoteValidation.instant(state.work.getDeadline()).isAfter(now);
        if (release) {
            appendReleased(state, message, failure.getLeaseToken(), failure.getOutcome(),
                    failure.getCompletionId(), retryNotBefore(state, failure.getOutcome(), now));
        } else {
            appendFailed(state, code, message, failure.getLeaseToken(), failure.getOutcome(),
                    failure.getCompletionId(), now);
        }
    }

    @Override
    public synchronized Completion awaitCompletion(String deliveryId, Instant deadline)
            throws InterruptedException {
        requireOpen();
        Objects.requireNonNull(deadline, "deadline");
        Duration budget = Duration.between(clock.instant(), deadline);
        long budgetNanos;
        try {
            budgetNanos = budget.toNanos();
        } catch (ArithmeticException e) {
            budgetNanos = Long.MAX_VALUE;
        }
        long endNanos = saturatedAdd(System.nanoTime(), Math.max(0, budgetNanos));
        while (true) {
            expire(clock.instant());
            WorkState state = requireState(deliveryId);
            if (state.status == DeliveryState.COMPLETED
                    || state.status == DeliveryState.SETTLED) {
                return state.completion;
            }
            if (state.status == DeliveryState.FAILED) {
                throw new RemoteProcessorException(
                        state.failureCode, state.failureMessage);
            }
            long remaining = endNanos - System.nanoTime();
            if (remaining <= 0) {
                throw new RemoteProcessorException("remote-deadline-exceeded",
                        "delivery " + deliveryId + " did not complete before " + deadline);
            }
            long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
            int nanos = (int) (remaining - TimeUnit.MILLISECONDS.toNanos(millis));
            wait(millis, nanos);
            requireOpen();
        }
    }

    @Override
    public synchronized void settle(String deliveryId, String leaseToken, Instant now) {
        requireOpen();
        Objects.requireNonNull(now, "now");
        expire(now);
        WorkState state = requireState(deliveryId);
        if (state.status == DeliveryState.SETTLED
                && state.lastLeaseToken.equals(leaseToken)) {
            return;
        }
        if (state.status != DeliveryState.COMPLETED) {
            throw new IllegalStateException("delivery " + deliveryId
                    + " is not completed; state=" + state.status);
        }
        requireToken(state, leaseToken);
        append(ChannelRecord.newBuilder()
                .setSettled(WorkSettled.newBuilder()
                        .setDeliveryId(deliveryId)
                        .setLeaseToken(leaseToken)));
    }

    @Override
    public synchronized void release(
            String deliveryId, String leaseToken, String reason, Instant now) {
        requireOpen();
        Objects.requireNonNull(now, "now");
        WorkState state = requireState(deliveryId);
        if (state.status == DeliveryState.PENDING
                && state.lastLeaseToken.equals(leaseToken)) {
            return;
        }
        if (state.status != DeliveryState.CLAIMED
                && state.status != DeliveryState.COMPLETED) {
            throw new IllegalStateException("delivery " + deliveryId
                    + " cannot be released from state " + state.status);
        }
        requireToken(state, leaseToken);
        if (state.attempt >= state.work.getMaxAttempts()
                || !RemoteValidation.instant(state.work.getDeadline()).isAfter(now)) {
            appendFailed(state, "downstream-failed",
                    bounded(reason, 8_192, "downstream processing failed"), leaseToken,
                    now);
        } else {
            String message = bounded(reason, 8_192, "delivery released");
            ProcessorOutcome outcome = ProcessorOutcomes.retryable(
                    "downstream-released", message,
                    state.work.getContract().getProcessorId(), state.attempt,
                    state.work.getMaxAttempts());
            appendReleased(state, message, leaseToken, outcome,
                    stableCompletionId(state, "release"), retryNotBefore(state, outcome, now));
        }
    }

    @Override
    public synchronized void releaseWorker(String workerId, String reason, Instant now) {
        requireOpen();
        RemoteValidation.workerId(workerId);
        List<WorkState> owned = states.values().stream()
                .filter(state -> state.status == DeliveryState.CLAIMED)
                .filter(state -> state.claim.getWorkerId().equals(workerId))
                .toList();
        for (WorkState state : owned) {
            release(state.work.getDeliveryId(), state.lastLeaseToken, reason, now);
        }
    }

    @Override
    public synchronized int expire(Instant now) {
        requireOpen();
        Objects.requireNonNull(now, "now");
        int transitions = 0;
        for (WorkState state : List.copyOf(states.values())) {
            if (state.status == DeliveryState.FAILED
                    || state.status == DeliveryState.SETTLED) {
                continue;
            }
            if (!RemoteValidation.instant(state.work.getDeadline()).isAfter(now)) {
                appendFailed(state, "work-deadline-exceeded",
                        "delivery deadline elapsed before downstream settlement",
                        state.lastLeaseToken, now);
                transitions++;
                continue;
            }
            if (state.status == DeliveryState.CLAIMED
                    && !RemoteValidation.instant(state.claim.getLeaseExpiresAt()).isAfter(now)) {
                if (state.attempt >= state.work.getMaxAttempts()) {
                    appendFailed(state, "lease-expired",
                            "delivery lease expired and max_attempts is exhausted",
                            state.lastLeaseToken, now);
                } else {
                    String message = "delivery lease expired";
                    ProcessorOutcome outcome = ProcessorOutcomes.retryable(
                            "lease-expired", message,
                            state.work.getContract().getProcessorId(), state.attempt,
                            state.work.getMaxAttempts());
                    appendReleased(state, message, state.lastLeaseToken, outcome,
                            stableCompletionId(state, "lease-expired"),
                            retryNotBefore(state, outcome, now));
                }
                transitions++;
            }
        }
        return transitions;
    }

    @Override
    public synchronized Optional<DeliveryView> delivery(String deliveryId) {
        WorkState state = states.get(deliveryId);
        return state == null ? Optional.empty() : Optional.of(state.view());
    }

    @Override
    public synchronized List<DeliveryView> deliveries() {
        return states.values().stream().map(WorkState::view).toList();
    }

    @Override
    public synchronized List<ChannelRecord> records() {
        return List.copyOf(log);
    }

    @Override
    public synchronized DeadLetterPage deadLetters(
            String namespace, long afterSequence, int limit) {
        requireOpen();
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("dead-letter-namespace-required");
        }
        if (afterSequence < 0) {
            throw new IllegalArgumentException("dead-letter cursor must not be negative");
        }
        if (limit < 1 || limit > 1_000) {
            throw new IllegalArgumentException("dead-letter limit must be between 1 and 1000");
        }
        NavigableMap<Long, String> index = deadLettersByNamespace.get(namespace);
        if (index == null) {
            return new DeadLetterPage(List.of(), afterSequence);
        }
        List<DeadLetterPage.Entry> entries = index.tailMap(afterSequence, false)
                .entrySet().stream()
                .limit(limit)
                .map(entry -> new DeadLetterPage.Entry(entry.getKey(),
                        deadLetters.get(entry.getValue())))
                .toList();
        long nextSequence = entries.isEmpty()
                ? afterSequence : entries.getLast().sequence();
        return new DeadLetterPage(entries, nextSequence);
    }

    @Override
    public synchronized Optional<DeadLetterRecord> deadLetter(String deadLetterId) {
        return Optional.ofNullable(deadLetters.get(deadLetterId));
    }

    @Override
    public synchronized DeadLetterRecord cancelRetry(
            String deliveryId, String reason, Instant now) {
        requireOpen();
        Objects.requireNonNull(now, "now");
        WorkState state = requireState(deliveryId);
        if (state.status == DeliveryState.FAILED && state.deadLetter != null) {
            return state.deadLetter;
        }
        if (state.status != DeliveryState.PENDING || state.lastOutcome == null
                || state.retryNotBefore.equals(Instant.MIN)) {
            throw new IllegalStateException(
                    "retry-cancel-refused: delivery has no scheduled retry");
        }
        String message = bounded(reason, 8_192, "scheduled retry cancelled");
        ProcessorOutcome outcome = ProcessorOutcomes.cancelled(
                message, state.work.getContract().getProcessorId(), state.attempt);
        String completionId = stableCompletionId(state, "retry-cancelled");
        appendFailed(state, "retry-cancelled", message, state.lastLeaseToken,
                outcome, completionId, now);
        return states.get(deliveryId).deadLetter;
    }

    @Override
    public synchronized DeadLetterRecord changeDeadLetterStatus(
            String deadLetterId,
            DeadLetterReplayStatus status,
            String replayRunId,
            String reason,
            boolean retentionHold) {
        requireOpen();
        RemoteValidation.uuid(deadLetterId, "dead_letter_id");
        if (status == DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_UNSPECIFIED
                || status == DeadLetterReplayStatus.UNRECOGNIZED) {
            throw new IllegalArgumentException("dead-letter status must be explicit");
        }
        DeadLetterRecord current = deadLetters.get(deadLetterId);
        if (current == null) {
            throw new IllegalArgumentException("unknown dead_letter_id " + deadLetterId);
        }
        DeadLetterRecord next = current.toBuilder()
                .setReplayStatus(status)
                .setRetentionHold(retentionHold)
                .setReplayRunId(replayRunId == null ? "" : replayRunId)
                .build();
        if (next.equals(current)) {
            return current;
        }
        append(ChannelRecord.newBuilder()
                .setDeadLetterStatusChanged(DeadLetterStatusChanged.newBuilder()
                        .setDeadLetterId(deadLetterId)
                        .setReplayStatus(status)
                        .setReplayRunId(replayRunId == null ? "" : replayRunId)
                        .setReason(bounded(reason, 8_192, "status changed"))
                        .setRetentionHold(retentionHold)));
        return deadLetters.get(deadLetterId);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        notifyAll();
        journal.close();
    }

    private WorkState requireClaim(
            String workerId, String deliveryId, String leaseToken, Instant now) {
        RemoteValidation.workerId(workerId);
        Objects.requireNonNull(now, "now");
        RemoteValidation.uuid(deliveryId, "delivery_id");
        RemoteValidation.uuid(leaseToken, "lease_token");
        expire(now);
        WorkState state = requireState(deliveryId);
        if (state.status != DeliveryState.CLAIMED) {
            throw new IllegalStateException("delivery " + deliveryId
                    + " has no active claim; state=" + state.status);
        }
        requireToken(state, leaseToken);
        if (!state.claim.getWorkerId().equals(workerId)) {
            throw new IllegalArgumentException("delivery " + deliveryId
                    + " is claimed by another worker");
        }
        return state;
    }

    private void validateCompletion(
            ProcessorContract contract, ProcessorCompletion completion) {
        if (completion.getOutputsCount() > contract.getMaxOutputs()) {
            throw new IllegalArgumentException("processor " + contract.getProcessorId()
                    + " returned " + completion.getOutputsCount()
                    + " outputs, exceeding max_outputs " + contract.getMaxOutputs());
        }
        for (TypedPayload output : completion.getOutputsList()) {
            DescriptorIdentity actual = DescriptorIdentity.of(
                    RuntimeSchemas.unpack(descriptors, output).getDescriptorForType());
            boolean declared = contract.getOutputSchemasList().stream()
                    .map(RuntimeSchemas::identity)
                    .anyMatch(actual::equals);
            if (!declared) {
                throw new IllegalArgumentException("processor " + contract.getProcessorId()
                        + " returned undeclared schema " + actual);
            }
        }
    }

    private void appendReleased(
            WorkState state,
            String reason,
            String leaseToken,
            ProcessorOutcome outcome,
            String completionId,
            Instant retryNotBefore) {
        append(ChannelRecord.newBuilder()
                .setReleased(WorkReleased.newBuilder()
                        .setDeliveryId(state.work.getDeliveryId())
                        .setLeaseToken(leaseToken)
                        .setReason(reason)
                        .setOutcome(outcome)
                        .setCompletionId(completionId)
                        .setRetryNotBefore(RemoteValidation.timestamp(retryNotBefore))
                        .setFinishedAt(RemoteValidation.timestamp(clock.instant()))));
    }

    private void appendFailed(
            WorkState state, String code, String message, String leaseToken, Instant now) {
        ProcessorOutcome outcome = ProcessorOutcomes.permanent(code, message,
                state.work.getContract().getProcessorId(), state.attempt);
        appendFailed(state, code, message, leaseToken, outcome,
                stableCompletionId(state, code), now);
    }

    private void appendFailed(
            WorkState state,
            String code,
            String message,
            String leaseToken,
            ProcessorOutcome outcome,
            String completionId,
            Instant now) {
        DeadLetterRecord deadLetter = deadLetter(
                state, outcome, completionId, now);
        append(ChannelRecord.newBuilder()
                .setFailed(WorkFailed.newBuilder()
                        .setDeliveryId(state.work.getDeliveryId())
                        .setLeaseToken(leaseToken)
                        .setCode(code)
                        .setMessage(message)
                        .setOutcome(outcome)
                        .setCompletionId(completionId)
                        .setDeadLetter(deadLetter)));
    }

    private DeadLetterRecord deadLetter(
            WorkState state,
            ProcessorOutcome outcome,
            String completionId,
            Instant now) {
        ProcessorWork work = state.work;
        String deadLetterId = EntityEnvelopes.stableUuid(
                "dead-letter\0" + work.getDeliveryId() + '\0' + completionId);
        List<DeliveryAttemptRecord> attempts = finishedAttempts(
                state, outcome.getOutcomeId(), now);
        Instant firstFailure = attempts.stream()
                .filter(DeliveryAttemptRecord::hasFinishedAt)
                .map(DeliveryAttemptRecord::getFinishedAt)
                .map(RemoteValidation::instant)
                .min(Instant::compareTo)
                .orElse(now);
        DeadLetterRecord.Builder record = DeadLetterRecord.newBuilder()
                .setDeadLetterId(deadLetterId)
                .setDeliveryId(work.getDeliveryId())
                .setRunId(work.getRunId())
                .setInvocationId(work.getInvocationId())
                .setWorkflowName(work.getWorkflowName())
                .setWorkflowVersion(work.getWorkflowVersion())
                .setPlanFingerprint(work.getPlanFingerprint())
                .setDeploymentRevision(work.getDeploymentRevision())
                .setProcessorId(work.getContract().getProcessorId())
                .setNodeId(work.getNodeId())
                .setEdgeId(work.getEdgeId())
                .setInput(work.getInput())
                .setOutcome(outcome)
                .addAllAttempts(attempts)
                .setFirstFailureAt(RemoteValidation.timestamp(firstFailure))
                .setLastFailureAt(RemoteValidation.timestamp(now))
                .setChannelPolicyId(work.getChannelPolicyId())
                .setSourceHistorySequence(work.getSourceHistorySequence())
                .setReplayStatus(DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_PENDING)
                .setNamespace(work.getNamespace().isBlank()
                        ? work.getInput().getHeader().getScopeId() : work.getNamespace())
                .setRetentionPolicyReference(work.getRetentionPolicyReference())
                .setLegalHoldPolicyReference(work.getLegalHoldPolicyReference())
                .setPayloadStoreProfile(work.getPayloadStoreProfile())
                .setCompletionId(completionId)
                .setProcessorContract(work.getContract());
        return record.build();
    }

    private static int attemptsCeiling(WorkState state, ProcessorOutcome outcome) {
        int advised = outcome.getRetryAdvice().getMaximumAttempts();
        return advised == 0 ? state.work.getMaxAttempts()
                : Math.min(state.work.getMaxAttempts(), advised);
    }

    private static Instant retryNotBefore(
            WorkState state, ProcessorOutcome outcome, Instant now) {
        var advice = outcome.getRetryAdvice();
        return switch (advice.getStrategy()) {
            case RETRY_STRATEGY_FIXED_DELAY -> now.plus(RemoteValidation.duration(advice.getDelay()));
            case RETRY_STRATEGY_EXPONENTIAL_BACKOFF -> {
                Duration base = RemoteValidation.duration(advice.getDelay());
                int shift = Math.min(30, Math.max(0, state.attempt - 1));
                Duration delay;
                try {
                    delay = base.multipliedBy(1L << shift);
                } catch (ArithmeticException overflow) {
                    delay = Duration.ofDays(3650);
                }
                if (advice.hasMaximumDelay()) {
                    Duration maximum = RemoteValidation.duration(advice.getMaximumDelay());
                    if (delay.compareTo(maximum) > 0) {
                        delay = maximum;
                    }
                }
                yield now.plus(delay);
            }
            case RETRY_STRATEGY_RETRY_AFTER -> RemoteValidation.instant(advice.getRetryAfter());
            case RETRY_STRATEGY_NONE, RETRY_STRATEGY_UNSPECIFIED, UNRECOGNIZED -> now;
        };
    }

    private static String stableCompletionId(WorkState state, String transition) {
        return EntityEnvelopes.stableUuid("channel-completion\0"
                + state.work.getDeliveryId() + '\0' + state.attempt + '\0' + transition);
    }

    private static List<DeliveryAttemptRecord> finishedAttempts(
            WorkState state, String outcomeId, Instant now) {
        List<DeliveryAttemptRecord> attempts = new ArrayList<>(state.attempts);
        if (!attempts.isEmpty()) {
            int last = attempts.size() - 1;
            DeliveryAttemptRecord current = attempts.get(last);
            if (!current.hasFinishedAt()) {
                attempts.set(last, current.toBuilder()
                        .setFinishedAt(RemoteValidation.timestamp(now))
                        .setOutcomeId(outcomeId)
                        .build());
            }
        }
        return attempts;
    }

    private void append(ChannelRecord.Builder event) {
        ChannelRecord record = event
                .setSequence(log.size() + 1L)
                .setRecordedAt(RemoteValidation.timestamp(clock.instant()))
                .build();
        validate(record);
        journal.append(record);
        apply(record);
        log.add(record);
        notifyAll();
    }

    /** Validates the complete reducer transition before any bytes become durable. */
    private void validate(ChannelRecord record) {
        RemoteValidation.annotations(record);
        long expected = log.size() + 1L;
        if (record.getSequence() != expected) {
            throw new IllegalArgumentException("channel sequence gap: expected "
                    + expected + " but found " + record.getSequence());
        }
        if (!record.hasRecordedAt()) {
            throw new IllegalArgumentException("channel record requires recorded_at");
        }
        RemoteValidation.instant(record.getRecordedAt());
        switch (record.getEventCase()) {
            case ENQUEUED -> validateEnqueued(record.getEnqueued());
            case CLAIMED -> validateClaimed(record.getClaimed());
            case COMPLETED -> validateCompleted(record.getCompleted());
            case RELEASED -> validateReleased(record.getReleased());
            case FAILED -> validateFailed(record.getFailed());
            case SETTLED -> validateSettled(record.getSettled());
            case DEAD_LETTER_STATUS_CHANGED -> validateDeadLetterStatusChanged(
                    record.getDeadLetterStatusChanged());
            case EVENT_NOT_SET -> throw new IllegalArgumentException(
                    "channel record " + record.getSequence() + " has no event");
        }
    }

    private void validateEnqueued(WorkEnqueued event) {
        ProcessorWork work = event.getWork();
        RemoteValidation.work(work, descriptors);
        if (states.containsKey(work.getDeliveryId())) {
            throw new IllegalArgumentException("channel re-enqueues delivery_id "
                    + work.getDeliveryId());
        }
    }

    private void validateClaimed(WorkClaimed event) {
        DeliveryClaim claim = event.getClaim();
        RemoteValidation.claim(claim, descriptors);
        WorkState state = requireState(claim.getWork().getDeliveryId());
        if (state.status != DeliveryState.PENDING || !state.work.equals(claim.getWork())) {
            throw new IllegalArgumentException("invalid claim transition for delivery "
                    + state.work.getDeliveryId());
        }
        if (claim.getAttempt() != state.attempt + 1) {
            throw new IllegalArgumentException("delivery " + state.work.getDeliveryId()
                    + " claim attempt must be " + (state.attempt + 1));
        }
        if (!event.hasClaimedAt()) {
            throw new IllegalArgumentException("work claim requires claimed_at");
        }
        RemoteValidation.instant(event.getClaimedAt());
    }

    private void validateCompleted(WorkCompleted event) {
        ProcessorCompletion completion = event.getCompletion();
        RemoteValidation.workerId(event.getWorkerId());
        RemoteValidation.uuid(completion.getDeliveryId(), "delivery_id");
        RemoteValidation.uuid(completion.getLeaseToken(), "lease_token");
        RemoteValidation.uuid(completion.getCompletionId(), "completion_id");
        WorkState state = requireState(completion.getDeliveryId());
        if (state.status != DeliveryState.CLAIMED
                || !state.claim.getWorkerId().equals(event.getWorkerId())) {
            throw new IllegalArgumentException("invalid completion transition for delivery "
                    + completion.getDeliveryId());
        }
        requireToken(state, completion.getLeaseToken());
        requireNewCompletionId(state, completion.getCompletionId());
        if (!event.hasCompletedAt()) {
            throw new IllegalArgumentException("work completion requires completed_at");
        }
        RemoteValidation.instant(event.getCompletedAt());
        validateCompletion(state.work.getContract(), completion);
    }

    private void validateReleased(WorkReleased event) {
        RemoteValidation.uuid(event.getDeliveryId(), "delivery_id");
        RemoteValidation.uuid(event.getLeaseToken(), "lease_token");
        WorkState state = requireState(event.getDeliveryId());
        if (state.status != DeliveryState.CLAIMED
                && state.status != DeliveryState.COMPLETED) {
            throw new IllegalArgumentException("invalid release transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        requireToken(state, event.getLeaseToken());
        if (event.getReason().isBlank() || event.getReason().length() > 8_192) {
            throw new IllegalArgumentException(
                    "release reason must contain between 1 and 8192 characters");
        }
        if (!event.hasOutcome() || !event.hasRetryNotBefore()) {
            throw new IllegalArgumentException(
                    "retry-schedule-missing: release requires outcome and retry_not_before");
        }
        RemoteValidation.outcome(event.getOutcome());
        RemoteValidation.instant(event.getRetryNotBefore());
        RemoteValidation.uuid(event.getCompletionId(), "completion_id");
        requireNewCompletionId(state, event.getCompletionId());
        if (!event.hasFinishedAt()) {
            throw new IllegalArgumentException("retry schedule requires finished_at");
        }
        RemoteValidation.instant(event.getFinishedAt());
    }

    private void validateFailed(WorkFailed event) {
        RemoteValidation.uuid(event.getDeliveryId(), "delivery_id");
        WorkState state = requireState(event.getDeliveryId());
        if (state.status == DeliveryState.SETTLED
                || state.status == DeliveryState.FAILED) {
            throw new IllegalArgumentException("invalid failure transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        if (!event.getLeaseToken().isEmpty()) {
            RemoteValidation.uuid(event.getLeaseToken(), "lease_token");
            requireToken(state, event.getLeaseToken());
        }
        if (event.getCode().isBlank() || event.getCode().length() > 128) {
            throw new IllegalArgumentException(
                    "failure code must contain between 1 and 128 characters");
        }
        if (event.getMessage().isBlank() || event.getMessage().length() > 8_192) {
            throw new IllegalArgumentException(
                    "failure message must contain between 1 and 8192 characters");
        }
        if (!event.hasOutcome()) {
            throw new IllegalArgumentException("failed work requires a typed outcome");
        }
        RemoteValidation.outcome(event.getOutcome());
        RemoteValidation.uuid(event.getCompletionId(), "completion_id");
        requireNewCompletionId(state, event.getCompletionId());
        if (!event.hasDeadLetter()) {
            throw new IllegalArgumentException(
                    "dead-letter-missing: terminal failure requires recovery record");
        }
        validateDeadLetter(event.getDeadLetter(), state, event);
    }

    private void validateSettled(WorkSettled event) {
        RemoteValidation.uuid(event.getDeliveryId(), "delivery_id");
        RemoteValidation.uuid(event.getLeaseToken(), "lease_token");
        WorkState state = requireState(event.getDeliveryId());
        if (state.status != DeliveryState.COMPLETED) {
            throw new IllegalArgumentException("invalid settlement transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        requireToken(state, event.getLeaseToken());
    }

    private void validateDeadLetterStatusChanged(DeadLetterStatusChanged event) {
        RemoteValidation.uuid(event.getDeadLetterId(), "dead_letter_id");
        if (!deadLetters.containsKey(event.getDeadLetterId())) {
            throw new IllegalArgumentException(
                    "unknown dead_letter_id " + event.getDeadLetterId());
        }
        if (event.getReplayStatus()
                == DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_UNSPECIFIED
                || event.getReplayStatus() == DeadLetterReplayStatus.UNRECOGNIZED) {
            throw new IllegalArgumentException("dead-letter status must be explicit");
        }
        if (!event.getReplayRunId().isBlank()) {
            RemoteValidation.uuid(event.getReplayRunId(), "replay_run_id");
        }
    }

    private static void validateDeadLetter(
            DeadLetterRecord record, WorkState state, WorkFailed failure) {
        RemoteValidation.uuid(record.getDeadLetterId(), "dead_letter_id");
        if (!record.getDeliveryId().equals(state.work.getDeliveryId())
                || !record.getRunId().equals(state.work.getRunId())
                || !record.getInvocationId().equals(state.work.getInvocationId())
                || !record.getInput().equals(state.work.getInput())
                || !record.getProcessorContract().equals(state.work.getContract())
                || !record.getPayloadStoreProfile().equals(
                        state.work.getPayloadStoreProfile())
                || !record.getOutcome().equals(failure.getOutcome())
                || !record.getCompletionId().equals(failure.getCompletionId())) {
            throw new IllegalArgumentException(
                    "dead-letter-identity-mismatch: terminal record does not match work");
        }
        if (record.getNamespace().isBlank()
                || !record.hasFirstFailureAt() || !record.hasLastFailureAt()
                || record.getReplayStatus()
                == DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_UNSPECIFIED) {
            throw new IllegalArgumentException(
                    "dead-letter-invalid: namespace, timestamps, and status are required");
        }
    }

    private static void requireNewCompletionId(WorkState state, String completionId) {
        if (state.lastCompletionId.equals(completionId)) {
            throw new IllegalArgumentException(
                    "completion-id-conflict: transition repeats a consumed completion_id");
        }
    }

    private void apply(ChannelRecord record) {
        long expected = log.size() + 1L;
        if (record.getSequence() != expected) {
            throw new IllegalArgumentException("channel sequence gap: expected "
                    + expected + " but found " + record.getSequence());
        }
        switch (record.getEventCase()) {
            case ENQUEUED -> applyEnqueued(record.getEnqueued());
            case CLAIMED -> applyClaimed(record.getClaimed());
            case COMPLETED -> applyCompleted(record.getCompleted());
            case RELEASED -> applyReleased(record.getReleased());
            case FAILED -> applyFailed(record.getFailed());
            case SETTLED -> applySettled(record.getSettled());
            case DEAD_LETTER_STATUS_CHANGED -> applyDeadLetterStatusChanged(
                    record.getDeadLetterStatusChanged());
            case EVENT_NOT_SET -> throw new IllegalArgumentException(
                    "channel record " + record.getSequence() + " has no event");
        }
    }

    private void applyEnqueued(WorkEnqueued event) {
        ProcessorWork work = event.getWork();
        RemoteValidation.work(work, descriptors);
        if (states.putIfAbsent(work.getDeliveryId(), new WorkState(work)) != null) {
            throw new IllegalArgumentException("channel re-enqueues delivery_id "
                    + work.getDeliveryId());
        }
    }

    private void applyClaimed(WorkClaimed event) {
        DeliveryClaim claim = event.getClaim();
        WorkState state = requireState(claim.getWork().getDeliveryId());
        if (state.status != DeliveryState.PENDING || !state.work.equals(claim.getWork())) {
            throw new IllegalArgumentException("invalid claim transition for delivery "
                    + state.work.getDeliveryId());
        }
        RemoteValidation.workerId(claim.getWorkerId());
        RemoteValidation.uuid(claim.getLeaseToken(), "lease_token");
        RemoteValidation.instant(claim.getLeaseExpiresAt());
        if (claim.getAttempt() != state.attempt + 1) {
            throw new IllegalArgumentException("delivery " + state.work.getDeliveryId()
                    + " claim attempt must be " + (state.attempt + 1));
        }
        state.status = DeliveryState.CLAIMED;
        state.attempt = claim.getAttempt();
        state.claim = claim;
        state.lastLeaseToken = claim.getLeaseToken();
        state.completion = null;
        state.failureCode = "";
        state.failureMessage = "";
        state.retryNotBefore = Instant.MIN;
        state.attempts.add(DeliveryAttemptRecord.newBuilder()
                .setAttempt(claim.getAttempt())
                .setWorkerId(claim.getWorkerId())
                .setLeaseToken(claim.getLeaseToken())
                .setClaimedAt(event.getClaimedAt())
                .build());
    }

    private void applyCompleted(WorkCompleted event) {
        ProcessorCompletion completion = event.getCompletion();
        WorkState state = requireState(completion.getDeliveryId());
        if (state.status != DeliveryState.CLAIMED
                || !state.claim.getWorkerId().equals(event.getWorkerId())) {
            throw new IllegalArgumentException("invalid completion transition for delivery "
                    + completion.getDeliveryId());
        }
        requireToken(state, completion.getLeaseToken());
        validateCompletion(state.work.getContract(), completion);
        state.status = DeliveryState.COMPLETED;
        state.completion = new Completion(event.getWorkerId(), completion);
        state.lastCompletionId = completion.getCompletionId();
        state.lastOutcome = ProcessorOutcome.getDefaultInstance();
    }

    private void applyReleased(WorkReleased event) {
        WorkState state = requireState(event.getDeliveryId());
        if (state.status != DeliveryState.CLAIMED
                && state.status != DeliveryState.COMPLETED) {
            throw new IllegalArgumentException("invalid release transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        requireToken(state, event.getLeaseToken());
        state.status = DeliveryState.PENDING;
        state.claim = null;
        state.completion = null;
        state.lastOutcome = event.getOutcome();
        state.lastCompletionId = event.getCompletionId();
        state.retryNotBefore = RemoteValidation.instant(event.getRetryNotBefore());
        state.lastFailure = ProcessorFailure.newBuilder()
                .setDeliveryId(event.getDeliveryId())
                .setLeaseToken(event.getLeaseToken())
                .setCode(event.getOutcome().getCausesCount() == 0
                        ? "retry-scheduled" : event.getOutcome().getCauses(0).getCode())
                .setMessage(event.getReason())
                .setOutcome(event.getOutcome())
                .setCompletionId(event.getCompletionId())
                .build();
        finishAttempt(state, event.getOutcome().getOutcomeId(), event.getFinishedAt());
    }

    private void applyFailed(WorkFailed event) {
        WorkState state = requireState(event.getDeliveryId());
        if (state.status == DeliveryState.SETTLED
                || state.status == DeliveryState.FAILED) {
            throw new IllegalArgumentException("invalid failure transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        if (!event.getLeaseToken().isEmpty()) {
            requireToken(state, event.getLeaseToken());
        }
        state.status = DeliveryState.FAILED;
        state.failureCode = event.getCode();
        state.failureMessage = event.getMessage();
        state.completion = null;
        state.lastOutcome = event.getOutcome();
        state.lastCompletionId = event.getCompletionId();
        state.retryNotBefore = Instant.MIN;
        state.deadLetter = event.getDeadLetter();
        long sequence = deadLetters.size() + 1L;
        deadLetters.put(event.getDeadLetter().getDeadLetterId(), event.getDeadLetter());
        deadLettersByNamespace
                .computeIfAbsent(event.getDeadLetter().getNamespace(), ignored -> new TreeMap<>())
                .put(sequence, event.getDeadLetter().getDeadLetterId());
        state.lastFailure = ProcessorFailure.newBuilder()
                .setDeliveryId(event.getDeliveryId())
                .setLeaseToken(event.getLeaseToken())
                .setCode(event.getCode())
                .setMessage(event.getMessage())
                .setOutcome(event.getOutcome())
                .setCompletionId(event.getCompletionId())
                .build();
        state.attempts.clear();
        state.attempts.addAll(event.getDeadLetter().getAttemptsList());
    }

    private void applySettled(WorkSettled event) {
        WorkState state = requireState(event.getDeliveryId());
        if (state.status != DeliveryState.COMPLETED) {
            throw new IllegalArgumentException("invalid settlement transition for delivery "
                    + event.getDeliveryId() + " from " + state.status);
        }
        requireToken(state, event.getLeaseToken());
        state.status = DeliveryState.SETTLED;
    }

    private void applyDeadLetterStatusChanged(DeadLetterStatusChanged event) {
        DeadLetterRecord current = deadLetters.get(event.getDeadLetterId());
        DeadLetterRecord next = current.toBuilder()
                .setReplayStatus(event.getReplayStatus())
                .setReplayRunId(event.getReplayRunId())
                .setRetentionHold(event.getRetentionHold())
                .build();
        deadLetters.put(event.getDeadLetterId(), next);
        WorkState state = states.get(current.getDeliveryId());
        if (state != null) {
            state.deadLetter = next;
        }
    }

    private static void finishAttempt(
            WorkState state, String outcomeId, com.google.protobuf.Timestamp finishedAt) {
        if (state.attempts.isEmpty()) {
            return;
        }
        int last = state.attempts.size() - 1;
        DeliveryAttemptRecord current = state.attempts.get(last);
        if (current.hasFinishedAt()) {
            return;
        }
        state.attempts.set(last, current.toBuilder()
                .setFinishedAt(finishedAt)
                .setOutcomeId(outcomeId)
                .build());
    }

    private void initializeAndReplay() {
        for (ChannelRecord record : journal.load()) {
            validate(record);
            apply(record);
            log.add(record);
        }
    }

    private WorkState requireState(String deliveryId) {
        WorkState state = states.get(deliveryId);
        if (state == null) {
            throw new IllegalArgumentException("unknown delivery_id " + deliveryId);
        }
        return state;
    }

    private static void requireToken(WorkState state, String leaseToken) {
        if (!state.lastLeaseToken.equals(leaseToken)) {
            throw new IllegalArgumentException("stale lease_token for delivery "
                    + state.work.getDeliveryId());
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("processor channel is closed");
        }
    }

    private static String bounded(
            String value, int maximum, String fallback) {
        String result = value == null || value.isBlank() ? fallback : value;
        return result.length() <= maximum ? result : result.substring(0, maximum);
    }

    private static long saturatedAdd(long first, long second) {
        long result = first + second;
        if (((first ^ result) & (second ^ result)) < 0) {
            return Long.MAX_VALUE;
        }
        return result;
    }

    private static final class WorkState {
        private final ProcessorWork work;
        private DeliveryState status = DeliveryState.PENDING;
        private int attempt;
        private DeliveryClaim claim;
        private Completion completion;
        private String lastLeaseToken = "";
        private String failureCode = "";
        private String failureMessage = "";
        private ProcessorOutcome lastOutcome = ProcessorOutcome.getDefaultInstance();
        private Instant retryNotBefore = Instant.MIN;
        private DeadLetterRecord deadLetter;
        private String lastCompletionId = "";
        private ProcessorFailure lastFailure;
        private final List<DeliveryAttemptRecord> attempts = new ArrayList<>();

        private WorkState(ProcessorWork work) {
            this.work = work;
        }

        private DeliveryView view() {
            return new DeliveryView(work, status, attempt, claim, completion,
                    failureCode, failureMessage, lastOutcome, retryNotBefore, deadLetter);
        }
    }
}
