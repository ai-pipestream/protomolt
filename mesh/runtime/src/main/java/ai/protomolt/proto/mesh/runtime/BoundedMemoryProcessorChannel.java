package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.v1.ChannelOverflowAction;
import ai.protomolt.proto.mesh.runtime.v1.ChannelRecord;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterRecord;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterReplayStatus;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Fixed item/byte-budget channel for explicitly non-durable work. */
public final class BoundedMemoryProcessorChannel implements DurableProcessorChannel {

    private final FileDurableProcessorChannel memory;
    private final DurableProcessorChannel spill;
    private final ChannelOverflowAction overflow;
    private final int maxItems;
    private final long maxBytes;
    private final Clock clock;
    private final Map<String, Integer> reservations = new LinkedHashMap<>();
    private long reservedBytes;
    private boolean closed;

    public BoundedMemoryProcessorChannel(
            DescriptorRegistry descriptors,
            int maxItems,
            long maxBytes,
            ChannelOverflowAction overflow,
            DurableProcessorChannel namedSpill,
            Clock clock) {
        if (maxItems < 1 || maxItems > 1_000_000 || maxBytes < 1) {
            throw new IllegalArgumentException(
                    "memory-channel-bounds-invalid: positive item and byte bounds are required");
        }
        this.overflow = Objects.requireNonNull(overflow, "overflow");
        if (overflow == ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_DURABLE_SPILL
                && namedSpill == null) {
            throw new IllegalArgumentException(
                    "memory-channel-spill-undeclared: durable spill target is required");
        }
        if (overflow == ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_UNSPECIFIED
                || overflow == ChannelOverflowAction.UNRECOGNIZED) {
            throw new IllegalArgumentException(
                    "memory-channel-overflow-unspecified: overflow action is required");
        }
        this.maxItems = maxItems;
        this.maxBytes = maxBytes;
        this.spill = namedSpill;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.memory = new FileDurableProcessorChannel(
                new InMemoryProcessorChannelJournal(), descriptors,
                clock);
    }

    @Override
    public DeliveryView enqueue(ProcessorWork work) {
        Objects.requireNonNull(work, "work");
        int bytes = work.getSerializedSize();
        if (bytes > maxBytes) {
            return overflow(work, "one work item exceeds max_bytes");
        }
        synchronized (this) {
            requireOpen();
            Optional<DeliveryView> existing = memory.delivery(work.getDeliveryId());
            if (existing.isPresent()) {
                return memory.enqueue(work);
            }
            while (reservations.size() >= maxItems || reservedBytes + bytes > maxBytes) {
                if (overflow == ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_REFUSE) {
                    throw new IllegalStateException(
                            "memory-channel-full: item or byte budget is exhausted");
                }
                if (overflow == ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_DURABLE_SPILL) {
                    return spill.enqueue(work);
                }
                Instant deadline = RemoteValidation.instant(work.getDeadline());
                long millis = Duration.between(clock.instant(), deadline).toMillis();
                if (millis <= 0) {
                    throw new IllegalStateException(
                            "memory-channel-backpressure-deadline: capacity did not free in time");
                }
                try {
                    wait(Math.min(millis, 1_000L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "memory-channel-backpressure-interrupted", e);
                }
                requireOpen();
            }
            DeliveryView accepted = memory.enqueue(work);
            reservations.put(work.getDeliveryId(), bytes);
            reservedBytes += bytes;
            return accepted;
        }
    }

    private DeliveryView overflow(ProcessorWork work, String reason) {
        if (overflow == ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_DURABLE_SPILL) {
            return spill.enqueue(work);
        }
        throw new IllegalStateException("memory-channel-full: " + reason);
    }

    @Override
    public List<DeliveryClaim> claim(String workerId, Collection<ProcessorContract> contracts,
            int permits, Duration leaseDuration, Instant now) {
        return claim(workerId, contracts, contracts.stream().collect(
                java.util.stream.Collectors.toMap(ProcessorContract::getProcessorId,
                        ignored -> permits, (a, b) -> a, LinkedHashMap::new)),
                permits, leaseDuration, now);
    }

    @Override
    public List<DeliveryClaim> claim(String workerId, Collection<ProcessorContract> contracts,
            Map<String, Integer> processorPermits, int permits,
            Duration leaseDuration, Instant now) {
        List<DeliveryClaim> result = new ArrayList<>(memory.claim(workerId, contracts,
                processorPermits, permits, leaseDuration, now));
        if (spill != null && result.size() < permits) {
            result.addAll(spill.claim(workerId, contracts, processorPermits,
                    permits - result.size(), leaseDuration, now));
        }
        return List.copyOf(result);
    }

    @Override
    public void complete(String workerId, ProcessorCompletion completion, Instant now) {
        owner(completion.getDeliveryId()).complete(workerId, completion, now);
    }

    @Override
    public void fail(String workerId, ProcessorFailure failure, Instant now) {
        DurableProcessorChannel owner = owner(failure.getDeliveryId());
        owner.fail(workerId, failure, now);
        releaseIfTerminal(owner, failure.getDeliveryId());
    }

    @Override
    public Completion awaitCompletion(String deliveryId, Instant deadline)
            throws InterruptedException {
        return owner(deliveryId).awaitCompletion(deliveryId, deadline);
    }

    @Override
    public void settle(String deliveryId, String leaseToken, Instant now) {
        DurableProcessorChannel owner = owner(deliveryId);
        owner.settle(deliveryId, leaseToken, now);
        releaseIfTerminal(owner, deliveryId);
    }

    @Override
    public void release(String deliveryId, String leaseToken, String reason, Instant now) {
        DurableProcessorChannel owner = owner(deliveryId);
        owner.release(deliveryId, leaseToken, reason, now);
        releaseIfTerminal(owner, deliveryId);
    }

    @Override
    public void releaseWorker(String workerId, String reason, Instant now) {
        memory.releaseWorker(workerId, reason, now);
        if (spill != null) {
            spill.releaseWorker(workerId, reason, now);
        }
        releaseTerminals();
    }

    @Override
    public int expire(Instant now) {
        int result = memory.expire(now);
        if (spill != null) {
            result += spill.expire(now);
        }
        releaseTerminals();
        return result;
    }

    @Override
    public Optional<DeliveryView> delivery(String deliveryId) {
        Optional<DeliveryView> local = memory.delivery(deliveryId);
        return local.isPresent() || spill == null ? local : spill.delivery(deliveryId);
    }

    @Override
    public List<DeliveryView> deliveries() {
        List<DeliveryView> result = new ArrayList<>(memory.deliveries());
        if (spill != null) {
            result.addAll(spill.deliveries());
        }
        return List.copyOf(result);
    }

    @Override
    public List<ChannelRecord> records() {
        List<ChannelRecord> result = new ArrayList<>(memory.records());
        if (spill != null) {
            result.addAll(spill.records());
        }
        return List.copyOf(result);
    }

    @Override
    public DeadLetterPage deadLetters(String namespace, long afterSequence, int limit) {
        requireOpen();
        validateDeadLetterPage(namespace, afterSequence, limit);
        DeadLetterPage local = memory.deadLetters(namespace, 0, 1);
        DeadLetterPage spilled = spill == null
                ? new DeadLetterPage(List.of(), 0)
                : spill.deadLetters(namespace, 0, 1);
        if (!local.entries().isEmpty() && !spilled.entries().isEmpty()) {
            throw new IllegalStateException(
                    "dead-letter-cursor-ambiguous: namespace spans memory and spill channels");
        }
        if (!local.entries().isEmpty()) {
            return memory.deadLetters(namespace, afterSequence, limit);
        }
        if (!spilled.entries().isEmpty()) {
            return spill.deadLetters(namespace, afterSequence, limit);
        }
        return new DeadLetterPage(List.of(), afterSequence);
    }

    private static void validateDeadLetterPage(
            String namespace, long afterSequence, int limit) {
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("dead-letter-namespace-required");
        }
        if (afterSequence < 0) {
            throw new IllegalArgumentException("dead-letter cursor must not be negative");
        }
        if (limit < 1 || limit > 1_000) {
            throw new IllegalArgumentException(
                    "dead-letter limit must be between 1 and 1000");
        }
    }

    @Override
    public Optional<DeadLetterRecord> deadLetter(String deadLetterId) {
        Optional<DeadLetterRecord> local = memory.deadLetter(deadLetterId);
        return local.isPresent() || spill == null ? local : spill.deadLetter(deadLetterId);
    }

    @Override
    public DeadLetterRecord cancelRetry(String deliveryId, String reason, Instant now) {
        DurableProcessorChannel owner = owner(deliveryId);
        DeadLetterRecord result = owner.cancelRetry(deliveryId, reason, now);
        releaseIfTerminal(owner, deliveryId);
        return result;
    }

    @Override
    public DeadLetterRecord changeDeadLetterStatus(String deadLetterId,
            DeadLetterReplayStatus status, String replayRunId, String reason,
            boolean retentionHold) {
        if (memory.deadLetter(deadLetterId).isPresent()) {
            return memory.changeDeadLetterStatus(deadLetterId, status, replayRunId,
                    reason, retentionHold);
        }
        if (spill != null) {
            return spill.changeDeadLetterStatus(deadLetterId, status, replayRunId,
                    reason, retentionHold);
        }
        throw new IllegalArgumentException("unknown dead_letter_id " + deadLetterId);
    }

    public synchronized int reservedItems() {
        return reservations.size();
    }

    public synchronized long reservedBytes() {
        return reservedBytes;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        memory.close();
        if (spill != null) {
            spill.close();
        }
        reservations.clear();
        reservedBytes = 0;
        notifyAll();
    }

    private DurableProcessorChannel owner(String deliveryId) {
        requireOpen();
        if (memory.delivery(deliveryId).isPresent()) {
            return memory;
        }
        if (spill != null && spill.delivery(deliveryId).isPresent()) {
            return spill;
        }
        throw new IllegalArgumentException("unknown delivery_id " + deliveryId);
    }

    private void releaseIfTerminal(DurableProcessorChannel owner, String deliveryId) {
        if (owner != memory) {
            return;
        }
        owner.delivery(deliveryId).ifPresent(view -> {
            if (view.state() == DeliveryState.FAILED
                    || view.state() == DeliveryState.SETTLED) {
                releaseReservation(deliveryId);
            }
        });
    }

    private void releaseTerminals() {
        memory.deliveries().forEach(view -> releaseIfTerminal(
                memory, view.work().getDeliveryId()));
    }

    private synchronized void releaseReservation(String deliveryId) {
        Integer removed = reservations.remove(deliveryId);
        if (removed != null) {
            reservedBytes -= removed;
            notifyAll();
        }
    }

    private synchronized void requireOpen() {
        if (closed) {
            throw new IllegalStateException("bounded memory processor channel is closed");
        }
    }
}
