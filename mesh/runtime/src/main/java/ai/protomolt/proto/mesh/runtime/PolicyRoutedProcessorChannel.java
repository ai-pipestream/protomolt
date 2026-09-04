package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.v1.ChannelDeliveryMode;
import ai.protomolt.proto.mesh.runtime.v1.ChannelPolicy;
import ai.protomolt.proto.mesh.runtime.v1.ChannelRecord;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterRecord;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterReplayStatus;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;
import com.google.protobuf.ByteString;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Selects the compiled edge channel without changing the processor protocol. */
public final class PolicyRoutedProcessorChannel implements DurableProcessorChannel {

    private static final int MAX_MEMORY_POLICIES = 256;

    private final DescriptorRegistry descriptors;
    private final DurableProcessorChannel localWal;
    private final Map<String, DurableProcessorChannel> transactional;
    private final Clock clock;
    private final Map<MemoryKey, BoundedMemoryProcessorChannel> memory =
            new LinkedHashMap<>();
    private boolean closed;

    public PolicyRoutedProcessorChannel(
            DescriptorRegistry descriptors,
            DurableProcessorChannel localWal,
            Map<String, DurableProcessorChannel> transactional,
            Clock clock) {
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.localWal = Objects.requireNonNull(localWal, "localWal");
        this.transactional = Map.copyOf(transactional);
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public synchronized DeliveryView enqueue(ProcessorWork work) {
        requireOpen();
        Optional<DurableProcessorChannel> existing = ownerOptional(work.getDeliveryId());
        if (existing.isPresent()) {
            return existing.orElseThrow().enqueue(work);
        }
        return select(work.getChannelPolicy(), work.getDurableSpillPolicy()).enqueue(work);
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
        if (permits <= 0) {
            return List.of();
        }
        Map<String, Integer> remaining = new LinkedHashMap<>(processorPermits);
        List<DeliveryClaim> claims = new ArrayList<>();
        for (DurableProcessorChannel channel : channels()) {
            if (claims.size() >= permits) {
                break;
            }
            List<DeliveryClaim> next = channel.claim(workerId, contracts, remaining,
                    permits - claims.size(), leaseDuration, now);
            claims.addAll(next);
            for (DeliveryClaim claim : next) {
                remaining.computeIfPresent(
                        claim.getWork().getContract().getProcessorId(),
                        (ignored, available) -> Math.max(0, available - 1));
            }
        }
        return List.copyOf(claims);
    }

    @Override
    public synchronized void complete(
            String workerId, ProcessorCompletion completion, Instant now) {
        owner(completion.getDeliveryId()).complete(workerId, completion, now);
    }

    @Override
    public synchronized void fail(
            String workerId, ProcessorFailure failure, Instant now) {
        owner(failure.getDeliveryId()).fail(workerId, failure, now);
    }

    @Override
    public Completion awaitCompletion(String deliveryId, Instant deadline)
            throws InterruptedException {
        DurableProcessorChannel owner;
        synchronized (this) {
            owner = owner(deliveryId);
        }
        return owner.awaitCompletion(deliveryId, deadline);
    }

    @Override
    public synchronized void settle(String deliveryId, String leaseToken, Instant now) {
        owner(deliveryId).settle(deliveryId, leaseToken, now);
    }

    @Override
    public synchronized void release(
            String deliveryId, String leaseToken, String reason, Instant now) {
        owner(deliveryId).release(deliveryId, leaseToken, reason, now);
    }

    @Override
    public synchronized void releaseWorker(String workerId, String reason, Instant now) {
        channels().forEach(channel -> channel.releaseWorker(workerId, reason, now));
    }

    @Override
    public synchronized int expire(Instant now) {
        return channels().stream().mapToInt(channel -> channel.expire(now)).sum();
    }

    @Override
    public synchronized Optional<DeliveryView> delivery(String deliveryId) {
        return ownerOptional(deliveryId).flatMap(channel -> channel.delivery(deliveryId));
    }

    @Override
    public synchronized List<DeliveryView> deliveries() {
        return channels().stream().flatMap(channel -> channel.deliveries().stream()).toList();
    }

    @Override
    public synchronized List<ChannelRecord> records() {
        return channels().stream().flatMap(channel -> channel.records().stream()).toList();
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
            throw new IllegalArgumentException(
                    "dead-letter limit must be between 1 and 1000");
        }
        List<DurableProcessorChannel> owners = channels().stream()
                .filter(channel -> !channel.deadLetters(namespace, 0, 1).entries().isEmpty())
                .toList();
        if (owners.size() > 1) {
            throw new IllegalStateException(
                    "dead-letter-cursor-ambiguous: namespace spans processor channels");
        }
        return owners.isEmpty()
                ? new DeadLetterPage(List.of(), afterSequence)
                : owners.getFirst().deadLetters(namespace, afterSequence, limit);
    }

    @Override
    public synchronized Optional<DeadLetterRecord> deadLetter(String deadLetterId) {
        return channels().stream()
                .map(channel -> channel.deadLetter(deadLetterId))
                .flatMap(Optional::stream)
                .findFirst();
    }

    @Override
    public synchronized DeadLetterRecord cancelRetry(
            String deliveryId, String reason, Instant now) {
        return owner(deliveryId).cancelRetry(deliveryId, reason, now);
    }

    @Override
    public synchronized DeadLetterRecord changeDeadLetterStatus(
            String deadLetterId,
            DeadLetterReplayStatus status,
            String replayRunId,
            String reason,
            boolean retentionHold) {
        return channels().stream()
                .filter(channel -> channel.deadLetter(deadLetterId).isPresent())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "unknown dead_letter_id " + deadLetterId))
                .changeDeadLetterStatus(deadLetterId, status, replayRunId,
                        reason, retentionHold);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        memory.values().forEach(BoundedMemoryProcessorChannel::close);
        memory.clear();
        Set<DurableProcessorChannel> durable = new LinkedHashSet<>();
        durable.add(localWal);
        durable.addAll(transactional.values());
        durable.forEach(DurableProcessorChannel::close);
    }

    private DurableProcessorChannel select(
            ChannelPolicy policy, ChannelPolicy durableSpillPolicy) {
        requirePolicy(policy);
        return switch (policy.getDeliveryMode()) {
            case CHANNEL_DELIVERY_MODE_LOCAL_DURABLE_WAL -> localWal;
            case CHANNEL_DELIVERY_MODE_TRANSACTIONAL_EXTERNAL -> {
                DurableProcessorChannel channel = transactional.get(
                        policy.getTransactionalChannelProfile());
                if (channel == null) {
                    throw new IllegalStateException(
                            "transactional-channel-unavailable: "
                                    + policy.getTransactionalChannelProfile());
                }
                yield channel;
            }
            case CHANNEL_DELIVERY_MODE_BOUNDED_MEMORY -> memory(
                    policy, durableSpillPolicy);
            case CHANNEL_DELIVERY_MODE_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("channel-delivery-mode-required");
        };
    }

    private BoundedMemoryProcessorChannel memory(
            ChannelPolicy policy, ChannelPolicy spillPolicy) {
        MemoryKey key = new MemoryKey(policy.toByteString(), spillPolicy.toByteString());
        BoundedMemoryProcessorChannel existing = memory.get(key);
        if (existing != null) {
            return existing;
        }
        if (memory.size() >= MAX_MEMORY_POLICIES) {
            throw new IllegalStateException("memory-channel-policy-limit-exceeded");
        }
        DurableProcessorChannel spill = null;
        if (!policy.getDurableSpillPolicyId().isBlank()) {
            requirePolicy(spillPolicy);
            DurableProcessorChannel target = select(
                    spillPolicy, ChannelPolicy.getDefaultInstance());
            spill = new SharedChannelView(target);
        }
        BoundedMemoryProcessorChannel created = new BoundedMemoryProcessorChannel(
                descriptors, Math.toIntExact(policy.getMaxItems()), policy.getMaxBytes(),
                policy.getOverflowAction(), spill, clock);
        memory.put(key, created);
        return created;
    }

    private Optional<DurableProcessorChannel> ownerOptional(String deliveryId) {
        return channels().stream()
                .filter(channel -> channel.delivery(deliveryId).isPresent())
                .findFirst();
    }

    private DurableProcessorChannel owner(String deliveryId) {
        return ownerOptional(deliveryId).orElseThrow(() ->
                new IllegalArgumentException("unknown delivery_id " + deliveryId));
    }

    private List<DurableProcessorChannel> channels() {
        List<DurableProcessorChannel> channels = new ArrayList<>();
        channels.add(localWal);
        transactional.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .filter(channel -> channel != localWal)
                .forEach(channels::add);
        channels.addAll(memory.values());
        return channels.stream().distinct().toList();
    }

    private static void requirePolicy(ChannelPolicy policy) {
        Objects.requireNonNull(policy, "channelPolicy");
        if (policy.getDeliveryMode() == ChannelDeliveryMode.CHANNEL_DELIVERY_MODE_UNSPECIFIED) {
            throw new IllegalArgumentException("channel-delivery-mode-required");
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("policy-routed processor channel is closed");
        }
    }

    private record MemoryKey(ByteString policy, ByteString spillPolicy) {
    }

    /** A shared durable target whose lifecycle remains owned by the router. */
    private record SharedChannelView(DurableProcessorChannel delegate)
            implements DurableProcessorChannel {
        private SharedChannelView {
            Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public DeliveryView enqueue(ProcessorWork work) {
            return delegate.enqueue(work);
        }

        @Override
        public List<DeliveryClaim> claim(String workerId,
                Collection<ProcessorContract> contracts, int permits,
                Duration leaseDuration, Instant now) {
            return List.of();
        }

        @Override
        public List<DeliveryClaim> claim(String workerId,
                Collection<ProcessorContract> contracts,
                Map<String, Integer> processorPermits, int permits,
                Duration leaseDuration, Instant now) {
            return List.of();
        }

        @Override
        public void complete(String workerId, ProcessorCompletion completion, Instant now) {
            delegate.complete(workerId, completion, now);
        }

        @Override
        public void fail(String workerId, ProcessorFailure failure, Instant now) {
            delegate.fail(workerId, failure, now);
        }

        @Override
        public Completion awaitCompletion(String deliveryId, Instant deadline)
                throws InterruptedException {
            return delegate.awaitCompletion(deliveryId, deadline);
        }

        @Override
        public void settle(String deliveryId, String leaseToken, Instant now) {
            delegate.settle(deliveryId, leaseToken, now);
        }

        @Override
        public void release(String deliveryId, String leaseToken, String reason, Instant now) {
            delegate.release(deliveryId, leaseToken, reason, now);
        }

        @Override
        public void releaseWorker(String workerId, String reason, Instant now) {
            // The router invokes the shared target directly.
        }

        @Override
        public int expire(Instant now) {
            return 0;
        }

        @Override
        public Optional<DeliveryView> delivery(String deliveryId) {
            return Optional.empty();
        }

        @Override
        public List<DeliveryView> deliveries() {
            return List.of();
        }

        @Override
        public List<ChannelRecord> records() {
            return List.of();
        }

        @Override
        public DeadLetterPage deadLetters(
                String namespace, long afterSequence, int limit) {
            return new DeadLetterPage(List.of(), afterSequence);
        }

        @Override
        public Optional<DeadLetterRecord> deadLetter(String deadLetterId) {
            return Optional.empty();
        }

        @Override
        public DeadLetterRecord cancelRetry(
                String deliveryId, String reason, Instant now) {
            return delegate.cancelRetry(deliveryId, reason, now);
        }

        @Override
        public DeadLetterRecord changeDeadLetterStatus(String deadLetterId,
                DeadLetterReplayStatus status, String replayRunId,
                String reason, boolean retentionHold) {
            return delegate.changeDeadLetterStatus(deadLetterId, status,
                    replayRunId, reason, retentionHold);
        }

        @Override
        public void close() {
            // The router owns and closes the shared durable target.
        }
    }
}
