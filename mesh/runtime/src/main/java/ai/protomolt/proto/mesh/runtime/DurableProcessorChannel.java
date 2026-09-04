package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelRecord;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterRecord;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorOutcome;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** One durable, fenced work channel used by every remote processor. */
public interface DurableProcessorChannel extends AutoCloseable {

    enum DeliveryState {
        PENDING,
        CLAIMED,
        COMPLETED,
        FAILED,
        SETTLED
    }

    record Completion(String workerId, ProcessorCompletion completion) {
    }

    record DeliveryView(
            ProcessorWork work,
            DeliveryState state,
            int attempt,
            DeliveryClaim claim,
            Completion completion,
            String failureCode,
            String failureMessage,
            ProcessorOutcome lastOutcome,
            Instant retryNotBefore,
            DeadLetterRecord deadLetter) {
    }

    record DeadLetterPage(List<Entry> entries, long nextSequence) {
        public DeadLetterPage {
            entries = List.copyOf(entries);
            if (nextSequence < 0) {
                throw new IllegalArgumentException("dead-letter cursor must not be negative");
            }
        }

        public record Entry(long sequence, DeadLetterRecord record) {
            public Entry {
                if (sequence < 1) {
                    throw new IllegalArgumentException(
                            "dead-letter sequence must be positive");
                }
                Objects.requireNonNull(record, "record");
            }
        }
    }

    /** Idempotently appends work by delivery id; conflicting duplicates are refused. */
    DeliveryView enqueue(ProcessorWork work);

    /** Claims up to the granted demand, in durable enqueue order. */
    List<DeliveryClaim> claim(
            String workerId,
            Collection<ProcessorContract> contracts,
            int permits,
            Duration leaseDuration,
            Instant now);

    /**
     * Claims in one bounded channel scan while enforcing an independent ceiling per processor.
     * A missing or zero processor ceiling means that processor receives no claim.
     */
    default List<DeliveryClaim> claim(
            String workerId,
            Collection<ProcessorContract> contracts,
            Map<String, Integer> processorPermits,
            int permits,
            Duration leaseDuration,
            Instant now) {
        Objects.requireNonNull(processorPermits, "processorPermits");
        Collection<ProcessorContract> eligible = contracts.stream()
                .filter(contract -> processorPermits.getOrDefault(
                        contract.getProcessorId(), 0) > 0)
                .toList();
        if (eligible.isEmpty()) {
            return List.of();
        }
        return claim(workerId, eligible, permits, leaseDuration, now);
    }

    /** Durably records successful worker output under the active lease fence. */
    void complete(String workerId, ProcessorCompletion completion, Instant now);

    /** Records a worker failure, requeueing only when the contract permits another attempt. */
    void fail(String workerId, ProcessorFailure failure, Instant now);

    /** Waits for durable completion or terminal failure. */
    Completion awaitCompletion(String deliveryId, Instant deadline)
            throws InterruptedException;

    /** Commits a completed delivery after all downstream work succeeds. */
    void settle(String deliveryId, String leaseToken, Instant now);

    /** Releases claimed or completed work after downstream failure. */
    void release(String deliveryId, String leaseToken, String reason, Instant now);

    /** Releases every live claim owned by a disconnected worker. */
    void releaseWorker(String workerId, String reason, Instant now);

    /** Expires work deadlines and lease fences, returning the number of transitions. */
    int expire(Instant now);

    Optional<DeliveryView> delivery(String deliveryId);

    List<DeliveryView> deliveries();

    List<ChannelRecord> records();

    /** Returns one namespace-scoped, storage-bounded page in channel order. */
    DeadLetterPage deadLetters(String namespace, long afterSequence, int limit);

    /** Looks up one recovery record; durable adapters should override with their index. */
    Optional<DeadLetterRecord> deadLetter(String deadLetterId);

    /** Cancels one durably scheduled retry and dead-letters the delivery. */
    default DeadLetterRecord cancelRetry(String deliveryId, String reason, Instant now) {
        throw new IllegalStateException(
                "retry-cancellation-unsupported: channel has no durable retry schedule");
    }

    /** Appends a recovery-only status transition without mutating source evidence. */
    default DeadLetterRecord changeDeadLetterStatus(
            String deadLetterId,
            ai.protomolt.proto.mesh.runtime.v1.DeadLetterReplayStatus status,
            String replayRunId,
            String reason,
            boolean retentionHold) {
        throw new IllegalStateException(
                "dead-letter-status-transition-unsupported: channel has no recovery ledger");
    }

    @Override
    void close();
}
