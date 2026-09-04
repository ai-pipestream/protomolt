package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelRecord;
import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorCompletion;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
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
            String failureMessage) {
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

    @Override
    void close();
}
