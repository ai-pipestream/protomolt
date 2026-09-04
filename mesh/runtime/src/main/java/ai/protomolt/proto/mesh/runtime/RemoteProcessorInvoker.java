package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;

import java.time.Clock;
import java.util.Objects;

/** Processor endpoint backed by the single durable demand channel. */
public final class RemoteProcessorInvoker implements ProcessorInvoker {

    private final ProcessorContract contract;
    private final DurableProcessorChannel channel;
    private final Runnable workAvailable;
    private final Clock clock;
    private final int maxAttempts;

    public RemoteProcessorInvoker(
            ProcessorContract contract,
            DurableProcessorChannel channel,
            Runnable workAvailable,
            Clock clock,
            int maxAttempts) {
        this.contract = Objects.requireNonNull(contract, "contract");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.workAvailable = Objects.requireNonNull(workAvailable, "workAvailable");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maxAttempts < 1 || maxAttempts > 100) {
            throw new IllegalArgumentException("maxAttempts must be between 1 and 100");
        }
        this.maxAttempts = maxAttempts;
    }

    @Override
    public ProcessorContract contract() {
        return contract;
    }

    @Override
    public ProcessorInvocationResult invoke(ProcessorInvocation invocation) throws Exception {
        String deliveryId = EntityEnvelopes.stableUuid("delivery\0"
                + contract.getProcessorId() + '\0' + invocation.context().invocationId());
        ProcessorWork work = ProcessorWork.newBuilder()
                .setDeliveryId(deliveryId)
                .setRunId(invocation.context().runId())
                .setNodeId(invocation.context().nodeId())
                .setInvocationId(invocation.context().invocationId())
                .setInvocationOrdinal(invocation.context().invocationOrdinal())
                .setContract(contract)
                .setInput(invocation.input())
                .setDeadline(RemoteValidation.timestamp(invocation.context().deadline()))
                .setMaxAttempts(maxAttempts)
                .build();
        channel.enqueue(work);
        workAvailable.run();
        DurableProcessorChannel.Completion completion;
        try {
            completion = channel.awaitCompletion(
                    deliveryId, invocation.context().deadline());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            abandon(deliveryId, "invocation thread was interrupted");
            throw e;
        } catch (RuntimeException e) {
            abandon(deliveryId, e.getMessage());
            throw e;
        }
        String leaseToken = completion.completion().getLeaseToken();
        InvocationSettlement settlement = settlement(deliveryId, leaseToken);
        return new ProcessorInvocationResult(
                completion.completion().getOutputsList(), settlement);
    }

    @Override
    public InvocationSettlement recoverSettlement(String deliveryId) {
        DurableProcessorChannel.DeliveryView view = channel.delivery(deliveryId)
                .orElseThrow(() -> new IllegalStateException(
                        "durable delivery is unavailable during recovery: " + deliveryId));
        if (view.state() != DurableProcessorChannel.DeliveryState.COMPLETED
                && view.state() != DurableProcessorChannel.DeliveryState.SETTLED) {
            throw new IllegalStateException("durable delivery " + deliveryId
                    + " cannot recover settlement from state " + view.state());
        }
        if (view.completion() == null) {
            throw new IllegalStateException("durable delivery " + deliveryId
                    + " has no completion to settle");
        }
        return settlement(deliveryId,
                view.completion().completion().getLeaseToken());
    }

    private void abandon(String deliveryId, String reason) {
        channel.delivery(deliveryId).ifPresent(view -> {
            if ((view.state() == DurableProcessorChannel.DeliveryState.CLAIMED
                    || view.state() == DurableProcessorChannel.DeliveryState.COMPLETED)
                    && view.claim() != null) {
                channel.release(deliveryId, view.claim().getLeaseToken(),
                        reason == null ? "remote invocation abandoned" : reason,
                        clock.instant());
                workAvailable.run();
            }
        });
    }

    private InvocationSettlement settlement(String deliveryId, String leaseToken) {
        return new InvocationSettlement() {
            @Override
            public String deliveryId() {
                return deliveryId;
            }

            @Override
            public void settle() {
                channel.settle(deliveryId, leaseToken, clock.instant());
            }

            @Override
            public void release(String reason) {
                DurableProcessorChannel.DeliveryView view = channel.delivery(deliveryId)
                        .orElseThrow(() -> new IllegalStateException(
                                "durable delivery disappeared: " + deliveryId));
                if (view.state() == DurableProcessorChannel.DeliveryState.SETTLED) {
                    return;
                }
                channel.release(deliveryId, leaseToken, reason, clock.instant());
                workAvailable.run();
            }
        };
    }
}
