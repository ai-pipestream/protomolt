package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicyException;
import io.grpc.ChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Shares one {@link ManagedChannel} per target among the pipeline calls that are in flight to
 * it. A channel is opened for the first call and closed when its last call finishes, so an
 * {@code OutboundChannelPolicy} slot is held only while a call needs it and never by an idle
 * channel that other users of the policy could not reclaim. A channel is never shut down
 * underneath an in-flight call.
 */
final class PipelineChannelCache implements AutoCloseable {

    private record ChannelKey(String target, boolean tls) {
    }

    static final class Lease {
        private final PipelineChannelCache owner;
        private final ManagedChannel channel;
        private int activeCalls;
        private boolean evicted;

        private Lease(PipelineChannelCache owner, ManagedChannel channel) {
            this.owner = owner;
            this.channel = channel;
        }

        ManagedChannel channel() {
            return channel;
        }

        void release() {
            owner.releaseShared(this);
        }

        private boolean isUsable() {
            return !evicted && !channel.isShutdown() && !channel.isTerminated();
        }
    }

    private final ChannelFactory channels;
    private final Object lock = new Object();
    private final Map<ChannelKey, Lease> openChannels = new HashMap<>();
    private volatile boolean closed;

    PipelineChannelCache(ChannelFactory channels) {
        this.channels = Objects.requireNonNull(channels, "channels");
    }

    boolean isClosed() {
        return closed;
    }

    void validateTargetAndDeadline(String target, boolean tls, long deadlineMillis) {
        channels.validateTarget(target, tls);
        channels.validateDeadline(deadlineMillis);
    }

    ManagedChannel openDedicated(String target, ChannelCredentials customTls) {
        return open(() -> Objects.requireNonNull(
                channels.open(target, customTls), "channel factory returned null"));
    }

    Lease acquireShared(String target, boolean tls) {
        ChannelKey key = new ChannelKey(target, tls);
        ManagedChannel stale = null;
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("pipeline transport is closed");
            }
            Lease existing = openChannels.get(key);
            if (existing != null) {
                if (existing.isUsable()) {
                    existing.activeCalls++;
                    return existing;
                }
                openChannels.remove(key);
                existing.evicted = true;
                if (existing.activeCalls == 0) {
                    stale = existing.channel;
                }
            }
        }
        if (stale != null) {
            stale.shutdownNow();
        }

        ManagedChannel opened = open(() -> Objects.requireNonNull(
                channels.open(key.target(), key.tls()), "channel factory returned null"));
        ManagedChannel duplicate = null;
        Lease acquired;
        synchronized (lock) {
            if (closed) {
                opened.shutdownNow();
                throw new IllegalStateException("pipeline transport is closed");
            }
            Lease existing = openChannels.get(key);
            if (existing != null && existing.isUsable()) {
                existing.activeCalls++;
                duplicate = opened;
                acquired = existing;
            } else {
                acquired = new Lease(this, opened);
                acquired.activeCalls = 1;
                openChannels.put(key, acquired);
            }
        }
        if (duplicate != null) {
            duplicate.shutdownNow();
        }
        return acquired;
    }

    private static ManagedChannel open(Supplier<ManagedChannel> opener) {
        try {
            return opener.get();
        } catch (OutboundChannelPolicyException refused) {
            throw translateOpenRejection(refused);
        }
    }

    private void releaseShared(Lease entry) {
        boolean lastCall;
        boolean force;
        synchronized (lock) {
            entry.activeCalls--;
            lastCall = entry.activeCalls == 0;
            force = closed;
            if (lastCall) {
                entry.evicted = true;
                openChannels.values().remove(entry);
            }
        }
        if (lastCall) {
            if (force) {
                entry.channel.shutdownNow();
            } else {
                entry.channel.shutdown();
            }
        }
    }

    private static RuntimeException translateOpenRejection(OutboundChannelPolicyException error) {
        if (error.channelLimitExhausted()) {
            return Status.RESOURCE_EXHAUSTED
                    .withDescription("outbound channel policy rejected step: " + error.getMessage())
                    .withCause(error)
                    .asRuntimeException();
        }
        return error;
    }

    @Override
    public void close() {
        List<ManagedChannel> channelsToClose;
        synchronized (lock) {
            closed = true;
            channelsToClose = new ArrayList<>(openChannels.size());
            for (Lease entry : openChannels.values()) {
                entry.evicted = true;
                channelsToClose.add(entry.channel);
            }
            openChannels.clear();
        }
        for (ManagedChannel channel : channelsToClose) {
            channel.shutdownNow();
        }
        for (ManagedChannel channel : channelsToClose) {
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
