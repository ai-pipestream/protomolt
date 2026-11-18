package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicyException;
import io.grpc.ChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Shares one {@link ManagedChannel} per target among the pipeline calls that are in flight to
 * it. A channel is opened for the first call and closed when its last call finishes, so an
 * {@code OutboundChannelPolicy} slot is held only while a call needs it and never by an idle
 * channel that other users of the policy could not reclaim. A channel is never shut down
 * underneath an in-flight call while the cache is open.
 *
 * <p>A call with its own TLS credentials gets a dedicated channel that is not shared. The
 * cache still owns it: it is opened under the same lock, refused once the cache is closed,
 * and shut down with the cache if its call has not finished.
 */
final class PipelineChannelCache implements AutoCloseable {

    private record ChannelKey(String target, boolean tls) {
    }

    static final class Lease {
        private final PipelineChannelCache owner;
        private final ManagedChannel channel;
        private final boolean dedicated;
        private int activeCalls;
        private boolean evicted;

        private Lease(PipelineChannelCache owner, ManagedChannel channel, boolean dedicated) {
            this.owner = owner;
            this.channel = channel;
            this.dedicated = dedicated;
        }

        ManagedChannel channel() {
            return channel;
        }

        void release() {
            if (dedicated) {
                owner.releaseDedicated(this);
            } else {
                owner.releaseShared(this);
            }
        }

        private boolean isUsable() {
            return !evicted && !channel.isShutdown() && !channel.isTerminated();
        }
    }

    private final ChannelFactory channels;
    private final Object lock = new Object();
    private final Map<ChannelKey, Lease> openChannels = new HashMap<>();
    private final Set<Lease> dedicatedChannels = new HashSet<>();
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

    /** A channel for one call with its own TLS credentials; released when the call ends. */
    Lease openDedicated(String target, ChannelCredentials customTls) {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("pipeline transport is closed");
            }
            Lease opened = new Lease(this, open(() -> Objects.requireNonNull(
                    channels.open(target, customTls), "channel factory returned null")), true);
            dedicatedChannels.add(opened);
            return opened;
        }
    }

    /**
     * The channel for {@code target}, shared with any call already in flight to it. The first
     * call opens it while holding the cache lock, so two first calls cannot both open one;
     * opening builds the channel and takes a policy slot without waiting on the network.
     */
    Lease acquireShared(String target, boolean tls) {
        ChannelKey key = new ChannelKey(target, tls);
        ManagedChannel stale = null;
        Lease acquired;
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("pipeline transport is closed");
            }
            Lease existing = openChannels.get(key);
            if (existing != null && existing.isUsable()) {
                existing.activeCalls++;
                return existing;
            }
            if (existing != null) {
                // Shut down from outside while calls still hold it; they release it as they end.
                openChannels.remove(key);
                existing.evicted = true;
                if (existing.activeCalls == 0) {
                    stale = existing.channel;
                }
            }
            acquired = new Lease(this, open(() -> Objects.requireNonNull(
                    channels.open(key.target(), key.tls()), "channel factory returned null")),
                    false);
            acquired.activeCalls = 1;
            openChannels.put(key, acquired);
        }
        if (stale != null) {
            stale.shutdownNow();
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

    private void releaseDedicated(Lease entry) {
        synchronized (lock) {
            dedicatedChannels.remove(entry);
        }
        entry.channel.shutdownNow();
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
            for (Lease entry : dedicatedChannels) {
                channelsToClose.add(entry.channel);
            }
            dedicatedChannels.clear();
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
