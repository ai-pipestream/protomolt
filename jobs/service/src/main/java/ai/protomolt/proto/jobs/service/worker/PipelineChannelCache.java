package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicyException;
import io.grpc.ChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Package-private reference-counted LRU channel cache for {@link ServiceProfilePipelineTransport}.
 *
 * <p>Reuses healthy non-custom-TLS {@link ManagedChannel}s across sequential and concurrent RPCs,
 * evicts idle ({@code activeCalls == 0}) channels when {@code OutboundChannelPolicy} saturates
 * {@code maxActiveChannels} so active-channel slots are reclaimed without restarting the worker,
 * and never shuts down an active channel underneath an in-flight RPC.
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
    private final LinkedHashMap<ChannelKey, Lease> openChannels =
            new LinkedHashMap<>(16, 0.75f, true);
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
        return openWithIdleEviction(() -> Objects.requireNonNull(
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

        ManagedChannel opened = openWithIdleEviction(() -> Objects.requireNonNull(
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

    private ManagedChannel openWithIdleEviction(Supplier<ManagedChannel> opener) {
        while (true) {
            try {
                return opener.get();
            } catch (OutboundChannelPolicyException e) {
                if (!isConcurrencyLimit(e) || !evictOneIdleChannel()) {
                    throw translateOpenRejection(e);
                }
            }
        }
    }

    private boolean evictOneIdleChannel() {
        ManagedChannel idle = null;
        synchronized (lock) {
            Iterator<Map.Entry<ChannelKey, Lease>> iterator = openChannels.entrySet().iterator();
            while (iterator.hasNext()) {
                Lease candidate = iterator.next().getValue();
                if (candidate.activeCalls == 0) {
                    iterator.remove();
                    candidate.evicted = true;
                    idle = candidate.channel;
                    break;
                }
            }
        }
        if (idle == null) {
            return false;
        }
        idle.shutdown();
        return true;
    }

    private void releaseShared(Lease entry) {
        ManagedChannel toShutdown = null;
        boolean force = false;
        synchronized (lock) {
            entry.activeCalls--;
            if (entry.activeCalls == 0 && (closed || entry.evicted)) {
                toShutdown = entry.channel;
                force = closed;
            }
        }
        if (toShutdown != null) {
            if (force) {
                toShutdown.shutdownNow();
            } else {
                toShutdown.shutdown();
            }
        }
    }

    private static boolean isConcurrencyLimit(OutboundChannelPolicyException error) {
        String message = error.getMessage();
        return message != null && message.contains("concurrency limit");
    }

    private static RuntimeException translateOpenRejection(OutboundChannelPolicyException error) {
        if (isConcurrencyLimit(error)) {
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
