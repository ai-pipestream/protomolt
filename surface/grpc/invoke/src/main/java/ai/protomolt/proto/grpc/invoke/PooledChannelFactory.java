package ai.protomolt.proto.grpc.invoke;

import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import io.grpc.CallOptions;
import io.grpc.ChannelCredentials;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Bounded pool of credentialed TLS {@link ManagedChannel}s behind an {@link OutboundChannelPolicy}.
 *
 * <p>Callers ({@code GrpcInvokeAction}, {@code ServiceActionSupport}, {@code ReflectedMethodAction})
 * shut down their per-invocation channel handle in a {@code finally} block so
 * {@link OutboundChannelPolicy} releases its concurrency permit immediately. Wrapping the pooled
 * underlying channel in a per-invocation {@link BorrowedChannel} lets {@code shutdownNow()} release
 * that permit and return the healthy Netty channel and {@code SslContext} to this pool, while
 * credential rotation ({@link #evict(ChannelCredentials)}), LRU capacity bounds, or host shutdown
 * ({@link #close()}) retires underlying channels deterministically.
 */
final class PooledChannelFactory implements ChannelFactory.Closeable {

    static final int DEFAULT_MAX_POOLED_CHANNELS = 64;

    private final OutboundChannelPolicy policy;
    private final int maxPooledChannels;
    private final BiFunction<String, ChannelCredentials, ManagedChannel> credentialedOpener;
    private final BiFunction<String, Boolean, ManagedChannel> transportOpener;
    private final AtomicInteger createdCredentialedChannels = new AtomicInteger();
    private final Object lock = new Object();
    private final LinkedHashMap<PoolKey, PoolEntry> pool = new LinkedHashMap<>(16, 0.75f, true);
    private boolean closed;

    PooledChannelFactory(OutboundChannelPolicy policy) {
        this(policy, DEFAULT_MAX_POOLED_CHANNELS,
                (canonical, credentials) -> Grpc.newChannelBuilder(canonical, credentials).build(),
                defaultTransportOpener());
    }

    PooledChannelFactory(OutboundChannelPolicy policy, int maxPooledChannels,
                         BiFunction<String, ChannelCredentials, ManagedChannel> credentialedOpener) {
        this(policy, maxPooledChannels, credentialedOpener, defaultTransportOpener());
    }

    PooledChannelFactory(OutboundChannelPolicy policy, int maxPooledChannels,
                         BiFunction<String, ChannelCredentials, ManagedChannel> credentialedOpener,
                         BiFunction<String, Boolean, ManagedChannel> transportOpener) {
        if (policy == null) {
            throw new IllegalArgumentException("channel policy must not be null");
        }
        if (maxPooledChannels <= 0) {
            throw new IllegalArgumentException("max pooled channels must be positive");
        }
        this.policy = policy;
        this.maxPooledChannels = maxPooledChannels;
        this.credentialedOpener = Objects.requireNonNull(credentialedOpener, "credentialedOpener");
        this.transportOpener = Objects.requireNonNull(transportOpener, "transportOpener");
    }

    private static BiFunction<String, Boolean, ManagedChannel> defaultTransportOpener() {
        return (canonical, tls) -> {
            ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(canonical);
            if (!tls) {
                builder.usePlaintext();
            }
            return builder.build();
        };
    }

    @Override
    public ManagedChannel open(String target, boolean tls) {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("channel factory is closed");
            }
        }
        return policy.open(target, tls, canonical -> transportOpener.apply(canonical, tls));
    }

    @Override
    public ManagedChannel open(String target, ChannelCredentials credentials) {
        if (credentials == null) {
            throw new IllegalArgumentException("channel credentials must not be null");
        }
        return policy.open(target, true, canonical -> borrow(canonical, credentials));
    }

    @Override
    public void validateTarget(String target, boolean tls) {
        policy.validateTarget(target, tls);
    }

    @Override
    public void validateDeadline(long deadlineMillis) {
        policy.validateDeadline(deadlineMillis);
    }

    @Override
    public OutboundChannelPolicy policy() {
        return policy;
    }

    @Override
    public void evict(ChannelCredentials credentials) {
        if (credentials == null) {
            return;
        }
        List<ManagedChannel> idle = new ArrayList<>();
        synchronized (lock) {
            Iterator<Map.Entry<PoolKey, PoolEntry>> iterator = pool.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<PoolKey, PoolEntry> entry = iterator.next();
                if (entry.getKey().credentials == credentials) {
                    PoolEntry removed = entry.getValue();
                    iterator.remove();
                    removed.evicted = true;
                    if (removed.activeLeases == 0) {
                        idle.add(removed.underlying);
                    }
                }
            }
        }
        for (ManagedChannel channel : idle) {
            channel.shutdown();
        }
    }

    @Override
    public void close() {
        List<ManagedChannel> toShutdown = new ArrayList<>();
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            for (PoolEntry entry : pool.values()) {
                entry.evicted = true;
                toShutdown.add(entry.underlying);
            }
            pool.clear();
        }
        for (ManagedChannel channel : toShutdown) {
            channel.shutdownNow();
        }
    }

    int createdCredentialedChannels() {
        return createdCredentialedChannels.get();
    }

    int pooledCount() {
        synchronized (lock) {
            return pool.size();
        }
    }

    private ManagedChannel borrow(String canonicalTarget, ChannelCredentials credentials) {
        ManagedChannel stale = null;
        ManagedChannel evictedIdle = null;
        BorrowedChannel borrowed;
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("channel factory is closed");
            }
            PoolKey key = new PoolKey(canonicalTarget, credentials);
            PoolEntry entry = pool.get(key);
            if (entry != null && !entry.isUsable()) {
                pool.remove(key);
                entry.evicted = true;
                if (entry.activeLeases == 0) {
                    stale = entry.underlying;
                }
                entry = null;
            }
            if (entry == null) {
                ManagedChannel underlying = Objects.requireNonNull(
                        credentialedOpener.apply(canonicalTarget, credentials),
                        "credentialed channel opener returned null");
                createdCredentialedChannels.incrementAndGet();
                entry = new PoolEntry(this, underlying);
                pool.put(key, entry);
                if (pool.size() > maxPooledChannels) {
                    Iterator<Map.Entry<PoolKey, PoolEntry>> iterator = pool.entrySet().iterator();
                    PoolEntry oldest = iterator.next().getValue();
                    iterator.remove();
                    oldest.evicted = true;
                    if (oldest.activeLeases == 0) {
                        evictedIdle = oldest.underlying;
                    }
                }
            }
            entry.activeLeases++;
            borrowed = new BorrowedChannel(entry);
        }
        if (stale != null) {
            stale.shutdownNow();
        }
        if (evictedIdle != null) {
            evictedIdle.shutdown();
        }
        return borrowed;
    }

    private void releaseLease(PoolEntry entry) {
        ManagedChannel toShutdown = null;
        boolean force = false;
        synchronized (lock) {
            if (entry.activeLeases > 0) {
                entry.activeLeases--;
            }
            if (entry.activeLeases == 0 && (closed || entry.evicted || !entry.isUsable())) {
                toShutdown = entry.underlying;
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

    private static final class PoolKey {
        private final String canonicalTarget;
        private final ChannelCredentials credentials;
        private final int hash;

        private PoolKey(String canonicalTarget, ChannelCredentials credentials) {
            this.canonicalTarget = canonicalTarget;
            this.credentials = credentials;
            this.hash = 31 * canonicalTarget.hashCode() + System.identityHashCode(credentials);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof PoolKey that
                    && this.credentials == that.credentials
                    && this.canonicalTarget.equals(that.canonicalTarget);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private static final class PoolEntry {
        private final PooledChannelFactory owner;
        private final ManagedChannel underlying;
        private int activeLeases;
        private boolean evicted;

        private PoolEntry(PooledChannelFactory owner, ManagedChannel underlying) {
            this.owner = owner;
            this.underlying = underlying;
        }

        private boolean isUsable() {
            if (evicted || underlying.isShutdown() || underlying.isTerminated()) {
                return false;
            }
            try {
                return underlying.getState(false) != ConnectivityState.SHUTDOWN;
            } catch (UnsupportedOperationException ignored) {
                return true;
            }
        }

        private void release() {
            owner.releaseLease(this);
        }
    }

    private static final class BorrowedChannel extends ManagedChannel {
        private final PoolEntry entry;
        private final AtomicBoolean leaseClosed = new AtomicBoolean();

        private BorrowedChannel(PoolEntry entry) {
            this.entry = entry;
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            if (leaseClosed.get()) {
                return new ClosedLeaseCall<>();
            }
            return entry.underlying.newCall(methodDescriptor, callOptions);
        }

        @Override
        public String authority() {
            return entry.underlying.authority();
        }

        @Override
        public ConnectivityState getState(boolean requestConnection) {
            return leaseClosed.get()
                    ? ConnectivityState.SHUTDOWN
                    : entry.underlying.getState(requestConnection);
        }

        @Override
        public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
            if (!leaseClosed.get()) {
                entry.underlying.notifyWhenStateChanged(source, callback);
            }
        }

        @Override
        public void resetConnectBackoff() {
            if (!leaseClosed.get()) {
                entry.underlying.resetConnectBackoff();
            }
        }

        @Override
        public void enterIdle() {
            if (!leaseClosed.get()) {
                entry.underlying.enterIdle();
            }
        }

        @Override
        public ManagedChannel shutdown() {
            closeLease();
            return this;
        }

        @Override
        public ManagedChannel shutdownNow() {
            closeLease();
            return this;
        }

        @Override
        public boolean isShutdown() {
            return leaseClosed.get() || entry.underlying.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return leaseClosed.get() || entry.underlying.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            if (leaseClosed.get()) {
                return true;
            }
            return entry.underlying.awaitTermination(timeout, unit);
        }

        private void closeLease() {
            if (leaseClosed.compareAndSet(false, true)) {
                entry.release();
            }
        }
    }

    private static final class ClosedLeaseCall<RequestT, ResponseT>
            extends ClientCall<RequestT, ResponseT> {
        @Override
        public void start(Listener<ResponseT> responseListener, Metadata headers) {
            responseListener.onClose(
                    Status.UNAVAILABLE.withDescription("channel lease is already closed"),
                    new Metadata());
        }

        @Override
        public void request(int numMessages) {
        }

        @Override
        public void cancel(String message, Throwable cause) {
        }

        @Override
        public void halfClose() {
        }

        @Override
        public void sendMessage(RequestT message) {
        }
    }
}
