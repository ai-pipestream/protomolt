package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import io.grpc.*;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/**
 * Header-time admission for explicitly selected synchronous unary handlers.
 * Reserves two maximum serialized requests: gRPC's unary handler requests two
 * messages to detect a protocol violation. This does not measure decoded heap,
 * network buffers or detached work. Selected protobuf responses reserve an
 * additional fixed allowance through terminal delivery. Never install on handlers that
 * return before their request-consuming work completes.
 */
final class UnaryRequestAdmission implements ServerInterceptor, AutoCloseable {
    private final PayloadBudget budget;
    private final int maxRequestBytes;
    private final int maxActive;
    private final Set<String> methods;
    private final java.util.Map<String, Integer> responseLimits;
    private int active;
    private boolean closed;

    UnaryRequestAdmission(PayloadBudget budget, int maxRequestBytes, int maxActive, Set<String> methods) {
        this(budget, maxRequestBytes, maxActive, methods, java.util.Map.of());
    }

    UnaryRequestAdmission(PayloadBudget budget, int maxRequestBytes, int maxActive, Set<String> methods,
            java.util.Map<String, Integer> responseLimits) {
        this.budget = Objects.requireNonNull(budget);
        if (maxRequestBytes < 1 || maxRequestBytes > 256 * 1024 * 1024 || maxActive < 1 || maxActive > 1024)
            throw new IllegalArgumentException("Invalid unary request admission limits");
        this.maxRequestBytes = maxRequestBytes;
        this.maxActive = maxActive;
        this.methods = Set.copyOf(methods);
        if (this.methods.isEmpty()) throw new IllegalArgumentException("Unary admission requires an explicit method set");
        this.responseLimits = java.util.Map.copyOf(responseLimits);
        this.responseLimits.forEach((method, limit) -> {
            if (!this.methods.contains(method) || limit < 1 || limit > 256 * 1024 * 1024)
                throw new IllegalArgumentException("Invalid unary response admission limit");
        });
    }

    /** Install on a dedicated server; no unbounded service may share this listener. */
    void configure(ServerBuilder<?> builder) {
        builder.maxInboundMessageSize(maxRequestBytes).intercept(this);
    }

    @Override public <Q, S> ServerCall.Listener<Q> interceptCall(ServerCall<Q, S> call, Metadata headers,
            ServerCallHandler<Q, S> next) {
        var method = call.getMethodDescriptor();
        if (method.getType() != MethodDescriptor.MethodType.UNARY || !methods.contains(method.getFullMethodName())) {
            call.close(Status.UNIMPLEMENTED.withDescription("Method is unavailable in the bounded unary profile"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        final Scope scope;
        int responseLimit = responseLimits.getOrDefault(method.getFullMethodName(), 0);
        try { scope = admit(responseLimit); }
        catch (StatusRuntimeException failure) {
            call.close(failure.getStatus(), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        try {
            var guarded = new ForwardingServerCall.SimpleForwardingServerCall<Q, S>(call) {
                private boolean refused;
                @Override public void sendMessage(S message) {
                    if (refused) return;
                    if (responseLimit != 0) {
                        Status failure = !(message instanceof com.google.protobuf.MessageLite proto)
                                ? Status.INTERNAL.withDescription("Bounded response must be protobuf")
                                : proto.getSerializedSize() < 0 || proto.getSerializedSize() > responseLimit
                                ? Status.RESOURCE_EXHAUSTED.withDescription("Unary response exceeds configured limit") : null;
                        if (failure != null) { refused = true; super.close(failure, new Metadata()); return; }
                    }
                    super.sendMessage(message);
                }
                @Override public void close(Status status, Metadata trailers) {
                    if (!refused) super.close(status, trailers);
                }
            };
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(guarded, headers)) {
                // gRPC serializes callbacks for one listener, including terminal
                // callbacks. A cancelled synchronous onHalfClose must return before
                // onCancel can release the request allowance.
                @Override public void onCancel() {
                    try { super.onCancel(); } finally { scope.close(); }
                }
                @Override public void onComplete() {
                    try { super.onComplete(); } finally { scope.close(); }
                }
            };
        } catch (RuntimeException | Error failure) {
            scope.close();
            throw failure;
        }
    }

    private synchronized Scope admit(int responseLimit) {
        if (closed) throw Status.UNAVAILABLE.withDescription("Unary request admission is closed").asRuntimeException();
        if (active >= maxActive) throw exhausted();
        final PayloadBudget.Lease lease;
        try { lease = budget.reserve(2L * maxRequestBytes + responseLimit); }
        catch (PayloadBudget.CapacityExceededException failure) { throw exhausted(); }
        active++;
        return new Scope(lease);
    }

    private static StatusRuntimeException exhausted() {
        return Status.RESOURCE_EXHAUSTED.withDescription("Unary request capacity exhausted").asRuntimeException();
    }

    private final class Scope implements AutoCloseable {
        private final PayloadBudget.Lease lease;
        private boolean released;
        private Scope(PayloadBudget.Lease lease) { this.lease = lease; }
        @Override public void close() {
            synchronized (UnaryRequestAdmission.this) {
                if (released) return;
                released = true;
                lease.close();
                active--;
                UnaryRequestAdmission.this.notifyAll();
            }
        }
    }

    @Override public synchronized void close() { closed = true; }

    synchronized boolean awaitIdle(Duration timeout) throws InterruptedException {
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative drain timeout");
        long remaining = timeout.toNanos();
        long started = System.nanoTime();
        while (active != 0 && remaining > 0) {
            java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this, remaining);
            remaining = timeout.toNanos() - (System.nanoTime() - started);
        }
        return active == 0;
    }
}
