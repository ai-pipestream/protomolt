package ai.protomolt.proto.grpc.validate;

import com.google.protobuf.MessageLite;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Header-time admission for explicitly selected synchronous unary handlers.
 * Reserves two maximum serialized requests conservatively for gRPC's unary
 * request window: {@code grpc-java 1.85.1} rejects duplicates before protobuf
 * decoding and delays decoding a valid unary request until half-close, so raw
 * request frames held before half-close must be bounded at header time before
 * {@link ValidatingServerInterceptor} runs in {@code onMessage}.
 *
 * <p>Selected protobuf responses may reserve an additional fixed allowance
 * through terminal delivery. Never install on handlers that return before
 * their request-consuming work completes.</p>
 */
public final class UnaryRequestAdmission implements ServerInterceptor, AutoCloseable {

    /** Maximum permitted request or response byte ceiling (256 MiB). */
    public static final int MAX_CEILING_BYTES = 256 * 1024 * 1024;

    /** Default per-request serialized byte cap (1 MiB). */
    public static final int DEFAULT_MAX_REQUEST_BYTES = 1024 * 1024;

    /** Default maximum concurrent admitted unary calls. */
    public static final int DEFAULT_MAX_ACTIVE = 64;

    private static final int MAX_ACTIVE_CEILING = 1024;

    private final long capacityBytes;
    private final int maxRequestBytes;
    private final int maxActive;
    private final Set<String> methods;
    private final Map<String, Integer> responseLimits;
    private final Set<String> openServices;
    private long reserved;
    private int active;
    private boolean closed;

    /**
     * Creates an admission gate for the named unary {@code methods} without
     * per-method response limits or open pass-through services.
     *
     * @param capacityBytes total byte capacity available for reservations
     * @param maxRequestBytes maximum inbound request size in bytes
     * @param maxActive maximum concurrent admitted calls
     * @param methods full method names ({@code pkg.Service/Method}) admitted
     */
    public UnaryRequestAdmission(long capacityBytes, int maxRequestBytes, int maxActive,
            Set<String> methods) {
        this(capacityBytes, maxRequestBytes, maxActive, methods, Map.of(), Set.of());
    }

    /**
     * Creates an admission gate for the named unary {@code methods} with optional
     * response size limits.
     *
     * @param capacityBytes total byte capacity available for reservations
     * @param maxRequestBytes maximum inbound request size in bytes
     * @param maxActive maximum concurrent admitted calls
     * @param methods full method names ({@code pkg.Service/Method}) admitted
     * @param responseLimits optional per-method response byte caps
     */
    public UnaryRequestAdmission(long capacityBytes, int maxRequestBytes, int maxActive,
            Set<String> methods, Map<String, Integer> responseLimits) {
        this(capacityBytes, maxRequestBytes, maxActive, methods, responseLimits, Set.of());
    }

    /**
     * Creates an admission gate for the named unary {@code methods}, with optional
     * response size limits and explicitly named plumbing services (such as health
     * and reflection) that pass through without unary method rejection.
     *
     * @param capacityBytes total byte capacity available for reservations
     * @param maxRequestBytes maximum inbound request size in bytes
     * @param maxActive maximum concurrent admitted calls
     * @param methods full method names ({@code pkg.Service/Method}) admitted
     * @param responseLimits optional per-method response byte caps
     * @param openServices full service names of plumbing services allowed alongside
     *        the guarded unary methods
     */
    public UnaryRequestAdmission(long capacityBytes, int maxRequestBytes, int maxActive,
            Set<String> methods, Map<String, Integer> responseLimits, Set<String> openServices) {
        if (maxRequestBytes < 1 || maxRequestBytes > MAX_CEILING_BYTES
                || maxActive < 1 || maxActive > MAX_ACTIVE_CEILING
                || capacityBytes < 2L * maxRequestBytes) {
            throw new IllegalArgumentException("Invalid unary request admission limits");
        }
        this.capacityBytes = capacityBytes;
        this.maxRequestBytes = maxRequestBytes;
        this.maxActive = maxActive;
        this.methods = Set.copyOf(Objects.requireNonNull(methods, "methods"));
        if (this.methods.isEmpty()) {
            throw new IllegalArgumentException("Unary admission requires an explicit method set");
        }
        this.responseLimits = Map.copyOf(Objects.requireNonNull(responseLimits, "responseLimits"));
        this.responseLimits.forEach((method, limit) -> {
            if (!this.methods.contains(method) || limit == null
                    || limit < 1 || limit > MAX_CEILING_BYTES) {
                throw new IllegalArgumentException("Invalid unary response admission limit");
            }
        });
        this.openServices = Set.copyOf(Objects.requireNonNull(openServices, "openServices"));
    }

    /**
     * Installs {@code maxInboundMessageSize(maxRequestBytes)} and this interceptor
     * onto {@code builder}.
     *
     * @param builder the server builder to configure
     */
    public void configure(ServerBuilder<?> builder) {
        Objects.requireNonNull(builder, "builder")
                .maxInboundMessageSize(maxRequestBytes)
                .intercept(this);
    }

    /**
     * Returns the total byte capacity configured on this admission gate.
     *
     * @return the capacity in bytes
     */
    public long capacityBytes() {
        return capacityBytes;
    }

    /**
     * Returns the per-request maximum serialized size in bytes.
     *
     * @return the maximum request size in bytes
     */
    public int maxRequestBytes() {
        return maxRequestBytes;
    }

    /**
     * Returns the maximum number of concurrent admitted calls.
     *
     * @return the concurrency limit
     */
    public int maxActive() {
        return maxActive;
    }

    /**
     * Returns the bytes currently reserved by in-flight admitted calls.
     *
     * @return the currently reserved bytes
     */
    public synchronized long reservedBytes() {
        return reserved;
    }

    /**
     * Returns the number of currently active admitted calls.
     *
     * @return the active call count
     */
    public synchronized int activeCalls() {
        return active;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        MethodDescriptor<ReqT, RespT> method = call.getMethodDescriptor();
        String serviceName = method.getServiceName();
        if (serviceName != null && openServices.contains(serviceName)) {
            return next.startCall(call, headers);
        }
        if (method.getType() != MethodDescriptor.MethodType.UNARY
                || !methods.contains(method.getFullMethodName())) {
            call.close(Status.UNIMPLEMENTED.withDescription(
                    "Method is unavailable in the bounded unary profile"), new Metadata());
            return new ServerCall.Listener<>() { };
        }
        final Scope scope;
        int responseLimit = responseLimits.getOrDefault(method.getFullMethodName(), 0);
        try {
            scope = admit(responseLimit);
        } catch (StatusRuntimeException failure) {
            call.close(failure.getStatus(), new Metadata());
            return new ServerCall.Listener<>() { };
        }
        try {
            ServerCall<ReqT, RespT> guarded =
                    new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                        private boolean refused;

                        @Override
                        public void sendMessage(RespT message) {
                            if (refused) {
                                return;
                            }
                            if (responseLimit != 0) {
                                Status failure = !(message instanceof MessageLite proto)
                                        ? Status.INTERNAL.withDescription(
                                                "Bounded response must be protobuf")
                                        : proto.getSerializedSize() < 0
                                                || proto.getSerializedSize() > responseLimit
                                        ? Status.RESOURCE_EXHAUSTED.withDescription(
                                                "Unary response exceeds configured limit")
                                        : null;
                                if (failure != null) {
                                    refused = true;
                                    super.close(failure, new Metadata());
                                    return;
                                }
                            }
                            super.sendMessage(message);
                        }

                        @Override
                        public void close(Status status, Metadata trailers) {
                            if (!refused) {
                                super.close(status, trailers);
                            }
                        }
                    };
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
                    next.startCall(guarded, headers)) {
                @Override
                public void onCancel() {
                    try {
                        super.onCancel();
                    } finally {
                        scope.close();
                    }
                }

                @Override
                public void onComplete() {
                    try {
                        super.onComplete();
                    } finally {
                        scope.close();
                    }
                }
            };
        } catch (RuntimeException | Error failure) {
            scope.close();
            throw failure;
        }
    }

    private synchronized Scope admit(int responseLimit) {
        if (closed) {
            throw Status.UNAVAILABLE.withDescription("Unary request admission is closed")
                    .asRuntimeException();
        }
        long bytes = 2L * maxRequestBytes + responseLimit;
        if (active >= maxActive || bytes > capacityBytes - reserved) {
            throw exhausted();
        }
        reserved += bytes;
        active++;
        return new Scope(bytes);
    }

    private static StatusRuntimeException exhausted() {
        return Status.RESOURCE_EXHAUSTED.withDescription("Unary request capacity exhausted")
                .asRuntimeException();
    }

    private final class Scope implements AutoCloseable {
        private final long bytes;
        private boolean released;

        private Scope(long bytes) {
            this.bytes = bytes;
        }

        @Override
        public void close() {
            synchronized (UnaryRequestAdmission.this) {
                if (released) {
                    return;
                }
                released = true;
                reserved -= bytes;
                active--;
                UnaryRequestAdmission.this.notifyAll();
            }
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
    }

    /**
     * Waits up to {@code timeout} for all in-flight admitted calls to drain.
     *
     * @param timeout maximum time to wait; must not be negative
     * @return {@code true} if no calls remain active, {@code false} if the timeout elapsed
     * @throws InterruptedException if interrupted while waiting
     */
    public synchronized boolean awaitIdle(Duration timeout) throws InterruptedException {
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("Negative drain timeout");
        }
        long remaining = timeout.toNanos();
        long started = System.nanoTime();
        while (active != 0 && remaining > 0) {
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
            remaining = timeout.toNanos() - (System.nanoTime() - started);
        }
        return active == 0;
    }
}
