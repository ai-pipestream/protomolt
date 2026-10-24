package ai.protomolt.proto.authz.grpc;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.ScopeBudgets;
import ai.protomolt.proto.actions.Scopes;
import com.google.protobuf.MessageLite;
import io.grpc.Context;
import io.grpc.Decompressor;
import io.grpc.DecompressorRegistry;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The per-service scope table for a hand-built gRPC surface (the serving roles): every mounted
 * service declares the scope its methods require, method entries override their service, and
 * openly-served plumbing (health, reflection) is named explicitly. A scoped caller reaching a
 * service the table does not name is refused by name — a newly mounted service is never
 * silently open. Runs behind {@link ApiTokenServerInterceptor}, reading the caller it
 * resolved; a call with no resolved caller (an open server) passes with process authority.
 *
 * <p>When attached via {@link #configure(ServerBuilder)} (or paired with
 * {@link #streamTracerFactory()}), positive per-scope payload caps and rate budgets are
 * evaluated as soon as {@code MessageDeframer} reads each inbound frame, closing refused
 * calls with {@code RESOURCE_EXHAUSTED} before {@code halfClose()} without waiting for unary
 * message decoding.
 */
public final class ScopeServerInterceptor implements ServerInterceptor {

    private static final long MAX_DECOMPRESSED_COUNT_BYTES = 64L * 1024L;
    private static final int SCRATCH_BUFFER_BYTES = 4096;
    private static final ThreadLocal<InboundFrameTracker> PENDING_COMPRESSED_TRACKER =
            new ThreadLocal<>();
    private static final DecompressorRegistry DECOMPRESSOR_REGISTRY = buildDecompressorRegistry();

    private final Map<String, String> serviceScopes;
    private final Map<String, String> methodScopes;
    private final Set<String> openServices;
    private final ScopeBudgets budgets;
    private final Context.Key<InboundFrameTracker> frameTrackerKey =
            Context.key("protomolt.authz.grpc.inbound-frame-tracker");
    private final ServerStreamTracer.Factory tracerFactory = new ServerStreamTracer.Factory() {
        @Override
        public ServerStreamTracer newServerStreamTracer(String fullMethodName, Metadata headers) {
            return new InboundFrameTracker(frameTrackerKey);
        }
    };

    private static DecompressorRegistry buildDecompressorRegistry() {
        DecompressorRegistry defaults = DecompressorRegistry.getDefaultInstance();
        Set<String> advertised = defaults.getAdvertisedMessageEncodings();
        DecompressorRegistry registry = DecompressorRegistry.emptyInstance();
        for (String encoding : defaults.getKnownMessageEncodings()) {
            Decompressor delegate = defaults.lookupDecompressor(encoding);
            if (delegate != null) {
                registry = registry.with(
                        new PayloadCapDecompressor(delegate),
                        advertised.contains(encoding));
            }
        }
        return registry;
    }

    /**
     * An interceptor spending on its own ledger, for a surface that is the node's only
     * enforcement point.
     *
     * @param serviceScopes required scope by full service name
     * @param methodScopes overrides by full method name ({@code pkg.Service/Method})
     * @param openServices full service names served under authentication alone
     */
    public ScopeServerInterceptor(Map<String, String> serviceScopes,
                                  Map<String, String> methodScopes,
                                  Set<String> openServices) {
        this(serviceScopes, methodScopes, openServices, new ScopeBudgets());
    }

    /**
     * An interceptor spending on {@code budgets}: the node's action catalog and its other
     * guarded surfaces take the same ledger, so a caller cannot spend a per-scope budget
     * once per transport.
     *
     * @param serviceScopes required scope by full service name
     * @param methodScopes overrides by full method name ({@code pkg.Service/Method})
     * @param openServices full service names served under authentication alone
     * @param budgets the node's spending ledger
     */
    public ScopeServerInterceptor(Map<String, String> serviceScopes,
                                  Map<String, String> methodScopes,
                                  Set<String> openServices,
                                  ScopeBudgets budgets) {
        this.budgets = Objects.requireNonNull(budgets, "budgets");
        for (String scope : serviceScopes.values()) {
            requireKnown(scope);
        }
        for (String scope : methodScopes.values()) {
            requireKnown(scope);
        }
        this.serviceScopes = Map.copyOf(serviceScopes);
        this.methodScopes = Map.copyOf(methodScopes);
        this.openServices = Set.copyOf(openServices);
    }

    private static void requireKnown(String scope) {
        if (!Scopes.VOCABULARY.contains(scope)) {
            throw new IllegalArgumentException("unknown scope '" + scope
                    + "'; the vocabulary is " + String.join(", ", Scopes.VOCABULARY));
        }
    }

    /**
     * Stream tracer factory that observes inbound frame sizes in {@code MessageDeframer} before
     * {@code halfClose()} triggers unary message decoding.
     *
     * @return the stream tracer factory for this interceptor
     */
    public ServerStreamTracer.Factory streamTracerFactory() {
        return tracerFactory;
    }

    /**
     * Attaches the payload-cap-aware decompressor registry, {@link #streamTracerFactory()},
     * and this interceptor to {@code builder} so positive per-scope payload caps and rate
     * budgets are enforced on frame arrival before {@code halfClose()} for both uncompressed
     * and compressed messages.
     *
     * @param builder the server builder to configure
     */
    public void configure(ServerBuilder<?> builder) {
        Objects.requireNonNull(builder, "builder");
        builder.decompressorRegistry(DECOMPRESSOR_REGISTRY)
                .addStreamTracerFactory(tracerFactory)
                .intercept(this);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        Caller caller = CallerContexts.current();
        if (caller.unrestricted()) {
            return next.startCall(call, headers);
        }
        String method = call.getMethodDescriptor().getFullMethodName();
        String service = call.getMethodDescriptor().getServiceName();
        if (openServices.contains(service)) {
            return next.startCall(call, headers);
        }
        String scope = methodScopes.get(method);
        if (scope == null) {
            scope = serviceScopes.get(service);
        }
        if (scope == null) {
            call.close(Status.PERMISSION_DENIED.withDescription("no scope is declared for "
                    + Objects.toString(service, method)
                    + "; a scoped caller cannot call it"), new Metadata());
            return new ServerCall.Listener<>() { };
        }
        if (!caller.holds(scope)) {
            call.close(Status.PERMISSION_DENIED.withDescription("caller '" + caller.name()
                    + "' does not hold '" + scope + "', which " + method + " requires"),
                    new Metadata());
            return new ServerCall.Listener<>() { };
        }
        // The caller's budget spends once per request. Without a payload cap that is
        // decided here; with one, the whole consult waits for the message so payload
        // and rate are one spend.
        Caller.Budget budget = caller.budgets().get(scope);
        if (budget == null) {
            return next.startCall(call, headers);
        }
        String budgetScope = scope;
        if (budget.maxPayloadBytes() == 0) {
            Optional<String> refusal = budgets.refuse(caller, budgetScope, -1);
            if (refusal.isPresent()) {
                call.close(Status.RESOURCE_EXHAUSTED.withDescription(refusal.get()),
                        new Metadata());
                return new ServerCall.Listener<>() { };
            }
            return next.startCall(call, headers);
        }
        InboundFrameTracker tracker = frameTrackerKey.get();
        if (tracker != null) {
            tracker.arm(call, caller, budgetScope, budget, budgets);
        }
        ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
                delegate) {
            private boolean refused;

            @Override
            public void onEvent(Object event) {
                if (event instanceof FrameRefusalEvent refusalEvent) {
                    if (!refused) {
                        refused = true;
                        call.close(Status.RESOURCE_EXHAUSTED
                                .withDescription(refusalEvent.description()), new Metadata());
                    }
                    return;
                }
                if (event instanceof CompressedFrameArrivalEvent arrival) {
                    if (refused || tracker == null) {
                        return;
                    }
                    InputStream delayed = delayedMessageOf(call);
                    if (delayed == null) {
                        if (arrival.attempt() < 2) {
                            call.triggerEvent(new CompressedFrameArrivalEvent(
                                    arrival.attempt() + 1));
                        }
                        return;
                    }
                    PENDING_COMPRESSED_TRACKER.set(tracker);
                    try {
                        delayed.available();
                    } catch (IOException e) {
                        refused = true;
                        call.close(Status.INTERNAL
                                .withDescription("Failed to decompress inbound frame")
                                .withCause(e), new Metadata());
                    } finally {
                        PENDING_COMPRESSED_TRACKER.remove();
                    }
                    return;
                }
                super.onEvent(event);
            }

            @Override
            public void onMessage(ReqT message) {
                if (refused) {
                    return;
                }
                FrameVerdict verdict = tracker != null ? tracker.pollVerdict() : null;
                if (verdict != null && verdict.evaluated()) {
                    if (verdict.refusal() != null) {
                        refused = true;
                        call.close(Status.RESOURCE_EXHAUSTED
                                .withDescription(verdict.refusal()), new Metadata());
                        return;
                    }
                    super.onMessage(message);
                    return;
                }
                long size = message instanceof MessageLite proto
                        ? proto.getSerializedSize() : -1;
                Optional<String> refusal = budgets.refuse(caller, budgetScope, size);
                if (refusal.isPresent()) {
                    refused = true;
                    call.close(Status.RESOURCE_EXHAUSTED
                            .withDescription(refusal.get()), new Metadata());
                    return;
                }
                super.onMessage(message);
            }

            @Override
            public void onHalfClose() {
                if (!refused) {
                    super.onHalfClose();
                }
            }
        };
    }

    private record FrameRefusalEvent(String description) {
    }

    private record CompressedFrameArrivalEvent(int attempt) {
    }

    private record FrameVerdict(boolean evaluated, String refusal) {
    }

    private record Gate(ServerCall<?, ?> call, Caller caller, String scope,
            Caller.Budget budget, ScopeBudgets budgets) {
    }

    private static InputStream delayedMessageOf(ServerCall<?, ?> call) {
        try {
            Object current = call;
            while (current != null && !"io.grpc.internal.ServerCallImpl".equals(
                    current.getClass().getName())) {
                Method delegateMethod = findNoArgMethod(current.getClass(), "delegate");
                if (delegateMethod == null) {
                    return null;
                }
                delegateMethod.setAccessible(true);
                current = delegateMethod.invoke(current);
            }
            if (current == null) {
                return null;
            }
            Field streamField = current.getClass().getDeclaredField("stream");
            streamField.setAccessible(true);
            Object stream = streamField.get(current);
            if (stream == null) {
                return null;
            }
            Method transportStateMethod = findNoArgMethod(stream.getClass(), "transportState");
            if (transportStateMethod == null) {
                return null;
            }
            transportStateMethod.setAccessible(true);
            Object transportState = transportStateMethod.invoke(stream);
            if (transportState == null) {
                return null;
            }
            Method listenerMethod = findNoArgMethod(transportState.getClass(), "listener");
            if (listenerMethod == null) {
                return null;
            }
            listenerMethod.setAccessible(true);
            Object jumpListener = listenerMethod.invoke(transportState);
            if (jumpListener == null) {
                return null;
            }
            Field appListenerField = jumpListener.getClass().getDeclaredField("listener");
            appListenerField.setAccessible(true);
            Object appListener = appListenerField.get(jumpListener);
            if (appListener == null) {
                return null;
            }
            Field delayedMessageField = appListener.getClass().getDeclaredField("delayedMessage");
            delayedMessageField.setAccessible(true);
            return (InputStream) delayedMessageField.get(appListener);
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }

    private static Method findNoArgMethod(Class<?> type, String name) {
        for (Class<?> cursor = type; cursor != null; cursor = cursor.getSuperclass()) {
            for (Method method : cursor.getDeclaredMethods()) {
                if (method.getName().equals(name) && method.getParameterCount() == 0) {
                    return method;
                }
            }
        }
        return null;
    }

    private static final class InboundFrameTracker extends ServerStreamTracer {
        private final Context.Key<InboundFrameTracker> key;
        private final Queue<FrameVerdict> verdicts = new ConcurrentLinkedQueue<>();
        private volatile Gate gate;
        private volatile boolean refused;

        private InboundFrameTracker(Context.Key<InboundFrameTracker> key) {
            this.key = key;
        }

        private void arm(ServerCall<?, ?> call, Caller caller, String scope,
                Caller.Budget budget, ScopeBudgets budgets) {
            this.gate = new Gate(call, caller, scope, budget, budgets);
        }

        private FrameVerdict pollVerdict() {
            return verdicts.poll();
        }

        @Override
        public Context filterContext(Context context) {
            return context.withValue(key, this);
        }

        @Override
        public void inboundMessageRead(int seqNo, long optionalWireSize,
                long optionalUncompressedSize) {
            Gate active = this.gate;
            if (active == null || refused) {
                return;
            }
            if (optionalUncompressedSize >= 0) {
                evaluateUncompressedSize(active, optionalUncompressedSize);
            } else {
                active.call().triggerEvent(new CompressedFrameArrivalEvent(0));
            }
        }

        private void evaluateUncompressedSize(Gate active, long uncompressedSize) {
            Optional<String> refusal = active.budgets().refuse(
                    active.caller(), active.scope(), uncompressedSize);
            verdicts.add(new FrameVerdict(true, refusal.orElse(null)));
            if (refusal.isPresent()) {
                refused = true;
                active.call().triggerEvent(new FrameRefusalEvent(refusal.get()));
            }
        }

        private InputStream inspectCompressedStream(InputStream decompressed) throws IOException {
            Gate active = this.gate;
            if (active == null || refused) {
                return decompressed;
            }
            long rawCap = active.budget().maxPayloadBytes();
            if (rawCap <= 0) {
                return decompressed;
            }
            int cap = (int) Math.min(rawCap, Integer.MAX_VALUE - 8);
            byte[] prefix = decompressed.readNBytes(cap);
            int next = decompressed.read();
            if (next == -1) {
                decompressed.close();
                evaluateUncompressedSize(active, prefix.length);
                if (refused) {
                    return InputStream.nullInputStream();
                }
                return new ByteArrayInputStream(prefix);
            }
            long totalUncompressed = (long) prefix.length + 1L;
            long countLimit = Math.max((long) cap + 1L, MAX_DECOMPRESSED_COUNT_BYTES);
            byte[] scratch = new byte[SCRATCH_BUFFER_BYTES];
            while (totalUncompressed < countLimit) {
                int toRead = (int) Math.min(scratch.length, countLimit - totalUncompressed);
                int read = decompressed.read(scratch, 0, toRead);
                if (read < 0) {
                    break;
                }
                totalUncompressed += read;
            }
            decompressed.close();
            evaluateUncompressedSize(active, totalUncompressed);
            return InputStream.nullInputStream();
        }
    }

    private static final class PayloadCapDecompressor implements Decompressor {
        private final Decompressor delegate;

        private PayloadCapDecompressor(Decompressor delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getMessageEncoding() {
            return delegate.getMessageEncoding();
        }

        @Override
        public InputStream decompress(InputStream is) throws IOException {
            InboundFrameTracker tracker = PENDING_COMPRESSED_TRACKER.get();
            PENDING_COMPRESSED_TRACKER.remove();
            InputStream decompressed = delegate.decompress(is);
            if (tracker == null) {
                return decompressed;
            }
            return tracker.inspectCompressedStream(decompressed);
        }
    }
}
