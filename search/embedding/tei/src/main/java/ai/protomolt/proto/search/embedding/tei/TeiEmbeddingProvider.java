package ai.protomolt.proto.search.embedding.tei;

import ai.protomolt.proto.search.embedding.EmbeddingProvider;
import ai.protomolt.proto.search.embedding.EmbeddingInFlightLimit;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import tei.v1.EmbedGrpc;
import tei.v1.Tei.EmbedRequest;
import tei.v1.Tei.EmbedResponse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * {@link EmbeddingProvider} that calls a Hugging Face Text Embeddings Inference (TEI) server
 * over its gRPC API. TEI serves one embedding model per process; the model is chosen server
 * side, so the only configuration this provider needs is the server target.
 *
 * <p>The channel uses plaintext: TEI serves plain gRPC behind a trusted network boundary, and
 * TLS can be added later when a deployment calls for it. Every call carries a 30 second
 * deadline.
 *
 * <p>Every request asks the server to truncate inputs past the model's maximum sequence
 * length instead of failing the call. A deployment that wants overlength input rejected sets
 * the {@value #TRUNCATE_PROPERTY} system property or, when that is unset, the
 * {@value #TRUNCATE_ENVIRONMENT_VARIABLE} environment variable to {@code false}; the knob is
 * read once, on the first call. {@code normalize} is deliberately left unset so the server's
 * own default applies.
 *
 * <p>The {@link #TeiEmbeddingProvider(String)} constructor connects eagerly and
 * {@link #close()} shuts the channel down. The no-argument ServiceLoader constructor resolves
 * the target on first use from the {@value #TARGET_PROPERTY} system property, falling back to
 * the {@value #TARGET_ENVIRONMENT_VARIABLE} environment variable, so discovery through
 * {@link ai.protomolt.proto.search.embedding.EmbeddingProviders} never fails on an unconfigured
 * provider that is not actually used. The {@link #TeiEmbeddingProvider(ManagedChannel)}
 * constructor adopts a caller-owned channel that {@link #close()} leaves open.
 *
 * <p>The provider is safe for concurrent use; the gRPC channel multiplexes calls.
 */
public final class TeiEmbeddingProvider implements EmbeddingProvider, AutoCloseable {

    /** The id this provider registers under: {@value}. */
    public static final String PROVIDER_ID = "tei";

    /** System property naming the TEI gRPC target ({@code host:port}): {@value}. */
    public static final String TARGET_PROPERTY = "protomolt.embeddings.tei.target";

    /** Environment variable consulted when {@link #TARGET_PROPERTY} is unset: {@value}. */
    public static final String TARGET_ENVIRONMENT_VARIABLE = "PROTOMOLT_TEI_TARGET";

    /**
     * System property controlling server-side truncation of overlength inputs: {@value}.
     * Unset or {@code true}, inputs past the model's maximum sequence length truncate server
     * side; {@code false} makes the server reject them instead.
     */
    public static final String TRUNCATE_PROPERTY = "protomolt.embeddings.tei.truncate";

    /** Environment variable consulted when {@link #TRUNCATE_PROPERTY} is unset: {@value}. */
    public static final String TRUNCATE_ENVIRONMENT_VARIABLE = "PROTOMOLT_TEI_TRUNCATE";

    /**
     * System property capping Embed RPCs across all TEI instances in this process
     * at once: {@value}. A positive integer; unset means {@value #DEFAULT_MAX_IN_FLIGHT}.
     */
    public static final String MAX_IN_FLIGHT_PROPERTY = "protomolt.embeddings.tei.max-in-flight";

    /** Environment variable consulted when {@link #MAX_IN_FLIGHT_PROPERTY} is unset: {@value}. */
    public static final String MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE = "PROTOMOLT_TEI_MAX_IN_FLIGHT";

    /** In-flight cap applied when neither knob is set: {@value}. */
    public static final int DEFAULT_MAX_IN_FLIGHT = 8;

    /**
     * System property bounding how long one Embed call waits for a process-wide slot, in
     * whole seconds: {@value}. A positive integer; unset means 30. Unlike the cap, callers
     * need not agree on it.
     */
    public static final String ADMISSION_TIMEOUT_PROPERTY = "protomolt.embeddings.tei.admission-timeout-seconds";

    /** Environment variable consulted when {@link #ADMISSION_TIMEOUT_PROPERTY} is unset: {@value}. */
    public static final String ADMISSION_TIMEOUT_ENVIRONMENT_VARIABLE = "PROTOMOLT_TEI_ADMISSION_TIMEOUT_SECONDS";

    /** Fixed text embedded once to learn the vector length; see {@link #dimension()}. */
    private static final String DIMENSION_PROBE = "dimension probe";

    private static final long RPC_DEADLINE_SECONDS = 30L;
    private static final String CLOSED_MESSAGE = "TEI embedding provider is closed";
    private static final String INTERRUPTED_BATCH_MESSAGE = "Interrupted while embedding a batch";
    private static final String INTERRUPTED_DIMENSION_MESSAGE =
            "Interrupted while waiting for embedding dimension";

    private static final EmbeddingInFlightLimit IN_FLIGHT = new EmbeddingInFlightLimit();

    private final Object lock = new Object();
    private final boolean callerOwned;
    private final Map<String, String> environment;
    private final AtomicInteger activeBatchWorkers = new AtomicInteger();
    private volatile ManagedChannel channel;
    private volatile String target;
    private volatile Integer dimension;
    private volatile Boolean truncate;
    private volatile Integer maxInFlight;
    private volatile Duration admissionTimeout;
    private volatile boolean closed;
    private CompletableFuture<Integer> dimensionProbe;

    /**
     * ServiceLoader constructor. The target is resolved on the first {@link #dimension()},
     * {@link #embed(String)}, or {@link #embedAll(List)} call, from the
     * {@value #TARGET_PROPERTY} system property or, when that is unset, the
     * {@value #TARGET_ENVIRONMENT_VARIABLE} environment variable. {@link #close()} shuts down
     * the channel once it has been created.
     */
    public TeiEmbeddingProvider() {
        this(System.getenv());
    }

    /**
     * Same as the ServiceLoader constructor, except that the environment variables are read
     * from {@code environment} instead of the process environment. System properties still
     * take precedence over it.
     *
     * @param environment environment variables consulted when a system property is unset
     */
    public TeiEmbeddingProvider(Map<String, String> environment) {
        this.environment = Objects.requireNonNull(environment, "environment");
        this.callerOwned = false;
    }

    /**
     * Connects to {@code target} ({@code host:port}) over plaintext. The provider owns the
     * channel; {@link #close()} shuts it down.
     *
     * @param target the gRPC target ({@code host:port}) of the TEI server
     */
    public TeiEmbeddingProvider(String target) {
        this.target = Objects.requireNonNull(target, "target");
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        this.callerOwned = false;
        this.environment = System.getenv();
    }

    /**
     * Embeds over a caller-owned channel. {@link #close()} leaves {@code channel} open.
     *
     * @param channel caller-owned channel to the TEI server
     */
    public TeiEmbeddingProvider(ManagedChannel channel) {
        this(channel, System.getenv());
    }

    /**
     * Embeds over a caller-owned channel, reading the {@value #TRUNCATE_ENVIRONMENT_VARIABLE}
     * environment variable from {@code environment} instead of the process environment. The
     * {@value #TRUNCATE_PROPERTY} system property still takes precedence over it.
     * {@link #close()} leaves {@code channel} open.
     *
     * @param channel caller-owned channel to the TEI server
     * @param environment environment variables consulted when a system property is unset
     */
    public TeiEmbeddingProvider(ManagedChannel channel, Map<String, String> environment) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.target = channel.authority();
        this.environment = Objects.requireNonNull(environment, "environment");
        this.callerOwned = true;
    }

    @Override
    public String providerId() {
        return PROVIDER_ID;
    }

    /**
     * {@inheritDoc}
     *
     * <p>TEI's Info RPC reports no vector dimension, so the dimension is learned by embedding
     * a fixed probe string once and caching the vector length. Concurrent cold callers share a
     * single in-flight probe outside the provider lock so neither admission wait nor the probe
     * RPC blocks channel setup or {@link #close()}.
     *
     * @throws IllegalStateException when this provider came from the ServiceLoader constructor
     *         and neither configuration knob names a target, when this provider has been closed,
     *         or when the probe call fails
     */
    @Override
    public int dimension() {
        while (true) {
            ensureOpen();
            Integer learned = dimension;
            if (learned != null) {
                return learned;
            }
            CompletableFuture<Integer> probe;
            boolean owner = false;
            synchronized (lock) {
                ensureOpen();
                learned = dimension;
                if (learned != null) {
                    return learned;
                }
                probe = dimensionProbe;
                if (probe == null) {
                    probe = new CompletableFuture<>();
                    dimensionProbe = probe;
                    owner = true;
                }
            }
            if (owner) {
                return runDimensionProbe(probe);
            }
            Integer discovered = awaitDimensionProbe(probe);
            if (discovered != null) {
                return discovered;
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException when this provider came from the ServiceLoader constructor
     *         and neither configuration knob names a target, when this provider has been closed,
     *         or when the call to the server fails
     */
    @Override
    public float[] embed(String text) {
        Objects.requireNonNull(text, "text");
        ensureOpen();
        int cap = maxInFlight();
        Duration wait = admissionTimeout();
        ManagedChannel active = channel();
        EmbedRequest request = EmbedRequest.newBuilder().setInputs(text).setTruncate(truncate()).build();
        Optional<EmbeddingInFlightLimit.Permit> admitted = IN_FLIGHT.acquire(cap, wait, () -> closed);
        if (admitted.isEmpty()) {
            throw new IllegalStateException(CLOSED_MESSAGE);
        }
        try (EmbeddingInFlightLimit.Permit _ = admitted.get()) {
            return embedAdmitted(active, request);
        }
    }

    private float[] embedAdmitted(ManagedChannel active, EmbedRequest request) {
        ensureOpen();
        try {
            EmbedResponse response = EmbedGrpc.newBlockingStub(active)
                    .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .embed(request);
            float[] vector = new float[response.getEmbeddingsCount()];
            for (int i = 0; i < vector.length; i++) {
                vector[i] = response.getEmbeddings(i);
            }
            return vector;
        } catch (StatusRuntimeException e) {
            throw new IllegalStateException(
                    "TEI embed failed against target '" + target + "'", e);
        }
    }

    /** Symmetric model: document text uses the same batched call as {@link #embedAll(List)}. */
    @Override
    public List<float[]> embedAllDocuments(List<String> texts) {
        return embedAll(texts);
    }

    /** Symmetric model: queries use the same batched call as {@link #embedAll(List)}. */
    @Override
    public List<float[]> embedAllQueries(List<String> texts) {
        return embedAll(texts);
    }

    /**
     * Embeds each text with its own unary Embed call, issued concurrently on virtual threads.
     * TEI batches concurrent requests server side (dynamic batching), so per-text unary calls
     * issued at the same time get the batching benefit without a batch API.
     *
     * <p>The process-wide cap covers every TEI instance and entry point, including the
     * dimension probe. Each instance resolves its cap once; overlapping calls configured
     * with different caps fail explicitly. Reconfiguration is possible when admission is
     * idle. Closing one provider does not reset the shared cap or close another's channel.
     *
     * <p>Admission is acquired on the calling thread before each virtual-thread worker is
     * started, so waiting batches never queue extra workers in admission and active workers
     * never exceed the calls permitted to run. Results retain input order; the first worker
     * failure cancels the remaining active workers. RPCs have a 30 second deadline; waiting
     * for admission is interruptible.
     *
     * @throws IllegalStateException when any embed call fails, when this provider has been
     *         closed, or when the calling thread is interrupted while waiting
     */
    @Override
    public List<float[]> embedAll(List<String> texts) {
        Objects.requireNonNull(texts, "texts");
        ensureOpen();
        if (texts.isEmpty()) {
            return new ArrayList<>();
        }
        for (String text : texts) {
            Objects.requireNonNull(text, "text");
        }
        if (texts.size() == 1) {
            List<float[]> single = new ArrayList<>(1);
            single.add(embed(texts.getFirst()));
            return single;
        }
        int cap = maxInFlight();
        Duration wait = admissionTimeout();
        ManagedChannel active = channel();
        boolean shouldTruncate = truncate();
        Batch batch = new Batch(texts.size(), activeBatchWorkers);
        int dispatched = 0;
        try {
            for (int i = 0; i < texts.size(); i++) {
                Optional<EmbeddingInFlightLimit.Permit> admitted =
                        IN_FLIGHT.acquire(cap, wait, () -> closed || batch.isCancelled());
                if (admitted.isEmpty()) {
                    ensureOpen();
                    break;
                }
                EmbeddingInFlightLimit.Permit permit = admitted.get();
                EmbedRequest request = EmbedRequest.newBuilder()
                        .setInputs(texts.get(i))
                        .setTruncate(shouldTruncate)
                        .build();
                if (!batch.startWorker(i, () -> embedAdmitted(active, request), permit)) {
                    break;
                }
                dispatched++;
            }
            return batch.awaitResults(dispatched);
        } catch (Throwable failure) {
            batch.cancelAll();
            throw failure;
        } finally {
            batch.awaitWorkersExitedUninterruptibly();
        }
    }

    /**
     * Shuts the channel down unless this provider adopted a caller-owned one. Shutting down a
     * channel never created yet (ServiceLoader constructor, no use so far) is a no-op.
     */
    @Override
    public void close() {
        if (callerOwned) {
            return;
        }
        synchronized (lock) {
            closed = true;
            if (channel != null) {
                channel.shutdown();
            }
        }
    }

    static EmbeddingInFlightLimit inFlightLimit() {
        return IN_FLIGHT;
    }

    int activeBatchWorkers() {
        return activeBatchWorkers.get();
    }

    ManagedChannel activeChannel() {
        return channel;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(CLOSED_MESSAGE);
        }
    }

    private int runDimensionProbe(CompletableFuture<Integer> probe) {
        try {
            int discovered = embed(DIMENSION_PROBE).length;
            synchronized (lock) {
                if (!closed) {
                    dimension = discovered;
                }
                if (dimensionProbe == probe) {
                    dimensionProbe = null;
                }
            }
            probe.complete(discovered);
            return discovered;
        } catch (Throwable failure) {
            synchronized (lock) {
                if (dimensionProbe == probe) {
                    dimensionProbe = null;
                }
            }
            if (isInterruption(failure)) {
                probe.complete(null);
            } else {
                probe.completeExceptionally(failure);
            }
            throw Batch.rethrow(failure);
        }
    }

    private static Integer awaitDimensionProbe(CompletableFuture<Integer> probe) {
        try {
            return probe.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(INTERRUPTED_DIMENSION_MESSAGE, e);
        } catch (ExecutionException e) {
            throw Batch.rethrow(e.getCause());
        }
    }

    private static boolean isInterruption(Throwable failure) {
        return Thread.currentThread().isInterrupted()
                || failure instanceof InterruptedException
                || failure.getCause() instanceof InterruptedException;
    }

    private ManagedChannel channel() {
        ensureOpen();
        ManagedChannel open = channel;
        if (open != null) {
            return open;
        }
        synchronized (lock) {
            ensureOpen();
            if (channel == null) {
                String resolved = configuredTarget();
                channel = ManagedChannelBuilder.forTarget(resolved).usePlaintext().build();
                target = resolved;
            }
            return channel;
        }
    }

    private boolean truncate() {
        Boolean resolved = truncate;
        if (resolved != null) {
            return resolved;
        }
        synchronized (lock) {
            if (truncate == null) {
                truncate = configuredTruncate();
            }
            return truncate;
        }
    }

    private int maxInFlight() {
        Integer resolved = maxInFlight;
        if (resolved != null) {
            return resolved;
        }
        synchronized (lock) {
            if (maxInFlight == null) {
                maxInFlight = configuredMaxInFlight();
            }
            return maxInFlight;
        }
    }

    private Duration admissionTimeout() {
        Duration resolved = admissionTimeout;
        if (resolved != null) {
            return resolved;
        }
        synchronized (lock) {
            if (admissionTimeout == null) {
                String configured = System.getProperty(ADMISSION_TIMEOUT_PROPERTY);
                if (configured == null) {
                    configured = environment.get(ADMISSION_TIMEOUT_ENVIRONMENT_VARIABLE);
                }
                admissionTimeout = EmbeddingInFlightLimit.admissionTimeoutSeconds(configured,
                        ADMISSION_TIMEOUT_PROPERTY + " / " + ADMISSION_TIMEOUT_ENVIRONMENT_VARIABLE);
            }
            return admissionTimeout;
        }
    }

    private static final class Batch {
        private final ReentrantLock batchLock = new ReentrantLock();
        private final Condition doneOrFailed = batchLock.newCondition();
        private final Condition workersExited = batchLock.newCondition();
        private final Set<Thread> runningWorkers = new HashSet<>();
        private final float[][] vectors;
        private final AtomicInteger providerWorkers;
        private volatile boolean cancelled;
        private Throwable firstFailure;
        private int activeWorkers;

        private Batch(int size, AtomicInteger providerWorkers) {
            this.vectors = new float[size][];
            this.providerWorkers = providerWorkers;
        }

        private boolean isCancelled() {
            return cancelled;
        }

        private boolean startWorker(
                int index,
                Supplier<float[]> call,
                EmbeddingInFlightLimit.Permit permit) {
            boolean closePermit = true;
            try {
                batchLock.lock();
                try {
                    if (cancelled) {
                        return false;
                    }
                    Thread worker;
                    try {
                        worker = Thread.ofVirtual().unstarted(() -> runWorker(index, call, permit));
                    } catch (Throwable startFailure) {
                        recordFailureLocked(startFailure);
                        throw startFailure;
                    }
                    activeWorkers++;
                    providerWorkers.incrementAndGet();
                    runningWorkers.add(worker);
                    try {
                        worker.start();
                        closePermit = false;
                    } catch (Throwable startFailure) {
                        runningWorkers.remove(worker);
                        providerWorkers.decrementAndGet();
                        activeWorkers--;
                        recordFailureLocked(startFailure);
                        if (activeWorkers == 0) {
                            workersExited.signalAll();
                        }
                        throw startFailure;
                    }
                    return true;
                } finally {
                    batchLock.unlock();
                }
            } finally {
                if (closePermit) {
                    permit.close();
                }
            }
        }

        private void runWorker(
                int index,
                Supplier<float[]> call,
                EmbeddingInFlightLimit.Permit permit) {
            Throwable problem = null;
            try {
                vectors[index] = call.get();
            } catch (Throwable failure) {
                problem = failure;
            } finally {
                if (problem != null) {
                    batchLock.lock();
                    try {
                        recordFailureLocked(problem);
                    } finally {
                        batchLock.unlock();
                    }
                }
                permit.close();
                batchLock.lock();
                try {
                    runningWorkers.remove(Thread.currentThread());
                    providerWorkers.decrementAndGet();
                    activeWorkers--;
                    if (activeWorkers == 0) {
                        doneOrFailed.signalAll();
                        workersExited.signalAll();
                    }
                } finally {
                    batchLock.unlock();
                }
            }
        }

        private void recordFailureLocked(Throwable failure) {
            if (firstFailure == null) {
                firstFailure = failure;
            }
            cancelled = true;
            Thread current = Thread.currentThread();
            for (Thread worker : runningWorkers) {
                if (worker != current) {
                    worker.interrupt();
                }
            }
            doneOrFailed.signalAll();
        }

        private void cancelAll() {
            batchLock.lock();
            try {
                cancelled = true;
                for (Thread worker : runningWorkers) {
                    worker.interrupt();
                }
                doneOrFailed.signalAll();
            } finally {
                batchLock.unlock();
            }
        }

        private List<float[]> awaitResults(int dispatched) {
            batchLock.lock();
            try {
                while (!cancelled && activeWorkers > 0) {
                    doneOrFailed.await();
                }
                if (firstFailure != null) {
                    throw rethrow(firstFailure);
                }
                if (dispatched != vectors.length) {
                    throw new IllegalStateException("Batch embedding stopped after "
                            + dispatched + " of " + vectors.length + " items");
                }
                return new ArrayList<>(Arrays.asList(vectors));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(INTERRUPTED_BATCH_MESSAGE, e);
            } finally {
                batchLock.unlock();
            }
        }

        private void awaitWorkersExitedUninterruptibly() {
            batchLock.lock();
            try {
                while (activeWorkers > 0) {
                    workersExited.awaitUninterruptibly();
                }
            } finally {
                batchLock.unlock();
            }
        }

        private static RuntimeException rethrow(Throwable failure) {
            if (failure instanceof RuntimeException runtime) {
                return runtime;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            return new IllegalStateException("Embedding a batch failed", failure);
        }
    }

    private String configuredTarget() {
        String resolved = System.getProperty(TARGET_PROPERTY);
        if (resolved == null) {
            resolved = environment.get(TARGET_ENVIRONMENT_VARIABLE);
        }
        if (resolved == null) {
            throw new IllegalStateException("No TEI target configured; set the '"
                    + TARGET_PROPERTY + "' system property or the " + TARGET_ENVIRONMENT_VARIABLE
                    + " environment variable to the server's host:port");
        }
        return resolved;
    }

    private int configuredMaxInFlight() {
        String configured = System.getProperty(MAX_IN_FLIGHT_PROPERTY);
        if (configured == null) {
            configured = environment.get(MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE);
        }
        if (configured == null) {
            return DEFAULT_MAX_IN_FLIGHT;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(configured.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid " + MAX_IN_FLIGHT_PROPERTY + " / "
                    + MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE + ": " + configured, e);
        }
        if (parsed < 1) {
            throw new IllegalStateException("The TEI in-flight cap must be a positive integer; set the '"
                    + MAX_IN_FLIGHT_PROPERTY + "' system property or the "
                    + MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE + " environment variable, found '"
                    + configured + "'");
        }
        return parsed;
    }

    private boolean configuredTruncate() {
        String configured = System.getProperty(TRUNCATE_PROPERTY);
        if (configured == null) {
            configured = environment.get(TRUNCATE_ENVIRONMENT_VARIABLE);
        }
        return configured == null || Boolean.parseBoolean(configured);
    }
}
