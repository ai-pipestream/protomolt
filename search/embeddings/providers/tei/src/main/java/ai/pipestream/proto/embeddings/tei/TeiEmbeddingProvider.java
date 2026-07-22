package ai.pipestream.proto.embeddings.tei;

import ai.pipestream.proto.embeddings.EmbeddingProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import tei.v1.EmbedGrpc;
import tei.v1.Tei.EmbedRequest;
import tei.v1.Tei.EmbedResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
 * {@link ai.pipestream.proto.embeddings.EmbeddingProviders} never fails on an unconfigured
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

    /** Fixed text embedded once to learn the vector length; see {@link #dimension()}. */
    private static final String DIMENSION_PROBE = "dimension probe";

    private final Object lock = new Object();
    private final boolean callerOwned;
    private volatile ManagedChannel channel;
    private volatile String target;
    private volatile Integer dimension;
    private volatile Boolean truncate;

    /**
     * ServiceLoader constructor. The target is resolved on the first {@link #dimension()},
     * {@link #embed(String)}, or {@link #embedAll(List)} call, from the
     * {@value #TARGET_PROPERTY} system property or, when that is unset, the
     * {@value #TARGET_ENVIRONMENT_VARIABLE} environment variable. {@link #close()} shuts down
     * the channel once it has been created.
     */
    public TeiEmbeddingProvider() {
        this.callerOwned = false;
    }

    /**
     * Connects to {@code target} ({@code host:port}) over plaintext. The provider owns the
     * channel; {@link #close()} shuts it down.
     */
    public TeiEmbeddingProvider(String target) {
        this.target = Objects.requireNonNull(target, "target");
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        this.callerOwned = false;
    }

    /**
     * Embeds over a caller-owned channel. {@link #close()} leaves {@code channel} open.
     */
    public TeiEmbeddingProvider(ManagedChannel channel) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.target = channel.authority();
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
     * a fixed probe string once and caching the vector length. The probe runs at most once per
     * provider instance, guarded for concurrent callers.
     *
     * @throws IllegalStateException when this provider came from the ServiceLoader constructor
     *         and neither configuration knob names a target, or when the probe call fails
     */
    @Override
    public int dimension() {
        Integer learned = dimension;
        if (learned != null) {
            return learned;
        }
        synchronized (lock) {
            if (dimension == null) {
                dimension = embed(DIMENSION_PROBE).length;
            }
            return dimension;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException when this provider came from the ServiceLoader constructor
     *         and neither configuration knob names a target, or when the call to the server
     *         fails
     */
    @Override
    public float[] embed(String text) {
        Objects.requireNonNull(text, "text");
        try {
            EmbedResponse response = EmbedGrpc.newBlockingStub(channel())
                    .withDeadlineAfter(30, TimeUnit.SECONDS)
                    .embed(EmbedRequest.newBuilder()
                            .setInputs(text)
                            .setTruncate(truncate())
                            .build());
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

    /**
     * Embeds each text with its own unary Embed call, issued concurrently on virtual threads.
     * TEI batches concurrent requests server side (dynamic batching), so per-text unary calls
     * issued at the same time get the batching benefit without a batch API.
     *
     * <p>Results are collected in input order. When a collected call has failed, every call
     * not yet collected is cancelled and the failure propagates. A failure is only noticed
     * once collection reaches it, so a still-running call earlier in the input order delays
     * it; that wait is bounded by the per-call 30 second deadline.
     *
     * <p>A structured-concurrency scope would express this fork-join directly, but
     * {@code java.util.concurrent.StructuredTaskScope} is a preview API in JDK 21 and the
     * module compiles to the Java 21 baseline without preview, so the scope is spelled out
     * with a virtual-thread executor and futures instead: one fork per text, unordered
     * completion, input-order collection, cancellation of the uncollected on failure.
     *
     * @throws IllegalStateException when any embed call fails or the calling thread is
     *         interrupted while waiting
     */
    @Override
    public List<float[]> embedAll(List<String> texts) {
        Objects.requireNonNull(texts, "texts");
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<float[]>> futures = new ArrayList<>(texts.size());
            for (String text : texts) {
                futures.add(executor.submit(() -> embed(text)));
            }
            List<float[]> vectors = new ArrayList<>(texts.size());
            for (int collected = 0; collected < futures.size(); collected++) {
                try {
                    vectors.add(await(futures.get(collected)));
                } catch (RuntimeException e) {
                    for (int rest = collected + 1; rest < futures.size(); rest++) {
                        futures.get(rest).cancel(true);
                    }
                    throw e;
                }
            }
            return vectors;
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
            if (channel != null) {
                channel.shutdown();
            }
        }
    }

    private ManagedChannel channel() {
        ManagedChannel open = channel;
        if (open != null) {
            return open;
        }
        synchronized (lock) {
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

    private static float[] await(Future<float[]> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while embedding a batch", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("Embedding a batch failed", e.getCause());
        }
    }

    private static String configuredTarget() {
        String resolved = System.getProperty(TARGET_PROPERTY);
        if (resolved == null) {
            resolved = System.getenv(TARGET_ENVIRONMENT_VARIABLE);
        }
        if (resolved == null) {
            throw new IllegalStateException("No TEI target configured; set the '"
                    + TARGET_PROPERTY + "' system property or the " + TARGET_ENVIRONMENT_VARIABLE
                    + " environment variable to the server's host:port");
        }
        return resolved;
    }

    private static boolean configuredTruncate() {
        String configured = System.getProperty(TRUNCATE_PROPERTY);
        if (configured == null) {
            configured = System.getenv(TRUNCATE_ENVIRONMENT_VARIABLE);
        }
        return configured == null || Boolean.parseBoolean(configured);
    }
}
