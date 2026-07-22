package ai.pipestream.proto.rerank.tei;

import ai.pipestream.proto.rerank.RerankProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import tei.v1.RerankGrpc;
import tei.v1.Tei.Rank;
import tei.v1.Tei.RerankRequest;
import tei.v1.Tei.RerankResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * {@link RerankProvider} that calls a Hugging Face Text Embeddings Inference (TEI) server
 * over its gRPC Rerank API. TEI serves one reranker model per process; the model is chosen
 * server side, so the only configuration this provider needs is the server target.
 *
 * <p>The channel uses plaintext: TEI serves plain gRPC behind a trusted network boundary, and
 * TLS can be added later when a deployment calls for it. Every call carries a 30 second
 * deadline.
 *
 * <p>The {@link #TeiRerankProvider(String)} constructor connects eagerly and
 * {@link #close()} shuts the channel down. The no-argument ServiceLoader constructor resolves
 * the target on first use from the {@value #TARGET_PROPERTY} system property, falling back to
 * the {@value #TARGET_ENVIRONMENT_VARIABLE} environment variable, so discovery through
 * {@link ai.pipestream.proto.rerank.RerankProviders} never fails on an unconfigured provider
 * that is not actually used. The {@link #TeiRerankProvider(ManagedChannel)} constructor
 * adopts a caller-owned channel that {@link #close()} leaves open.
 *
 * <p>The provider is safe for concurrent use; the gRPC channel multiplexes calls.
 */
public final class TeiRerankProvider implements RerankProvider, AutoCloseable {

    /** The id this provider registers under: {@value}. */
    public static final String PROVIDER_ID = "tei";

    /** System property naming the TEI gRPC target ({@code host:port}): {@value}. */
    public static final String TARGET_PROPERTY = "protomolt.rerank.tei.target";

    /** Environment variable consulted when {@link #TARGET_PROPERTY} is unset: {@value}. */
    public static final String TARGET_ENVIRONMENT_VARIABLE = "PROTOMOLT_RERANK_TEI_TARGET";

    private final Object lock = new Object();
    private final boolean callerOwned;
    private volatile ManagedChannel channel;
    private volatile String target;

    /**
     * ServiceLoader constructor. The target is resolved on the first
     * {@link #score(String, List)} call, from the {@value #TARGET_PROPERTY} system property
     * or, when that is unset, the {@value #TARGET_ENVIRONMENT_VARIABLE} environment variable.
     * {@link #close()} shuts down the channel once it has been created.
     */
    public TeiRerankProvider() {
        this.callerOwned = false;
    }

    /**
     * Connects to {@code target} ({@code host:port}) over plaintext. The provider owns the
     * channel; {@link #close()} shuts it down.
     */
    public TeiRerankProvider(String target) {
        this.target = Objects.requireNonNull(target, "target");
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        this.callerOwned = false;
    }

    /**
     * Reranks over a caller-owned channel. {@link #close()} leaves {@code channel} open.
     */
    public TeiRerankProvider(ManagedChannel channel) {
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
     * <p>One unary Rerank call carries the query and every candidate text, with
     * {@code raw_scores} left false so the server applies its sigmoid normalization, the
     * score scale most deployments compare. The response's ranks arrive sorted by score, each
     * carrying the index of the text it scores, so the scores are scattered back into input
     * order. An empty candidate list short-circuits to an empty result without a call.
     *
     * @throws IllegalStateException when this provider came from the ServiceLoader constructor
     *         and neither configuration knob names a target, when the call to the server
     *         fails, or when the response ranks do not cover the request's texts exactly once
     */
    @Override
    public List<Double> score(String query, List<String> texts) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(texts, "texts");
        if (texts.isEmpty()) {
            return List.of();
        }
        RerankResponse response;
        try {
            response = RerankGrpc.newBlockingStub(channel())
                    .withDeadlineAfter(30, TimeUnit.SECONDS)
                    .rerank(RerankRequest.newBuilder()
                            .setQuery(query)
                            .addAllTexts(texts)
                            .setRawScores(false)
                            .setReturnText(false)
                            .build());
        } catch (StatusRuntimeException e) {
            throw new IllegalStateException(
                    "TEI rerank failed against target '" + target + "'", e);
        }
        double[] scores = new double[texts.size()];
        boolean[] scored = new boolean[texts.size()];
        for (Rank rank : response.getRanksList()) {
            int index = rank.getIndex();
            if (index >= texts.size()) {
                throw new IllegalStateException("TEI rerank returned rank index " + index
                        + " for a batch of " + texts.size() + " texts");
            }
            if (scored[index]) {
                throw new IllegalStateException(
                        "TEI rerank returned rank index " + index + " twice");
            }
            scored[index] = true;
            scores[index] = rank.getScore();
        }
        for (int i = 0; i < scored.length; i++) {
            if (!scored[i]) {
                throw new IllegalStateException("TEI rerank returned no rank for text " + i
                        + " of " + texts.size());
            }
        }
        List<Double> aligned = new ArrayList<>(texts.size());
        for (double score : scores) {
            aligned.add(score);
        }
        return aligned;
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

    private static String configuredTarget() {
        String resolved = System.getProperty(TARGET_PROPERTY);
        if (resolved == null) {
            resolved = System.getenv(TARGET_ENVIRONMENT_VARIABLE);
        }
        if (resolved == null) {
            throw new IllegalStateException("No TEI rerank target configured; set the '"
                    + TARGET_PROPERTY + "' system property or the " + TARGET_ENVIRONMENT_VARIABLE
                    + " environment variable to the server's host:port");
        }
        return resolved;
    }
}
