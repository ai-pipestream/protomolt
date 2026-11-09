package ai.protomolt.proto.search.embedding.turboembed;

import ai.protomolt.proto.search.embedding.EmbeddingProvider;
import ai.protomolt.proto.search.embedding.EmbeddingInFlightLimit;
import ai.protomolt.proto.search.embedding.ovms.KserveTensors;
import com.google.protobuf.ByteString;
import inference.GRPCInferenceServiceGrpc;
import inference.GrpcPredictV2.InferParameter;
import inference.GrpcPredictV2.InferTensorContents;
import inference.GrpcPredictV2.ModelInferRequest;
import inference.GrpcPredictV2.ModelInferResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Optional Turboembed KServe provider. Queries and documents explicitly select
 * their bundle prefixes. Every response must match the configured model revision,
 * manifest SHA-256 and dimension before any vector is returned.
 *
 * <p>The ServiceLoader constructor is inert until selected. Configuration requires
 * PROTOMOLT_TURBOEMBED_TARGET, MODEL, REVISION, MANIFEST_SHA256, DIMS and TRANSPORT
 * (each with the same PROTOMOLT_TURBOEMBED_ prefix); transport is tls or plaintext.
 * Credentials and custom TLS belong on the caller-owned channel constructor.
 */
public final class TurboembedEmbeddingProvider implements EmbeddingProvider {
    private static final String PREFIX = "PROTOMOLT_TURBOEMBED_";
    private static final int MAX_BATCH = 32;
    /** Process-wide RPC cap; system property overrides the environment, default 8. */
    public static final String MAX_IN_FLIGHT_PROPERTY = "protomolt.embeddings.turboembed.max-in-flight";
    /** Environment fallback for the process-wide RPC cap. */
    public static final String MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE = PREFIX + "MAX_IN_FLIGHT";
    /** Seconds one RPC may wait for a process-wide slot; system property overrides the environment, default 30. */
    public static final String ADMISSION_TIMEOUT_PROPERTY = "protomolt.embeddings.turboembed.admission-timeout-seconds";
    /** Environment fallback for the admission timeout. */
    public static final String ADMISSION_TIMEOUT_ENVIRONMENT_VARIABLE = PREFIX + "ADMISSION_TIMEOUT_SECONDS";
    private static final EmbeddingInFlightLimit IN_FLIGHT = new EmbeddingInFlightLimit();
    private Integer maxInFlight;
    private Duration admissionTimeout;
    private final Map<String, String> environment;
    private Session session;
    private boolean closed;

    /** Immutable expected vector-space identity. */
    public record Model(String name, String revision, String manifestSha256, int dimensions) {
        /** Validates the complete expected identity before opening a channel. */
        public Model {
            requireText(name, "model");
            requireText(revision, "revision");
            requireText(manifestSha256, "manifest SHA-256");
            if (manifestSha256.length() != 64) {
                throw new IllegalArgumentException("manifest SHA-256 must be 64 lowercase hex characters");
            }
            for (int i = 0; i < manifestSha256.length(); i++) {
                char c = manifestSha256.charAt(i);
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                    throw new IllegalArgumentException("manifest SHA-256 must be lowercase hex");
                }
            }
            if (dimensions <= 0) {
                throw new IllegalArgumentException("dimensions must be positive");
            }
        }
    }

    private record Session(ManagedChannel channel, Model model, boolean owned) {}

    /** Creates an unconfigured, inert provider for ServiceLoader discovery. */
    public TurboembedEmbeddingProvider() {
        this(System.getenv());
    }

    /** Creates a lazy provider with explicit environment configuration. */
    public TurboembedEmbeddingProvider(Map<String, String> environment) {
        this.environment = Map.copyOf(environment);
    }

    /** Uses a caller-owned channel; closing this provider never closes that channel. */
    public TurboembedEmbeddingProvider(ManagedChannel channel, Model model) {
        this.environment = Map.of();
        this.session = new Session(Objects.requireNonNull(channel), Objects.requireNonNull(model), false);
    }

    @Override
    public synchronized String providerId() {
        // The policy digest must change when the model bundle changes. Discovery
        // must remain inert when this optional provider has not been configured.
        String digest = session == null ? environment.get(PREFIX + "MANIFEST_SHA256")
                : session.model().manifestSha256();
        return digest == null ? "turboembed:unconfigured" : "turboembed-v1:" + digest;
    }

    @Override
    public int dimension() {
        return configured().model().dimensions();
    }

    /** Ordinary embedding explicitly uses no query or document prefix. */
    @Override
    public float[] embed(String text) {
        return infer(List.of(text), "PROMPT_NONE").getFirst();
    }

    @Override
    public List<float[]> embedAll(List<String> texts) {
        return infer(texts, "PROMPT_NONE");
    }

    @Override
    public List<float[]> embedAllDocuments(List<String> texts) {
        return infer(texts, "PROMPT_DOCUMENT");
    }

    @Override
    public List<float[]> embedAllQueries(List<String> texts) {
        return infer(texts, "PROMPT_QUERY");
    }

    @Override
    public float[] embedQuery(String text) {
        return infer(List.of(text), "PROMPT_QUERY").getFirst();
    }

    @Override
    public float[] embedDocument(String text) {
        return infer(List.of(text), "PROMPT_DOCUMENT").getFirst();
    }

    private List<float[]> infer(List<String> texts, String purpose) {
        Objects.requireNonNull(texts, "texts");
        Session active = configured();
        Model model = active.model();
        List<float[]> vectors = new ArrayList<>(texts.size());
        for (int start = 0; start < texts.size(); start += MAX_BATCH) {
            List<String> batch = texts.subList(start, Math.min(start + MAX_BATCH, texts.size()));
            InferTensorContents.Builder contents = InferTensorContents.newBuilder();
            for (String text : batch) {
                contents.addBytesContents(ByteString.copyFromUtf8(Objects.requireNonNull(text, "text")));
            }
            ModelInferRequest request = ModelInferRequest.newBuilder()
                    .setModelName(model.name()).setModelVersion(model.revision())
                    .addInputs(ModelInferRequest.InferInputTensor.newBuilder()
                            .setName("texts").setDatatype("BYTES").addShape(batch.size()).setContents(contents))
                    .putParameters("prompt_role", parameter(purpose))
                    .putParameters("truncate", parameter("TRUNCATE_NONE"))
                    .putParameters("normalize", parameter("NORMALIZE_MODEL"))
                    .putParameters("quantization", parameter("QUANTIZE_NONE"))
                    .build();
            ModelInferResponse response = IN_FLIGHT.execute(maxInFlight(), admissionTimeout(), () -> {
                configured(); // Refuse queued calls if this provider was closed while waiting.
                return GRPCInferenceServiceGrpc.newBlockingStub(active.channel())
                        .withDeadlineAfter(30, TimeUnit.SECONDS).modelInfer(request);
            });
            if (!response.getModelName().equals(model.name())
                    || !response.getModelVersion().equals(model.revision())
                    || !model.manifestSha256().equals(response.getParametersOrDefault(
                            "manifest_sha256", InferParameter.getDefaultInstance()).getStringParam())) {
                throw new IllegalStateException("Turboembed returned a different model revision or manifest");
            }
            List<float[]> decoded = KserveTensors.decode(response, batch.size(), "vectors");
            for (float[] vector : decoded) {
                if (vector.length != model.dimensions()) {
                    throw new IllegalStateException("Turboembed vector dimension does not match the pinned model");
                }
                for (float value : vector) {
                    if (!Float.isFinite(value)) {
                        throw new IllegalStateException("Turboembed returned a non-finite vector component");
                    }
                }
            }
            vectors.addAll(decoded);
        }
        return vectors;
    }

    private synchronized int maxInFlight() {
        if (maxInFlight == null) {
            String value = System.getProperty(MAX_IN_FLIGHT_PROPERTY);
            if (value == null) {
                value = environment.get(MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE);
            }
            int parsed = 8;
            if (value != null) {
                try {
                    parsed = Integer.parseInt(value.trim());
                } catch (NumberFormatException failure) {
                    throw new IllegalStateException("Invalid " + MAX_IN_FLIGHT_PROPERTY + " / "
                            + MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE + ": " + value, failure);
                }
            }
            if (parsed < 1) {
                throw new IllegalStateException(MAX_IN_FLIGHT_PROPERTY + " / "
                        + MAX_IN_FLIGHT_ENVIRONMENT_VARIABLE + " must be positive: " + value);
            }
            maxInFlight = parsed;
        }
        return maxInFlight;
    }

    private synchronized Duration admissionTimeout() {
        if (admissionTimeout == null) {
            String value = System.getProperty(ADMISSION_TIMEOUT_PROPERTY);
            if (value == null) {
                value = environment.get(ADMISSION_TIMEOUT_ENVIRONMENT_VARIABLE);
            }
            admissionTimeout = EmbeddingInFlightLimit.admissionTimeoutSeconds(value,
                    ADMISSION_TIMEOUT_PROPERTY + " / " + ADMISSION_TIMEOUT_ENVIRONMENT_VARIABLE);
        }
        return admissionTimeout;
    }

    private static InferParameter parameter(String value) {
        return InferParameter.newBuilder().setStringParam(value).build();
    }

    private synchronized Session configured() {
        if (closed) {
            throw new IllegalStateException("Turboembed provider is closed");
        }
        if (session != null) {
            return session;
        }
        String target = required("TARGET");
        String transport = required("TRANSPORT");
        if (!transport.equals("tls") && !transport.equals("plaintext")) {
            throw new IllegalArgumentException(PREFIX + "TRANSPORT must be tls or plaintext");
        }
        int dimensions;
        try {
            dimensions = Integer.parseInt(required("DIMS"));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(PREFIX + "DIMS must be a positive integer", failure);
        }
        Model model = new Model(required("MODEL"), required("REVISION"), required("MANIFEST_SHA256"), dimensions);
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target);
        if (transport.equals("plaintext")) {
            builder.usePlaintext();
        } else {
            builder.useTransportSecurity();
        }
        session = new Session(builder.build(), model, true);
        return session;
    }

    private String required(String suffix) {
        String key = PREFIX + suffix;
        String value = environment.get(key);
        requireText(value, key);
        return value;
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank() || !value.equals(value.strip())) {
            throw new IllegalArgumentException(name + " must be nonblank with no surrounding whitespace");
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (session != null && session.owned()) {
            session.channel().shutdownNow();
        }
    }
}
