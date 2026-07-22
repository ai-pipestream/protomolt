package ai.pipestream.proto.embeddings.ovms;

import ai.pipestream.proto.embeddings.EmbeddingProvider;
import com.google.protobuf.ByteString;
import inference.GRPCInferenceServiceGrpc;
import inference.GrpcPredictV2.InferTensorContents;
import inference.GrpcPredictV2.ModelInferRequest;
import inference.GrpcPredictV2.ModelInferResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * {@link EmbeddingProvider} that calls an OpenVINO Model Server (OVMS) embeddings servable
 * over the KServe v2 gRPC prediction protocol (the same wire protocol NVIDIA Triton speaks).
 * OVMS embeddings servables accept raw strings and tokenize server side, so each
 * {@code ModelInferRequest} carries a single BYTES input tensor of shape {@code [N]} with the
 * N texts as UTF-8, answered with an FP32 output tensor of shape {@code [N, dim]}; batches
 * larger than {@value #MAX_TEXTS_PER_REQUEST} texts go out as multiple requests.
 *
 * <p>The channel uses plaintext: OVMS serves plain gRPC behind a trusted network boundary,
 * and TLS can be added later when a deployment calls for it. Every call carries a 30 second
 * deadline.
 *
 * <p>The {@link #OvmsEmbeddingProvider(String, String)} constructor connects eagerly and
 * {@link #close()} shuts the channel down. The no-argument ServiceLoader constructor resolves
 * target and model on first use from the {@value #TARGET_PROPERTY} and
 * {@value #MODEL_PROPERTY} system properties, falling back to the
 * {@value #TARGET_ENVIRONMENT_VARIABLE} and {@value #MODEL_ENVIRONMENT_VARIABLE} environment
 * variables, so discovery through
 * {@link ai.pipestream.proto.embeddings.EmbeddingProviders} never fails on an unconfigured
 * provider that is not actually used. The tensor names default to {@value #DEFAULT_INPUT_NAME}
 * and {@value #DEFAULT_OUTPUT_NAME} and can be overridden with the
 * {@value #INPUT_NAME_PROPERTY} and {@value #OUTPUT_NAME_PROPERTY} system properties or the
 * {@value #INPUT_NAME_ENVIRONMENT_VARIABLE} and {@value #OUTPUT_NAME_ENVIRONMENT_VARIABLE}
 * environment variables. The
 * {@link #OvmsEmbeddingProvider(ManagedChannel, String, String, String)} constructor adopts a
 * caller-owned channel that {@link #close()} leaves open.
 *
 * <p>The provider is safe for concurrent use; the gRPC channel multiplexes calls.
 */
public final class OvmsEmbeddingProvider implements EmbeddingProvider, AutoCloseable {

    /** The id this provider registers under: {@value}. */
    public static final String PROVIDER_ID = "ovms";

    /** System property naming the OVMS gRPC target ({@code host:port}): {@value}. */
    public static final String TARGET_PROPERTY = "protomolt.embeddings.ovms.target";

    /** Environment variable consulted when {@link #TARGET_PROPERTY} is unset: {@value}. */
    public static final String TARGET_ENVIRONMENT_VARIABLE = "PROTOMOLT_OVMS_TARGET";

    /** System property naming the servable to infer against: {@value}. */
    public static final String MODEL_PROPERTY = "protomolt.embeddings.ovms.model";

    /** Environment variable consulted when {@link #MODEL_PROPERTY} is unset: {@value}. */
    public static final String MODEL_ENVIRONMENT_VARIABLE = "PROTOMOLT_OVMS_MODEL";

    /** System property overriding the input tensor name; defaults to
     * {@value #DEFAULT_INPUT_NAME}: {@value}. */
    public static final String INPUT_NAME_PROPERTY = "protomolt.embeddings.ovms.input";

    /** Environment variable consulted when {@link #INPUT_NAME_PROPERTY} is unset: {@value}. */
    public static final String INPUT_NAME_ENVIRONMENT_VARIABLE = "PROTOMOLT_OVMS_INPUT";

    /** System property overriding the output tensor name; defaults to
     * {@value #DEFAULT_OUTPUT_NAME}: {@value}. */
    public static final String OUTPUT_NAME_PROPERTY = "protomolt.embeddings.ovms.output";

    /** Environment variable consulted when {@link #OUTPUT_NAME_PROPERTY} is unset: {@value}. */
    public static final String OUTPUT_NAME_ENVIRONMENT_VARIABLE = "PROTOMOLT_OVMS_OUTPUT";

    /** Input tensor name used when {@value #INPUT_NAME_PROPERTY} is unset: {@value}. */
    public static final String DEFAULT_INPUT_NAME = "input";

    /** Output tensor name used when {@value #OUTPUT_NAME_PROPERTY} is unset: {@value}. */
    public static final String DEFAULT_OUTPUT_NAME = "embedding";

    /**
     * Cap on texts per {@code ModelInferRequest}. Two ceilings force chunking: gRPC's default
     * 4MB message cap turns a few thousand texts into RESOURCE_EXHAUSTED, and a big CPU batch
     * can outrun the 30 second per-call deadline.
     */
    static final int MAX_TEXTS_PER_REQUEST = 256;

    /** Fixed text embedded once to learn the vector length; see {@link #dimension()}. */
    private static final String DIMENSION_PROBE = "dimension probe";

    private final Object lock = new Object();
    private final boolean callerOwned;
    private volatile ManagedChannel channel;
    private volatile String target;
    private volatile String modelName;
    private volatile String inputName;
    private volatile String outputName;
    private volatile Integer dimension;

    /**
     * ServiceLoader constructor. Target and model are resolved on the first
     * {@link #dimension()}, {@link #embed(String)}, or {@link #embedAll(List)} call, from the
     * {@value #TARGET_PROPERTY} and {@value #MODEL_PROPERTY} system properties or, when those
     * are unset, the {@value #TARGET_ENVIRONMENT_VARIABLE} and
     * {@value #MODEL_ENVIRONMENT_VARIABLE} environment variables. {@link #close()} shuts down
     * the channel once it has been created.
     */
    public OvmsEmbeddingProvider() {
        this.callerOwned = false;
    }

    /**
     * Connects to {@code target} ({@code host:port}) over plaintext, inferring against
     * {@code modelName}. The provider owns the channel; {@link #close()} shuts it down.
     */
    public OvmsEmbeddingProvider(String target, String modelName) {
        this.target = Objects.requireNonNull(target, "target");
        this.modelName = Objects.requireNonNull(modelName, "modelName");
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        this.callerOwned = false;
    }

    /**
     * Infers over a caller-owned channel with explicit tensor names. {@link #close()} leaves
     * {@code channel} open.
     */
    public OvmsEmbeddingProvider(ManagedChannel channel, String modelName,
            String inputName, String outputName) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.target = channel.authority();
        this.modelName = Objects.requireNonNull(modelName, "modelName");
        this.inputName = Objects.requireNonNull(inputName, "inputName");
        this.outputName = Objects.requireNonNull(outputName, "outputName");
        this.callerOwned = true;
    }

    @Override
    public String providerId() {
        return PROVIDER_ID;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The KServe v2 protocol reports tensor shapes only through model metadata, so the
     * dimension is learned by inferring a fixed probe string once and caching the vector
     * length. The probe runs at most once per provider instance, guarded for concurrent
     * callers.
     *
     * @throws IllegalStateException when this provider came from the ServiceLoader constructor
     *         and the configuration knobs do not name a target and a model, or when the probe
     *         call fails
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
     *         and the configuration knobs do not name a target and a model, when the call to
     *         the server fails, or when the response tensor is malformed
     */
    @Override
    public float[] embed(String text) {
        Objects.requireNonNull(text, "text");
        return embedAll(List.of(text)).get(0);
    }

    /**
     * Embeds the batch in chunks of at most {@value #MAX_TEXTS_PER_REQUEST} texts, one
     * {@code ModelInferRequest} per chunk under its own fresh 30 second deadline: a BYTES
     * input tensor of shape {@code [N]} holding the chunk's N texts as UTF-8, an FP32 output
     * tensor of shape {@code [N, dim]} back, the chunks' vectors concatenated in input order.
     * OVMS embeddings servables accept raw strings and tokenize server side, so there is no
     * client-side tokenization to keep in sync with the model.
     *
     * @throws IllegalStateException when a call fails or a response tensor is missing,
     *         wrongly shaped, or carries neither {@code fp32_contents} nor raw contents
     */
    @Override
    public List<float[]> embedAll(List<String> texts) {
        Objects.requireNonNull(texts, "texts");
        resolveConfiguration();
        List<float[]> vectors = new ArrayList<>(texts.size());
        for (int from = 0; from < texts.size(); from += MAX_TEXTS_PER_REQUEST) {
            List<String> chunk = texts.subList(from,
                    Math.min(from + MAX_TEXTS_PER_REQUEST, texts.size()));
            ModelInferRequest request = inferRequest(chunk);
            ModelInferResponse response;
            try {
                response = GRPCInferenceServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(30, TimeUnit.SECONDS)
                        .modelInfer(request);
            } catch (StatusRuntimeException e) {
                throw new IllegalStateException("OVMS ModelInfer failed for model '" + modelName
                        + "' against target '" + target + "'", e);
            }
            vectors.addAll(vectors(response, chunk.size()));
        }
        return vectors;
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

    private ModelInferRequest inferRequest(List<String> texts) {
        InferTensorContents.Builder contents = InferTensorContents.newBuilder();
        for (String text : texts) {
            contents.addBytesContents(ByteString.copyFrom(text, StandardCharsets.UTF_8));
        }
        return ModelInferRequest.newBuilder()
                .setModelName(modelName)
                .addInputs(ModelInferRequest.InferInputTensor.newBuilder()
                        .setName(inputName())
                        .setDatatype("BYTES")
                        .addShape(texts.size())
                        .setContents(contents))
                .build();
    }

    private List<float[]> vectors(ModelInferResponse response, int texts) {
        for (int index = 0; index < response.getOutputsCount(); index++) {
            ModelInferResponse.InferOutputTensor output = response.getOutputs(index);
            if (!output.getName().equals(outputName())) {
                continue;
            }
            return vectors(response, index, output, texts);
        }
        throw new IllegalStateException("OVMS response carries no output tensor named '"
                + outputName() + "'");
    }

    private List<float[]> vectors(ModelInferResponse response, int index,
            ModelInferResponse.InferOutputTensor output, int texts) {
        String name = output.getName();
        if (!"FP32".equals(output.getDatatype())) {
            throw new IllegalStateException("OVMS output tensor '" + name
                    + "' has datatype '" + output.getDatatype() + "', expected FP32");
        }
        if (output.getShapeCount() != 2 || output.getShape(0) != texts) {
            throw new IllegalStateException("OVMS output tensor '" + name + "' has shape "
                    + output.getShapeList() + ", expected [" + texts + ", dim]");
        }
        int dim = Math.toIntExact(output.getShape(1));
        float[] flat = floats(response, index, output, texts * dim);
        List<float[]> vectors = new ArrayList<>(texts);
        for (int i = 0; i < texts; i++) {
            float[] vector = new float[dim];
            System.arraycopy(flat, i * dim, vector, 0, dim);
            vectors.add(vector);
        }
        return vectors;
    }

    private static float[] floats(ModelInferResponse response, int index,
            ModelInferResponse.InferOutputTensor output, int expected) {
        String name = output.getName();
        if (output.getContents().getFp32ContentsCount() > 0) {
            if (output.getContents().getFp32ContentsCount() != expected) {
                throw new IllegalStateException("OVMS output tensor '" + name + "' carries "
                        + output.getContents().getFp32ContentsCount()
                        + " fp32 values, expected " + expected);
            }
            float[] flat = new float[expected];
            for (int i = 0; i < expected; i++) {
                flat[i] = output.getContents().getFp32Contents(i);
            }
            return flat;
        }
        // KServe servers commonly answer with raw contents: the flattened little-endian F32
        // payload of the tensor at the same index as 'outputs'.
        if (index < response.getRawOutputContentsCount()) {
            byte[] raw = response.getRawOutputContents(index).toByteArray();
            if (raw.length != expected * Float.BYTES) {
                throw new IllegalStateException("OVMS output tensor '" + name
                        + "' carries " + raw.length + " raw bytes, expected "
                        + expected * Float.BYTES);
            }
            ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
            float[] flat = new float[expected];
            for (int i = 0; i < expected; i++) {
                flat[i] = buffer.getFloat();
            }
            return flat;
        }
        throw new IllegalStateException("OVMS output tensor '" + name
                + "' carries neither fp32_contents nor raw_output_contents");
    }

    /**
     * Resolves the target and model for the ServiceLoader constructor on first use, failing
     * with one message that names every missing knob. A no-op for the eager constructors,
     * which set both fields.
     */
    private void resolveConfiguration() {
        if (channel != null && modelName != null) {
            return;
        }
        synchronized (lock) {
            if (channel == null && modelName == null) {
                String resolvedTarget = configured(TARGET_PROPERTY, TARGET_ENVIRONMENT_VARIABLE);
                String resolvedModel = configured(MODEL_PROPERTY, MODEL_ENVIRONMENT_VARIABLE);
                if (resolvedTarget == null || resolvedModel == null) {
                    StringBuilder message = new StringBuilder("OVMS provider is not configured;");
                    if (resolvedTarget == null) {
                        message.append(" set the '").append(TARGET_PROPERTY)
                                .append("' system property or the ")
                                .append(TARGET_ENVIRONMENT_VARIABLE)
                                .append(" environment variable to the server's host:port;");
                    }
                    if (resolvedModel == null) {
                        message.append(" set the '").append(MODEL_PROPERTY)
                                .append("' system property or the ")
                                .append(MODEL_ENVIRONMENT_VARIABLE)
                                .append(" environment variable to the servable name;");
                    }
                    throw new IllegalStateException(message.toString());
                }
                channel = ManagedChannelBuilder.forTarget(resolvedTarget).usePlaintext().build();
                target = resolvedTarget;
                modelName = resolvedModel;
                return;
            }
            // The eager constructors never leave one field set and the other unset.
            throw new IllegalStateException("OVMS provider is not configured");
        }
    }

    private String inputName() {
        String name = inputName;
        if (name != null) {
            return name;
        }
        synchronized (lock) {
            if (inputName == null) {
                String resolved = configured(INPUT_NAME_PROPERTY, INPUT_NAME_ENVIRONMENT_VARIABLE);
                inputName = resolved == null ? DEFAULT_INPUT_NAME : resolved;
            }
            return inputName;
        }
    }

    private String outputName() {
        String name = outputName;
        if (name != null) {
            return name;
        }
        synchronized (lock) {
            if (outputName == null) {
                String resolved = configured(OUTPUT_NAME_PROPERTY, OUTPUT_NAME_ENVIRONMENT_VARIABLE);
                outputName = resolved == null ? DEFAULT_OUTPUT_NAME : resolved;
            }
            return outputName;
        }
    }

    /** The value of {@code property}, falling back to {@code environmentVariable}; null when
     * neither is set. */
    private static String configured(String property, String environmentVariable) {
        String value = System.getProperty(property);
        if (value == null) {
            value = System.getenv(environmentVariable);
        }
        return value;
    }
}
