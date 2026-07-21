package ai.pipestream.proto.embeddings.model2vec;

import ai.pipestream.proto.embeddings.EmbeddingProvider;
import opennlp.embeddings.StaticEmbeddingModel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Objects;

/**
 * {@link EmbeddingProvider} backed by an OpenNLP {@link StaticEmbeddingModel}: a distilled
 * Model2Vec token-vector table with subword tokenization and mean pooling, no model forward
 * pass. The model directory is a Model2Vec release layout as
 * {@link StaticEmbeddingModel#load(Path)} reads it.
 *
 * <p>The {@link #Model2VecEmbeddingProvider(Path)} constructor loads the model eagerly. The
 * no-argument ServiceLoader constructor resolves the directory on first use from the
 * {@value #PATH_PROPERTY} system property, falling back to the {@value #PATH_ENVIRONMENT_VARIABLE}
 * environment variable, so discovery through
 * {@link ai.pipestream.proto.embeddings.EmbeddingProviders} never fails on an unconfigured
 * provider that is not actually used.
 *
 * <p>The loaded model is immutable and this provider is safe for concurrent use.
 */
public final class Model2VecEmbeddingProvider implements EmbeddingProvider {

    /** The id this provider registers under: {@value}. */
    public static final String PROVIDER_ID = "model2vec";

    /** System property naming the model directory for the ServiceLoader constructor: {@value}. */
    public static final String PATH_PROPERTY = "protomolt.embeddings.model2vec.path";

    /** Environment variable consulted when {@link #PATH_PROPERTY} is unset: {@value}. */
    public static final String PATH_ENVIRONMENT_VARIABLE = "PROTOMOLT_MODEL2VEC_PATH";

    private final Object lock = new Object();
    private volatile StaticEmbeddingModel model;

    /**
     * ServiceLoader constructor. The model directory is resolved and loaded on the first
     * {@link #dimension()} or {@link #embed(String)} call, from the {@value #PATH_PROPERTY}
     * system property or, when that is unset, the {@value #PATH_ENVIRONMENT_VARIABLE}
     * environment variable.
     */
    public Model2VecEmbeddingProvider() {
    }

    /**
     * Loads the model in {@code modelDirectory} eagerly.
     *
     * @param modelDirectory Model2Vec model directory in the layout
     *        {@link StaticEmbeddingModel#load(Path)} accepts
     * @throws IllegalArgumentException when the directory is missing files or malformed
     * @throws UncheckedIOException when reading a model file fails
     */
    public Model2VecEmbeddingProvider(Path modelDirectory) {
        Objects.requireNonNull(modelDirectory, "modelDirectory");
        this.model = load(modelDirectory);
    }

    @Override
    public String providerId() {
        return PROVIDER_ID;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException when this provider came from the ServiceLoader constructor
     *         and neither configuration knob names a model directory
     */
    @Override
    public int dimension() {
        return model().dimension();
    }

    /**
     * {@inheritDoc}
     *
     * <p>A text with no in-vocabulary tokens yields a zero vector.
     *
     * @throws IllegalStateException when this provider came from the ServiceLoader constructor
     *         and neither configuration knob names a model directory
     */
    @Override
    public float[] embed(String text) {
        return model().embed(text);
    }

    private StaticEmbeddingModel model() {
        StaticEmbeddingModel loaded = model;
        if (loaded != null) {
            return loaded;
        }
        synchronized (lock) {
            if (model == null) {
                model = load(configuredDirectory());
            }
            return model;
        }
    }

    private static Path configuredDirectory() {
        String path = System.getProperty(PATH_PROPERTY);
        if (path == null) {
            path = System.getenv(PATH_ENVIRONMENT_VARIABLE);
        }
        if (path == null) {
            throw new IllegalStateException("No Model2Vec model directory configured; set the '"
                    + PATH_PROPERTY + "' system property or the " + PATH_ENVIRONMENT_VARIABLE
                    + " environment variable to a Model2Vec model directory");
        }
        return Path.of(path);
    }

    private static StaticEmbeddingModel load(Path modelDirectory) {
        try {
            return StaticEmbeddingModel.load(modelDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot load Model2Vec model from " + modelDirectory, e);
        }
    }
}
