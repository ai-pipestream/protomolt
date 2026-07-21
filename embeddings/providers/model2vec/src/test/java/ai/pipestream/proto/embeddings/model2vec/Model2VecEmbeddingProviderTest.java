package ai.pipestream.proto.embeddings.model2vec;

import ai.pipestream.proto.embeddings.EmbeddingProvider;
import ai.pipestream.proto.embeddings.EmbeddingProviders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

class Model2VecEmbeddingProviderTest {

    @TempDir
    static Path modelDirectory;

    private String savedPathProperty;

    @BeforeAll
    static void writeModel() throws IOException {
        Model2VecTestModel.write(modelDirectory);
    }

    @BeforeEach
    void savePathProperty() {
        savedPathProperty = System.clearProperty(Model2VecEmbeddingProvider.PATH_PROPERTY);
    }

    @AfterEach
    void restorePathProperty() {
        if (savedPathProperty == null) {
            System.clearProperty(Model2VecEmbeddingProvider.PATH_PROPERTY);
        } else {
            System.setProperty(Model2VecEmbeddingProvider.PATH_PROPERTY, savedPathProperty);
        }
    }

    @Test
    void loadsTheModelDirectoryAndReportsItsDimension() {
        Model2VecEmbeddingProvider provider = new Model2VecEmbeddingProvider(modelDirectory);

        assertThat(provider.providerId()).isEqualTo("model2vec");
        assertThat(provider.dimension()).isEqualTo(Model2VecTestModel.DIMENSION);
        assertThat(provider.embed("dog")).hasSize(Model2VecTestModel.DIMENSION);
    }

    @Test
    void dogIsCloserToPuppyThanToCar() {
        Model2VecEmbeddingProvider provider = new Model2VecEmbeddingProvider(modelDirectory);

        float[] dog = provider.embed("dog");
        assertThat(cosine(dog, provider.embed("puppy")))
                .isGreaterThan(cosine(dog, provider.embed("car")));
    }

    @Test
    void serviceLoaderFindsTheProviderByIdAndResolvesThePathProperty() {
        System.setProperty(Model2VecEmbeddingProvider.PATH_PROPERTY, modelDirectory.toString());

        EmbeddingProvider provider = EmbeddingProviders.byId("model2vec");

        assertThat(provider).isInstanceOf(Model2VecEmbeddingProvider.class);
        assertThat(provider.dimension()).isEqualTo(Model2VecTestModel.DIMENSION);
    }

    @Test
    void enumerationSucceedsWithoutConfiguration() {
        // The no-arg constructor must not touch the knobs, or an unconfigured provider would
        // break discovery of every other provider on the classpath.
        assertThat(EmbeddingProviders.all()).containsKey("model2vec");
    }

    @Test
    void firstUseWithoutConfigurationNamesBothKnobs() {
        assumeThat(System.getenv(Model2VecEmbeddingProvider.PATH_ENVIRONMENT_VARIABLE)).isNull();

        Model2VecEmbeddingProvider provider = new Model2VecEmbeddingProvider();

        assertThatThrownBy(provider::dimension)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(Model2VecEmbeddingProvider.PATH_PROPERTY)
                .hasMessageContaining(Model2VecEmbeddingProvider.PATH_ENVIRONMENT_VARIABLE);
    }

    private static double cosine(float[] a, float[] b) {
        double dot = 0;
        double normA = 0;
        double normB = 0;
        for (int d = 0; d < a.length; d++) {
            dot += (double) a[d] * b[d];
            normA += (double) a[d] * a[d];
            normB += (double) b[d] * b[d];
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }
}
