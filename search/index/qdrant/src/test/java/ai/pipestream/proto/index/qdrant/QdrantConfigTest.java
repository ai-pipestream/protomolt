package ai.pipestream.proto.index.qdrant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

class QdrantConfigTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty(QdrantConfig.TARGET_PROPERTY);
        System.clearProperty(QdrantConfig.COLLECTION_PROPERTY);
        System.clearProperty(QdrantConfig.API_KEY_PROPERTY);
        System.clearProperty(QdrantConfig.USE_TLS_PROPERTY);
    }

    @Test
    void defaultsWhenNothingIsConfigured() {
        assumeThat(System.getenv(QdrantConfig.TARGET_ENVIRONMENT_VARIABLE)).isNull();
        assumeThat(System.getenv(QdrantConfig.COLLECTION_ENVIRONMENT_VARIABLE)).isNull();
        assumeThat(System.getenv(QdrantConfig.API_KEY_ENVIRONMENT_VARIABLE)).isNull();
        assumeThat(System.getenv(QdrantConfig.USE_TLS_ENVIRONMENT_VARIABLE)).isNull();

        QdrantConfig config = QdrantConfig.fromEnvironment();

        assertThat(config.target()).isEqualTo(QdrantConfig.DEFAULT_TARGET);
        assertThat(config.collection()).isEqualTo(QdrantConfig.DEFAULT_COLLECTION);
        assertThat(config.hasApiKey()).isFalse();
        assertThat(config.useTls()).isFalse();
    }

    @Test
    void systemPropertiesWin() {
        System.setProperty(QdrantConfig.TARGET_PROPERTY, "qdrant.internal:6334");
        System.setProperty(QdrantConfig.COLLECTION_PROPERTY, "chunks-v2");
        System.setProperty(QdrantConfig.API_KEY_PROPERTY, "secret");
        System.setProperty(QdrantConfig.USE_TLS_PROPERTY, "false");

        QdrantConfig config = QdrantConfig.fromEnvironment();

        assertThat(config.target()).isEqualTo("qdrant.internal:6334");
        assertThat(config.collection()).isEqualTo("chunks-v2");
        assertThat(config.apiKey()).isEqualTo("secret");
        // An explicit use-tls beats the api-key default.
        assertThat(config.useTls()).isFalse();
    }

    @Test
    void apiKeyDefaultsTlsOn() {
        assumeThat(System.getenv(QdrantConfig.USE_TLS_ENVIRONMENT_VARIABLE)).isNull();
        System.setProperty(QdrantConfig.API_KEY_PROPERTY, "secret");

        assertThat(QdrantConfig.fromEnvironment().useTls()).isTrue();
    }

    @Test
    void validation() {
        assertThatThrownBy(() -> new QdrantConfig(" ", "c", "", false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QdrantConfig("t", "", "", false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(QdrantConfig.plaintext("localhost:6334", "c").useTls()).isFalse();
        assertThat(new QdrantConfig("t", "c", null, false).apiKey()).isEmpty();
    }
}
