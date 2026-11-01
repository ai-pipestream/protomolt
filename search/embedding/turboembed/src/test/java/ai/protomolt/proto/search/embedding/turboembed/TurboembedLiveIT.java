package ai.protomolt.proto.search.embedding.turboembed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Live server test: configured model must have a query prefix, and valid reference text support. */
class TurboembedLiveIT {
    @Test
    void realModelAnswersBothPurposesAndRefusesWrongManifest() {
        try (var provider = new TurboembedEmbeddingProvider()) {
            float[] document = provider.embedDocument("The cat sat on the mat.");
            float[] query = provider.embedQuery("The cat sat on the mat.");
            assertThat(document).hasSize(provider.dimension());
            assertThat(query).hasSize(provider.dimension());
            assertThat(query).isNotEqualTo(document);
            List<float[]> batch = provider.embedAll(List.of("The cat sat on the mat.", "A dog runs."));
            assertThat(batch).hasSize(2);
            assertThat(batch.getFirst()).hasSize(provider.dimension());
        }
        var wrong = new HashMap<>(System.getenv());
        String actual = wrong.get("PROTOMOLT_TURBOEMBED_MANIFEST_SHA256");
        wrong.put("PROTOMOLT_TURBOEMBED_MANIFEST_SHA256", actual.equals("0".repeat(64)) ? "1".repeat(64) : "0".repeat(64));
        try (var provider = new TurboembedEmbeddingProvider(wrong)) {
            assertThatThrownBy(() -> provider.embedDocument("The cat sat on the mat."))
                    .hasMessageContaining("model revision or manifest");
        }
    }
}
