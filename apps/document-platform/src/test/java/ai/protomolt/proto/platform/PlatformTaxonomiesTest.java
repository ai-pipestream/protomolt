package ai.protomolt.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The strict read of DOCUMENT_PLATFORM_TAXONOMIES: a comma list of names,
 * trimmed and de-duplicated in first-seen order; absent means no follow
 * and no document gate; set-but-empty is a contradiction refused by name.
 */
class PlatformTaxonomiesTest {

    @Test
    void absentMeansNoFollow() {
        assertThat(PlatformEnvironment.taxonomiesFromEnvironment(Map.of())).isEmpty();
        assertThat(PlatformEnvironment.taxonomiesFromEnvironment(
                Map.of(DocumentPlatformConfig.ENV_TAXONOMIES, ""))).isEmpty();
    }

    @Test
    void namesParseTrimmedAndDeduplicatedInOrder() {
        assertThat(PlatformEnvironment.taxonomiesFromEnvironment(Map.of(
                DocumentPlatformConfig.ENV_TAXONOMIES,
                " products, regions ,products,, topics ")))
                .containsExactly("products", "regions", "topics");
    }

    @Test
    void setButNamingNothingRefusesByName() {
        assertThatThrownBy(() -> PlatformEnvironment.taxonomiesFromEnvironment(Map.of(
                DocumentPlatformConfig.ENV_TAXONOMIES, " , ,")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DocumentPlatformConfig.ENV_TAXONOMIES);
    }
}
