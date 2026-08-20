package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.validate.spi.PostalCodeCatalog;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The strict read of DOCUMENT_PLATFORM_POSTAL_CODES and the swappable catalog's stance:
 * empty (every region unchecked, the pack's per-region opt-in) until the mounts swap in.
 */
class PlatformPostalCodesTest {

    @Test
    void absentOrBlankMeansNoFollow() {
        assertThat(DocumentPlatform.postalCodesFromEnvironment(Map.of())).isFalse();
        assertThat(DocumentPlatform.postalCodesFromEnvironment(
                Map.of(DocumentPlatformConfig.ENV_POSTAL_CODES, "  "))).isFalse();
    }

    @Test
    void trueTurnsTheFollowOn() {
        assertThat(DocumentPlatform.postalCodesFromEnvironment(
                Map.of(DocumentPlatformConfig.ENV_POSTAL_CODES, "true"))).isTrue();
        assertThat(DocumentPlatform.postalCodesFromEnvironment(
                Map.of(DocumentPlatformConfig.ENV_POSTAL_CODES, " true "))).isTrue();
    }

    @Test
    void anythingElseRefusesByName() {
        assertThatThrownBy(() -> DocumentPlatform.postalCodesFromEnvironment(
                Map.of(DocumentPlatformConfig.ENV_POSTAL_CODES, "yes")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DocumentPlatformConfig.ENV_POSTAL_CODES)
                .hasMessageContaining("yes");
    }

    @Test
    void theSwappableCatalogIsUncheckedUntilThePackApplies() {
        SwappablePostalCodes catalog = new SwappablePostalCodes();
        assertThat(catalog.region("CH")).isEmpty();

        catalog.swap(regionCode -> "CH".equals(regionCode)
                ? java.util.Optional.of(new PostalCodeCatalog.Mounted(
                        "CH", "v7", List.of("NNNN")))
                : java.util.Optional.empty());
        assertThat(catalog.region("CH")).isPresent();
        assertThat(catalog.region("DE")).isEmpty();

        assertThatThrownBy(() -> catalog.swap(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
