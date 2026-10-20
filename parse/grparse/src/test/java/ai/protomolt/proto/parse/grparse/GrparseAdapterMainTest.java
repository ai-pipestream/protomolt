package ai.protomolt.proto.parse.grparse;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Startup configuration validation: the previews flag is strictly true/false and the
 * port must be in range; anything else is refused by variable name at boot instead of
 * silently booting with defaults the operator never chose.
 */
class GrparseAdapterMainTest {

    @Test
    void previewsAcceptsTrueAndFalseCaseInsensitively() {
        assertThat(GrparseAdapterMain.emitsPreviews("true")).isTrue();
        assertThat(GrparseAdapterMain.emitsPreviews("TRUE")).isTrue();
        assertThat(GrparseAdapterMain.emitsPreviews(" True ")).isTrue();
        assertThat(GrparseAdapterMain.emitsPreviews("false")).isFalse();
        assertThat(GrparseAdapterMain.emitsPreviews("FALSE")).isFalse();
    }

    @Test
    void previewsUnsetOrBlankIsFalse() {
        assertThat(GrparseAdapterMain.emitsPreviews(null)).isFalse();
        assertThat(GrparseAdapterMain.emitsPreviews("  ")).isFalse();
    }

    @Test
    void previewsRefusesMalformedValuesByNameWithoutEchoingThem() {
        for (String bad : new String[]{"yes", "1", "on", "enabled"}) {
            assertThatThrownBy(() -> GrparseAdapterMain.emitsPreviews(bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(GrparseAdapterMain.ENV_EMITS_PREVIEWS)
                    .hasMessageContaining("true or false")
                    .hasMessageNotContaining("'" + bad + "'");
        }
    }

    @Test
    void portDefaultsAndTrims() {
        assertThat(GrparseAdapterMain.port(null)).isEqualTo(GrparseAdapterMain.DEFAULT_PORT);
        assertThat(GrparseAdapterMain.port("  ")).isEqualTo(GrparseAdapterMain.DEFAULT_PORT);
        assertThat(GrparseAdapterMain.port(" 8080 ")).isEqualTo(8080);
        assertThat(GrparseAdapterMain.port("0")).isZero();
        assertThat(GrparseAdapterMain.port("65535")).isEqualTo(65535);
    }

    @Test
    void portRefusesMalformedAndOutOfRangeValuesByName() {
        for (String bad : new String[]{"http", "-1", "65536", "999999999999"}) {
            assertThatThrownBy(() -> GrparseAdapterMain.port(bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(GrparseAdapterMain.ENV_PORT)
                    .hasMessageContaining("0..65535");
        }
    }
}
