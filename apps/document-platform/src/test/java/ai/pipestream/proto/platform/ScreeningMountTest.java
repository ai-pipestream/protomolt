package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.types.ScreeningConfig;
import ai.pipestream.proto.types.ScreeningPolicy;
import java.io.UncheckedIOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The platform's screening mount plumbing: the strict env read and the
 * model-reference resolution. A reference the host cannot resolve throws,
 * which the config lane logs without applying, so the service stays on the
 * previous mount or the fail-closed absence.
 */
class ScreeningMountTest {

    private static ScreeningConfig config(String modelRef) {
        return ScreeningConfig.newBuilder()
                .setSensitivityClass("screened")
                .setModelRef(modelRef)
                .setThreshold(0.5)
                .setPolicy(ScreeningPolicy.SCREENING_POLICY_MASK)
                .build();
    }

    @Test
    void onlyFileReferencesResolveInThisSlice() {
        assertThatThrownBy(() -> DocumentPlatform.mountScreener(config("s3:models/ner")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file:");
    }

    @Test
    void aMissingModelFileRefusesTheMount() {
        assertThatThrownBy(() -> DocumentPlatform.mountScreener(
                config("file:/does/not/exist.bin")))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("screening model");
    }

    @Test
    void theEnvReadIsAbsentOrOneTrimmedName() {
        assertThat(DocumentPlatform.screeningFromEnvironment(Map.of())).isNull();
        assertThat(DocumentPlatform.screeningFromEnvironment(
                Map.of(DocumentPlatformConfig.ENV_SCREENING, "  "))).isNull();
        assertThat(DocumentPlatform.screeningFromEnvironment(
                Map.of(DocumentPlatformConfig.ENV_SCREENING, " pii-mount ")))
                .isEqualTo("pii-mount");
    }
}
