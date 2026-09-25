package ai.protomolt.proto.correction;

import ai.protomolt.proto.inference.spi.InferenceException;
import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.ModelEntry;
import ai.protomolt.proto.inference.v1.StructuredOutputConstraint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KimiCliProviderTest {
    @TempDir Path temp;

    private static ModelEntry model() {
        return ModelEntry.newBuilder().setId("kimi").setBackendModel("kimi-code/k3").build();
    }

    private KimiCliProvider provider(Path workspace) {
        // This executable deliberately does not exist. Rejected requests must fail before launch.
        return new KimiCliProvider(temp.resolve("no-such-kimi-executable").toString(), workspace);
    }

    @Test
    void nativeOutputConstraintIsRejectedBeforeCreatingSessionOrLaunchingCli() {
        Path workspace = temp.resolve("native");
        GenerateRequest request = GenerateRequest.newBuilder().setModel("kimi")
                .setStructuredOutput(StructuredOutputConstraint.newBuilder()
                        .setName("output").setJsonSchema("{}"))
                .build();

        assertThatThrownBy(() -> provider(workspace).generate(model(), request))
                .isInstanceOf(InferenceException.class)
                .hasMessageContaining("does not support native output constraints");
        org.assertj.core.api.Assertions.assertThat(Files.exists(workspace)).isFalse();
    }

    @Test
    void unsupportedSamplingAndTokenControlsAreRejectedBeforeLaunchingCli() {
        for (GenerateRequest request : new GenerateRequest[] {
                GenerateRequest.newBuilder().setModel("kimi").setTemperature(0.2).build(),
                GenerateRequest.newBuilder().setModel("kimi").setTopP(0.8).build(),
                GenerateRequest.newBuilder().setModel("kimi").setMaxOutputTokens(100).build()
        }) {
            Path workspace = temp.resolve("controls-" + request.getMaxOutputTokens()
                    + "-" + request.getTemperature() + "-" + request.getTopP());
            assertThatThrownBy(() -> provider(workspace).generate(model(), request))
                    .isInstanceOf(InferenceException.class)
                    .hasMessageContaining("does not support sampling or token-limit controls");
            org.assertj.core.api.Assertions.assertThat(Files.exists(workspace)).isFalse();
        }
    }
}
