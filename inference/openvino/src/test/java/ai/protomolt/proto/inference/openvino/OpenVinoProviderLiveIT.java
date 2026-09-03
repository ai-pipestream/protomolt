package ai.protomolt.proto.inference.openvino;

import ai.protomolt.proto.inference.v1.ChatTurn;
import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.GenerateResponse;
import ai.protomolt.proto.inference.v1.ModelCapabilities;
import ai.protomolt.proto.inference.v1.ModelEntry;
import ai.protomolt.proto.inference.v1.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live smoke test against a real OVMS endpoint. Opt-in: set
 * INFERENCE_LIVE_OVMS_URL (e.g. http://krick-1:9300) and
 * INFERENCE_LIVE_OVMS_MODEL (e.g. OpenVINO/gemma-3-12b-it-int4-ov).
 */
@EnabledIfEnvironmentVariable(named = "INFERENCE_LIVE_OVMS_URL", matches = ".+")
class OpenVinoProviderLiveIT {

    private final OpenVinoProvider provider = new OpenVinoProvider();

    @Test
    void liveGenerate() {
        String endpoint = System.getenv("INFERENCE_LIVE_OVMS_URL");
        String backendModel = System.getenv().getOrDefault("INFERENCE_LIVE_OVMS_MODEL",
                "OpenVINO/gemma-3-12b-it-int4-ov");
        ModelEntry model = ModelEntry.newBuilder()
                .setId("live")
                .setProvider("openvino")
                .setEndpoint(endpoint)
                .setBackendModel(backendModel)
                .setCapabilities(ModelCapabilities.newBuilder()
                        .setMaxContextTokens(8192).setStreaming(true).build())
                .build();
        GenerateResponse response = provider.generate(model,
                GenerateRequest.newBuilder()
                        .setModel("live")
                        .addMessages(ChatTurn.newBuilder()
                                .setRole(Role.ROLE_USER)
                                .setContent("Reply with exactly: OK"))
                        .setMaxOutputTokens(16)
                        .build());
        assertThat(response.getText()).isNotBlank();
        assertThat(response.getUsage().getPromptTokens()).isPositive();
    }
}
