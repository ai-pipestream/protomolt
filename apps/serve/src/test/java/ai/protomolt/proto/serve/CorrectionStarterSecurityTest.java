package ai.protomolt.proto.serve;

import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CorrectionStarterSecurityTest {

    @Test
    void starterRefusesAnOpenApiAndConsoleBeforeStartingListeners() {
        CorrectionStarter starter = CorrectionStarter.fromEnvironment(Map.of(
                "PROTOMOLT_CORRECTION_TARGET", "localhost:9090",
                "PROTOMOLT_CORRECTION_API_TOKEN", "private-test-token"));
        ProtoMoltServe.Options open = new ProtoMoltServe.Options("127.0.0.1", 0, 0,
                null, 0);
        assertThatThrownBy(() -> ProtoMoltServe.start(open, starter))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires API and console authentication");

        ProtoMoltServe.Options apiOnly = new ProtoMoltServe.Options("127.0.0.1", 0, 0,
                null, 0, "operator-test-token");
        assertThatThrownBy(() -> ProtoMoltServe.start(apiOnly, starter))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires API and console authentication");
    }
}
