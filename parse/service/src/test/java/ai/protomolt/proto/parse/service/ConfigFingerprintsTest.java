package ai.protomolt.proto.parse.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

/**
 * Pins the fingerprint function: stable for equal inputs (whatever their
 * construction order), different for a changed rule identity OR a changed
 * parser configuration — both invalidate a stored result.
 */
class ConfigFingerprintsTest {

    private static Struct config(String firstKey, String secondKey) {
        return Struct.newBuilder()
                .putFields(firstKey, Value.newBuilder().setBoolValue(true).build())
                .putFields(secondKey, Value.newBuilder().setNumberValue(300).build())
                .build();
    }

    @Test
    void equalInputsFingerprintEqualRegardlessOfInsertionOrder() {
        Struct forward = config("ocr", "dpi");
        Struct backward = Struct.newBuilder()
                .putFields("dpi", Value.newBuilder().setNumberValue(300).build())
                .putFields("ocr", Value.newBuilder().setBoolValue(true).build())
                .build();
        String a = ConfigFingerprints.fingerprint("r-1", forward);
        String b = ConfigFingerprints.fingerprint("r-1", backward);
        assertThat(a).isEqualTo(b);
        assertThat(a).hasSize(64).matches("[0-9a-f]{64}");
    }

    @Test
    void aChangedRuleIdChangesTheFingerprint() {
        Struct config = config("ocr", "dpi");
        assertThat(ConfigFingerprints.fingerprint("r-1", config))
                .isNotEqualTo(ConfigFingerprints.fingerprint("r-2", config));
    }

    @Test
    void aChangedConfigChangesTheFingerprint() {
        assertThat(ConfigFingerprints.fingerprint("r-1", config("ocr", "dpi")))
                .isNotEqualTo(ConfigFingerprints.fingerprint("r-1", config("ocr", "dpi2")));
    }

    @Test
    void emptyRuleIdAndEmptyConfigAreValidInputs() {
        String override = ConfigFingerprints.fingerprint("", Struct.getDefaultInstance());
        assertThat(override).hasSize(64);
        assertThat(ConfigFingerprints.fingerprint("", Struct.getDefaultInstance()))
                .isEqualTo(override);
    }
}
