package ai.protomolt.proto.descriptors;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class MessageFingerprintsTest {
    @Test
    void preservesOriginalReceiptFingerprintAcrossMapInsertionOrders() {
        var number = Value.newBuilder().setNumberValue(3).build();
        var text = Value.newBuilder().setStringValue("last").build();
        var original = Struct.newBuilder().putFields("z", text).putFields("a", number).build();
        var reversed = Struct.newBuilder().putFields("a", number).putFields("z", text).build();
        // Captured from WorkRecords before extraction, not calculated by the new helper.
        assertThat(HexFormat.of().formatHex(MessageFingerprints.deterministicBytes(original)))
                .isEqualTo("0a0e0a016112091100000000000008400a0b0a017a12061a046c617374");
        assertThat(MessageFingerprints.fingerprint(original))
                .isEqualTo("b7e36e4f881f95229c7593bf40e5cddb96a2ebf364aae0167da35140ff6be7f6");
        assertThat(MessageFingerprints.fingerprint(reversed))
                .isEqualTo(MessageFingerprints.fingerprint(original));
        assertThat(MessageFingerprints.fingerprint(original.toBuilder()
                .putFields("a", Value.newBuilder().setNumberValue(4).build()).build()))
                .isNotEqualTo(MessageFingerprints.fingerprint(original));
    }
}
