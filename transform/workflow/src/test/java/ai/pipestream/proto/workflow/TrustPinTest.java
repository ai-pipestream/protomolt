package ai.pipestream.proto.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.receipt.KeyState;
import ai.pipestream.proto.receipt.SignatureAlgorithm;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.TrustedIssuer;
import ai.pipestream.proto.receipt.TrustedKey;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TrustPinTest {

    private static TrustSnapshot snapshot() {
        return TrustSnapshot.newBuilder()
                .addIssuers(TrustedIssuer.newBuilder()
                        .setIssuer("records.protomolt.dev")
                        .addKeys(TrustedKey.newBuilder()
                                .setKeyId("key-2026")
                                .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                                .setPublicKey(ByteString.copyFrom(new byte[32]))
                                .setState(KeyState.KEY_STATE_ACTIVE))
                        .addSubjectKinds("workflow-run"))
                .build();
    }

    @Test
    void unsetOrBlankMeansNoPin() {
        assertThat(TrustPin.fromEnvironment(Map.of())).isNull();
        assertThat(TrustPin.fromEnvironment(
                Map.of(TrustPin.ENV_TRUST_SNAPSHOT, "  "))).isNull();
    }

    @Test
    void aPinnedFileLoadsAndVerifies(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("trust.json");
        Files.writeString(file, JsonFormat.printer().print(snapshot()));
        TrustPin pin = TrustPin.fromEnvironment(
                Map.of(TrustPin.ENV_TRUST_SNAPSHOT, file.toString()));
        assertThat(pin.snapshot()).isEqualTo(snapshot());
    }

    @Test
    void aMissingFileRefusesAtStartupNamingThePath(@TempDir Path dir) {
        Path file = dir.resolve("absent.json");
        assertThatThrownBy(() -> TrustPin.fromEnvironment(
                Map.of(TrustPin.ENV_TRUST_SNAPSHOT, file.toString())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("absent.json");
    }

    @Test
    void anInvalidDocumentRefusesLoudly(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("trust.binpb");
        Files.write(file, TrustSnapshot.getDefaultInstance().toByteArray());
        assertThatThrownBy(() -> TrustPin.fromEnvironment(
                Map.of(TrustPin.ENV_TRUST_SNAPSHOT, file.toString())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid");
    }
}
