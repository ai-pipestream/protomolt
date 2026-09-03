package ai.protomolt.proto.receipt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.KeyPair;
import java.security.Signature;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class RecordKeysTest {

    /** RFC 8032 section 7.1 TEST 1: empty message, known signature. */
    @Test
    void matchesTheRfc8032TestVector() throws Exception {
        Signature signer = Signature.getInstance("Ed25519");
        signer.initSign(RecordKeys.privateKey(ConformanceCorpus.SEED));
        signer.update(new byte[0]);
        assertThat(HexFormat.of().formatHex(signer.sign())).isEqualTo(
                "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb882"
                        + "1590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b");

        Signature verifier = Signature.getInstance("Ed25519");
        verifier.initVerify(RecordKeys.publicKey(ConformanceCorpus.PUBLIC_KEY));
        verifier.update(new byte[0]);
        assertThat(verifier.verify(HexFormat.of().parseHex(
                "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb882"
                        + "1590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b")))
                .isTrue();
    }

    @Test
    void rawPublicKeyRoundTrips() {
        assertThat(RecordKeys.rawPublicKey(
                RecordKeys.publicKey(ConformanceCorpus.PUBLIC_KEY)))
                .isEqualTo(ConformanceCorpus.PUBLIC_KEY);
        assertThat(RecordKeys.rawPublicKey(
                RecordKeys.publicKey(ConformanceCorpus.SECOND_PUBLIC_KEY)))
                .isEqualTo(ConformanceCorpus.SECOND_PUBLIC_KEY);
    }

    @Test
    void generatedPairsRoundTripAndInteroperate() throws Exception {
        KeyPair pair = RecordKeys.generate();
        byte[] raw = RecordKeys.rawPublicKey(pair.getPublic());
        assertThat(raw).hasSize(32);

        byte[] message = "the record".getBytes();
        Signature signer = Signature.getInstance("Ed25519");
        signer.initSign(pair.getPrivate());
        signer.update(message);
        byte[] signature = signer.sign();

        Signature verifier = Signature.getInstance("Ed25519");
        verifier.initVerify(RecordKeys.publicKey(raw));
        verifier.update(message);
        assertThat(verifier.verify(signature)).isTrue();
    }

    /**
     * Both RFC 8032 sample keys happen to have an even x coordinate, so
     * this test generates until both parities are seen: the sign bit must
     * survive the encode/decode round trip for each.
     */
    @Test
    void bothPointParitiesRoundTrip() {
        boolean sawOdd = false;
        boolean sawEven = false;
        for (int i = 0; i < 256 && !(sawOdd && sawEven); i++) {
            byte[] raw = RecordKeys.rawPublicKey(RecordKeys.generate().getPublic());
            boolean odd = (raw[31] & 0x80) != 0;
            sawOdd |= odd;
            sawEven |= !odd;
            assertThat(RecordKeys.rawPublicKey(RecordKeys.publicKey(raw)))
                    .as("round trip of a key with xOdd=%s", odd)
                    .isEqualTo(raw);
        }
        assertThat(sawOdd).isTrue();
        assertThat(sawEven).isTrue();
    }

    @Test
    void wrongLengthsRefuse() {
        assertThatThrownBy(() -> RecordKeys.privateKey(new byte[31]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("32 bytes");
        assertThatThrownBy(() -> RecordKeys.publicKey(new byte[33]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("32 bytes");
        assertThatThrownBy(() -> RecordKeys.privateKey(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RecordKeys.publicKey(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
