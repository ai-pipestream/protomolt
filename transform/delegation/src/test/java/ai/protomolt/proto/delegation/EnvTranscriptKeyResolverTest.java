package ai.protomolt.proto.delegation;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnvTranscriptKeyResolverTest {

    private static final String REFERENCE = "env:VERY_PRIVATE_KEY_NAME";

    @Test
    void resolvesBase64EncodedAes256Key() {
        byte[] bytes = "0123456789abcdef0123456789abcdef"
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        EnvRepositoryStateKeyResolver resolver = new EnvRepositoryStateKeyResolver(
                ignored -> Base64.getEncoder().encodeToString(bytes));

        assertThat(resolver.resolve(REFERENCE).getEncoded()).isEqualTo(bytes);
        assertThat(resolver.resolve(REFERENCE).getAlgorithm()).isEqualTo("AES");
    }

    @Test
    void failuresNeverEchoReferenceOrEnvironmentValue() {
        String secret = "plain-text-secret-that-is-not-base64!";
        EnvRepositoryStateKeyResolver malformed =
                new EnvRepositoryStateKeyResolver(ignored -> secret);
        EnvRepositoryStateKeyResolver missing =
                new EnvRepositoryStateKeyResolver(ignored -> null);

        assertThatThrownBy(() -> malformed.resolve(REFERENCE))
                .hasMessageNotContaining(REFERENCE)
                .hasMessageNotContaining(secret);
        assertThatThrownBy(() -> missing.resolve(REFERENCE))
                .hasMessageNotContaining(REFERENCE);
        assertThatThrownBy(() -> missing.resolve("vault:SECRET"))
                .hasMessageNotContaining("vault:SECRET");
    }

    @Test
    void rejectsDecodedKeysThatAreNotExactly256Bits() {
        String shortKey = Base64.getEncoder().encodeToString(new byte[16]);
        EnvRepositoryStateKeyResolver resolver =
                new EnvRepositoryStateKeyResolver(ignored -> shortKey);

        assertThatThrownBy(() -> resolver.resolve(REFERENCE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exactly 32 bytes")
                .hasMessageNotContaining(shortKey);
    }
}
