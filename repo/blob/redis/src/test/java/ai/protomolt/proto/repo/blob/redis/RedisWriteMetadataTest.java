package ai.protomolt.proto.repo.blob.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Metadata is encoded and bounded before anything reaches Redis. The 64 KiB budget counts UTF-8
 * bytes across the content type, every name and every value together, so the bound is checked
 * with multi-byte text and not only with ASCII.
 */
class RedisWriteMetadataTest {

    private static final int BUDGET = 64 * 1024;

    @Test
    void contentTypeAndEntriesAreEncodedAsUtf8NameValuePairsInIterationOrder() {
        var metadata = new LinkedHashMap<String, String>();
        metadata.put("title", "café");
        metadata.put("empty", "");
        var encoded = new RedisWriteMetadata("text/plain", metadata);

        assertThat(encoded.contentType).isEqualTo("text/plain".getBytes(StandardCharsets.UTF_8));
        assertThat(encoded.attributes).hasSize(4);
        assertThat(new String(encoded.attributes.get(0), StandardCharsets.UTF_8)).isEqualTo("title");
        assertThat(encoded.attributes.get(1)).isEqualTo(new byte[] {'c', 'a', 'f', (byte) 0xC3, (byte) 0xA9});
        assertThat(encoded.attributes.get(2)).isEqualTo("empty".getBytes(StandardCharsets.UTF_8));
        assertThat(encoded.attributes.get(3)).isEmpty();
        assertThatThrownBy(() -> encoded.attributes.add(new byte[0]))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void nullContentTypeAndNullMetadataEncodeAsEmpty() {
        var encoded = new RedisWriteMetadata(null, null);
        assertThat(encoded.contentType).isEmpty();
        assertThat(encoded.attributes).isEmpty();
    }

    @Test
    void nullNamesAndValuesAreRefusedByName() {
        var nullValue = new HashMap<String, String>();
        nullValue.put("k", null);
        assertThatThrownBy(() -> new RedisWriteMetadata("t", nullValue))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Redis metadata cannot contain null names or values");
        var nullName = new HashMap<String, String>();
        nullName.put(null, "v");
        assertThatThrownBy(() -> new RedisWriteMetadata("t", nullName))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Redis metadata cannot contain null names or values");
    }

    @Test
    void exactly256EntriesAreAcceptedAnd257AreRefused() {
        var metadata = new HashMap<String, String>();
        for (int i = 0; i < 256; i++) {
            metadata.put("k" + i, "");
        }
        assertThat(new RedisWriteMetadata("", metadata).attributes).hasSize(512);
        metadata.put("k256", "");
        assertThatThrownBy(() -> new RedisWriteMetadata("", metadata))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Redis metadata exceeds 256 entries");
    }

    @Test
    void theByteBudgetIsSharedByTheContentTypeNamesAndValues() {
        String type = "a/b";
        String name = "n";
        String fits = "x".repeat(BUDGET - type.length() - name.length());
        assertThat(new RedisWriteMetadata(type, Map.of(name, fits)).attributes.get(1)).hasSize(fits.length());
        assertThatThrownBy(() -> new RedisWriteMetadata(type, Map.of(name, fits + "x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Redis metadata exceeds 64 KiB UTF-8");
        assertThatThrownBy(() -> new RedisWriteMetadata("x".repeat(BUDGET + 1), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Redis metadata exceeds 64 KiB UTF-8");
    }

    @Test
    void multiByteTextIsBoundedByEncodedBytesNotCharacters() {
        // 32768 two-byte characters are exactly the budget; one more passes the character
        // pre-check yet overflows once encoded.
        assertThat(new RedisWriteMetadata("é".repeat(BUDGET / 2), null).contentType).hasSize(BUDGET);
        String overflow = "é".repeat(BUDGET / 2 + 1);
        assertThat(overflow.length()).isLessThan(BUDGET);
        assertThatThrownBy(() -> new RedisWriteMetadata(overflow, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Redis metadata exceeds 64 KiB UTF-8");
    }

    @Test
    void loneSurrogatesAreRefusedWithTheCodingFailureAsCause() {
        assertThatThrownBy(() -> new RedisWriteMetadata("t", Map.of("k", "a\uD800b")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Redis metadata contains malformed UTF-16")
                .hasCauseInstanceOf(CharacterCodingException.class);
        assertThatThrownBy(() -> new RedisWriteMetadata("\uDC00", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Redis metadata contains malformed UTF-16");
        var pair = new RedisWriteMetadata("😀", null);
        assertThat(pair.contentType).hasSize(4);
    }
}
