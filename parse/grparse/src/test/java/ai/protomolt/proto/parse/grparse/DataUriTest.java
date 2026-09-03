package ai.protomolt.proto.parse.grparse;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Base64;
import org.junit.jupiter.api.Test;

class DataUriTest {

    @Test
    void parsesTheBase64FormTheDoclingModelEmbeds() {
        byte[] bytes = {1, 2, 3, 4};
        DataUri parsed = DataUri.parse(
                "data:image/webp;base64," + Base64.getEncoder().encodeToString(bytes));
        assertThat(parsed.mimeType()).isEqualTo("image/webp");
        assertThat(parsed.data().toByteArray()).isEqualTo(bytes);
    }

    @Test
    void everythingElseIsNull() {
        assertThat(DataUri.parse(null)).isNull();
        assertThat(DataUri.parse("")).isNull();
        assertThat(DataUri.parse("s3://bucket/key.png")).isNull();
        assertThat(DataUri.parse("data:image/png,unencoded")).isNull();
        assertThat(DataUri.parse("data:;base64,AAAA")).isNull();
        assertThat(DataUri.parse("data:image/png;base64,not!!base64")).isNull();
        // An empty payload is not an image.
        assertThat(DataUri.parse("data:image/png;base64,")).isNull();
        // ";base64" without the payload comma is not the encoded form.
        assertThat(DataUri.parse("data:image/png;base64")).isNull();
        // Whitespace is not legal inside a base64 payload.
        assertThat(DataUri.parse("data:image/png;base64,QUJD RA==")).isNull();
    }
}
