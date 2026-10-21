package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.Headers;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class UploadHttpHandlerAnswerTest {

    private static Headers accept(String... values) {
        var headers = new Headers();
        for (String value : values) headers.add("Accept", value);
        return headers;
    }

    @Test
    void protobufAnswersOnlyWhenAcceptNamesApplicationProto() {
        assertThat(UploadHttpHandler.acceptsProto(accept("application/proto"))).isTrue();
        assertThat(UploadHttpHandler.acceptsProto(accept("Application/Proto; q=0.5"))).isTrue();
        assertThat(UploadHttpHandler.acceptsProto(accept("text/html, application/proto;q=0.9"))).isTrue();
        assertThat(UploadHttpHandler.acceptsProto(accept("text/html", "application/proto"))).isTrue();
        assertThat(UploadHttpHandler.acceptsProto(accept())).isFalse();
        assertThat(UploadHttpHandler.acceptsProto(accept("application/json"))).isFalse();
        assertThat(UploadHttpHandler.acceptsProto(accept("*/*"))).isFalse();
        assertThat(UploadHttpHandler.acceptsProto(accept("application/protobuf"))).isFalse();
        assertThat(UploadHttpHandler.acceptsProto(accept("application/proto-x"))).isFalse();
    }

    @Test
    void grpcMessagesKeepPrintableAsciiAndPercentEncodeTheRest() {
        assertThat(UploadHttpHandler.percentEncode("expected revision 3, current 4"))
                .isEqualTo("expected revision 3, current 4");
        assertThat(UploadHttpHandler.percentEncode("100% full\nnext")).isEqualTo("100%25 full%0Anext");
        String unicode = "Größe überschritten: 文書";
        String encoded = UploadHttpHandler.percentEncode(unicode);
        assertThat(encoded).doesNotContain("ö").contains("%C3%B6");
        assertThat(URLDecoder.decode(encoded.replace("+", "%2B"), StandardCharsets.UTF_8)).isEqualTo(unicode);
    }
}
