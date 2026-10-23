package ai.protomolt.proto.emit.okf;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regex-free trailing-slash strip, checked against the
 * {@code replaceAll("/+$", "")} it replaced, including {@code $} matching before a final
 * line terminator.
 */
class OkfRegistryBundlesUrlTest {

    @Test
    void stripMatchesTheFormerReplaceAll() {
        for (String url : new String[]{
                "http://registry:8081", "http://registry:8081/", "http://registry:8081///",
                "", "/", "///", "a/b", "a/b/", "a//\n", "a//\r\n", "a//\r",
                "a//\u0085", "a//\u2028", "a//\u2029", "a//\nb", "a/\n/",
                "\n", "/\n", "//\n//"}) {
            assertThat(OkfRegistryBundles.stripTrailingSlashRun(url))
                    .as("strip of %s", url.replace("\n", "\\n").replace("\r", "\\r"))
                    .isEqualTo(url.replaceAll("/+$", ""));
        }
    }
}
