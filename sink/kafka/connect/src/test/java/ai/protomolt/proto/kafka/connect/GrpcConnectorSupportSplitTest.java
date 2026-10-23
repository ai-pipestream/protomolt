package ai.protomolt.proto.kafka.connect;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regex-free dot splitter behind nested-message resolution, checked against the
 * {@code String.split("\\.")} (limit 0) it replaced.
 */
class GrpcConnectorSupportSplitTest {

    @Test
    void splittingMatchesStringSplit() {
        for (String text : new String[]{
                "Outer.Inner", "Outer", "", ".", "..", "a.", "a..b", ".a", "a.b.c."}) {
            assertThat(GrpcConnectorSupport.splitOnDot(text))
                    .as("split of '%s'", text)
                    .containsExactly(text.split("\\."));
        }
    }
}
