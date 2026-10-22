package ai.protomolt.proto.metric.iceberg;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regex-free splitter behind rollup CSV and field-path parsing, checked against the
 * {@code String.split} (limit 0) calls it replaced.
 */
class RollupTextTest {

    @Test
    void splittingMatchesStringSplit() {
        for (String text : new String[]{
                "a,b,c", "a", "", ",", ",,", "a,", "a,,b", ",a", "a,b,"}) {
            assertThat(RollupText.splitOn(text, ','))
                    .as("split of '%s' on ','", text)
                    .containsExactly(text.split(","));
        }
        for (String path : new String[]{
                "a.b.c", "a", "", ".", "a.", ".a", "a..b"}) {
            assertThat(RollupText.splitOn(path, '.'))
                    .as("split of '%s' on '.'", path)
                    .containsExactly(path.split("\\."));
        }
    }
}
