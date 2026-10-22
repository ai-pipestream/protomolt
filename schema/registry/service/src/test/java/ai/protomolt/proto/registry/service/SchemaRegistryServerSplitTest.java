package ai.protomolt.proto.registry.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regex-free token splitter behind query-parameter and path-segment parsing, checked
 * against the {@code String.split} (limit 0) calls it replaced.
 */
class SchemaRegistryServerSplitTest {

    @Test
    void splittingMatchesStringSplit() {
        for (String raw : new String[]{
                "a=1&b=2", "a", "", "&", "&&", "a&", "a&&b", "&a", "a=1&", "a==&b"}) {
            assertThat(SchemaRegistryServer.splitOn(raw, '&'))
                    .as("split of '%s' on '&'", raw)
                    .containsExactly(raw.split("&"));
        }
        for (String rawPath : new String[]{
                "/subjects/orders/versions/1", "", "/", "//", "a/b/", "///a", "a//b/"}) {
            assertThat(SchemaRegistryServer.splitOn(rawPath, '/'))
                    .as("split of '%s' on '/'", rawPath)
                    .containsExactly(rawPath.split("/"));
        }
    }
}
