package ai.pipestream.proto.acquire.confluence;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Config validation, defaults, and the no-secrets-in-logs contract. */
class ConfluenceConnectorConfigTest {

    private static ConfluenceConnectorConfig.Builder valid() {
        return ConfluenceConnectorConfig.builder()
                .baseUrl("https://pipestreamai.atlassian.net/wiki")
                .email("bot@pipestream.ai")
                .apiToken("secret-token");
    }

    @Test
    void builderDefaults() {
        ConfluenceConnectorConfig config = valid().build();

        assertThat(config.pageSize()).isEqualTo(100);
        assertThat(config.bodyFormat()).isEqualTo("storage");
        assertThat(config.spaces()).isEmpty();
        assertThat(config.hasSpaceAllowlist()).isFalse();
    }

    @Test
    void trimsTrailingSlashesOffBaseUrl() {
        assertThat(valid().baseUrl("https://example.atlassian.net/wiki/").build().baseUrl())
                .isEqualTo("https://example.atlassian.net/wiki");
    }

    @Test
    void requiresBaseUrlEmailAndToken() {
        assertThatThrownBy(() -> valid().baseUrl(null).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CONFLUENCE_BASE_URL");
        assertThatThrownBy(() -> valid().baseUrl(" ").build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> valid().email(null).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CONFLUENCE_EMAIL");
        assertThatThrownBy(() -> valid().apiToken("").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CONFLUENCE_API_TOKEN");
    }

    @Test
    void capsPageSizeAtTheApiMaximum() {
        assertThat(valid().pageSize(1000).build().pageSize()).isEqualTo(250);
        assertThat(valid().pageSize(-1).build().pageSize()).isEqualTo(100);
    }

    @Test
    void validatesBodyFormat() {
        assertThat(valid().bodyFormat("atlas_doc_format").build().bodyFormat())
                .isEqualTo("atlas_doc_format");
        assertThatThrownBy(() -> valid().bodyFormat("pdf").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CONFLUENCE_BODY_FORMAT");
    }

    @Test
    void spaceAllowlist() {
        ConfluenceConnectorConfig config = valid().spaces(List.of("ENG", "DOCS")).build();
        assertThat(config.hasSpaceAllowlist()).isTrue();
        assertThat(config.spaces()).containsExactly("ENG", "DOCS");
    }

    @Test
    void apiTokenNeverAppearsInToString() {
        String rendered = valid().build().toString();
        assertThat(rendered).doesNotContain("secret-token");
        assertThat(rendered).contains("***");
        assertThat(rendered).contains("bot@pipestream.ai");
    }
}
