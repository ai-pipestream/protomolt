package ai.protomolt.proto.acquire.confluence;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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

    @Test
    void environmentAliasesSupplyCredentials() {
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.fromEnvironment(Map.of(
                "CONFLUENCE_BASE_URL", "https://pipestreamai.atlassian.net/wiki",
                "CONFLUENCE_USER", "me@pipestream.ai",
                "CONFLUENCE_TOKEN", "alias-token"));

        assertThat(config.email()).isEqualTo("me@pipestream.ai");
        assertThat(config.apiToken()).isEqualTo("alias-token");
    }

    @Test
    void canonicalCredentialNamesBeatAliases() {
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.fromEnvironment(Map.of(
                "CONFLUENCE_BASE_URL", "https://pipestreamai.atlassian.net/wiki",
                "CONFLUENCE_EMAIL", "bot@pipestream.ai",
                "CONFLUENCE_USER", "me@pipestream.ai",
                "CONFLUENCE_API_TOKEN", "canonical-token",
                "CONFLUENCE_TOKEN", "alias-token"));

        assertThat(config.email()).isEqualTo("bot@pipestream.ai");
        assertThat(config.apiToken()).isEqualTo("canonical-token");
    }

    @Test
    void environmentWithoutCredentialsFails() {
        assertThatThrownBy(() -> ConfluenceConnectorConfig.fromEnvironment(Map.of(
                "CONFLUENCE_BASE_URL", "https://pipestreamai.atlassian.net/wiki")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CONFLUENCE_EMAIL");
    }

    @Test
    void sinkConfigDefaultsToDisabled() {
        ConfluenceConnectorConfig config = valid().build();

        assertThat(config.kafkaEnabled()).isFalse();
        assertThat(config.repoEnabled()).isFalse();
        assertThat(config.kafkaTopic()).isEqualTo("confluence-events");
        assertThat(config.kafkaSnapshotsTopic()).isEqualTo("confluence-snapshots");
        assertThat(config.repoDrive()).isEqualTo("default");
        assertThat(config.repoAccountId()).isEqualTo("confluence");
        assertThat(config.repoDatasourceId()).isEqualTo("confluence");
    }

    @Test
    void sinkConfigFromEnvironment() {
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.fromEnvironment(Map.of(
                "CONFLUENCE_BASE_URL", "https://pipestreamai.atlassian.net/wiki",
                "CONFLUENCE_EMAIL", "bot@pipestream.ai",
                "CONFLUENCE_API_TOKEN", "token",
                "CONFLUENCE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "CONFLUENCE_KAFKA_TOPIC", "events-x",
                "CONFLUENCE_REPO_TARGET", "repo:9090"));

        assertThat(config.kafkaEnabled()).isTrue();
        assertThat(config.kafkaBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(config.kafkaTopic()).isEqualTo("events-x");
        assertThat(config.schemaRegistryUrl()).isNull();
        assertThat(config.repoEnabled()).isTrue();
        assertThat(config.repoTarget()).isEqualTo("repo:9090");
    }
}
