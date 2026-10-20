package ai.protomolt.proto.agenthost;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The host identity, MCP token environment name and OpenAI model name checks are character
 * scans. Each is held to the regular expression it replaced, over every ASCII character and
 * a set of non-ASCII letters and digits in the first and later positions and at the length
 * bounds, and each rejection keeps its message.
 */
class InputGrammarTest {

    private static final String IDENTITY = "[A-Za-z0-9][A-Za-z0-9._-]{0,127}";
    private static final String TOKEN_ENVIRONMENT = "[A-Z][A-Z0-9_]{0,127}";
    private static final String MODEL = "[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}";
    private static final String NON_ASCII = "\u00C9\u00E9\u00DF\u0130\u0661\uFF21\uFF11\u212A";

    @Test
    void identityAcceptsExactlyWhatItsRegexAccepted() {
        assertAgrees(AgentHost.Config::isIdentity, IDENTITY, 128, "a");
        assertThat(AgentHost.Config.isIdentity("a")).isTrue();
        assertThat(AgentHost.Config.isIdentity("0.worker_1-x")).isTrue();
        assertThat(AgentHost.Config.isIdentity("a".repeat(128))).isTrue();
        assertThat(AgentHost.Config.isIdentity("a".repeat(129))).isFalse();
        assertThat(AgentHost.Config.isIdentity("")).isFalse();
        assertThat(AgentHost.Config.isIdentity(".a")).isFalse();
        assertThat(AgentHost.Config.isIdentity("-a")).isFalse();
        assertThat(AgentHost.Config.isIdentity("_a")).isFalse();
        assertThat(AgentHost.Config.isIdentity("a:b")).isFalse();
        assertThat(AgentHost.Config.isIdentity("w\u00E9")).isFalse();
        assertThat(AgentHost.Config.isIdentity("\u0661")).isFalse();
    }

    @Test
    void invalidIdentityKeepsTheConfigurationMessage() {
        assertThat(config("worker-1").identity()).isEqualTo("worker-1");
        assertThat(config("w".repeat(128)).identity()).hasSize(128);
        for (String identity : List.of("", "w".repeat(129), "-worker", "worker/1",
                "w\u00F6rker", "worker\n")) {
            assertThatThrownBy(() -> config(identity))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("invalid agent host configuration");
        }
    }

    @Test
    void tokenEnvironmentAcceptsExactlyWhatItsRegexAccepted() {
        assertAgrees(McpHttpClient::isTokenEnvironmentName, TOKEN_ENVIRONMENT, 128, "A");
        assertThat(McpHttpClient.isTokenEnvironmentName("A")).isTrue();
        assertThat(McpHttpClient.isTokenEnvironmentName("MCP_TOKEN_2")).isTrue();
        assertThat(McpHttpClient.isTokenEnvironmentName("A".repeat(128))).isTrue();
        assertThat(McpHttpClient.isTokenEnvironmentName("A".repeat(129))).isFalse();
        assertThat(McpHttpClient.isTokenEnvironmentName("")).isFalse();
        assertThat(McpHttpClient.isTokenEnvironmentName("_TOKEN")).isFalse();
        assertThat(McpHttpClient.isTokenEnvironmentName("2TOKEN")).isFalse();
        assertThat(McpHttpClient.isTokenEnvironmentName("mcp_token")).isFalse();
        assertThat(McpHttpClient.isTokenEnvironmentName("MCP-TOKEN")).isFalse();
        assertThat(McpHttpClient.isTokenEnvironmentName("\uFF21")).isFalse();
    }

    @Test
    void invalidTokenEnvironmentKeepsItsMessageAndNullStillMeansNoToken() {
        URI endpoint = URI.create("http://127.0.0.1:1/mcp");
        McpHttpClient.usingTokenEnvironment(endpoint, null).close();
        McpHttpClient.usingTokenEnvironment(endpoint, "MCP_TOKEN").close();
        McpHttpClient.usingTokenEnvironment(endpoint, "T".repeat(128)).close();
        for (String name : List.of("", "T".repeat(129), "mcp_token", "_MCP", "MCP.TOKEN",
                "\u00C9TAT")) {
            assertThatThrownBy(() -> McpHttpClient.usingTokenEnvironment(endpoint, name))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("token environment name is invalid");
        }
    }

    @Test
    void modelAcceptsExactlyWhatItsRegexAccepted() {
        assertAgrees(OpenAiAgentProvider::isModelName, MODEL, 256, "m");
        assertThat(OpenAiAgentProvider.isModelName("meta-models/Muse-Glimmer-30B")).isTrue();
        assertThat(OpenAiAgentProvider.isModelName("qwen3:8b")).isTrue();
        assertThat(OpenAiAgentProvider.isModelName("m".repeat(256))).isTrue();
        assertThat(OpenAiAgentProvider.isModelName("m".repeat(257))).isFalse();
        assertThat(OpenAiAgentProvider.isModelName("")).isFalse();
        assertThat(OpenAiAgentProvider.isModelName("/models")).isFalse();
        assertThat(OpenAiAgentProvider.isModelName(":latest")).isFalse();
        assertThat(OpenAiAgentProvider.isModelName("model name")).isFalse();
        assertThat(OpenAiAgentProvider.isModelName("mod\u00E8le")).isFalse();
    }

    @Test
    void invalidModelKeepsItsMessage() {
        try (OpenAiAgentProvider accepted = provider("m".repeat(256))) {
            assertThat(accepted.name()).isEqualTo("openai");
        }
        for (String model : List.of("", "m".repeat(257), "-model", "model@1", "\uFF11b")) {
            assertThatThrownBy(() -> provider(model))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("provider model must be a bounded "
                            + "identifier such as meta-models/Muse-Glimmer-30B");
        }
        assertThatThrownBy(() -> provider(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("provider model must be a bounded "
                        + "identifier such as meta-models/Muse-Glimmer-30B");
    }

    /**
     * Checks one character at a time in the first position and after a valid first
     * character, then the empty string and lengths around the maximum.
     */
    private static void assertAgrees(Predicate<String> scan, String regex, int max,
                                     String valid) {
        List<String> candidates = new ArrayList<>();
        candidates.add("");
        for (char c = 0; c < 0x80; c++) {
            candidates.add(String.valueOf(c));
            candidates.add(valid + c);
            candidates.add(valid.repeat(max - 1) + c);
            candidates.add(valid.repeat(max) + c);
        }
        for (char c : NON_ASCII.toCharArray()) {
            candidates.add(String.valueOf(c));
            candidates.add(valid + c);
            candidates.add(c + valid);
        }
        candidates.add(valid.repeat(max - 1));
        candidates.add(valid.repeat(max));
        candidates.add(valid.repeat(max + 1));
        for (String candidate : candidates) {
            assertThat(scan.test(candidate))
                    .as("%s against %s (length %d)", regex, candidate.length() > 8
                            ? candidate.substring(candidate.length() - 2) : candidate,
                            candidate.length())
                    .isEqualTo(candidate.matches(regex));
        }
    }

    private static AgentHost.Config config(String identity) {
        return new AgentHost.Config(AgentRole.WORKER, identity, null,
                Path.of("").toAbsolutePath(), Duration.ofSeconds(1), 1);
    }

    private static OpenAiAgentProvider provider(String model) {
        return new OpenAiAgentProvider(URI.create("http://127.0.0.1:1/v1"), model,
                AgentRole.WORKER, "", Duration.ofMinutes(1));
    }
}
