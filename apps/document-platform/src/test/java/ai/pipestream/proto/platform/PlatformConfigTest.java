package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.parse.v1.RoutingRule;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The config lane end to end on the platform: the routing contract
 * publishes to the co-mounted registry at boot, a parse-routing config
 * document gates against it at the registry gate, the running node
 * follows a put on its refresh interval and swaps the live routing
 * rules, an invalid document refuses at the service and never applies, and
 * a rebooted node reads the distributed config ahead of its environment
 * defaults.
 */
class PlatformConfigTest {

    @TempDir
    Path work;

    private DocumentPlatformConfig config(Path registryGit) {
        Map<String, String> environment = new HashMap<>();
        // The parse role wires a repo channel; this node has no repo role
        // and the config lane never dials it.
        environment.put("PROTOMOLT_REPO_TARGET", "127.0.0.1:1");
        environment.put(DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS, "1");
        return new DocumentPlatformConfig(
                null, null, registryGit, 0, 0, 0, 0,
                null, null, null,
                60L, 1, 0, null, 0, 0,
                List.of("registry", "parser-text", "parse"), environment);
    }

    private static HttpResponse<String> putConfig(int registryPort, String envelope)
            throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + registryPort + "/protomolt/configs/"
                                + DocumentPlatform.PARSE_ROUTING_CONFIG_SUBJECT))
                        .header("Content-Type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(envelope))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static List<String> ruleIds(DocumentPlatform platform) {
        return platform.parseRoutingRules().rules().stream()
                .map(RoutingRule::getRuleId)
                .toList();
    }

    @Test
    void theNodeFollowsTheConfigAndRefusesInvalidDocumentsAtTheGate() throws Exception {
        Path registryGit = work.resolve("registry-git");
        try (DocumentPlatform platform = DocumentPlatform.start(config(registryGit), null)) {
            // Boot state: the environment's default rules.
            assertThat(ruleIds(platform)).containsExactly("default-text");

            // An invalid document (no rules) refuses at the registry gate
            // against the published routing contract and never applies.
            HttpResponse<String> refused = putConfig(platform.registryPort(), """
                    {"messageType": "ai.pipestream.proto.parse.v1.RoutingConfig",
                     "config": {}}""");
            assertThat(refused.statusCode()).isEqualTo(422);
            assertThat(ruleIds(platform)).containsExactly("default-text");

            HttpResponse<String> accepted = putConfig(platform.registryPort(), """
                    {"messageType": "ai.pipestream.proto.parse.v1.RoutingConfig",
                     "config": {"rules": [{
                         "ruleId": "config-markdown-only",
                         "when": "mime_type == 'text/markdown'",
                         "parserName": "text",
                         "priority": 5}]}}""");
            assertThat(accepted.statusCode()).isEqualTo(200);

            // The running node follows on its interval: no reboot.
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline
                    && !ruleIds(platform).equals(List.of("config-markdown-only"))) {
                Thread.sleep(200);
            }
            assertThat(ruleIds(platform))
                    .as("the live rules swapped from the config document")
                    .containsExactly("config-markdown-only");
        }

        // A rebooted node reads the distributed config at boot, ahead of
        // its environment defaults.
        try (DocumentPlatform rebooted = DocumentPlatform.start(config(registryGit), null)) {
            assertThat(ruleIds(rebooted)).containsExactly("config-markdown-only");
        }
    }
}
