package ai.protomolt.proto.mcp;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.service.ProtoMoltCatalog;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Keeps generated catalog documentation synchronized with the registrations that ship. */
class CatalogInventorySystemTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern MANUAL_INVENTORY_COUNT = Pattern.compile(
            "(?i)\\b(?:1[7-9]|2[0-9]|3[0-9]|seventeen|eighteen|nineteen|twenty|thirty)"
                    + "(?:[- ](?:one|two|three|four|five|six|seven|eight|nine))?\\s+"
                    + "(?:actions?|verbs?|tools?|operations?|rpcs?)\\b");

    @Test
    void generatedInventoryMatchesTheDefaultStandaloneAndFullCatalogs() throws Exception {
        JsonNode inventory = MAPPER.readTree(repositoryRoot()
                .resolve("docs/generated/action-inventory.json").toFile());
        ActionContext context = ActionContext.create();

        assertCatalog(inventory, "defaults", ActionCatalog.defaults(context), 17);
        assertCatalog(inventory, "standaloneMcp", McpMain.catalog(context), 34);
        assertCatalog(inventory, "full", ProtoMoltCatalog.full(context), 44);
    }

    @Test
    void catalogDocumentationLinksTheGeneratedInventoryInsteadOfOwningCounts() throws Exception {
        Path root = repositoryRoot();
        Map<String, String> docs = Map.of(
                "docs/surface/actions.md", "../generated/action-inventory.json",
                "docs/surface/mcp.md", "../generated/action-inventory.json",
                "docs/surface/grpc-service.md", "../generated/action-inventory.json",
                "docs/apps/cli.md", "../generated/action-inventory.json",
                "docs/tutorials/python.md", "../generated/action-inventory.json",
                "docs/tutorials/openvino.md", "../generated/action-inventory.json",
                "docs/operations/building.md", "generateActionInventory");
        for (Map.Entry<String, String> entry : docs.entrySet()) {
            String text = Files.readString(root.resolve(entry.getKey()));
            assertThat(text).as(entry.getKey()).contains(entry.getValue());
            assertThat(MANUAL_INVENTORY_COUNT.matcher(text).find())
                    .as(entry.getKey() + " must not maintain an action count")
                    .isFalse();
        }
    }

    private static void assertCatalog(JsonNode inventory, String key, ActionCatalog catalog,
                                      int expectedCount) {
        List<String> generated = new ArrayList<>();
        inventory.path(key).forEach(node -> generated.add(node.asText()));
        assertThat(generated).as(key + " names").containsExactlyElementsOf(catalog.names());
        assertThat(inventory.path(key + "Count").asInt()).as(key + " count")
                .isEqualTo(catalog.names().size());
        assertThat(catalog.names()).as(key + " acceptance count").hasSize(expectedCount);
    }

    private static Path repositoryRoot() throws IOException {
        Path current = Path.of("").toAbsolutePath().normalize();
        while (current != null && !Files.exists(current.resolve("settings.gradle"))) {
            current = current.getParent();
        }
        if (current == null) {
            throw new IOException("could not locate repository root from "
                    + Path.of("").toAbsolutePath());
        }
        return current;
    }
}
