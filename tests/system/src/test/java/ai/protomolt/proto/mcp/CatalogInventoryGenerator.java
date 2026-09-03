package ai.protomolt.proto.mcp;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.service.ProtoMoltCatalog;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Writes the machine-readable catalog inventory consumed by the documentation checks. */
public final class CatalogInventoryGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CatalogInventoryGenerator() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: CatalogInventoryGenerator <output>");
        }
        ActionContext context = ActionContext.create();
        ActionCatalog defaults = ActionCatalog.defaults(context);
        ActionCatalog standalone = McpMain.catalog(context);
        ActionCatalog full = ProtoMoltCatalog.full(context);

        ObjectNode inventory = MAPPER.createObjectNode();
        inventory.put("generatedBy", "CatalogInventoryGenerator");
        names(inventory, "defaults", defaults);
        names(inventory, "standaloneMcp", standalone);
        names(inventory, "full", full);

        Path output = Path.of(args[0]);
        Files.createDirectories(output.toAbsolutePath().normalize().getParent());
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(output.toFile(), inventory);
    }

    private static void names(ObjectNode inventory, String key, ActionCatalog catalog) {
        ArrayNode names = inventory.putArray(key);
        catalog.names().forEach(names::add);
        inventory.put(key + "Count", names.size());
    }
}
