package ai.pipestream.proto.acp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionException;
import com.agentclientprotocol.sdk.agent.SyncPromptContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Runs one console line against the catalog: {@code list}/{@code help} name the verbs,
 * anything else dispatches {@code <verb> <json>}. Results stream back as message chunks;
 * failures print their error and keep the session going, same contract as the CLI console.
 */
final class CatalogLineRunner {

    private final ActionCatalog catalog;
    private final ObjectMapper mapper = new ObjectMapper();

    CatalogLineRunner(ActionCatalog catalog) {
        this.catalog = catalog;
    }

    void run(String line, SyncPromptContext context) {
        line = line.trim();
        if (line.isEmpty()) {
            return;
        }
        if (line.equals("list") || line.equals("help")) {
            StringBuilder out = new StringBuilder();
            catalog.list().forEach(entry -> out.append(String.format("%-22s %s%n",
                    entry.path("name").asText(), entry.path("description").asText())));
            context.sendMessage(out.toString());
            return;
        }
        int space = line.indexOf(' ');
        String verb = space < 0 ? line : line.substring(0, space);
        String json = space < 0 ? "" : line.substring(space + 1).trim();
        if (!catalog.names().contains(verb)) {
            context.sendMessage("Unknown verb '" + verb + "'. Try 'list' to see them.");
            return;
        }
        try {
            ObjectNode input = json.isBlank()
                    ? mapper.createObjectNode()
                    : (ObjectNode) mapper.readTree(json);
            context.sendThought("running " + verb);
            ObjectNode result = catalog.execute(verb, input);
            context.sendMessage(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
        } catch (ActionException e) {
            context.sendMessage(e.code() + ": " + e.getMessage());
        } catch (Exception e) {
            context.sendMessage("error: " + e.getMessage());
        }
    }
}
