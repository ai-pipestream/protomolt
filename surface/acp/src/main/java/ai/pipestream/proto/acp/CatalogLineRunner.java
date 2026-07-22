package ai.pipestream.proto.acp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionException;
import com.agentclientprotocol.sdk.agent.SyncPromptContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Locale;

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
        ObjectNode input;
        try {
            input = json.isBlank() ? mapper.createObjectNode() : readObject(json);
        } catch (JsonProcessingException e) {
            context.sendMessage("error: input is not JSON: " + e.getOriginalMessage());
            return;
        } catch (IllegalArgumentException e) {
            context.sendMessage("error: " + e.getMessage());
            return;
        }
        try {
            context.sendThought("running " + verb);
            // Streaming-capable verbs (e.g. grpc-invoke on a server-streaming method) emit
            // per response; unary verbs emit their single result. Either way each emission
            // is its own chunk, so the IDE renders results as they arrive.
            catalog.executeStreaming(verb, input, node -> {
                try {
                    context.sendMessage(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
        } catch (ActionException e) {
            context.sendMessage(e.code() + ": " + e.getMessage());
        } catch (Exception e) {
            context.sendMessage("error: " + e.getMessage());
        }
    }

    /**
     * Parses verb input. Every verb takes a JSON object, so an array, string or number is
     * reported here by the shape it was given; casting instead surfaced a raw
     * {@link ClassCastException} message naming Jackson's internal node classes to the IDE user.
     */
    private ObjectNode readObject(String json) throws JsonProcessingException {
        JsonNode node = mapper.readTree(json);
        if (!node.isObject()) {
            throw new IllegalArgumentException("input must be a JSON object, got "
                    + node.getNodeType().name().toLowerCase(Locale.ROOT));
        }
        return (ObjectNode) node;
    }
}
