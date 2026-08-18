package ai.pipestream.proto.registry.server;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.registry.ConfigSupport;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.registry.InvalidConfigException;
import ai.pipestream.proto.registry.RegistryStoreException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The config lane's write verb: publishes one typed config document
 * through exactly the gate the registry's HTTP config door mounts (strict
 * parse as the declared type, the type's own declared validate.v1 rules
 * enforced), then commits it. The commit id is the version every consumer
 * reports. Exact or refused: an invalid document never lands.
 */
public final class PublishConfigAction implements ProtoAction {

    /** The action name: {@value}. */
    public static final String NAME = "publish-config";

    private final GitSchemaRegistryStore store;

    /**
     * Creates the verb over the co-mounted store.
     *
     * @param store the registry store config documents commit to
     */
    public PublishConfigAction(GitSchemaRegistryStore store) {
        if (store == null) {
            throw new IllegalArgumentException("store must not be null");
        }
        this.store = store;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Publish one typed config document to the registry's config door: the "
                + "document parses strictly as the declared messageType and its type's "
                + "own validate.v1 rules are enforced before anything commits; the "
                + "commit id is the version consumers report.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode schema = mapper.createObjectNode();
        schema.put("type", "object");
        schema.put("additionalProperties", false);
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("name")
                .put("type", "string")
                .put("description", "The config subject to publish under, for example "
                        + "parse-routing or taxonomy:products.");
        properties.putObject("messageType")
                .put("type", "string")
                .put("description", "Full name of the document's protobuf type; must "
                        + "resolve from the registered schemas.");
        properties.putObject("config")
                .put("type", "object")
                .put("description", "The document as proto3 JSON.");
        schema.putArray("required").add("name").add("messageType").add("config");
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ObjectMapper mapper = context.objectMapper();
        JsonNode name = input.get("name");
        if (name == null || !name.isTextual() || name.asText().isBlank()) {
            throw new ActionException("invalid-input", "name is required");
        }
        JsonNode messageType = input.get("messageType");
        if (messageType == null || !messageType.isTextual()
                || messageType.asText().isBlank()) {
            throw new ActionException("invalid-input", "messageType is required");
        }
        JsonNode config = input.get("config");
        if (config == null || !config.isObject()) {
            throw new ActionException("invalid-input",
                    "config must be the document as a proto3 JSON object");
        }
        ObjectNode envelope = mapper.createObjectNode();
        envelope.put(ConfigSupport.MESSAGE_TYPE, messageType.asText());
        envelope.set(ConfigSupport.CONFIG, config);
        String json = envelope.toString();
        try {
            ConfigSupport.gate(store, json);
            String version = store.putConfig(name.asText(), json);
            ObjectNode result = mapper.createObjectNode();
            result.put("name", name.asText());
            result.put("messageType", messageType.asText());
            result.put("version", version);
            return result;
        } catch (InvalidConfigException e) {
            throw new ActionException("invalid-config", e.getMessage());
        } catch (RegistryStoreException e) {
            throw new ActionException("store-error", e.getMessage());
        }
    }
}
