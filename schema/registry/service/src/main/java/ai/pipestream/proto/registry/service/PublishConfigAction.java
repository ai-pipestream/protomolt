package ai.pipestream.proto.registry.service;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.pipestream.proto.schema.registry.v1.PublishConfigRequest;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.registry.ConfigSupport;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.registry.InvalidConfigException;
import ai.pipestream.proto.registry.RegistryStoreException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The config lane's write verb: publishes one typed config document
 * through exactly the gate the registry's HTTP config gate mounts (strict
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
    public String requiredScope() {
        return Scopes.SCHEMA_WRITE;
    }

    @Override
    public String description() {
        return "Publish one typed config document to the registry's config gate: the "
                + "document parses strictly as the declared messageType and its type's "
                + "own validate.v1 rules are enforced before anything commits; the "
                + "commit id is the version consumers report.";
    }

    @Override
    public ObjectNode inputSchema() {
        // Derived from the request message, so the caller sees that the type name must be a
        // fully qualified protobuf name and that all three members are required.
        return new ObjectMapper().valueToTree(ProtoJsonSchemaGenerator.create()
                .generate(PublishConfigRequest.getDescriptor()));
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ObjectMapper mapper = context.objectMapper();
        // The message declares all three members required and the type name as a fully
        // qualified protobuf name, so those checks live in the contract rather than here.
        JsonNode name = input.get("name");
        JsonNode messageType = input.get("messageType");
        JsonNode config = input.get("config");
        RegistryRequests.validate(input, PublishConfigRequest.newBuilder(), "publish-config");
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
