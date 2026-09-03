package ai.protomolt.proto.registry.service;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.registry.ConfigSupport;
import ai.protomolt.proto.registry.GitSchemaRegistryStore;
import ai.protomolt.proto.registry.InvalidConfigException;
import ai.protomolt.proto.registry.RegistryStoreException;
import ai.protomolt.proto.schema.registry.v1.PublishConfigRequest;
import ai.protomolt.proto.schema.registry.v1.PublishConfigResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

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
    public Descriptor requestType() {
        return PublishConfigRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return PublishConfigResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        // The message declares all three members required and the type name as a fully
        // qualified protobuf name, so those checks live in the contract rather than here.
        PublishConfigRequest request = CatalogContract.as(
                input, PublishConfigRequest.getDefaultInstance(), name());
        // The gate reads the document as text, and the config is carried as a structure
        // because only the caller's declared type knows its shape.
        ObjectNode envelope = context.objectMapper().createObjectNode();
        envelope.put(ConfigSupport.MESSAGE_TYPE, request.getMessageType());
        envelope.set(ConfigSupport.CONFIG,
                CatalogContract.toEnvelope(request.getConfig(), name()));
        try {
            String json = envelope.toString();
            ConfigSupport.gate(store, json);
            return PublishConfigResponse.newBuilder()
                    .setName(request.getName())
                    .setMessageType(request.getMessageType())
                    .setVersion(store.putConfig(request.getName(), json))
                    .build();
        } catch (InvalidConfigException e) {
            throw new ActionException("invalid-config", e.getMessage());
        } catch (RegistryStoreException e) {
            throw new ActionException("store-error", e.getMessage());
        }
    }
}
