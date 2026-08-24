package ai.pipestream.proto.registry.service;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.JsonAction;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.pipestream.proto.registry.RegistryFederation;
import ai.pipestream.proto.registry.RegistryStoreException;
import ai.pipestream.proto.schema.registry.v1.RegistryRemotesRequest;
import ai.pipestream.proto.schema.registry.v1.RegistryRemotesResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

/**
 * The {@code registry-remotes} verb: manages the git remotes this registry federates from.
 * Remotes are node-local git config (a deployment fact), so nothing here commits; every
 * operation answers with the resulting remote list.
 */
public final class RegistryRemotesAction implements JsonAction {

    /** The action name: {@value}. */
    public static final String NAME = "registry-remotes";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RegistryFederation federation;

    /**
     * Creates the action.
     *
     * @param federation the federation surface of this node's registry
     */
    public RegistryRemotesAction(RegistryFederation federation) {
        if (federation == null) {
            throw new IllegalArgumentException("federation must not be null");
        }
        this.federation = federation;
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
        return "Manages the git remotes this registry federates from: op=list shows them,"
                + " op=add registers a remote registry URL under a name (the origin prefix of"
                + " every subject later imported from it), op=remove forgets the remote"
                + " without touching subjects already imported.";
    }

    @Override
    public Descriptor requestType() {
        return RegistryRemotesRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return RegistryRemotesResponse.getDescriptor();
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        // The message declares the operation required and states which members each one
        // needs, so those checks live in the contract rather than being repeated here.
        RegistryRemotesRequest request = (RegistryRemotesRequest) RegistryRequests.validate(
                input, RegistryRemotesRequest.newBuilder(), "registry-remotes");
        try {
            switch (request.getOperation()) {
                case REMOTE_OPERATION_LIST -> {
                    // Listing changes nothing; the response below is the whole answer.
                }
                case REMOTE_OPERATION_ADD ->
                        federation.addRemote(request.getName(), request.getUrl());
                case REMOTE_OPERATION_REMOVE -> federation.removeRemote(request.getName());
                default -> throw new ActionException("invalid-input",
                        "operation must name one of the declared values");
            }
        } catch (IllegalArgumentException e) {
            throw new ActionException("invalid-input", e.getMessage());
        } catch (RegistryStoreException e) {
            throw new ActionException("remote-config-failed", e.getMessage());
        }
        ObjectNode output = MAPPER.createObjectNode();
        ArrayNode remotes = output.putArray("remotes");
        for (RegistryFederation.RemoteInfo remote : federation.remotes()) {
            remotes.addObject().put("name", remote.name()).put("url", remote.url());
        }
        return output;
    }

    private static String requiredString(ObjectNode input, String field) throws ActionException {
        String value = input.path(field).asText("");
        if (value.isBlank()) {
            throw new ActionException("invalid-input", field + " is required for op="
                    + input.path("op").asText(""));
        }
        return value;
    }
}
