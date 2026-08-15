package ai.pipestream.proto.registry.server;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.registry.RegistryFederation;
import ai.pipestream.proto.registry.RegistryStoreException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The {@code registry-remotes} verb: manages the git remotes this registry federates from.
 * Remotes are node-local git config (a deployment fact), so nothing here commits; every
 * operation answers with the resulting remote list.
 */
public final class RegistryRemotesAction implements ProtoAction {

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
    public String description() {
        return "Manages the git remotes this registry federates from: op=list shows them,"
                + " op=add registers a remote registry URL under a name (the origin prefix of"
                + " every subject later imported from it), op=remove forgets the remote"
                + " without touching subjects already imported.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        ObjectNode op = properties.putObject("op");
        op.put("type", "string");
        op.putArray("enum").add("list").add("add").add("remove");
        op.put("description", "The operation");
        properties.putObject("name")
                .put("type", "string")
                .put("description", "Remote name, [a-z][a-z0-9-]*; required for add and remove");
        properties.putObject("url")
                .put("type", "string")
                .put("description", "Git URL of the remote registry; required for add");
        schema.putArray("required").add("op");
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String op = input.path("op").asText("");
        try {
            switch (op) {
                case "list" -> {
                    // list takes no other input
                }
                case "add" -> federation.addRemote(
                        requiredString(input, "name"), requiredString(input, "url"));
                case "remove" -> federation.removeRemote(requiredString(input, "name"));
                default -> throw new ActionException("invalid-input",
                        "op must be list, add or remove; got '" + op + "'");
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
