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
 * The {@code registry-sync} verb: fetches one configured remote registry and imports its
 * subjects (namespaced {@code <remote>:<subject>}, references rewritten, every version
 * compatibility-gated) and descriptor artifacts. The answer is the full sync report — what
 * imported, what was already present, and what was rejected with the gate's violations.
 */
public final class RegistrySyncAction implements ProtoAction {

    /** The action name: {@value}. */
    public static final String NAME = "registry-sync";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RegistryFederation federation;

    /**
     * Creates the action.
     *
     * @param federation the federation surface of this node's registry
     */
    public RegistrySyncAction(RegistryFederation federation) {
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
        return "Fetches a configured remote registry (see registry-remotes) and imports its"
                + " subjects as <remote>:<subject> plus its descriptor artifacts; every"
                + " imported version passes the local compatibility gate, and the report"
                + " lists imports, already-present versions and rejections per subject.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("remote")
                .put("type", "string")
                .put("description", "Name of a remote added with registry-remotes");
        schema.putArray("required").add("remote");
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String remote = input.path("remote").asText("");
        if (remote.isBlank()) {
            throw new ActionException("invalid-input", "remote is required");
        }
        RegistryFederation.SyncReport report;
        try {
            report = federation.sync(remote);
        } catch (IllegalArgumentException e) {
            throw new ActionException("invalid-input", e.getMessage());
        } catch (RegistryStoreException e) {
            throw new ActionException("sync-failed", e.getMessage());
        }
        ObjectNode output = MAPPER.createObjectNode();
        output.put("remote", report.remote());
        ArrayNode subjects = output.putArray("subjects");
        for (RegistryFederation.SubjectSync subject : report.subjects()) {
            ObjectNode node = subjects.addObject()
                    .put("remoteSubject", subject.remoteSubject())
                    .put("localSubject", subject.localSubject())
                    .put("imported", subject.imported())
                    .put("alreadyPresent", subject.alreadyPresent());
            ArrayNode rejections = node.putArray("rejections");
            subject.rejections().forEach(rejections::add);
        }
        output.put("descriptorsImported", report.descriptorsImported());
        ArrayNode errors = output.putArray("errors");
        report.errors().forEach(errors::add);
        return output;
    }
}
