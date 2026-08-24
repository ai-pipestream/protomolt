package ai.pipestream.proto.registry.service;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.JsonAction;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.pipestream.proto.registry.RegistryFederation;
import ai.pipestream.proto.registry.RegistryStoreException;
import ai.pipestream.proto.schema.registry.v1.RegistrySyncRequest;
import ai.pipestream.proto.schema.registry.v1.RegistrySyncResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

/**
 * The {@code registry-sync} verb: fetches one configured remote registry and imports its
 * subjects (namespaced {@code <remote>:<subject>}, references rewritten, every version
 * compatibility-gated) and descriptor artifacts. The answer is the full sync report — what
 * imported, what was already present, and what was rejected with the gate's violations.
 */
public final class RegistrySyncAction implements JsonAction {

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
    public String requiredScope() {
        return Scopes.SCHEMA_WRITE;
    }

    @Override
    public String description() {
        return "Fetches a configured remote registry (see registry-remotes) and imports its"
                + " subjects as <remote>:<subject> plus its descriptor artifacts; every"
                + " imported version passes the local compatibility gate, and the report"
                + " lists imports, already-present versions and rejections per subject.";
    }

    @Override
    public Descriptor requestType() {
        return RegistrySyncRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return RegistrySyncResponse.getDescriptor();
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        RegistrySyncRequest request = (RegistrySyncRequest) RegistryRequests.validate(
                input, RegistrySyncRequest.newBuilder(), "registry-sync");
        String remote = request.getRemote();
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
