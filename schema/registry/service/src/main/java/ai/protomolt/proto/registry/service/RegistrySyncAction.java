package ai.protomolt.proto.registry.service;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.registry.RegistryFederation;
import ai.protomolt.proto.registry.RegistryStoreException;
import ai.protomolt.proto.schema.registry.v1.ImportedSubject;
import ai.protomolt.proto.schema.registry.v1.RegistrySyncRequest;
import ai.protomolt.proto.schema.registry.v1.RegistrySyncResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

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
    public Message execute(Message input, ActionContext context) throws ActionException {
        RegistrySyncRequest request = CatalogContract.as(
                input, RegistrySyncRequest.getDefaultInstance(), name());
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
        RegistrySyncResponse.Builder response = RegistrySyncResponse.newBuilder()
                .setRemote(report.remote())
                .setDescriptorsImported(report.descriptorsImported())
                .addAllErrors(report.errors());
        for (RegistryFederation.SubjectSync subject : report.subjects()) {
            response.addSubjects(ImportedSubject.newBuilder()
                    .setRemoteSubject(subject.remoteSubject())
                    .setLocalSubject(subject.localSubject())
                    .setImported(subject.imported())
                    .setAlreadyPresent(subject.alreadyPresent())
                    .addAllRejections(subject.rejections()));
        }
        return response.build();
    }
}
