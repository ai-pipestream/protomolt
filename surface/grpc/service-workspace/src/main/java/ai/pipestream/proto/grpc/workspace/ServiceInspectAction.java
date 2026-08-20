package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/** Reads one profile and renders its service/method contracts without returning descriptor bytes. */
public final class ServiceInspectAction implements ProtoAction {

    private final ServiceProfileRepository repository;
    private final SchemaRegistryStore registry;

    public ServiceInspectAction(ServiceProfileRepository repository, SchemaRegistryStore registry) {
        this.repository = repository;
        this.registry = registry;
    }

    @Override
    public String name() {
        return "service-inspect";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Inspects one registered service: connection profile, method names, streaming shapes, "
                + "request/response types, and their top-level fields, without copying descriptor bytes.";
    }

    @Override
    public ObjectNode inputSchema() {
        return ServiceActionSupport.nameSchema();
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ServiceProfileRepository store = ServiceActionSupport.requireRepository(repository);
        String name = ServiceActionSupport.requireString(input, "name");
        ServiceProfile profile;
        try {
            profile = store.find(name).orElseThrow(() -> ServiceActionSupport.notFound(name));
        } catch (IOException e) {
            throw ServiceActionSupport.storage("read service profile '" + name + "'", e);
        }
        ObjectNode result = context.objectMapper().createObjectNode();
        result.set("profile", ServiceActionSupport.profileJson(profile, context.objectMapper()));
        result.set("services", ServiceActionSupport.services(
                profile, store, registry, context.objectMapper()));
        return result;
    }
}
