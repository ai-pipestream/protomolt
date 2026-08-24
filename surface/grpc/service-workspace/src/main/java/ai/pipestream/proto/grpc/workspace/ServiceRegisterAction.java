package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.invoke.ReflectionException;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;

/** Registers a reflected gRPC service without returning its descriptor bytes to the caller. */
public final class ServiceRegisterAction implements ProtoAction {

    private final ServiceProfileRepository repository;
    private final ChannelFactory channels;
    private final SchemaRegistryStore registry;

    public ServiceRegisterAction(ServiceProfileRepository repository, SchemaRegistryStore registry,
                                 ChannelFactory channels) {
        this.repository = repository;
        this.registry = registry;
        this.channels = channels;
    }

    @Override
    public String name() {
        return "service-register";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Registers a stable gRPC service profile, reflects one named endpoint, stores the "
                + "descriptor set in the schema registry, and returns only profile and "
                + "schema summaries. Credential and key fields are opaque host references.";
    }

    @Override
    public ObjectNode inputSchema() {
        return ServiceActionJson.schemaFor("ServiceRegisterRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ServiceProfileRepository store = ServiceActionSupport.requireRepository(repository);
        DynamicMessage request = ServiceActionJson.parse(input, "ServiceRegisterRequest", name());
        ServiceProfile profile = ServiceActionJson.submessage(
                request, "profile", ServiceProfile.parser(), name());
        String endpoint = ServiceActionJson.string(request, "endpoint");
        try {
            ServiceProfile saved = ServiceActionSupport.reflectAndStore(profile,
                    endpoint.isEmpty() ? null : endpoint,
                    ServiceActionJson.number(request, "deadline_ms",
                            ServiceActionSupport.DEFAULT_DEADLINE_MS),
                    store, registry, channels);
            ObjectNode result = context.objectMapper().createObjectNode();
            result.put("ok", true);
            result.set("profile", ServiceActionSupport.profileJson(saved, context.objectMapper()));
            result.set("services", ServiceActionSupport.services(
                    saved, store, registry, context.objectMapper()));
            return result;
        } catch (ReflectionException e) {
            ObjectNode result = context.objectMapper().createObjectNode();
            result.put("ok", false);
            result.put("error", e.getMessage());
            return result;
        }
    }
}
