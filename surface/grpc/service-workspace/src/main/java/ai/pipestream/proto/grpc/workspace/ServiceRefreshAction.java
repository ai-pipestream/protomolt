package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.JsonAction;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.invoke.ReflectionException;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;

import com.google.protobuf.Descriptors.Descriptor;
import java.io.IOException;

/** Re-reflects a stored profile and advances its content-addressed schema identity. */
public final class ServiceRefreshAction implements JsonAction {

    private final ServiceProfileRepository repository;
    private final ChannelFactory channels;
    private final SchemaRegistryStore registry;

    public ServiceRefreshAction(ServiceProfileRepository repository, SchemaRegistryStore registry,
                                ChannelFactory channels) {
        this.repository = repository;
        this.registry = registry;
        this.channels = channels;
    }

    @Override
    public String name() {
        return "service-refresh";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Re-reflects a registered service endpoint, stores the descriptor as a "
                + "content-addressed registry artifact, and updates the profile fingerprint.";
    }

    @Override
    public Descriptor requestType() {
        return ServiceActionJson.request("ServiceRefreshRequest");
    }

    @Override
    public Descriptor responseType() {
        return ServiceActionJson.request("ServiceRefreshResponse");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ServiceProfileRepository store = ServiceActionSupport.requireRepository(repository);
        DynamicMessage request = ServiceActionJson.parse(input, "ServiceRefreshRequest", name());
        String name = ServiceActionJson.string(request, "name");
        ServiceProfile profile;
        try {
            profile = store.find(name).orElseThrow(() -> ServiceActionSupport.notFound(name));
        } catch (IOException e) {
            throw ServiceActionSupport.storage("read service profile '" + name + "'", e);
        }
        String endpoint = ServiceActionJson.string(request, "endpoint");
        try {
            ServiceProfile refreshed = ServiceActionSupport.reflectAndStore(profile,
                    endpoint.isEmpty() ? null : endpoint,
                    ServiceActionJson.number(request, "deadline_ms",
                            ServiceActionSupport.DEFAULT_DEADLINE_MS),
                    store, registry, channels);
            ObjectNode result = context.objectMapper().createObjectNode();
            result.put("ok", true);
            result.put("changed", !refreshed.getSchemaSource().getDescriptorFingerprint().equals(
                    profile.getSchemaSource().getDescriptorFingerprint()));
            result.set("profile", ServiceActionSupport.profileJson(refreshed, context.objectMapper()));
            result.set("services", ServiceActionSupport.services(
                    refreshed, store, registry, context.objectMapper()));
            return result;
        } catch (ReflectionException e) {
            ObjectNode result = context.objectMapper().createObjectNode();
            result.put("ok", false);
            result.put("error", e.getMessage());
            return result;
        }
    }
}
