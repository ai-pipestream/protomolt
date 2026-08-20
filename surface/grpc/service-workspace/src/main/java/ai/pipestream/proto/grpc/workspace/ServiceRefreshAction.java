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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/** Re-reflects a stored profile and advances its content-addressed schema identity. */
public final class ServiceRefreshAction implements ProtoAction {

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
    public ObjectNode inputSchema() {
        ObjectNode schema = ServiceActionSupport.nameSchema();
        ObjectNode properties = (ObjectNode) schema.get("properties");
        properties.putObject("endpoint").put("type", "string")
                .put("description", "Endpoint name to reflect; defaults to the first endpoint.");
        properties.putObject("deadlineMs").put("type", "integer").put("minimum", 1)
                .put("maximum", ServiceActionSupport.MAX_DEADLINE_MS)
                .put("default", ServiceActionSupport.DEFAULT_DEADLINE_MS);
        return schema;
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
        JsonNode endpoint = input.get("endpoint");
        if (endpoint != null && !endpoint.isTextual()) {
            throw ServiceActionSupport.invalid("'endpoint' must be a string", "/endpoint");
        }
        try {
            ServiceProfile refreshed = ServiceActionSupport.reflectAndStore(profile,
                    endpoint == null ? null : endpoint.asText(), ServiceActionSupport.deadline(input),
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
