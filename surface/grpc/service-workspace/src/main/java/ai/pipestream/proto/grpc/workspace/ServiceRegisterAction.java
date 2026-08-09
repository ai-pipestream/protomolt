package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.invoke.ReflectionException;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Registers a reflected gRPC service without returning its descriptor bytes to the caller. */
public final class ServiceRegisterAction implements ProtoAction {

    private final ServiceProfileRepository repository;
    private final ChannelFactory channels;

    public ServiceRegisterAction(ServiceProfileRepository repository, ChannelFactory channels) {
        this.repository = repository;
        this.channels = channels;
    }

    @Override
    public String name() {
        return "service-register";
    }

    @Override
    public String description() {
        return "Registers a stable gRPC service profile, reflects one named endpoint, stores the "
                + "descriptor set as a content-addressed artifact, and returns only profile and "
                + "schema summaries. Credential and key fields are opaque host references.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ServiceActionSupport.baseSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("profile")
                .put("type", "object")
                .put("description", "ServiceProfile proto3 JSON. schemaSource is replaced by the reflected descriptor identity.");
        properties.putObject("endpoint")
                .put("type", "string")
                .put("description", "Endpoint name to reflect; defaults to the first profile endpoint.");
        properties.putObject("deadlineMs").put("type", "integer").put("minimum", 1)
                .put("maximum", ServiceActionSupport.MAX_DEADLINE_MS)
                .put("default", ServiceActionSupport.DEFAULT_DEADLINE_MS);
        schema.putArray("required").add("profile");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ServiceProfileRepository store = ServiceActionSupport.requireRepository(repository);
        ServiceProfile profile = ServiceActionSupport.parseProfile(input);
        JsonNode endpoint = input.get("endpoint");
        if (endpoint != null && !endpoint.isTextual()) {
            throw ServiceActionSupport.invalid("'endpoint' must be a string", "/endpoint");
        }
        try {
            ServiceProfile saved = ServiceActionSupport.reflectAndStore(profile,
                    endpoint == null ? null : endpoint.asText(), ServiceActionSupport.deadline(input),
                    store, channels);
            ObjectNode result = context.objectMapper().createObjectNode();
            result.put("ok", true);
            result.set("profile", ServiceActionSupport.profileJson(saved, context.objectMapper()));
            result.set("services", ServiceActionSupport.services(saved, store, context.objectMapper()));
            return result;
        } catch (ReflectionException e) {
            ObjectNode result = context.objectMapper().createObjectNode();
            result.put("ok", false);
            result.put("error", e.getMessage());
            return result;
        }
    }
}
