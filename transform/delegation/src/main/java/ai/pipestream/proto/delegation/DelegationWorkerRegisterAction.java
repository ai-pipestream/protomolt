package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.WorkerCapability;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Registers a worker on the delegation bridge: opens a real delegation stream, sends
 * the hello, and returns the coordinator's admission decision. The worker session
 * outlives any MCP session; one live stream per worker id.
 */
final class DelegationWorkerRegisterAction extends DelegationAction {

    DelegationWorkerRegisterAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-worker-register";
    }

    @Override
    public String description() {
        return "Registers this agent as a delegation worker: opens the worker stream, sends "
                + "the hello (identity, provider metadata, capabilities), and returns the "
                + "coordinator's admission decision. Check 'admitted' before accepting work.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = DelegationActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        putString(properties, "workerId",
                "Stable path-safe worker identity for this session.");
        putString(properties, "provider",
                "The provider the worker drives (for example 'kimi' or 'codex'). Metadata only.");
        putString(properties, "model", "The model the provider runs, when configured.");
        putString(properties, "modelVersion",
                "The provider-reported model version or digest, when reported.");
        ObjectNode capabilities = properties.putObject("capabilities");
        capabilities.put("type", "array").put("maxItems", 64)
                .put("description", "What the worker can do, most relevant first.");
        ObjectNode capability = capabilities.putObject("items");
        capability.put("type", "object");
        ObjectNode capabilityProperties = capability.putObject("properties");
        capabilityProperties.putObject("name").put("type", "string");
        capabilityProperties.putObject("description").put("type", "string");
        capability.putArray("required").add("name");
        require(schema, "workerId");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        WorkerHello.Builder hello = WorkerHello.newBuilder()
                .setWorkerId(DelegationActionJson.identity(input, "workerId"))
                .setProtocolVersion(1);
        String provider = DelegationActionJson.optionalText(input, "provider");
        if (provider != null) {
            hello.setProvider(provider);
        }
        String model = DelegationActionJson.optionalText(input, "model");
        if (model != null) {
            hello.setModel(model);
        }
        String modelVersion = DelegationActionJson.optionalText(input, "modelVersion");
        if (modelVersion != null) {
            hello.setModelVersion(modelVersion);
        }
        JsonNode capabilities = input.get("capabilities");
        if (capabilities != null && !capabilities.isNull()) {
            if (!capabilities.isArray()) {
                throw DelegationActionJson.invalid("'capabilities' must be an array",
                        "/capabilities");
            }
            for (int i = 0; i < capabilities.size(); i++) {
                JsonNode entry = capabilities.get(i);
                if (!entry.isObject() || !entry.path("name").isTextual()) {
                    throw DelegationActionJson.invalid(
                            "capability entries must be objects with a name",
                            "/capabilities/" + i);
                }
                WorkerCapability.Builder capability = WorkerCapability.newBuilder()
                        .setName(entry.path("name").asText());
                if (entry.path("description").isTextual()) {
                    capability.setDescription(entry.path("description").asText());
                }
                hello.addCapabilities(capability);
            }
        }
        DelegationBridge.WorkerRegistration registration;
        try {
            registration = bridge.registerWorker(hello.build());
        } catch (RuntimeException e) {
            throw failure(hello.getWorkerId(), e);
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        output.put("workerId", registration.workerId());
        output.put("admitted", registration.admitted());
        if (registration.admitted()) {
            output.put("sessionId", registration.sessionId());
        } else {
            output.put("reason", registration.reason());
        }
        return output;
    }
}
