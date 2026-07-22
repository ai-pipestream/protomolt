package ai.pipestream.proto.grpc.invoke;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Base64;
import java.util.function.Function;

/**
 * {@code reflect}: ask a live gRPC server for its own schema over the server-reflection protocol,
 * so an agent can operate a service given nothing but its address. Returns the advertised service
 * names and a base64 descriptor set that feeds straight into {@code list-types}, {@code grpc-invoke},
 * and {@code generate-stubs}.
 *
 * <p>This is the verb that removes the last precondition from "any gRPC service is an MCP
 * integration": no schema needs to be registered or pasted first. Servers that do not enable
 * reflection return {@code ok: false} with the reason, rather than an error.</p>
 */
public final class ReflectAction implements ProtoAction {

    private static final int DEFAULT_DEADLINE_MS = 15_000;

    private final ChannelFactory channelFactory;

    public ReflectAction() {
        this(ChannelFactory.standard());
    }

    /** Visible for tests and custom transports: maps a target string to a channel. */
    public ReflectAction(Function<String, ManagedChannel> channelFactory) {
        this((target, tls) -> channelFactory.apply(target));
    }

    /** Full transport control: the factory sees the verb's {@code tls} input. */
    public ReflectAction(ChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    @Override
    public String name() {
        return "reflect";
    }

    @Override
    public String description() {
        return "Fetches a live gRPC server's own schema over the server-reflection protocol, "
                + "given only its address. Returns the service names and a base64 descriptor set "
                + "usable directly as the 'schema' input to list-types, grpc-invoke, and "
                + "generate-stubs. Servers without reflection enabled return ok:false.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("target")
                .put("type", "string")
                .put("description", "gRPC target, e.g. 'localhost:9090' or 'dns:///svc:443'.");
        properties.putObject("deadlineMs")
                .put("type", "integer")
                .put("description", "Reflection deadline in milliseconds; default " + DEFAULT_DEADLINE_MS + ".");
        properties.putObject("tls")
                .put("type", "boolean")
                .put("default", false)
                .put("description", "Connect with TLS (system trust roots); plaintext by default.");
        schema.putArray("required").add("target");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        JsonNode targetNode = input.get("target");
        if (targetNode == null || !targetNode.isTextual() || targetNode.asText().isBlank()) {
            throw invalidInput("'target' must be a non-empty string", "/target");
        }
        String target = targetNode.asText();
        long deadlineMs = DEFAULT_DEADLINE_MS;
        JsonNode deadlineNode = input.get("deadlineMs");
        if (deadlineNode != null && !deadlineNode.isNull()) {
            if (!deadlineNode.canConvertToInt() || deadlineNode.asInt() <= 0) {
                throw invalidInput("'deadlineMs' must be a positive integer", "/deadlineMs");
            }
            deadlineMs = deadlineNode.asInt();
        }

        ObjectNode result = context.objectMapper().createObjectNode();
        boolean tls = input.path("tls").asBoolean(false);
        ManagedChannel channel = channelFactory.open(target, tls);
        try {
            ReflectionClient.Result discovered = ReflectionClient.discover(channel, deadlineMs);
            result.put("ok", true);
            ArrayNode services = result.putArray("services");
            discovered.services().forEach(services::add);
            result.put("descriptorSetBase64",
                    Base64.getEncoder().encodeToString(discovered.descriptorSet().toByteArray()));
            result.put("fileCount", discovered.descriptorSet().getFileCount());
        } catch (ReflectionException e) {
            result.put("ok", false);
            result.put("error", e.getMessage());
        } finally {
            channel.shutdownNow();
        }
        return result;
    }

    private static ActionException invalidInput(String message, String pointer) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("pointer", pointer);
        return new ActionException("invalid-input", message + " (at '" + pointer + "')", details);
    }
}
