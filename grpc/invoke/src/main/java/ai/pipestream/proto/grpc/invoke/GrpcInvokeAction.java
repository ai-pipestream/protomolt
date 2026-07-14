package ai.pipestream.proto.grpc.invoke;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.SchemaResolver;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * {@code grpc-invoke}: call a unary or server-streaming gRPC method on a live server, driven
 * entirely by descriptors. The service is resolved from the standard schema-source convention
 * (inline sources, a descriptor set, or a registry type's file), the request is canonical
 * proto3 JSON, and no generated stubs are involved on either side of the call.
 *
 * <p>gRPC status failures are results ({@code ok: false} with the status name and description),
 * not action errors: an UNAVAILABLE backend is an outcome the caller needs to see, not an input
 * to repair. Input problems (unknown method, streaming shapes, malformed metadata) are
 * {@code invalid-input} action errors.</p>
 */
public final class GrpcInvokeAction implements ProtoAction {

    private static final int DEFAULT_DEADLINE_MS = 15_000;
    private static final int DEFAULT_MAX_RESPONSES = 64;

    private final Function<String, ManagedChannel> channelFactory;

    public GrpcInvokeAction() {
        this(target -> ManagedChannelBuilder.forTarget(target).usePlaintext().build());
    }

    /** Visible for tests and custom transports: maps a target string to a channel. */
    public GrpcInvokeAction(Function<String, ManagedChannel> channelFactory) {
        this.channelFactory = channelFactory;
    }

    @Override
    public String name() {
        return "grpc-invoke";
    }

    @Override
    public String description() {
        return "Invokes a unary or server-streaming gRPC method on a live server with no generated "
                + "stubs: the service comes from the schema source, the request is proto3 JSON, and "
                + "responses return as proto3 JSON. Plaintext by default. gRPC status failures "
                + "return ok:false with the status name rather than an error.";
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
        properties.putObject("method")
                .put("type", "string")
                .put("description", "Fully qualified method as 'package.Service/Method', "
                        + "e.g. 'grpc.health.v1.Health/Check'.");
        ObjectNode schemaSource = properties.putObject("schema");
        schemaSource.put("type", "object");
        schemaSource.put("description", "Schema source declaring the service; provide exactly one "
                + "of 'type', 'sources', 'descriptorSetBase64'. Reading the subject's resource from "
                + "the registry and passing its text as 'sources' works for any registered service.");
        properties.putObject("request")
                .put("type", "object")
                .put("description", "The request message as canonical proto3 JSON.");
        ObjectNode metadata = properties.putObject("metadata");
        metadata.put("type", "object");
        metadata.put("description", "Optional ASCII request headers, e.g. {\"authorization\": \"Bearer ...\"}.");
        metadata.putObject("additionalProperties").put("type", "string");
        properties.putObject("deadlineMs")
                .put("type", "integer")
                .put("description", "Call deadline in milliseconds; default " + DEFAULT_DEADLINE_MS + ".");
        properties.putObject("maxResponses")
                .put("type", "integer")
                .put("description", "Cap on collected server-streaming responses; default "
                        + DEFAULT_MAX_RESPONSES + ".");
        ArrayNode required = schema.putArray("required");
        required.add("target");
        required.add("method");
        required.add("schema");
        required.add("request");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String target = requireString(input, "target");
        String methodName = requireString(input, "method");
        JsonNode requestNode = input.get("request");
        if (requestNode == null || !requestNode.isObject()) {
            throw invalidInput("'request' must be the request message as a JSON object", "/request");
        }
        int deadlineMs = optionalInt(input, "deadlineMs", DEFAULT_DEADLINE_MS);
        int maxResponses = optionalInt(input, "maxResponses", DEFAULT_MAX_RESPONSES);

        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptors.MethodDescriptor method = findMethod(schema.files(), methodName);
        if (method.isClientStreaming()) {
            throw invalidInput("Method " + method.getFullName() + " is "
                    + DynamicGrpcCalls.methodType(method) + "; only unary and server-streaming "
                    + "methods can be invoked with a single request", "/method");
        }

        DynamicMessage request;
        try {
            request = context.transcoder().fromJsonDynamic(requestNode.toString(), method.getInputType());
        } catch (RuntimeException e) {
            throw invalidInput("Request does not parse as " + method.getInputType().getFullName()
                    + ": " + e.getMessage(), "/request");
        }
        Metadata headers = parseMetadata(input);

        ObjectNode result = context.objectMapper().createObjectNode();
        result.put("method", method.getFullName());
        result.put("methodType", DynamicGrpcCalls.methodType(method).name());
        ManagedChannel channel = channelFactory.apply(target);
        try {
            List<DynamicMessage> responses = DynamicGrpcCalls.call(
                    channel, method, request,
                    CallOptions.DEFAULT.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS),
                    headers, maxResponses);
            result.put("ok", true);
            result.put("status", "OK");
            ArrayNode out = result.putArray("responses");
            List<String> failures = new ArrayList<>();
            for (DynamicMessage response : responses) {
                try {
                    out.add(context.objectMapper().readTree(context.transcoder().toJson(response)));
                } catch (Exception e) {
                    failures.add(e.getMessage());
                }
            }
            if (!failures.isEmpty()) {
                result.put("ok", false);
                result.put("status", "RESPONSE_TRANSCODING_FAILED");
                result.put("description", String.join("; ", failures));
            }
        } catch (StatusRuntimeException e) {
            result.put("ok", false);
            result.put("status", e.getStatus().getCode().name());
            String description = e.getStatus().getDescription();
            if (description != null) {
                result.put("description", description);
            }
        } finally {
            channel.shutdownNow();
        }
        return result;
    }

    private static Descriptors.MethodDescriptor findMethod(
            List<Descriptors.FileDescriptor> files, String methodName) throws ActionException {
        int slash = methodName.lastIndexOf('/');
        if (slash <= 0 || slash == methodName.length() - 1) {
            throw invalidInput("'method' must be 'package.Service/Method' but was '"
                    + methodName + "'", "/method");
        }
        String serviceName = methodName.substring(0, slash);
        String simpleMethod = methodName.substring(slash + 1);
        List<String> available = new ArrayList<>();
        for (Descriptors.FileDescriptor file : files) {
            for (Descriptors.ServiceDescriptor service : file.getServices()) {
                for (Descriptors.MethodDescriptor method : service.getMethods()) {
                    available.add(service.getFullName() + "/" + method.getName());
                }
                if (service.getFullName().equals(serviceName)) {
                    Descriptors.MethodDescriptor method = service.findMethodByName(simpleMethod);
                    if (method != null) {
                        return method;
                    }
                }
            }
        }
        throw invalidInput("Method '" + methodName + "' not found in the schema. Available: "
                + (available.isEmpty() ? "(no services declared)" : String.join(", ", available)),
                "/method");
    }

    private static Metadata parseMetadata(ObjectNode input) throws ActionException {
        Metadata headers = new Metadata();
        JsonNode node = input.get("metadata");
        if (node == null || node.isNull()) {
            return headers;
        }
        if (!node.isObject()) {
            throw invalidInput("'metadata' must be an object of string values", "/metadata");
        }
        for (var it = node.properties().iterator(); it.hasNext(); ) {
            var entry = it.next();
            String key = entry.getKey();
            if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
                throw invalidInput("Binary metadata keys ('-bin') are not supported", "/metadata/" + key);
            }
            if (!entry.getValue().isTextual()) {
                throw invalidInput("Metadata values must be strings", "/metadata/" + key);
            }
            headers.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER),
                    entry.getValue().asText());
        }
        return headers;
    }

    private static String requireString(ObjectNode input, String field) throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || !node.isTextual() || node.asText().isBlank()) {
            throw invalidInput("'" + field + "' must be a non-empty string", "/" + field);
        }
        return node.asText();
    }

    private static int optionalInt(ObjectNode input, String field, int defaultValue)
            throws ActionException {
        JsonNode node = input.get(field);
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        if (!node.canConvertToInt() || node.asInt() <= 0) {
            throw invalidInput("'" + field + "' must be a positive integer", "/" + field);
        }
        return node.asInt();
    }

    // Mirrors the catalog's invalid-input envelope: {error, message, details: {pointer}}.
    private static ActionException invalidInput(String message, String pointer) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("pointer", pointer);
        return new ActionException("invalid-input", message + " (at '" + pointer + "')", details);
    }
}
