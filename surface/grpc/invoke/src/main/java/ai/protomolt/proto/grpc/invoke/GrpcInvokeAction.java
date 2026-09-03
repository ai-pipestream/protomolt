package ai.protomolt.proto.grpc.invoke;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.SchemaResolver;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.actions.StreamEmitter;
import ai.protomolt.proto.actions.StreamingAction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.google.protobuf.Descriptors.Descriptor;
import java.time.Duration;
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
public final class GrpcInvokeAction implements StreamingAction {

    private static final int DEFAULT_DEADLINE_MS = 15_000;
    private static final int DEFAULT_MAX_RESPONSES = 64;

    private final ChannelFactory channelFactory;

    public GrpcInvokeAction() {
        this(ChannelFactory.standard());
    }

    /** Visible for tests and custom transports: maps a target string to a channel. */
    public GrpcInvokeAction(Function<String, ManagedChannel> channelFactory) {
        this((target, tls) -> channelFactory.apply(target));
    }

    /** Full transport control: the factory sees the verb's {@code tls} input. */
    public GrpcInvokeAction(ChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    @Override
    public String name() {
        return "grpc-invoke";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Invokes a unary or server-streaming gRPC method on a live server with no generated "
                + "stubs: the service comes from the schema source, the request is proto3 JSON, and "
                + "responses return as proto3 JSON. Plaintext by default. gRPC status failures "
                + "return ok:false with the status name rather than an error.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("GrpcInvokeRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("GrpcInvokeResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        CallPlan plan = prepare(input, context);

        Reply result = Reply.of(responseType())
                .set("method", plan.method().getFullName())
                .set("methodType", DynamicGrpcCalls.methodType(plan.method()).name());
        ManagedChannel channel;
        try {
            channel = channelFactory.open(plan.target(), plan.tls());
        } catch (IllegalArgumentException e) {
            throw invalidInput(e.getMessage(), "/target");
        }
        try {
            List<DynamicMessage> responses = DynamicGrpcCalls.call(
                    channel, plan.method(), plan.request(), plan.options(), plan.headers(),
                    plan.maxResponses());
            result.set("ok", true).set("status", "OK");
            List<String> failures = new ArrayList<>();
            for (DynamicMessage response : responses) {
                try {
                    // The invoked service's replies have no shape this contract knows, so
                    // they travel as structures.
                    result.add("responses", context.transcoder().toJson(response));
                } catch (Exception e) {
                    failures.add(e.getMessage());
                }
            }
            if (!failures.isEmpty()) {
                result.set("ok", false)
                        .set("status", "RESPONSE_TRANSCODING_FAILED")
                        .set("description", String.join("; ", failures));
            }
        } catch (StatusRuntimeException e) {
            result.set("ok", false).set("status", e.getStatus().getCode().name());
            String description = e.getStatus().getDescription();
            if (description != null) {
                result.set("description", description);
            }
        } finally {
            channel.shutdownNow();
        }
        return result.build();
    }

    /**
     * Streams each response as its own document as it arrives: unary methods emit their single
     * response, server-streaming methods emit per message. Every run ends with a terminal
     * status document ({@code {ok, status, description?}}), so stream consumers get a clean
     * end marker whether the call succeeded or not.
     */
    @Override
    public void executeStreaming(Message input, ActionContext context, StreamEmitter emitter)
            throws ActionException {
        CallPlan plan = prepare(input, context);
        ManagedChannel channel;
        try {
            channel = channelFactory.open(plan.target(), plan.tls());
        } catch (IllegalArgumentException e) {
            throw invalidInput(e.getMessage(), "/target");
        }
        try {
            if (!plan.method().isServerStreaming()) {
                List<DynamicMessage> responses = DynamicGrpcCalls.call(
                        channel, plan.method(), plan.request(), plan.options(), plan.headers(), 1);
                for (DynamicMessage response : responses) {
                    emitter.emit(oneResponse(context, response));
                }
            } else {
                try (DynamicGrpcStream stream = DynamicGrpcCalls.openServerStream(
                        channel, plan.method(), plan.request(), plan.options(), plan.headers())) {
                    while (!stream.isClosed()) {
                        for (DynamicMessage response
                                : stream.take(1, Duration.ofMillis(plan.deadlineMs()))) {
                            emitter.emit(oneResponse(context, response));
                        }
                    }
                    Status terminal = stream.terminalStatus();
                    if (terminal != null && !terminal.isOk()) {
                        emitter.emit(terminal(terminal));
                        return;
                    }
                }
            }
            emitter.emit(okTerminal());
        } catch (StatusRuntimeException e) {
            emitter.emit(terminal(e.getStatus()));
        } finally {
            channel.shutdownNow();
        }
    }

    /** One streamed reply, carried as a structure because its shape is the callee's. */
    private Message oneResponse(ActionContext context, DynamicMessage response) {
        try {
            return Reply.of(responseType())
                    .set("ok", true)
                    .set("status", "OK")
                    .add("responses", context.transcoder().toJson(response))
                    .build();
        } catch (Exception e) {
            return Reply.of(responseType())
                    .set("ok", false)
                    .set("status", "RESPONSE_TRANSCODING_FAILED")
                    .set("description", e.getMessage())
                    .build();
        }
    }

    private Message okTerminal() {
        return Reply.of(responseType()).set("ok", true).set("status", "OK").build();
    }

    private Message terminal(Status status) {
        Reply terminal = Reply.of(responseType())
                .set("ok", status.isOk())
                .set("status", status.getCode().name());
        if (status.getDescription() != null) {
            terminal.set("description", status.getDescription());
        }
        return terminal.build();
    }

    private CallPlan prepare(Message input, ActionContext context) throws ActionException {
        String target = Fields.string(input, "target");
        String methodName = Fields.string(input, "method");
        // The request is a structure: its shape is the callee's input type, which this
        // contract does not describe.
        ObjectNode requestNode = Fields.json(input, "request");
        // Zero selects the default for both bounds, as the message says.
        int deadlineMs = orDefault(Fields.integer(input, "deadlineMs"), DEFAULT_DEADLINE_MS);
        int maxResponses = orDefault(Fields.integer(input, "maxResponses"), DEFAULT_MAX_RESPONSES);
        boolean tls = Fields.flag(input, "tls");

        try {
            channelFactory.validateTarget(target, tls);
            channelFactory.validateDeadline(deadlineMs);
        } catch (IllegalArgumentException e) {
            throw invalidInput(e.getMessage(), e.getMessage().contains("deadline")
                    ? "/deadlineMs" : "/target");
        }

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
        return new CallPlan(method, request, headers(input), deadlineMs, maxResponses,
                target, tls);
    }

    /** Zero means the caller said nothing, which the message documents as the default. */
    private static int orDefault(int asked, int fallback) {
        return asked == 0 ? fallback : asked;
    }

    private record CallPlan(
            Descriptors.MethodDescriptor method,
            DynamicMessage request,
            Metadata headers,
            int deadlineMs,
            int maxResponses,
            String target,
            boolean tls) {

        CallOptions options() {
            return CallOptions.DEFAULT.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
        }
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

    private static Metadata headers(Message input) throws ActionException {
        Metadata headers = new Metadata();
        for (var entry : Fields.map(input, "metadata").entrySet()) {
            String key = entry.getKey();
            if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
                throw invalidInput("Binary metadata keys ('-bin') are not supported",
                        "/metadata/" + key);
            }
            headers.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), entry.getValue());
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
