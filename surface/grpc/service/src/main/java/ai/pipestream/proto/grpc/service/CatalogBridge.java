package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import java.util.Locale;

/**
 * The message bridge between the typed service surface and the JSON action catalog.
 *
 * <p>Every request message's canonical proto3 JSON form is exactly the action's input envelope,
 * and every action's output envelope parses as the response message — so the bridge is one
 * print, one dispatch, one parse, for every verb alike.
 */
public final class CatalogBridge {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CatalogBridge() {
    }

    /** The catalog action name for an RPC: {@code ListTypes} → {@code list-types}. */
    public static String actionName(MethodDescriptor method) {
        String rpc = method.getName();
        StringBuilder name = new StringBuilder(rpc.length() + 4);
        for (int i = 0; i < rpc.length(); i++) {
            char c = rpc.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                name.append('-');
            }
            name.append(Character.toLowerCase(c));
        }
        return name.toString();
    }

    /**
     * Dispatches {@code request} to the action behind {@code method} and returns the result as
     * the method's output message.
     */
    public static DynamicMessage execute(ActionCatalog catalog, MethodDescriptor method,
                                         MessageOrBuilder request) throws ActionException {
        String action = actionName(method);
        ObjectNode input;
        try {
            String json = JsonFormat.printer().print(request);
            input = (ObjectNode) MAPPER.readTree(json.isBlank() ? "{}" : json);
        } catch (Exception e) {
            throw new ActionException("invalid-input",
                    "Request does not render as the '" + action + "' input envelope: "
                            + e.getMessage());
        }
        ObjectNode output = catalog.execute(action, input);
        Descriptor outputType = method.getOutputType();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(outputType);
        try {
            JsonFormat.parser().ignoringUnknownFields().merge(output.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("internal-error",
                    "Result of '" + action + "' does not parse as "
                            + outputType.getFullName() + ": " + e.getMessage());
        }
        return builder.build();
    }

    /** Maps an action failure onto a gRPC status: client-repairable codes are INVALID_ARGUMENT. */
    public static io.grpc.StatusRuntimeException toStatus(ActionException e) {
        io.grpc.Status status = switch (e.code().toLowerCase(Locale.ROOT)) {
            case "internal-error" -> io.grpc.Status.INTERNAL;
            case "unknown-action" -> io.grpc.Status.UNIMPLEMENTED;
            default -> io.grpc.Status.INVALID_ARGUMENT;
        };
        io.grpc.Metadata trailers = new io.grpc.Metadata();
        trailers.put(ERROR_CODE_KEY, e.code());
        e.details().ifPresent(details -> trailers.put(ERROR_DETAILS_KEY,
                details.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        return status.withDescription(e.code() + ": " + e.getMessage())
                .asRuntimeException(trailers);
    }

    /** Trailer carrying the stable kebab-case action error code. */
    public static final io.grpc.Metadata.Key<String> ERROR_CODE_KEY =
            io.grpc.Metadata.Key.of("protomolt-error", io.grpc.Metadata.ASCII_STRING_MARSHALLER);

    /** Trailer carrying the action error details document as UTF-8 JSON. */
    public static final io.grpc.Metadata.Key<byte[]> ERROR_DETAILS_KEY =
            io.grpc.Metadata.Key.of("protomolt-error-details-bin", io.grpc.Metadata.BINARY_BYTE_MARSHALLER);
}
