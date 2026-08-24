package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.Caller;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import java.util.Locale;

/**
 * The bridge between an RPC on the service and the catalog verb behind it.
 *
 * <p>Both sides speak messages, so the bridge names the verb and hands the request straight
 * to it. The only conversion left is the one the binding requires: a reply is re-read as a
 * dynamic message of the method's output type when the verb did not already answer with one.
 */
public final class CatalogBridge {

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
     * Dispatches {@code request} to the action behind {@code method} with process authority
     * and returns the result as the method's output message.
     */
    public static DynamicMessage execute(ActionCatalog catalog, MethodDescriptor method,
                                         MessageOrBuilder request) throws ActionException {
        return execute(catalog, method, request, Caller.operator());
    }

    /** Dispatches like {@link #execute}, as {@code caller}: the scope check runs first. */
    public static DynamicMessage execute(ActionCatalog catalog, MethodDescriptor method,
                                         MessageOrBuilder request, Caller caller)
            throws ActionException {
        Message message = request instanceof Message typed
                ? typed
                : ((Message.Builder) request).build();
        Message response = catalog.execute(actionName(method), message, caller);
        return asDynamic(response, method.getOutputType(), method);
    }

    /**
     * The response as a dynamic message of the method's output type.
     *
     * <p>A verb built on the compiled service definition already answers with one, and that
     * case costs nothing. A verb that answers with a generated message of the same type is
     * carried across by its encoding, because the binding's marshaller is built from the
     * descriptor and cannot write a class it was not given.
     */
    private static DynamicMessage asDynamic(Message response, Descriptor outputType,
                                            MethodDescriptor method) throws ActionException {
        if (response instanceof DynamicMessage dynamic
                && dynamic.getDescriptorForType() == outputType) {
            return dynamic;
        }
        if (!response.getDescriptorForType().getFullName().equals(outputType.getFullName())) {
            throw new ActionException("internal-error",
                    method.getName() + " answered with a "
                            + response.getDescriptorForType().getFullName() + ", not a "
                            + outputType.getFullName());
        }
        try {
            return DynamicMessage.parseFrom(outputType, response.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("internal-error",
                    "Result of " + method.getName() + " does not re-read as "
                            + outputType.getFullName() + ": " + e.getMessage());
        }
    }

    /** Maps an action failure onto a gRPC status: client-repairable codes are INVALID_ARGUMENT. */
    public static io.grpc.StatusRuntimeException toStatus(ActionException e) {
        io.grpc.Status status = switch (e.code().toLowerCase(Locale.ROOT)) {
            case "internal-error" -> io.grpc.Status.INTERNAL;
            case "unknown-action" -> io.grpc.Status.UNIMPLEMENTED;
            case "permission-denied" -> io.grpc.Status.PERMISSION_DENIED;
            case "resource-exhausted" -> io.grpc.Status.RESOURCE_EXHAUSTED;
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
