package ai.protomolt.proto.http.connect;

import com.google.protobuf.Any;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * The Connect protocol's error record and end-of-stream record. The protocol defines both as
 * JSON objects whatever the message codec, so they are the one place JSON is written on this
 * path; every message stays binary protobuf, and error details travel as base64 protobuf bytes
 * inside the record. Written by hand: the records have three fixed keys and string values.
 */
final class ConnectErrors {

    private ConnectErrors() {
    }

    /**
     * The Connect code name of a gRPC status code.
     *
     * @param code the gRPC code
     * @return the Connect code, as the protocol spells it
     */
    static String connectCode(Status.Code code) {
        return switch (code) {
            case CANCELLED -> "canceled";
            case UNKNOWN -> "unknown";
            case INVALID_ARGUMENT -> "invalid_argument";
            case DEADLINE_EXCEEDED -> "deadline_exceeded";
            case NOT_FOUND -> "not_found";
            case ALREADY_EXISTS -> "already_exists";
            case PERMISSION_DENIED -> "permission_denied";
            case RESOURCE_EXHAUSTED -> "resource_exhausted";
            case FAILED_PRECONDITION -> "failed_precondition";
            case ABORTED -> "aborted";
            case OUT_OF_RANGE -> "out_of_range";
            case UNIMPLEMENTED -> "unimplemented";
            case INTERNAL -> "internal";
            case UNAVAILABLE -> "unavailable";
            case DATA_LOSS -> "data_loss";
            case UNAUTHENTICATED -> "unauthenticated";
            case OK -> throw new IllegalArgumentException("OK is not an error code");
        };
    }

    /**
     * The HTTP status a unary error answers with, per the Connect protocol's code table.
     *
     * @param code the gRPC code
     * @return the HTTP status
     */
    static int httpStatus(Status.Code code) {
        return switch (code) {
            case CANCELLED -> 499;
            case INVALID_ARGUMENT, FAILED_PRECONDITION, OUT_OF_RANGE -> 400;
            case DEADLINE_EXCEEDED -> 504;
            case NOT_FOUND -> 404;
            case ALREADY_EXISTS, ABORTED -> 409;
            case PERMISSION_DENIED -> 403;
            case RESOURCE_EXHAUSTED -> 429;
            case UNIMPLEMENTED -> 501;
            case UNAVAILABLE -> 503;
            case UNAUTHENTICATED -> 401;
            case UNKNOWN, INTERNAL, DATA_LOSS -> 500;
            case OK -> throw new IllegalArgumentException("OK is not an error code");
        };
    }

    /**
     * The error record for a failed call: code, message when there is one, and the
     * {@code google.rpc.Status} details the service attached to its trailers.
     *
     * @param status the call's status; not OK
     * @param trailers the call's trailers, or null when the call never reached a service
     * @return the record as UTF-8 JSON bytes
     */
    static byte[] errorRecord(Status status, Metadata trailers) {
        return error(status, trailers).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * The end-of-stream record: empty on success, the error record under {@code error} on
     * failure.
     *
     * @param status the call's status
     * @param trailers the call's trailers, or null when the call never reached a service
     * @return the record as UTF-8 JSON bytes
     */
    static byte[] endOfStream(Status status, Metadata trailers) {
        String record = status.isOk() ? "{}" : "{\"error\":" + error(status, trailers) + "}";
        return record.getBytes(StandardCharsets.UTF_8);
    }

    private static String error(Status status, Metadata trailers) {
        StringBuilder out = new StringBuilder("{\"code\":");
        string(out, connectCode(status.getCode()));
        if (status.getDescription() != null && !status.getDescription().isEmpty()) {
            out.append(",\"message\":");
            string(out, status.getDescription());
        }
        List<Any> details = details(status, trailers);
        if (!details.isEmpty()) {
            out.append(",\"details\":[");
            Base64.Encoder base64 = Base64.getEncoder().withoutPadding();
            for (int i = 0; i < details.size(); i++) {
                Any detail = details.get(i);
                if (i > 0) {
                    out.append(',');
                }
                out.append("{\"type\":");
                string(out, typeName(detail.getTypeUrl()));
                out.append(",\"value\":");
                string(out, base64.encodeToString(detail.getValue().toByteArray()));
                out.append('}');
            }
            out.append(']');
        }
        return out.append('}').toString();
    }

    private static List<Any> details(Status status, Metadata trailers) {
        if (trailers == null) {
            return List.of();
        }
        com.google.rpc.Status rich = StatusProto.fromStatusAndTrailers(status, trailers);
        return rich == null ? List.of() : rich.getDetailsList();
    }

    /** Connect names a detail by its fully qualified message name, without the URL prefix. */
    private static String typeName(String typeUrl) {
        int slash = typeUrl.lastIndexOf('/');
        return slash < 0 ? typeUrl : typeUrl.substring(slash + 1);
    }

    /** A JSON string literal: quotes, backslash and control characters escaped. */
    private static void string(StringBuilder out, String value) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                default -> {
                    if (c < 0x20 || c == ' ' || c == ' ') {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
