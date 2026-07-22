package ai.pipestream.proto.server;

import ai.pipestream.proto.json.MalformedProtobufJsonException;
import ai.pipestream.proto.json.ProtobufJsonException;
import ai.pipestream.proto.rest.HttpMethodNotAllowedException;
import ai.pipestream.proto.rest.MalformedRequestException;
import ai.pipestream.proto.rest.MethodNotFoundException;
import ai.pipestream.proto.rest.ProtoRestException;
import ai.pipestream.proto.rest.RequestTooLargeException;
import ai.pipestream.proto.rest.ServiceNotFoundException;
import ai.pipestream.proto.rest.UnauthorizedProtoRestException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Shared HTTP helpers for all server hosts — status mapping, path parse, query/header flatten.
 */
public final class ProtoRestHttpSupport {

    /** HTTP methods the OpenAPI generator may document via {@code @ProtoRestExposed(httpMethods=...)}. */
    private static final Set<String> ALLOWED_HTTP_METHODS = Set.of("GET", "POST", "PUT", "PATCH", "DELETE");

    /** {@code Allow} header value for the REST invoke routes when no per-method verbs apply. */
    public static final String REST_ALLOW_HEADER = "GET, POST, PUT, PATCH, DELETE";

    /** Body sent to clients for any 5xx; details are logged server-side, never leaked. */
    public static final String INTERNAL_ERROR_MESSAGE = "Internal server error";

    private static final ObjectMapper JSON = new ObjectMapper();

    private ProtoRestHttpSupport() {
    }

    public static boolean isAllowedHttpMethod(String method) {
        return method != null && ALLOWED_HTTP_METHODS.contains(method.toUpperCase(Locale.ROOT));
    }

    /**
     * @return the request body, or {@code "{}"} when absent/blank (GET/DELETE typically carry none)
     */
    public static String bodyOrEmptyJson(String body) {
        return body == null || body.isBlank() ? "{}" : body;
    }

    /**
     * @throws RequestTooLargeException when the body's UTF-8 length exceeds {@code maxRequestBytes}
     */
    public static void checkBodySize(String body, int maxRequestBytes) {
        if (body != null && body.getBytes(StandardCharsets.UTF_8).length > maxRequestBytes) {
            throw new RequestTooLargeException(maxRequestBytes);
        }
    }

    /**
     * @return {@code [service, method]} or empty if the path is not {@code prefix/service/method}
     *         (trailing slashes are rejected: {@code prefix/S/M/} is a 404 on every host)
     */
    public static Optional<String[]> parseServiceMethod(String path, String restPathPrefix) {
        if (path == null || !path.startsWith(restPathPrefix)) {
            return Optional.empty();
        }
        String remainder = path.substring(restPathPrefix.length());
        if (remainder.startsWith("/")) {
            remainder = remainder.substring(1);
        } else if (!restPathPrefix.endsWith("/")) {
            // The prefix must be a whole path segment: /grpc-jsonFoo/Bar is not /grpc-json.
            return Optional.empty();
        }
        String[] parts = remainder.split("/", -1);
        if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
            return Optional.empty();
        }
        return Optional.of(parts);
    }

    /**
     * @throws MalformedRequestException on malformed percent-encoding (mapped to 400)
     */
    public static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> out = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return out;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            // Keep the first value for a repeated parameter, matching the host contract.
            out.putIfAbsent(decode(pair.substring(0, eq)), decode(pair.substring(eq + 1)));
        }
        return out;
    }

    public static Map<String, String> normalizeHeaders(Map<String, String> headers) {
        Map<String, String> out = new HashMap<>();
        if (headers == null) {
            return out;
        }
        headers.forEach((k, v) -> {
            if (k != null && v != null) {
                out.put(k.toLowerCase(Locale.ROOT), v);
            }
        });
        return out;
    }

    public static int statusFor(Throwable err) {
        Throwable cause = unwrap(err);
        if (cause instanceof UnauthorizedProtoRestException) {
            return 401;
        }
        if (cause instanceof ServiceNotFoundException || cause instanceof MethodNotFoundException) {
            return 404;
        }
        if (cause instanceof HttpMethodNotAllowedException) {
            return 405;
        }
        if (cause instanceof RequestTooLargeException) {
            return 413;
        }
        if (cause instanceof MalformedProtobufJsonException || cause instanceof MalformedRequestException) {
            return 400;
        }
        // Everything else is a server fault: a plain ProtobufJsonException means the server
        // failed to serialize its own response, and a plain ProtoRestException is an
        // invocation failure. Neither is client-repairable.
        return 500;
    }

    /**
     * @return the {@code Allow} header value for a 405, when {@code err} maps to one
     */
    public static Optional<String> allowHeaderFor(Throwable err) {
        Throwable cause = unwrap(err);
        if (cause instanceof HttpMethodNotAllowedException notAllowed) {
            return Optional.of(String.join(", ", notAllowed.allowedMethods()));
        }
        return Optional.empty();
    }

    /**
     * Builds the JSON error body for {@code err}. For 5xx the body is always the generic
     * {@value #INTERNAL_ERROR_MESSAGE}; internal exception details must never reach clients
     * (log them server-side via {@link #logIfServerError(Logger, Throwable)} at the catch site).
     */
    public static String errorJson(Throwable err) {
        Throwable cause = unwrap(err);
        int status = statusFor(cause);
        String message;
        if (status >= 500) {
            message = INTERNAL_ERROR_MESSAGE;
        } else {
            message = cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
        }
        ObjectNode node = JSON.createObjectNode();
        node.put("error", message);
        node.put("status", status);
        try {
            return JSON.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            // A node of a string and an int cannot fail to serialize; treat as a server fault.
            throw new IllegalStateException("failed to serialize error response JSON", e);
        }
    }

    /**
     * Logs {@code err} (with stack trace) at ERROR when it maps to a 5xx. Call at the
     * point of catch in every host so server faults are diagnosable despite the generic
     * client body.
     */
    public static void logIfServerError(Logger log, Throwable err) {
        if (statusFor(err) >= 500) {
            log.error("Request failed with an internal error", err);
        }
    }

    public static Throwable unwrap(Throwable err) {
        Throwable walk = err;
        while (walk != null) {
            if (walk instanceof ProtoRestException || walk instanceof ProtobufJsonException) {
                return walk;
            }
            walk = walk.getCause();
        }
        return err;
    }

    private static String decode(String value) {
        try {
            return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new MalformedRequestException("Malformed percent-encoding in query string", e);
        }
    }
}
