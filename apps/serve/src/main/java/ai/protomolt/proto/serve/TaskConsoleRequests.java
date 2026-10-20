package ai.protomolt.proto.serve;

import ai.protomolt.proto.delegation.v1.TaskMessageKind;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * The inbound decoding of the task console API: bounded bodies, bounded text fields, query
 * parameters, task-id UUIDs, and the message-kind token. Every refusal is an
 * {@link IllegalArgumentException} naming the field, which the handler maps to a 400. A body
 * over its byte bound reads as null so the handler can answer 413 instead.
 */
final class TaskConsoleRequests {

    private static final ObjectMapper JSON = new ObjectMapper();

    private TaskConsoleRequests() {
    }

    /**
     * Reads the request body up to {@code maxBytes}, or returns null when the body exceeds the
     * bound; the caller answers 413 and returns.
     */
    static byte[] readBody(HttpExchange exchange, int maxBytes) throws IOException {
        return BoundedBodies.read(exchange.getRequestBody(), maxBytes);
    }

    /**
     * Reads the request body as a JSON object. Null means the body exceeded the bound (the
     * caller answers 413); anything unparseable or not an object is refused with
     * {@code what} naming the route's payload.
     */
    static JsonNode readObjectBody(HttpExchange exchange, int maxBytes, String what)
            throws IOException {
        byte[] body = readBody(exchange, maxBytes);
        if (body == null) {
            return null;
        }
        JsonNode parsed;
        try {
            parsed = JSON.readTree(body);
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid JSON");
        }
        if (parsed == null || !parsed.isObject()) {
            throw new IllegalArgumentException(what + " body must be a JSON object");
        }
        return parsed;
    }

    /** The named field as bounded text, refusing a missing or blank value. */
    static String requiredText(JsonNode object, String field, int maxLength) {
        String value = optionalText(object, field, maxLength);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    /** The named field as bounded text, or the empty string when absent or null. */
    static String optionalText(JsonNode object, String field, int maxLength) {
        JsonNode node = object.get(field);
        if (node == null || node.isNull()) {
            return "";
        }
        if (!node.isTextual() || node.asText().length() > maxLength) {
            throw new IllegalArgumentException(field + " must be a bounded string");
        }
        return node.asText();
    }

    /** The named field as a positive 32-bit integer, refusing anything else. */
    static int requiredPositiveInt(JsonNode parsed, String name) {
        JsonNode value = parsed.get(name);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()
                || value.intValue() < 1) {
            throw new IllegalArgumentException(name + " must be a positive 32-bit integer");
        }
        return value.intValue();
    }

    /** The optional {@code failedChecks} array as bounded check names. */
    static List<String> failedChecks(JsonNode parsed) {
        JsonNode named = parsed.get("failedChecks");
        if (named == null || named.isNull()) {
            return List.of();
        }
        if (!named.isArray() || named.size() > 64) {
            throw new IllegalArgumentException(
                    "failedChecks must be an array of at most 64 check names");
        }
        List<String> checks = new ArrayList<>(named.size());
        for (JsonNode check : named) {
            if (!check.isTextual() || check.asText().isBlank()
                    || check.asText().length() > 128) {
                throw new IllegalArgumentException(
                        "each failed check is a non-empty name of at most 128 characters");
            }
            checks.add(check.asText());
        }
        return List.copyOf(checks);
    }

    /** The message-kind token, lower-cased; anything else is refused. */
    static TaskMessageKind messageKind(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "question" -> TaskMessageKind.TASK_MESSAGE_KIND_QUESTION;
            case "answer" -> TaskMessageKind.TASK_MESSAGE_KIND_ANSWER;
            case "guidance" -> TaskMessageKind.TASK_MESSAGE_KIND_GUIDANCE;
            case "note" -> TaskMessageKind.TASK_MESSAGE_KIND_NOTE;
            default -> throw new IllegalArgumentException(
                    "kind must be question, answer, guidance, or note");
        };
    }

    /** A possibly URL-encoded path or query segment as a canonical UUID string. */
    static String decodedUuid(String value) {
        String decoded = URLDecoder.decode(value, StandardCharsets.UTF_8);
        try {
            return UUID.fromString(decoded).toString();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("task id must be a UUID");
        }
    }

    /** The request's query string as decoded name/value pairs. */
    static Map<String, String> query(HttpExchange exchange) {
        Map<String, String> result = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null || raw.isBlank()) {
            return result;
        }
        for (String pair : ampersandSeparated(raw)) {
            int equals = pair.indexOf('=');
            String name = equals < 0 ? pair : pair.substring(0, equals);
            String value = equals < 0 ? "" : pair.substring(equals + 1);
            result.put(URLDecoder.decode(name, StandardCharsets.UTF_8),
                    URLDecoder.decode(value, StandardCharsets.UTF_8));
        }
        return result;
    }

    /**
     * Splits on the literal {@code &} with the semantics of {@code String.split} at limit 0:
     * interior empty pairs are kept, trailing empty pairs dropped.
     */
    private static List<String> ampersandSeparated(String raw) {
        List<String> pairs = new ArrayList<>();
        int start = 0;
        while (true) {
            int amp = raw.indexOf('&', start);
            if (amp < 0) {
                pairs.add(raw.substring(start));
                break;
            }
            pairs.add(raw.substring(start, amp));
            start = amp + 1;
        }
        int last = pairs.size();
        while (last > 0 && pairs.get(last - 1).isEmpty()) {
            last--;
        }
        return pairs.subList(0, last);
    }

    /**
     * A bounded integer parameter: absent or blank takes the fallback, a non-integer or an
     * out-of-range value is refused.
     */
    static long boundedLong(String value, long minimum, long maximum,
                            long fallback, String name) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed < minimum || parsed > maximum) {
                throw new IllegalArgumentException(name + " is outside its allowed range");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer");
        }
    }
}
