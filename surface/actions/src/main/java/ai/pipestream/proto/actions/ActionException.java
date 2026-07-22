package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;
import java.util.Optional;

/**
 * Structured action failure: a stable kebab-case {@code code} (e.g. {@code "unknown-type"},
 * {@code "invalid-input"}, {@code "compile-failed"}), a human-readable message, and an optional
 * {@code details} document with machine-readable specifics (offending JSON pointer, compiler
 * output, near-miss suggestions).
 */
public class ActionException extends Exception {

    private final String code;
    private final transient ObjectNode details;

    public ActionException(String code, String message) {
        this(code, message, null);
    }

    public ActionException(String code, String message, ObjectNode details) {
        super(message);
        this.code = Objects.requireNonNull(code, "code");
        this.details = details;
    }

    /** Stable kebab-case error code. */
    public String code() {
        return code;
    }

    /** Machine-readable specifics, when the failure carries any. */
    public Optional<ObjectNode> details() {
        return Optional.ofNullable(details);
    }

    /** The error as a JSON document: {@code {error, message, details?}} — the mount-ready shape. */
    public ObjectNode toJson(ObjectMapper mapper) {
        ObjectNode node = mapper.createObjectNode();
        node.put("error", code);
        node.put("message", getMessage());
        if (details != null) {
            node.set("details", details);
        }
        return node;
    }
}
