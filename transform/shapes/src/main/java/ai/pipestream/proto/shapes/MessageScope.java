package ai.pipestream.proto.shapes;

import com.google.protobuf.Message;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An ordered set of named messages — the resolution scope every combination surface shares.
 * Scoped mapping rules read {@code target = name.path} where the first path segment names an
 * entry here; CEL expressions see each entry as a variable of its message type.
 */
public final class MessageScope {

    private final Map<String, Message> entries;

    private MessageScope(Map<String, Message> entries) {
        this.entries = entries;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The entry, or {@code null} when the scope has no such name. */
    public Message get(String name) {
        return entries.get(name);
    }

    public Set<String> names() {
        return entries.keySet();
    }

    /** The entries as CEL bindings (name to message). */
    public Map<String, Object> asBindings() {
        return Collections.unmodifiableMap(entries);
    }

    public static final class Builder {
        private final Map<String, Message> entries = new LinkedHashMap<>();

        public Builder add(String name, Message message) {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(message, "message");
            if (entries.putIfAbsent(name, message) != null) {
                throw new IllegalArgumentException("Duplicate scope entry: " + name);
            }
            return this;
        }

        public MessageScope build() {
            if (entries.isEmpty()) {
                throw new IllegalStateException("A scope needs at least one entry");
            }
            return new MessageScope(new LinkedHashMap<>(entries));
        }
    }
}
