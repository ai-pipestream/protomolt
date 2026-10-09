package ai.protomolt.proto.repo.blob.spi;

import java.util.Map;
import java.util.Objects;

/**
 * Versioned physical location descriptor produced by a trusted provider.
 * Fields must contain only canonical, nonsecret location information, never
 * credentials or credential references. This value does not qualify a backend
 * for managed retention, grant access, or own a client. The host assigns the
 * immutable generation and storage realm separately.
 */
public record BackendIdentity(String provider, String schema, Map<String, String> location) {
    public BackendIdentity {
        if (provider == null || !provider.matches("[a-z][a-z0-9-]{0,31}"))
            throw new IllegalArgumentException("Invalid backend identity provider");
        if (schema == null || !schema.matches("[a-z][a-z0-9-]{0,31}/v[1-9][0-9]{0,8}")
                || !schema.startsWith(provider + "/"))
            throw new IllegalArgumentException("Invalid backend identity schema");
        Objects.requireNonNull(location, "location");
        if (location.isEmpty() || location.size() > 32)
            throw new IllegalArgumentException("Backend identity requires bounded location fields");
        for (var field : location.entrySet()) {
            if (field.getKey() == null || !field.getKey().matches("[a-z][a-z0-9-]{0,63}")
                    || field.getValue() == null || field.getValue().length() > 4096)
                throw new IllegalArgumentException("Invalid backend identity location field");
        }
        location = Map.copyOf(location);
    }
}
