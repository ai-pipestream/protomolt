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
        if (!isLowerIdentifier(provider, 32))
            throw new IllegalArgumentException("Invalid backend identity provider");
        if (!isValidSchema(schema, provider))
            throw new IllegalArgumentException("Invalid backend identity schema");
        Objects.requireNonNull(location, "location");
        if (location.isEmpty() || location.size() > 32)
            throw new IllegalArgumentException("Backend identity requires bounded location fields");
        for (var field : location.entrySet()) {
            if (!isLowerIdentifier(field.getKey(), 64)
                    || field.getValue() == null || field.getValue().length() > 4096)
                throw new IllegalArgumentException("Invalid backend identity location field");
        }
        location = Map.copyOf(location);
    }

    private static boolean isLowerIdentifier(String value, int maxLength) {
        if (value == null || value.isEmpty() || value.length() > maxLength) return false;
        char first = value.charAt(0);
        if (first < 'a' || first > 'z') return false;
        for (int i = 1; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) {
                return false;
            }
        }
        return true;
    }

    private static boolean isValidSchema(String schema, String provider) {
        if (schema == null) return false;
        int slash = schema.indexOf('/');
        if (slash != provider.length() || !schema.startsWith(provider)) return false;
        int versionStart = slash + 2;
        if (schema.length() <= versionStart || schema.charAt(slash + 1) != 'v') return false;
        int digits = schema.length() - versionStart;
        if (digits < 1 || digits > 9) return false;
        char firstDigit = schema.charAt(versionStart);
        if (firstDigit < '1' || firstDigit > '9') return false;
        for (int i = versionStart + 1; i < schema.length(); i++) {
            char c = schema.charAt(i);
            if (c < '0' || c > '9') return false;
        }
        return true;
    }
}
