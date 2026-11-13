package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.Locale;
import java.util.Optional;

/**
 * The five contract surfaces governed by ProtoMolt stability tiers and release-baseline
 * compatibility gates.
 */
@ApiStability(StabilityTier.STABLE)
public enum CompatibilitySurface {

    /** Exported Java packages, classes, interfaces, constructors, methods, and fields. */
    JAVA_API("java-api"),

    /** Wire and source protobuf contracts (messages, fields, enums, oneofs, services, methods). */
    PROTOBUF("protobuf"),

    /** Retained repository manifests, envelopes, archives, receipts, and descriptor closures. */
    STORED_FORMAT("stored-format"),

    /** HTTP/REST gateway routes, verbs, path/query parameters, status codes, and media types. */
    REST_ROUTE("rest-route"),

    /** Action catalog verbs, scopes, streaming modes, and input/output message schemas. */
    ACTION_ENVELOPE("action-envelope");

    private final String id;

    CompatibilitySurface(String id) {
        this.id = id;
    }

    /**
     * Stable kebab-case identifier used in baseline manifests and waiver records.
     *
     * @return the surface identifier
     */
    public String id() {
        return id;
    }

    /**
     * Parses a surface identifier from its enum name or kebab-case id.
     *
     * @param raw the raw surface string
     * @return matching surface, or empty if unknown
     */
    public static Optional<CompatibilitySurface> parseOptional(String raw) {
        if (raw == null) {
            return Optional.empty();
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return Optional.empty();
        }
        String normalized = trimmed.toUpperCase(Locale.ROOT).replace('-', '_');
        for (CompatibilitySurface surface : values()) {
            if (surface.name().equals(normalized) || surface.id.equalsIgnoreCase(trimmed)) {
                return Optional.of(surface);
            }
        }
        return Optional.empty();
    }
}
