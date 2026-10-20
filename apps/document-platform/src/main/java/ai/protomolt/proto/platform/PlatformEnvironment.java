package ai.protomolt.proto.platform;

import ai.protomolt.proto.config.kafka.KafkaConfigSource;
import ai.protomolt.proto.search.embedding.VectorizationPolicy;
import ai.protomolt.proto.search.service.RepoDocumentMapping;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The document platform's own environment surface: the strict readers of the
 * platform's {@code DOCUMENT_PLATFORM_*} and operator variables that
 * {@link DocumentPlatform} maps onto module configs for the selected roles.
 * Every reader is a pure function of the environment map: absent means the
 * historical default, a set-but-malformed value is refused naming the
 * variable, and nothing here reads the process environment directly, so
 * tests never depend on the developer shell. Role gating stays in
 * {@link DocumentPlatform}: these readers only interpret values.
 */
final class PlatformEnvironment {

    private PlatformEnvironment() {
    }

    // ------------------------------------------------------------------
    // Operator access
    // ------------------------------------------------------------------

    /** The operator token ({@code PROTOMOLT_API_TOKEN}), or null for an open node. */
    static String apiTokenFromEnvironment(Map<String, String> environment) {
        String value = environment.get(DocumentPlatformConfig.ENV_API_TOKEN);
        if (value != null && value.isBlank()) {
            throw new IllegalArgumentException(DocumentPlatformConfig.ENV_API_TOKEN
                    + " must be nonblank when set; omit it for an open node");
        }
        return value;
    }

    /** The boot access-policy file ({@code PROTOMOLT_ACCESS_POLICY}), or null. */
    static Path accessPolicyFromEnvironment(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_ACCESS_POLICY, "").trim();
        return value.isEmpty() ? null : Path.of(value);
    }

    /**
     * The operations panel's explicitly configured remote actions route
     * ({@code DOCUMENT_PLATFORM_ACTIONS_URL}), or empty when the co-mounted
     * registry serves it or the panel is disabled.
     */
    static String actionsUrl(Map<String, String> environment) {
        return environment.getOrDefault(DocumentPlatformConfig.ENV_ACTIONS_URL, "");
    }

    // ------------------------------------------------------------------
    // The config lane
    // ------------------------------------------------------------------

    /**
     * The strict read of
     * {@link DocumentPlatformConfig#ENV_CONFIG_REFRESH_SECONDS}: absent is
     * environment-only configuration ({@code 0}), anything set must be a
     * positive whole number of seconds.
     */
    static long configRefreshSeconds(Map<String, String> environment) {
        return positiveSeconds(environment, DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS,
                "unset it for environment-only configuration");
    }

    /**
     * The remote registry's native route prefix for config documents
     * ({@code DOCUMENT_PLATFORM_CONFIG_URL}), trimmed; empty when unset.
     */
    static String configUrl(Map<String, String> environment) {
        return environment.getOrDefault(DocumentPlatformConfig.ENV_CONFIG_URL, "").trim();
    }

    /**
     * The strict read of the config lane's Kafka family. Absent bootstrap
     * servers means the registry source, exactly as before. Set, the family
     * must be complete (the serde's schema registry URL is required, because
     * the lane is verify-then-swap or nothing) and must not contradict the
     * registry source's URL: two sources named is a contradiction, refused
     * by name, never resolved by preference.
     */
    static KafkaConfigSource.Config kafkaConfigFamily(Map<String, String> environment) {
        String bootstrap = environment.getOrDefault(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS, "").trim();
        if (bootstrap.isEmpty()) {
            return null;
        }
        String configUrl = configUrl(environment);
        if (!configUrl.isEmpty()) {
            throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS + " and "
                            + DocumentPlatformConfig.ENV_CONFIG_URL + " are both set:"
                            + " the config lane reads one source, so unset one");
        }
        String schemaRegistry = environment.getOrDefault(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_SCHEMA_REGISTRY_URL, "").trim();
        if (schemaRegistry.isEmpty()) {
            throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_CONFIG_KAFKA_SCHEMA_REGISTRY_URL
                            + " is required with "
                            + DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS
                            + ": the config serde verifies against the registry, and"
                            + " the lane is verify-then-swap or nothing");
        }
        String topic = environment.getOrDefault(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_TOPIC, "").trim();
        return new KafkaConfigSource.Config(
                bootstrap,
                topic.isEmpty()
                        ? DocumentPlatformConfig.DEFAULT_CONFIG_KAFKA_TOPIC : topic,
                schemaRegistry);
    }

    // ------------------------------------------------------------------
    // The search role
    // ------------------------------------------------------------------

    /** The strict read of {@link DocumentPlatformConfig#ENV_SEARCH_READ_ONLY}. */
    static boolean searchReadOnly(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_SEARCH_READ_ONLY, "").trim();
        return switch (value) {
            case "", "false" -> false;
            case "true" -> true;
            default -> throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_SEARCH_READ_ONLY
                            + " must be true or false, got '" + value + "'");
        };
    }

    /**
     * The strict read of
     * {@link DocumentPlatformConfig#ENV_SEARCH_REFRESH_SECONDS}: absent is
     * restart-only ({@code 0}), anything set must be a positive whole
     * number of seconds.
     */
    static long searchRefreshSeconds(Map<String, String> environment) {
        return positiveSeconds(environment, DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS,
                "unset it for restart-only");
    }

    /**
     * Which sensitivity classes this node may vectorize
     * ({@code DOCUMENT_PLATFORM_SEARCH_VECTORIZE_SENSITIVITY}). Absent means
     * unclassified content only; the single value {@code *} permits every class.
     */
    static VectorizationPolicy vectorizationFromEnvironment(
            Map<String, String> environment) {
        String value = environment.getOrDefault(
                DocumentPlatformConfig.ENV_SEARCH_VECTORIZE_SENSITIVITY, "").trim();
        if (value.isEmpty()) {
            // The platform's own decision, stated once rather than assumed: the
            // repo document's body is classified 'screened', which marks the
            // screening policy that applies to it rather than a restriction, and
            // indexing that body is what this product does. Every other class
            // still has to be named.
            return VectorizationPolicy.permitting(
                    Set.of(RepoDocumentMapping.BODY_SENSITIVITY));
        }
        if (value.equals("*")) {
            return VectorizationPolicy.unrestricted();
        }
        Set<String> classes = new LinkedHashSet<>();
        for (String entry : commaSeparated(value)) {
            String sensitivity = entry.trim();
            if (!sensitivity.isEmpty()) {
                classes.add(sensitivity);
            }
        }
        if (classes.isEmpty()) {
            throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_SEARCH_VECTORIZE_SENSITIVITY
                            + " names no sensitivity class; unset it to vectorize"
                            + " unclassified content only");
        }
        return VectorizationPolicy.permitting(classes);
    }

    // ------------------------------------------------------------------
    // The search document gate
    // ------------------------------------------------------------------

    /**
     * The strict read of {@link DocumentPlatformConfig#ENV_TAXONOMIES}: a
     * comma-separated list of taxonomy names, trimmed, blanks skipped,
     * duplicates collapsed in first-seen order. Absent means no taxonomy
     * follow and no document gate, exactly as before.
     */
    static List<String> taxonomiesFromEnvironment(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_TAXONOMIES, "").trim();
        if (value.isEmpty()) {
            return List.of();
        }
        LinkedHashSet<String> names = new LinkedHashSet<>();
        for (String part : commaSeparated(value)) {
            String name = part.trim();
            if (!name.isEmpty()) {
                names.add(name);
            }
        }
        if (names.isEmpty()) {
            throw new IllegalArgumentException(DocumentPlatformConfig.ENV_TAXONOMIES
                    + " is set but names no taxonomy; unset it or name at least one");
        }
        return List.copyOf(names);
    }

    /**
     * The strict read of {@link DocumentPlatformConfig#ENV_SCREENING}: one
     * screening mount name, trimmed. Absent means no screening, exactly as
     * before.
     */
    static String screeningFromEnvironment(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_SCREENING, "").trim();
        return value.isEmpty() ? null : value;
    }

    /** The strict read of DOCUMENT_PLATFORM_POSTAL_CODES: true, or unset. */
    static boolean postalCodesFromEnvironment(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_POSTAL_CODES, "").trim();
        if (value.isEmpty()) {
            return false;
        }
        if ("true".equals(value)) {
            return true;
        }
        throw new IllegalArgumentException(DocumentPlatformConfig.ENV_POSTAL_CODES
                + " must be 'true' or unset, not '" + value + "'");
    }

    // ------------------------------------------------------------------
    // The metrics lake
    // ------------------------------------------------------------------

    /** The default local metrics lake directory, per the environment. */
    static String metricsLakeDir(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_METRICS_LAKE_DIR, "").trim();
        return value.isEmpty() ? DocumentPlatformConfig.DEFAULT_METRICS_LAKE_DIR : value;
    }

    // ------------------------------------------------------------------
    // Parsing primitives
    // ------------------------------------------------------------------

    /**
     * A positive-seconds interval: absent or blank is {@code 0}, a malformed
     * value is refused naming the variable, and a non-positive value is
     * refused with the family-specific hint for turning the feature off.
     */
    private static long positiveSeconds(Map<String, String> environment, String name,
            String unsetHint) {
        String value = environment.getOrDefault(name, "").trim();
        if (value.isEmpty()) {
            return 0L;
        }
        long seconds;
        try {
            seconds = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    name + " must be a positive number of seconds, got '" + value + "'");
        }
        if (seconds <= 0) {
            throw new IllegalArgumentException(
                    name + " must be a positive number of seconds, got '" + value
                            + "'; " + unsetHint);
        }
        return seconds;
    }

    /**
     * Comma-separated segments without regex: no escaping, empty segments
     * kept so callers trim and skip blanks exactly as before.
     */
    private static List<String> commaSeparated(String value) {
        List<String> parts = new ArrayList<>();
        int from = 0;
        while (true) {
            int comma = value.indexOf(',', from);
            if (comma < 0) {
                parts.add(value.substring(from));
                return parts;
            }
            parts.add(value.substring(from, comma));
            from = comma + 1;
        }
    }
}
