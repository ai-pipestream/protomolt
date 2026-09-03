package ai.protomolt.proto.search.index.qdrant;

import java.util.Objects;

/**
 * Connection configuration for {@link QdrantSink}, resolved from system properties with
 * environment-variable fallback (the platform convention):
 *
 * <ul>
 *   <li>{@value #TARGET_PROPERTY} / {@value #TARGET_ENVIRONMENT_VARIABLE} — Qdrant gRPC
 *       target ({@code host:port}); default {@value #DEFAULT_TARGET}.</li>
 *   <li>{@value #COLLECTION_PROPERTY} / {@value #COLLECTION_ENVIRONMENT_VARIABLE} —
 *       collection the sink writes to; default {@value #DEFAULT_COLLECTION}.</li>
 *   <li>{@value #API_KEY_PROPERTY} / {@value #API_KEY_ENVIRONMENT_VARIABLE} — optional API
 *       key, sent as the {@code api-key} header (Qdrant Cloud).</li>
 *   <li>{@value #USE_TLS_PROPERTY} / {@value #USE_TLS_ENVIRONMENT_VARIABLE} — TLS on the
 *       channel; defaults to {@code true} when an API key is set (Qdrant Cloud requires
 *       TLS) and {@code false} otherwise (plaintext for local).</li>
 * </ul>
 */
public record QdrantConfig(String target, String collection, String apiKey, boolean useTls) {

    /** System property naming the Qdrant gRPC target ({@code host:port}): {@value}. */
    public static final String TARGET_PROPERTY = "protomolt.index.qdrant.target";

    /** Environment variable consulted when {@link #TARGET_PROPERTY} is unset: {@value}. */
    public static final String TARGET_ENVIRONMENT_VARIABLE = "PROTOMOLT_QDRANT_TARGET";

    /** System property naming the collection to write: {@value}. */
    public static final String COLLECTION_PROPERTY = "protomolt.index.qdrant.collection";

    /** Environment variable consulted when {@link #COLLECTION_PROPERTY} is unset: {@value}. */
    public static final String COLLECTION_ENVIRONMENT_VARIABLE = "PROTOMOLT_QDRANT_COLLECTION";

    /** System property carrying the API key (Qdrant Cloud): {@value}. */
    public static final String API_KEY_PROPERTY = "protomolt.index.qdrant.api-key";

    /** Environment variable consulted when {@link #API_KEY_PROPERTY} is unset: {@value}. */
    public static final String API_KEY_ENVIRONMENT_VARIABLE = "PROTOMOLT_QDRANT_API_KEY";

    /** System property forcing TLS on or off: {@value}. */
    public static final String USE_TLS_PROPERTY = "protomolt.index.qdrant.use-tls";

    /** Environment variable consulted when {@link #USE_TLS_PROPERTY} is unset: {@value}. */
    public static final String USE_TLS_ENVIRONMENT_VARIABLE = "PROTOMOLT_QDRANT_USE_TLS";

    /** Target used when nothing is configured: {@value}. */
    public static final String DEFAULT_TARGET = "localhost:6334";

    /** Collection used when nothing is configured: {@value}. */
    public static final String DEFAULT_COLLECTION = "protomolt-chunks";

    public QdrantConfig {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(collection, "collection");
        if (target.isBlank()) {
            throw new IllegalArgumentException("target must not be blank");
        }
        if (collection.isBlank()) {
            throw new IllegalArgumentException("collection must not be blank");
        }
        apiKey = apiKey == null ? "" : apiKey;
    }

    /** Plaintext local configuration with defaults for everything but the target. */
    public static QdrantConfig plaintext(String target, String collection) {
        return new QdrantConfig(target, collection, "", false);
    }

    /** True when an API key is configured. */
    public boolean hasApiKey() {
        return !apiKey.isEmpty();
    }

    /** Resolves the configuration from system properties, then environment, then defaults. */
    public static QdrantConfig fromEnvironment() {
        String target = resolve(TARGET_PROPERTY, TARGET_ENVIRONMENT_VARIABLE);
        String collection = resolve(COLLECTION_PROPERTY, COLLECTION_ENVIRONMENT_VARIABLE);
        String apiKey = resolve(API_KEY_PROPERTY, API_KEY_ENVIRONMENT_VARIABLE);
        String tls = resolve(USE_TLS_PROPERTY, USE_TLS_ENVIRONMENT_VARIABLE);
        boolean useTls = tls != null ? Boolean.parseBoolean(tls) : apiKey != null && !apiKey.isBlank();
        return new QdrantConfig(
                target == null || target.isBlank() ? DEFAULT_TARGET : target,
                collection == null || collection.isBlank() ? DEFAULT_COLLECTION : collection,
                apiKey == null ? "" : apiKey,
                useTls);
    }

    private static String resolve(String property, String environmentVariable) {
        String value = System.getProperty(property);
        return value != null ? value : System.getenv(environmentVariable);
    }
}
