package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.redis.RedisBackendIdentity;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Canonical identities of the stores behind a drive backing, so equivalent configurations
 * share one provider handle and conflicting ones can be refused.
 *
 * <p>S3 endpoints normalize through {@link S3BackendIdentity} and Redis/Valkey URIs through
 * {@link RedisBackendIdentity}; both provider aliases map to one Redis identity.
 */
final class DriveStoreIdentities {
    static final String OPT_URI = "uri";
    static final String OPT_ENDPOINT = "endpoint";
    static final String OPT_REGION = "region";
    static final String OPT_PATH_STYLE = "path-style";
    static final String OPT_CONDITIONAL_WRITES = "conditional-writes";
    private static final String PROVIDER_S3 = "s3";
    private static final String PROVIDER_REDIS = "redis";

    private DriveStoreIdentities() {
    }

    /**
     * Explicit canonical identity of an underlying {@link BlobStore} provider handle.
     *
     * <p>Compatible provider aliases ({@code valkey} and {@code redis}) and equivalent
     * normalized endpoint/URI spellings produce identical {@link StoreIdentity} values so
     * they share one physical client handle, while differing credentials, limits, write
     * policies, or timeouts remain distinct.
     */
    record StoreIdentity(String canonicalProvider, Map<String, String> canonicalOptions) {
        StoreIdentity {
            Objects.requireNonNull(canonicalProvider, "canonicalProvider");
            canonicalOptions = Map.copyOf(canonicalOptions);
        }

        /** Names the provider and option keys only; option values can hold resolved secrets. */
        @Override
        public String toString() {
            return "StoreIdentity[" + canonicalProvider + ", options=" + new TreeSet<>(canonicalOptions.keySet()) + "]";
        }
    }

    /** Canonical identity of a read-through cache configuration fronting an S3 store. */
    record CacheIdentity(StoreIdentity store, int ttlSeconds, long maxCacheableBytes) {
        CacheIdentity {
            Objects.requireNonNull(store, "store");
        }
    }

    /**
     * Complete canonical store configuration of a resolved {@link Backing}, combining its
     * authoritative store identity and optional read-through cache identity.
     */
    record BackingIdentity(StoreIdentity authoritative, CacheIdentity cache) {
        BackingIdentity {
            Objects.requireNonNull(authoritative, "authoritative");
        }
    }

    static StoreIdentity s3(Map<String, String> options) {
        String region = options.getOrDefault(OPT_REGION, "");
        boolean pathStyle = Boolean.parseBoolean(options.get(OPT_PATH_STYLE));
        String canonicalEndpoint = s3Endpoint(options.get(OPT_ENDPOINT), region, pathStyle);
        return s3(options, canonicalEndpoint, region, pathStyle);
    }

    static StoreIdentity s3(
            Map<String, String> options, String canonicalEndpoint, String region, boolean pathStyle) {
        var normalized = new HashMap<>(options);
        normalized.put(OPT_ENDPOINT, canonicalEndpoint);
        normalized.put(OPT_REGION, region);
        normalized.put(OPT_PATH_STYLE, Boolean.toString(pathStyle));
        normalized.put(
                OPT_CONDITIONAL_WRITES,
                Boolean.toString(Boolean.parseBoolean(options.get(OPT_CONDITIONAL_WRITES))));
        return new StoreIdentity(PROVIDER_S3, normalized);
    }

    static StoreIdentity redis(Map<String, String> options, String errorMessage) {
        var normalized = new HashMap<>(options);
        normalized.put(OPT_URI, canonicalRedisUri(options.get(OPT_URI), errorMessage));
        return new StoreIdentity(PROVIDER_REDIS, normalized);
    }

    private static String canonicalRedisUri(String value, String errorMessage) {
        final URI parsed;
        final Map<String, String> location;
        try {
            parsed = URI.create(value);
            location = RedisBackendIdentity.of(value, "").location();
        } catch (RuntimeException invalidUri) {
            throw invalid(errorMessage);
        }
        try {
            return new URI(
                    location.get("scheme"),
                    parsed.getUserInfo(),
                    location.get("host"),
                    Integer.parseInt(location.get("port")),
                    "/" + location.get("database"),
                    null,
                    null).toString();
        } catch (URISyntaxException invalidUri) {
            throw invalid(errorMessage);
        }
    }

    static String s3Endpoint(String value, String region, boolean pathStyle) {
        if (blank(value)) {
            return "";
        }
        final URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException invalidUri) {
            throw invalid("Invalid S3 endpoint");
        }
        // S3BackendIdentity deliberately covers origins; the unmanaged provider
        // also supports explicit path prefixes, whose exact spelling stays identity.
        if (uri.getRawPath() != null && !uri.getRawPath().isEmpty() && !uri.getRawPath().equals("/")) {
            return value;
        }
        try {
            return S3BackendIdentity.of(value, region, pathStyle).location().get(OPT_ENDPOINT);
        } catch (IllegalArgumentException invalidIdentity) {
            throw invalid("Invalid S3 drive configuration: " + invalidIdentity.getMessage());
        }
    }

    /** Canonical physical backing identity isolating cache entries across S3 backends. */
    static String s3CacheNamespace(String endpoint, String region, boolean pathStyle) {
        return "s3:" + encode(text(endpoint)) + ":" + encode(text(region)) + ":" + (pathStyle ? "path" : "virtual");
    }

    static String redisEndpoint(String value) {
        final Map<String, String> location;
        try {
            location = RedisBackendIdentity.of(value, "").location();
        } catch (IllegalArgumentException invalidUri) {
            throw invalid("Invalid drive endpoint");
        }
        try {
            return new URI(
                    location.get("scheme"),
                    null,
                    location.get("host"),
                    Integer.parseInt(location.get("port")),
                    "/" + location.get("database"),
                    null,
                    null).toString();
        } catch (URISyntaxException invalidUri) {
            // The identity has already removed credentials, so this cause is safe.
            throw new RepositoryException(
                    RepositoryException.Code.FAILED_PRECONDITION, "Invalid drive endpoint", invalidUri);
        }
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String text(String value) {
        return value == null ? "" : value;
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static RepositoryException invalid(String message) {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, message);
    }
}
