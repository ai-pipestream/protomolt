package ai.pipestream.proto.registry.server;

/**
 * Configuration for {@link SchemaRegistryServer}.
 *
 * @param host             bind host
 * @param port             bind port; {@code 0} picks an ephemeral port
 *                         (see {@link SchemaRegistryServer#actualPort()})
 * @param healthPath       liveness endpoint path
 * @param nativePathPrefix prefix of the non-Confluent extras
 *                         ({@code {prefix}/subjects/{subject}/descriptor-set}); a single
 *                         path segment, e.g. {@code /protomolt}
 * @param maxRequestBytes  maximum accepted request body size; larger bodies get 413
 * @param apiToken         shared secret required on every request except the health
 *                         endpoint ({@code api_token} header or bearer credential);
 *                         {@code null} serves unauthenticated
 */
public record SchemaRegistryServerConfig(
        String host,
        int port,
        String healthPath,
        String nativePathPrefix,
        int maxRequestBytes,
        String apiToken) {

    /** Default request body cap (16 MiB). */
    public static final int DEFAULT_MAX_REQUEST_BYTES = 16 * 1024 * 1024;

    public SchemaRegistryServerConfig {
        host = host == null || host.isBlank() ? "0.0.0.0" : host;
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        healthPath = normalize(healthPath == null ? "/health" : healthPath);
        nativePathPrefix = normalize(nativePathPrefix == null ? "/protomolt" : nativePathPrefix);
        // The router compares the prefix against the first decoded path segment, so a nested
        // or empty prefix would match nothing and silently 404 every native endpoint.
        if (nativePathPrefix.length() < 2 || nativePathPrefix.indexOf('/', 1) >= 0) {
            throw new IllegalArgumentException(
                    "nativePathPrefix must be a single path segment: " + nativePathPrefix);
        }
        if (maxRequestBytes <= 0) {
            throw new IllegalArgumentException("maxRequestBytes must be positive: " + maxRequestBytes);
        }
        apiToken = apiToken == null || apiToken.isBlank() ? null : apiToken;
    }

    public SchemaRegistryServerConfig(String host, int port, String healthPath,
                                      String nativePathPrefix, int maxRequestBytes) {
        this(host, port, healthPath, nativePathPrefix, maxRequestBytes, null);
    }

    /** {@code 0.0.0.0:8081} (the conventional Schema Registry port), default paths. */
    public static SchemaRegistryServerConfig defaults() {
        return new SchemaRegistryServerConfig("0.0.0.0", 8081, "/health", "/protomolt",
                DEFAULT_MAX_REQUEST_BYTES, null);
    }

    public SchemaRegistryServerConfig withHost(String host) {
        return new SchemaRegistryServerConfig(host, port, healthPath, nativePathPrefix,
                maxRequestBytes, apiToken);
    }

    public SchemaRegistryServerConfig withPort(int port) {
        return new SchemaRegistryServerConfig(host, port, healthPath, nativePathPrefix,
                maxRequestBytes, apiToken);
    }

    public SchemaRegistryServerConfig withApiToken(String apiToken) {
        return new SchemaRegistryServerConfig(host, port, healthPath, nativePathPrefix,
                maxRequestBytes, apiToken);
    }

    private static String normalize(String path) {
        String normalized = path.startsWith("/") ? path : "/" + path;
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
