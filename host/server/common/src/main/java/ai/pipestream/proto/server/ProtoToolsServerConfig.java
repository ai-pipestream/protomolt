package ai.pipestream.proto.server;

import java.util.Objects;

/**
 * Shared configuration for all protobuf JSON/REST server hosts
 * (JDK, Vert.x, Netty, Quarkus, Spring, Micronaut, …).
 *
 * @param maxRequestBytes maximum accepted request body size in bytes; larger bodies get 413
 */
public record ProtoToolsServerConfig(
        String host,
        int port,
        String restPathPrefix,
        String openApiPath,
        String healthPath,
        int maxRequestBytes) {

    /** Default request body cap (16 MiB), shared by every host. */
    public static final int DEFAULT_MAX_REQUEST_BYTES = 16 * 1024 * 1024;

    public ProtoToolsServerConfig {
        host = host == null || host.isBlank() ? "0.0.0.0" : host;
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        restPathPrefix = normalizePrefix(restPathPrefix == null ? "/grpc-json" : restPathPrefix);
        openApiPath = normalizeAbsolute(openApiPath == null ? "/openapi.json" : openApiPath);
        healthPath = normalizeAbsolute(healthPath == null ? "/health" : healthPath);
        if (maxRequestBytes <= 0) {
            throw new IllegalArgumentException("maxRequestBytes must be positive: " + maxRequestBytes);
        }
    }

    public ProtoToolsServerConfig(
            String host,
            int port,
            String restPathPrefix,
            String openApiPath,
            String healthPath) {
        this(host, port, restPathPrefix, openApiPath, healthPath, DEFAULT_MAX_REQUEST_BYTES);
    }

    public static ProtoToolsServerConfig defaults() {
        return new ProtoToolsServerConfig("0.0.0.0", 8080, "/grpc-json", "/openapi.json", "/health");
    }

    public ProtoToolsServerConfig withPort(int port) {
        return new ProtoToolsServerConfig(host, port, restPathPrefix, openApiPath, healthPath, maxRequestBytes);
    }

    public ProtoToolsServerConfig withHost(String host) {
        return new ProtoToolsServerConfig(host, port, restPathPrefix, openApiPath, healthPath, maxRequestBytes);
    }

    public ProtoToolsServerConfig withRestPathPrefix(String restPathPrefix) {
        return new ProtoToolsServerConfig(host, port, restPathPrefix, openApiPath, healthPath, maxRequestBytes);
    }

    public ProtoToolsServerConfig withMaxRequestBytes(int maxRequestBytes) {
        return new ProtoToolsServerConfig(host, port, restPathPrefix, openApiPath, healthPath, maxRequestBytes);
    }

    private static String normalizePrefix(String path) {
        Objects.requireNonNull(path, "path");
        String p = path.startsWith("/") ? path : "/" + path;
        while (p.endsWith("/") && p.length() > 1) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
    }

    private static String normalizeAbsolute(String path) {
        return normalizePrefix(path);
    }
}
