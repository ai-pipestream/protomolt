package ai.protomolt.proto.repo.catalog;

import java.util.Objects;

/** Connection settings for one PostgreSQL catalog. */
public record CatalogConfig(String jdbcUrl, String username, String password, int maxPoolSize) {
    public CatalogConfig {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        if (jdbcUrl.isBlank()) throw new IllegalArgumentException("Catalog JDBC URL is required");
        if (maxPoolSize < 1) throw new IllegalArgumentException("Catalog pool size must be positive");
    }
}
