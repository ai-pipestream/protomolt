package ai.pipestream.proto.platform;

import java.util.Map;

/**
 * Iceberg catalog configuration for the metrics role's lake backend, read
 * from the {@code DOCUMENT_PLATFORM_METRICS_ICEBERG_*} family. The catalog
 * URI is the switch: absent means the lake engine is off and every other
 * variable of the family must be absent too. The URI's scheme picks the
 * catalog: {@code jdbc:} is a JDBC catalog (sqlite works out of the box,
 * the one-container lake) and demands the warehouse by name; {@code http}
 * or {@code https} is a REST catalog service, where the warehouse is an
 * optional pass-through because the service may own that setting. Any
 * other scheme is refused naming the supported two.
 *
 * <p>Both shapes read a lake on a local filesystem the node can reach
 * (the sink's shared-volume rig): this build is Hadoop-free, so the
 * catalog is initialized with the sink's {@code LocalFileIO}, and DuckDB
 * reads the table's Parquet paths directly. An object-store lake is a
 * follow-up, because the reader would need object-store reach too.</p>
 *
 * @param catalogUri the catalog URI ({@code jdbc:} or {@code http(s):})
 * @param warehouse the warehouse location; required for a JDBC catalog,
 *        optional for REST
 * @param namespace the namespace metric tables live under; each metric
 *        subject reads the table named exactly like it
 */
record MetricsIcebergConfig(String catalogUri, String warehouse, String namespace) {

    /** Env var naming the catalog URI; absent turns the lake engine off. */
    static final String ENV_CATALOG_URI = "DOCUMENT_PLATFORM_METRICS_ICEBERG_CATALOG_URI";

    /** Env var for the warehouse location; required with a jdbc: URI. */
    static final String ENV_WAREHOUSE = "DOCUMENT_PLATFORM_METRICS_ICEBERG_WAREHOUSE";

    /** Env var for the table namespace (default {@value DEFAULT_NAMESPACE}). */
    static final String ENV_NAMESPACE = "DOCUMENT_PLATFORM_METRICS_ICEBERG_NAMESPACE";

    /** The default table namespace. */
    static final String DEFAULT_NAMESPACE = "protomolt";

    /**
     * The catalog name this node initializes with. For a JDBC catalog the
     * name scopes the table records in the backing database, so a writer
     * that shares the catalog database must initialize with this exact
     * name or its tables are invisible here.
     */
    static final String CATALOG_NAME = "protomolt";

    MetricsIcebergConfig {
        if (catalogUri == null || catalogUri.isBlank()) {
            throw new IllegalArgumentException("catalogUri must not be blank");
        }
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("namespace must not be blank");
        }
        warehouse = warehouse == null ? "" : warehouse;
        boolean jdbc = catalogUri.startsWith("jdbc:");
        boolean rest = catalogUri.startsWith("http://") || catalogUri.startsWith("https://");
        if (!jdbc && !rest) {
            throw new IllegalArgumentException(ENV_CATALOG_URI + " must be a jdbc:"
                    + " catalog URI or an http(s) REST catalog URI, got '"
                    + catalogUri + "'");
        }
        if (jdbc && warehouse.isEmpty()) {
            throw new IllegalArgumentException(ENV_WAREHOUSE + " is required: a JDBC"
                    + " catalog stores table locations under the warehouse");
        }
    }

    /** Whether the URI names a JDBC catalog (a REST service otherwise). */
    boolean jdbc() {
        return catalogUri.startsWith("jdbc:");
    }

    /**
     * Parses the family from an environment map, or returns {@code null}
     * when the lake engine is off. A family member set without the catalog
     * URI is a refusal, never a silent no-op.
     */
    static MetricsIcebergConfig fromEnvironment(Map<String, String> environment) {
        String catalogUri = value(environment, ENV_CATALOG_URI);
        if (catalogUri.isEmpty()) {
            for (String name : new String[] {ENV_WAREHOUSE, ENV_NAMESPACE}) {
                if (!value(environment, name).isEmpty()) {
                    throw new IllegalArgumentException(name + " is set but "
                            + ENV_CATALOG_URI + " is not: name the catalog or unset"
                            + " the family");
                }
            }
            return null;
        }
        String namespace = value(environment, ENV_NAMESPACE);
        return new MetricsIcebergConfig(
                catalogUri,
                value(environment, ENV_WAREHOUSE),
                namespace.isEmpty() ? DEFAULT_NAMESPACE : namespace);
    }

    private static String value(Map<String, String> environment, String name) {
        String value = environment.get(name);
        return value == null ? "" : value.trim();
    }
}
