package ai.protomolt.proto.platform;

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
 * <p>Without the {@code S3_*} group both shapes read a lake on a local
 * filesystem the node can reach (the sink's shared-volume rig): this
 * build is Hadoop-free, so the catalog is initialized with the sink's
 * {@code LocalFileIO}, and DuckDB reads the table's Parquet paths
 * directly.</p>
 *
 * <p>The {@code ..._S3_*} group puts the lake's file plane on an
 * S3-compatible object store through Iceberg's {@code S3FileIO}: any
 * member of the group demands the region by name, static credentials come
 * as a pair or not at all (absent falls to the AWS default provider
 * chain), and an endpoint override (LocalStack, RustFS) implies
 * path-style addressing. Without the group the lake stays on a local
 * filesystem through the sink's {@code LocalFileIO}. The metric reader
 * reaches an object-store lake through the table's own {@code FileIO}
 * (files materialize locally for the query's duration), so there is no
 * second credential path.</p>
 *
 * @param catalogUri the catalog URI ({@code jdbc:} or {@code http(s):})
 * @param warehouse the warehouse location; required for a JDBC catalog,
 *        optional for REST
 * @param namespace the namespace metric tables live under; each metric
 *        subject reads the table named exactly like it
 * @param s3Region the object store's region; the group's required member
 * @param s3Endpoint an endpoint override, or empty for AWS
 * @param s3AccessKey a static access key, or empty for the provider chain
 * @param s3SecretKey the static secret, or empty for the provider chain
 */
record MetricsIcebergConfig(String catalogUri, String warehouse, String namespace,
        String s3Region, String s3Endpoint, String s3AccessKey, String s3SecretKey) {

    /** Env var naming the catalog URI; absent turns the lake engine off. */
    static final String ENV_CATALOG_URI = "DOCUMENT_PLATFORM_METRICS_ICEBERG_CATALOG_URI";

    /** Env var for the warehouse location; required with a jdbc: URI. */
    static final String ENV_WAREHOUSE = "DOCUMENT_PLATFORM_METRICS_ICEBERG_WAREHOUSE";

    /** Env var for the table namespace (default {@value DEFAULT_NAMESPACE}). */
    static final String ENV_NAMESPACE = "DOCUMENT_PLATFORM_METRICS_ICEBERG_NAMESPACE";

    /** The default table namespace. */
    static final String DEFAULT_NAMESPACE = "protomolt";

    /** Env var for the object store's region; required with any S3 member. */
    static final String ENV_S3_REGION = "DOCUMENT_PLATFORM_METRICS_ICEBERG_S3_REGION";

    /** Env var for an endpoint override (LocalStack, RustFS), optional. */
    static final String ENV_S3_ENDPOINT = "DOCUMENT_PLATFORM_METRICS_ICEBERG_S3_ENDPOINT";

    /** Env var for a static access key, optional as a pair with the secret. */
    static final String ENV_S3_ACCESS_KEY = "DOCUMENT_PLATFORM_METRICS_ICEBERG_S3_ACCESS_KEY";

    /** Env var for the static secret, optional as a pair with the key. */
    static final String ENV_S3_SECRET_KEY = "DOCUMENT_PLATFORM_METRICS_ICEBERG_S3_SECRET_KEY";

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
        s3Region = s3Region == null ? "" : s3Region;
        s3Endpoint = s3Endpoint == null ? "" : s3Endpoint;
        s3AccessKey = s3AccessKey == null ? "" : s3AccessKey;
        s3SecretKey = s3SecretKey == null ? "" : s3SecretKey;
        boolean s3Member = !s3Endpoint.isEmpty() || !s3AccessKey.isEmpty()
                || !s3SecretKey.isEmpty();
        if (s3Member && s3Region.isEmpty()) {
            throw new IllegalArgumentException(ENV_S3_REGION + " is required: the"
                    + " S3 group is set, and S3FileIO needs the region");
        }
        if (s3AccessKey.isEmpty() != s3SecretKey.isEmpty()) {
            throw new IllegalArgumentException(ENV_S3_ACCESS_KEY + " and "
                    + ENV_S3_SECRET_KEY + " come together: set both or neither");
        }
    }

    /** Whether the lake's file plane is an S3-compatible object store. */
    boolean s3() {
        return !s3Region.isEmpty();
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
            for (String name : new String[] {ENV_WAREHOUSE, ENV_NAMESPACE,
                    ENV_S3_REGION, ENV_S3_ENDPOINT, ENV_S3_ACCESS_KEY, ENV_S3_SECRET_KEY}) {
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
                namespace.isEmpty() ? DEFAULT_NAMESPACE : namespace,
                value(environment, ENV_S3_REGION),
                value(environment, ENV_S3_ENDPOINT),
                value(environment, ENV_S3_ACCESS_KEY),
                value(environment, ENV_S3_SECRET_KEY));
    }

    private static String value(Map<String, String> environment, String name) {
        String value = environment.get(name);
        return value == null ? "" : value.trim();
    }
}
