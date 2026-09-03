package ai.protomolt.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The {@code DOCUMENT_PLATFORM_METRICS_ICEBERG_*} family: the catalog URI
 * is the switch, the URI's scheme picks the catalog, a JDBC catalog
 * demands the warehouse by name, and a family member set without the
 * switch refuses instead of silently doing nothing.
 */
class MetricsIcebergConfigTest {

    @Test
    void anAbsentFamilyMeansTheLakeEngineIsOff() {
        assertThat(MetricsIcebergConfig.fromEnvironment(Map.of())).isNull();
        assertThat(MetricsIcebergConfig.fromEnvironment(
                Map.of("DOCUMENT_PLATFORM_SEARCH_GRPC_PORT", "9094"))).isNull();
    }

    @Test
    void aFamilyMemberWithoutTheCatalogUriRefusesByName() {
        assertThatThrownBy(() -> MetricsIcebergConfig.fromEnvironment(
                Map.of(MetricsIcebergConfig.ENV_WAREHOUSE, "/lake/warehouse")))
                .hasMessageContaining(MetricsIcebergConfig.ENV_WAREHOUSE)
                .hasMessageContaining(MetricsIcebergConfig.ENV_CATALOG_URI);
        assertThatThrownBy(() -> MetricsIcebergConfig.fromEnvironment(
                Map.of(MetricsIcebergConfig.ENV_NAMESPACE, "lake")))
                .hasMessageContaining(MetricsIcebergConfig.ENV_NAMESPACE)
                .hasMessageContaining(MetricsIcebergConfig.ENV_CATALOG_URI);
    }

    @Test
    void aJdbcCatalogDemandsTheWarehouseByName() {
        assertThatThrownBy(() -> MetricsIcebergConfig.fromEnvironment(Map.of(
                MetricsIcebergConfig.ENV_CATALOG_URI, "jdbc:sqlite:/lake/catalog.db")))
                .hasMessageContaining(MetricsIcebergConfig.ENV_WAREHOUSE);

        MetricsIcebergConfig config = MetricsIcebergConfig.fromEnvironment(Map.of(
                MetricsIcebergConfig.ENV_CATALOG_URI, "jdbc:sqlite:/lake/catalog.db",
                MetricsIcebergConfig.ENV_WAREHOUSE, "/lake/warehouse"));
        assertThat(config.jdbc()).isTrue();
        assertThat(config.warehouse()).isEqualTo("/lake/warehouse");
        assertThat(config.namespace()).isEqualTo(MetricsIcebergConfig.DEFAULT_NAMESPACE);
    }

    @Test
    void aRestCatalogWorksWithoutAWarehouseAndTheNamespaceOverrides() {
        MetricsIcebergConfig config = MetricsIcebergConfig.fromEnvironment(Map.of(
                MetricsIcebergConfig.ENV_CATALOG_URI, "http://iceberg-rest:8181",
                MetricsIcebergConfig.ENV_NAMESPACE, "lake"));
        assertThat(config.jdbc()).isFalse();
        assertThat(config.warehouse()).isEmpty();
        assertThat(config.namespace()).isEqualTo("lake");
    }

    @Test
    void theS3GroupDemandsTheRegionAndPairedCredentials() {
        MetricsIcebergConfig config = MetricsIcebergConfig.fromEnvironment(Map.of(
                MetricsIcebergConfig.ENV_CATALOG_URI, "jdbc:sqlite:/lake/catalog.db",
                MetricsIcebergConfig.ENV_WAREHOUSE, "s3://metric-lake/lake",
                MetricsIcebergConfig.ENV_S3_REGION, "us-east-1",
                MetricsIcebergConfig.ENV_S3_ENDPOINT, "http://localstack:4566",
                MetricsIcebergConfig.ENV_S3_ACCESS_KEY, "ak",
                MetricsIcebergConfig.ENV_S3_SECRET_KEY, "sk"));
        assertThat(config.s3()).isTrue();
        assertThat(config.s3Region()).isEqualTo("us-east-1");

        assertThat(MetricsIcebergConfig.fromEnvironment(Map.of(
                MetricsIcebergConfig.ENV_CATALOG_URI, "jdbc:sqlite:/lake/catalog.db",
                MetricsIcebergConfig.ENV_WAREHOUSE, "/lake")).s3()).isFalse();

        assertThatThrownBy(() -> MetricsIcebergConfig.fromEnvironment(Map.of(
                MetricsIcebergConfig.ENV_CATALOG_URI, "jdbc:sqlite:/lake/catalog.db",
                MetricsIcebergConfig.ENV_WAREHOUSE, "s3://metric-lake/lake",
                MetricsIcebergConfig.ENV_S3_ENDPOINT, "http://localstack:4566")))
                .hasMessageContaining(MetricsIcebergConfig.ENV_S3_REGION);

        assertThatThrownBy(() -> MetricsIcebergConfig.fromEnvironment(Map.of(
                MetricsIcebergConfig.ENV_CATALOG_URI, "jdbc:sqlite:/lake/catalog.db",
                MetricsIcebergConfig.ENV_WAREHOUSE, "s3://metric-lake/lake",
                MetricsIcebergConfig.ENV_S3_REGION, "us-east-1",
                MetricsIcebergConfig.ENV_S3_ACCESS_KEY, "ak")))
                .hasMessageContaining(MetricsIcebergConfig.ENV_S3_SECRET_KEY);

        assertThatThrownBy(() -> MetricsIcebergConfig.fromEnvironment(Map.of(
                MetricsIcebergConfig.ENV_S3_REGION, "us-east-1")))
                .hasMessageContaining(MetricsIcebergConfig.ENV_CATALOG_URI);
    }

    @Test
    void anUnknownSchemeRefusesNamingTheSupportedTwo() {
        assertThatThrownBy(() -> MetricsIcebergConfig.fromEnvironment(Map.of(
                MetricsIcebergConfig.ENV_CATALOG_URI, "thrift://hive:9083")))
                .hasMessageContaining("jdbc:")
                .hasMessageContaining("REST catalog");
    }
}
