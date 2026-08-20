package ai.pipestream.proto.kafka.connect.iceberg;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Iceberg sink config's own contract: documented defaults, the required keys, the
 * pass-through of {@code iceberg.catalog.*} properties to the catalog builder (minus the
 * instance name), and the {@code column:transform} partition syntax — identity assumed when the
 * transform is omitted, blank entries ignored.
 */
class IcebergSinkConfigTest {

    private static Map<String, String> required() {
        Map<String, String> props = new HashMap<>();
        props.put(IcebergSinkConfig.DESCRIPTOR_SET, "unused-by-these-tests");
        props.put(IcebergSinkConfig.MESSAGE_TYPE, "shop.v1.Order");
        props.put(IcebergSinkConfig.TABLE, "shop.orders");
        return props;
    }

    private static IcebergSinkConfig config(Map<String, String> overrides) {
        Map<String, String> props = required();
        props.putAll(overrides);
        return new IcebergSinkConfig(props);
    }

    @Test
    void documentedDefaultsHold() {
        IcebergSinkConfig config = config(Map.of());
        assertThat(config.messageType()).isEqualTo("shop.v1.Order");
        assertThat(config.valueFormat()).isEqualTo(IcebergSinkConfig.ValueFormat.PROTOBUF);
        assertThat(config.catalogName()).isEqualTo("protomolt");
        assertThat(config.table()).isEqualTo(TableIdentifier.of("shop", "orders"));
        assertThat(config.tableLocation()).isNull();
        assertThat(config.partitionBy()).isEmpty();
        assertThat(config.catalogProperties()).isEmpty();
    }

    @Test
    void theRequiredKeysHaveNoDefaults() {
        assertThatThrownBy(() -> new IcebergSinkConfig(Map.of()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(IcebergSinkConfig.DESCRIPTOR_SET);

        Map<String, String> noTable = required();
        noTable.remove(IcebergSinkConfig.TABLE);
        assertThatThrownBy(() -> new IcebergSinkConfig(noTable))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(IcebergSinkConfig.TABLE);
    }

    @Test
    void valueFormatIsCaseInsensitiveAndValidated() {
        assertThat(config(Map.of(IcebergSinkConfig.VALUE_FORMAT, "JSON")).valueFormat())
                .isEqualTo(IcebergSinkConfig.ValueFormat.JSON);
        assertThat(config(Map.of(IcebergSinkConfig.VALUE_FORMAT, "Confluent")).valueFormat())
                .isEqualTo(IcebergSinkConfig.ValueFormat.CONFLUENT);
        assertThatThrownBy(() -> config(Map.of(IcebergSinkConfig.VALUE_FORMAT, "avro")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(IcebergSinkConfig.VALUE_FORMAT);
    }

    @Test
    void multiLevelNamespacesParseIntoTheTableIdentifier() {
        IcebergSinkConfig config = config(Map.of(IcebergSinkConfig.TABLE, "a.b.orders"));
        assertThat(config.table()).isEqualTo(TableIdentifier.of(
                org.apache.iceberg.catalog.Namespace.of("a", "b"), "orders"));
    }

    /**
     * Everything under {@code iceberg.catalog.} goes to Iceberg's catalog builder verbatim —
     * except {@code name}, which names the instance, not a builder property.
     */
    @Test
    void catalogPropertiesPassThroughWithoutThePrefixOrTheName() {
        IcebergSinkConfig config = config(Map.of(
                "iceberg.catalog.name", "lake",
                "iceberg.catalog.type", "rest",
                "iceberg.catalog.uri", "http://catalog:8181",
                "iceberg.catalog.warehouse", "s3://bucket/wh"));
        assertThat(config.catalogName()).isEqualTo("lake");
        assertThat(config.catalogProperties()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "type", "rest",
                "uri", "http://catalog:8181",
                "warehouse", "s3://bucket/wh"));
    }

    @Test
    void partitionEntriesParseAsColumnAndTransform() {
        IcebergSinkConfig config = config(Map.of(
                IcebergSinkConfig.PARTITION, "region,at:day,qty:bucket[16]"));
        assertThat(config.partitionBy()).containsExactly(
                new ai.pipestream.proto.iceberg.IcebergPartitions.PartitionField(
                        "region", "identity"),
                new ai.pipestream.proto.iceberg.IcebergPartitions.PartitionField(
                        "at", "day"),
                new ai.pipestream.proto.iceberg.IcebergPartitions.PartitionField(
                        "qty", "bucket[16]"));
    }

    @Test
    void blankPartitionEntriesAreIgnored() {
        IcebergSinkConfig config = config(Map.of(
                IcebergSinkConfig.PARTITION, "region, ,at:day"));
        assertThat(config.partitionBy()).hasSize(2);
        assertThat(config.partitionBy().get(0).sourceColumn()).isEqualTo("region");
        assertThat(config.partitionBy().get(1).sourceColumn()).isEqualTo("at");
    }

    @Test
    void theConnectorExposesTheSameDefinition() {
        assertThat(new IcebergSinkConnector().config().names())
                .contains(IcebergSinkConfig.DESCRIPTOR_SET, IcebergSinkConfig.MESSAGE_TYPE,
                        IcebergSinkConfig.VALUE_FORMAT, IcebergSinkConfig.CATALOG_NAME,
                        IcebergSinkConfig.TABLE, IcebergSinkConfig.TABLE_LOCATION,
                        IcebergSinkConfig.PARTITION);
    }

    @Test
    void thePluginReportsDevVersionOutsideAJar() {
        assertThat(new IcebergSinkConnector().version()).isEqualTo("dev");
        assertThat(new IcebergSinkTask().version()).isEqualTo("dev");
    }
}
