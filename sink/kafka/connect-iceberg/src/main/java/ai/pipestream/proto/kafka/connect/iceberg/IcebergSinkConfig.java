package ai.pipestream.proto.kafka.connect.iceberg;

import ai.pipestream.proto.lake.iceberg.IcebergPartitions;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Configuration for the Iceberg sink: the schema that declares the row message, how record
 * values decode, which catalog and table to land them in, and how to partition on create.
 *
 * <p>The catalog itself is configured with {@code iceberg.catalog.*} keys ({@code type},
 * {@code uri}, {@code warehouse}, {@code io-impl}, ...), read straight from the raw config and
 * handed to Iceberg's catalog builder, so any catalog Iceberg supports works without a code
 * change.</p>
 */
public final class IcebergSinkConfig extends AbstractConfig {

    public static final String DESCRIPTOR_SET = "schema.descriptor.set.base64";
    public static final String MESSAGE_TYPE = "message.type";
    public static final String VALUE_FORMAT = "value.format";
    public static final String CATALOG_NAME = "iceberg.catalog.name";
    public static final String CATALOG_PREFIX = "iceberg.catalog.";
    public static final String TABLE = "iceberg.table";
    public static final String TABLE_LOCATION = "iceberg.table.location";
    public static final String PARTITION = "iceberg.partition";

    /** How a record value becomes the row message. */
    public enum ValueFormat {
        /** The value bytes are the serialized row message. */
        PROTOBUF,
        /** Confluent wire format: magic byte, schema id, message indexes, then the message. */
        CONFLUENT,
        /** The value is the row message as canonical proto3 JSON text. */
        JSON
    }

    public static ConfigDef definition() {
        return new ConfigDef()
                .define(DESCRIPTOR_SET, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Base64-encoded serialized google.protobuf.FileDescriptorSet declaring the "
                                + "row message type (e.g. from ProtoMolt's compile or reflect "
                                + "verbs, or a registry descriptor-set endpoint).")
                .define(MESSAGE_TYPE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Fully qualified message type of the record values, e.g. 'shop.v1.Order'.")
                .define(VALUE_FORMAT, ConfigDef.Type.STRING, "protobuf",
                        ConfigDef.CaseInsensitiveValidString.in("protobuf", "confluent", "json"),
                        ConfigDef.Importance.MEDIUM,
                        "How record values decode into the row message: raw 'protobuf' bytes, "
                                + "'confluent' wire format (framed with a schema id), or proto3 "
                                + "'json' text.")
                .define(CATALOG_NAME, ConfigDef.Type.STRING, "protomolt",
                        ConfigDef.Importance.MEDIUM, "Iceberg catalog instance name.")
                .define(TABLE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Target table as 'namespace.table' (multi-level namespaces allowed).")
                .define(TABLE_LOCATION, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                        "Optional explicit table base location; unset uses the catalog's default.")
                .define(PARTITION, ConfigDef.Type.LIST, Collections.emptyList(),
                        ConfigDef.Importance.LOW,
                        "Optional partition columns as 'column:transform', e.g. 'at:day' or "
                                + "'region:identity'; identity is assumed when the transform is "
                                + "omitted. Applied only when the table is created.");
    }

    public IcebergSinkConfig(Map<String, String> props) {
        super(definition(), props);
    }

    public String descriptorSetBase64() {
        return getString(DESCRIPTOR_SET);
    }

    public String messageType() {
        return getString(MESSAGE_TYPE);
    }

    public ValueFormat valueFormat() {
        return ValueFormat.valueOf(getString(VALUE_FORMAT).toUpperCase(Locale.ROOT));
    }

    public String catalogName() {
        return getString(CATALOG_NAME);
    }

    public TableIdentifier table() {
        return TableIdentifier.parse(getString(TABLE));
    }

    public String tableLocation() {
        return getString(TABLE_LOCATION);
    }

    /** The {@code iceberg.catalog.*} properties (the instance name excluded) for the builder. */
    public Map<String, String> catalogProperties() {
        Map<String, String> props = new HashMap<>();
        originalsWithPrefix(CATALOG_PREFIX).forEach((key, value) -> {
            if (!key.equals("name")) {
                props.put(key, String.valueOf(value));
            }
        });
        return props;
    }

    /** Partition columns parsed from the {@value #PARTITION} list, in order. */
    public List<IcebergPartitions.PartitionField> partitionBy() {
        List<IcebergPartitions.PartitionField> fields = new ArrayList<>();
        for (String entry : getList(PARTITION)) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int colon = trimmed.indexOf(':');
            String column = colon < 0 ? trimmed : trimmed.substring(0, colon);
            String transform = colon < 0 ? "identity" : trimmed.substring(colon + 1);
            fields.add(new IcebergPartitions.PartitionField(column, transform));
        }
        return fields;
    }
}
