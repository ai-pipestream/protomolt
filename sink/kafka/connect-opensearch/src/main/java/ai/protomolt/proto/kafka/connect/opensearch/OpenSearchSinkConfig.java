package ai.protomolt.proto.kafka.connect.opensearch;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Locale;
import java.util.Map;

/**
 * Configuration for the OpenSearch sink: the schema that declares the document message,
 * how record values decode, where the cluster is, and how documents get their ids.
 *
 * <p>What lands in each document is decided by the schema itself: the
 * {@code (ai.protomolt.proto.index.hints.v1.index)} field options compiled into the
 * configured descriptor set drive field kinds, engine names, analyzers, vectors, and
 * {@code google.protobuf.Any} handling — there are no per-field keys here.</p>
 */
public final class OpenSearchSinkConfig extends AbstractConfig {

    public static final String DESCRIPTOR_SET = "schema.descriptor.set.base64";
    public static final String MESSAGE_TYPE = "message.type";
    public static final String VALUE_FORMAT = "value.format";
    public static final String URL = "opensearch.url";
    public static final String INDEX = "opensearch.index";
    public static final String ENSURE_INDEX = "opensearch.ensure.index";
    public static final String REFRESH = "opensearch.refresh";
    public static final String DOCUMENT_ID_PATH = "document.id.path";
    public static final String VALIDATE = "validate";

    /** How a record value becomes the document message. */
    public enum ValueFormat {
        /** The value bytes are the serialized document message. */
        PROTOBUF,
        /** Confluent wire format: magic byte, schema id, message indexes, then the message. */
        CONFLUENT,
        /** The value is the document message as canonical proto3 JSON text. */
        JSON
    }

    public static ConfigDef definition() {
        return new ConfigDef()
                .define(DESCRIPTOR_SET, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Base64-encoded serialized google.protobuf.FileDescriptorSet declaring the "
                                + "document message type, with the indexing-hint options compiled "
                                + "in (e.g. from ProtoMolt's compile or reflect verbs).")
                .define(MESSAGE_TYPE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Fully qualified message type of the record values, e.g. 'shop.v1.Order'.")
                .define(VALUE_FORMAT, ConfigDef.Type.STRING, "protobuf",
                        ConfigDef.CaseInsensitiveValidString.in("protobuf", "confluent", "json"),
                        ConfigDef.Importance.MEDIUM,
                        "How record values decode into the document message: raw 'protobuf' "
                                + "bytes, 'confluent' wire format (framed with a schema id), or "
                                + "proto3 'json' text.")
                .define(URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "OpenSearch base URL, e.g. 'http://localhost:9200'.")
                .define(INDEX, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Target index name.")
                .define(ENSURE_INDEX, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "Create the index from the mapping-generated properties at task start when it "
                                + "does not exist yet (idempotent).")
                .define(REFRESH, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                        "Request an index refresh on every bulk write, making documents "
                                + "immediately searchable at a throughput cost.")
                .define(DOCUMENT_ID_PATH, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                        "Dotted proto path read from each message as the document id, e.g. "
                                + "'doc_id'. Empty: ids derive from topic-partition-offset, so a "
                                + "redelivered record overwrites its own document.")
                .define(VALIDATE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "Validate each message, and every google.protobuf.Any payload it "
                                + "packs, against the declared "
                                + "ai.protomolt.proto.validate.v1 rules before indexing; "
                                + "violations are data errors routed by errors.tolerance. "
                                + "Types declaring no rules validate clean at no cost. "
                                + "'false' suspends both, e.g. to drain a topic with a "
                                + "misbehaving producer.");
    }

    public OpenSearchSinkConfig(Map<String, String> props) {
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

    public String url() {
        return getString(URL);
    }

    public String index() {
        return getString(INDEX);
    }

    public boolean ensureIndex() {
        return getBoolean(ENSURE_INDEX);
    }

    public boolean refresh() {
        return getBoolean(REFRESH);
    }

    public String documentIdPath() {
        return getString(DOCUMENT_ID_PATH);
    }

    public boolean validate() {
        return getBoolean(VALIDATE);
    }
}
