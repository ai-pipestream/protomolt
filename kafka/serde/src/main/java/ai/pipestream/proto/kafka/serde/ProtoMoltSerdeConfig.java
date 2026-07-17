package ai.pipestream.proto.kafka.serde;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * What the serdes need to know. The descriptor set is the only required piece: this serde reads
 * the schema from something the deployment already has rather than from a registry, so a
 * registry outage cannot stop a producer that has not changed its schema.
 */
public final class ProtoMoltSerdeConfig extends AbstractConfig {

    /** A serialized FileDescriptorSet on the classpath: what a build that compiled protos emits. */
    public static final String DESCRIPTOR_SET_RESOURCE = "protomolt.descriptor.set.resource";
    /** A serialized FileDescriptorSet inline, for deployments that configure rather than package. */
    public static final String DESCRIPTOR_SET_BASE64 = "protomolt.descriptor.set.base64";
    /** The fully qualified message type this serde reads or writes. */
    public static final String MESSAGE_TYPE = "protomolt.message.type";
    /** The schema id to stamp into the frame. */
    public static final String SCHEMA_ID = "protomolt.schema.id";
    /** Validate before writing, so invalid data never reaches the topic. */
    public static final String VALIDATE_ON_WRITE = "protomolt.validate.on.write";
    /** Validate after reading, which catches producers that never went through this serde. */
    public static final String VALIDATE_ON_READ = "protomolt.validate.on.read";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DESCRIPTOR_SET_RESOURCE, ConfigDef.Type.STRING, null,
                    ConfigDef.Importance.HIGH,
                    "Classpath resource holding a serialized FileDescriptorSet. Exactly one of "
                            + "this or " + DESCRIPTOR_SET_BASE64 + " is required.")
            .define(DESCRIPTOR_SET_BASE64, ConfigDef.Type.PASSWORD, null,
                    ConfigDef.Importance.MEDIUM,
                    "A serialized FileDescriptorSet, base64 encoded.")
            .define(MESSAGE_TYPE, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Fully qualified message type, e.g. acme.orders.v1.Order.")
            .define(SCHEMA_ID, ConfigDef.Type.INT, 0, ConfigDef.Importance.MEDIUM,
                    "Schema id written into the frame. Readers that resolve the type from "
                            + "configuration ignore it; a Confluent registry would assign it.")
            .define(VALIDATE_ON_WRITE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
                    "Validate against the schema's declared rules before serializing. Invalid "
                            + "messages are rejected rather than written.")
            .define(VALIDATE_ON_READ, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                    "Validate after deserializing. Off by default: a consumer usually cannot "
                            + "fix what a producer already wrote, so this is for topics whose "
                            + "producers do not all go through this serde.");

    public ProtoMoltSerdeConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }
}
