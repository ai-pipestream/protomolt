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
    /** The fully qualified message type this serde reads or writes; unset accepts any packaged type. */
    public static final String MESSAGE_TYPE = "protomolt.message.type";
    /** How subjects are named: {@code topic} (default), {@code record}, or {@code topic-record}. */
    public static final String SUBJECT_STRATEGY = "protomolt.subject.strategy";
    /** Whether the deserializer returns generated Java classes when they are on the classpath. */
    public static final String GENERATED_CLASSES = "protomolt.generated.classes";
    /** The schema id to stamp into the frame. */
    public static final String SCHEMA_ID = "protomolt.schema.id";
    /** A Confluent-compatible registry, if there is one. Without it the descriptor set stands alone. */
    public static final String REGISTRY_URL = "protomolt.registry.url";
    /** Overrides the default {@code <topic>-value} / {@code <topic>-key} subject. */
    public static final String SUBJECT = "protomolt.subject";
    /** How long a failed registry lookup stands before the registry is asked again. */
    public static final String REGISTRY_RETRY_BACKOFF_MS = "protomolt.registry.retry.backoff.ms";
    /** Validate before writing, so invalid data never reaches the topic. */
    public static final String VALIDATE_ON_WRITE = "protomolt.validate.on.write";
    /** Validate after reading, which catches producers that never went through this serde. */
    public static final String VALIDATE_ON_READ = "protomolt.validate.on.read";
    /** Score quality dimensions the schema declares, before writing. */
    public static final String QUALITY_ON_WRITE = "protomolt.quality.on.write";
    /** Score quality dimensions after reading. */
    public static final String QUALITY_ON_READ = "protomolt.quality.on.read";
    /** Reject writes whose composite quality score falls below this. Unset means measure only. */
    public static final String QUALITY_MIN = "protomolt.quality.min";
    /** Mapping rules applied to every message before validating and writing. */
    public static final String MAP_ON_WRITE = "protomolt.map.on.write";
    /** Mapping rules applied to every message right after parsing. */
    public static final String MAP_ON_READ = "protomolt.map.on.read";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DESCRIPTOR_SET_RESOURCE, ConfigDef.Type.STRING, null,
                    ConfigDef.Importance.HIGH,
                    "Classpath resource holding a serialized FileDescriptorSet. Exactly one of "
                            + "this or " + DESCRIPTOR_SET_BASE64 + " is required.")
            .define(DESCRIPTOR_SET_BASE64, ConfigDef.Type.PASSWORD, null,
                    ConfigDef.Importance.MEDIUM,
                    "A serialized FileDescriptorSet, base64 encoded.")
            .define(MESSAGE_TYPE, ConfigDef.Type.STRING, null,
                    ConfigDef.Importance.HIGH,
                    "Fully qualified message type, e.g. acme.orders.v1.Order. When set, the "
                            + "serde is pinned: it writes and reads exactly this type. When "
                            + "unset, the serializer accepts any type in the descriptor set "
                            + "(several event types can share a topic), and the deserializer "
                            + "resolves each frame's type through the registry — which is then "
                            + "required.")
            .define(SUBJECT_STRATEGY, ConfigDef.Type.STRING, Subjects.TOPIC,
                    ConfigDef.ValidString.in(Subjects.TOPIC, Subjects.RECORD,
                            Subjects.TOPIC_RECORD),
                    ConfigDef.Importance.MEDIUM,
                    "How the subject is named for registry lookups: 'topic' (<topic>-value, the "
                            + "Confluent default), 'record' (the message's full name), or "
                            + "'topic-record' (<topic>-<full name>). The record strategies give "
                            + "each type its own subject, which is what multi-type topics need.")
            .define(GENERATED_CLASSES, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                    "Return instances of the generated Java classes when they are on the "
                            + "classpath, derived from the descriptor set's java options. Types "
                            + "with no generated class come back as DynamicMessage either way.")
            .define(SCHEMA_ID, ConfigDef.Type.INT, 0, ConfigDef.Importance.MEDIUM,
                    "Schema id written into the frame. Readers that resolve the type from "
                            + "configuration ignore it; a Confluent registry would assign it.")
            .define(REGISTRY_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Base URL of a Confluent-compatible registry, e.g. http://localhost:8081. "
                            + "When set, the id is looked up by subject on write and the schema "
                            + "is resolved by id on read; when the registry cannot answer, the "
                            + "packaged descriptor set is used instead of failing.")
            .define(SUBJECT, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                    "Subject to look the id up under. Defaults to <topic>-value, or <topic>-key "
                            + "for a key serde.")
            .define(REGISTRY_RETRY_BACKOFF_MS, ConfigDef.Type.LONG, 30_000L,
                    ConfigDef.Range.atLeast(0), ConfigDef.Importance.LOW,
                    "How long a failed registry lookup stands before the registry is asked "
                            + "again. Successful lookups are cached for the life of the serde; "
                            + "this only paces retries during an outage, so a registry that "
                            + "recovers is noticed without costing every record an attempt.")
            .define(VALIDATE_ON_WRITE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
                    "Validate against the schema's declared rules before serializing. Invalid "
                            + "messages are rejected rather than written.")
            .define(VALIDATE_ON_READ, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                    "Validate after deserializing. Off by default: a consumer usually cannot "
                            + "fix what a producer already wrote, so this is for topics whose "
                            + "producers do not all go through this serde.")
            .define(QUALITY_ON_WRITE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                    "Score messages against the quality dimensions their schema declares "
                            + "(ai.pipestream.proto.quality.v1) before writing, reporting the "
                            + "scores to the metrics listeners. Types declaring no dimensions "
                            + "cost nothing.")
            .define(QUALITY_ON_READ, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                    "Score after deserializing. Measurement only; reads are never rejected on "
                            + "quality.")
            .define(QUALITY_MIN, ConfigDef.Type.DOUBLE, null, ConfigDef.Importance.MEDIUM,
                    "Reject writes whose composite quality score falls below this threshold "
                            + "(0..1). Unset, quality is measured and reported but never "
                            + "gates.")
            .define(MAP_ON_WRITE, ConfigDef.Type.LIST, java.util.List.of(),
                    ConfigDef.Importance.LOW,
                    "Mapping rules applied in order to every message before it is validated "
                            + "and written: text rules ('target = source', 'target += source', "
                            + "'-field') or CEL rules ('target := <cel>' optionally followed "
                            + "by ' if <cel>', with the message bound as 'input').")
            .define(MAP_ON_READ, ConfigDef.Type.LIST, java.util.List.of(),
                    ConfigDef.Importance.LOW,
                    "The same rule forms, applied right after parsing - how a consumer "
                            + "reshapes records written before a schema moved, without waiting "
                            + "for producers to upgrade.");

    public ProtoMoltSerdeConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }
}
