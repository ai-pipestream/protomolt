package ai.pipestream.proto.compat;

/**
 * The canonical rule identifiers {@link SchemaDiff} emits in {@link SchemaChange#ruleId()}.
 *
 * <p>Rule IDs are part of the public contract: they are stable strings, safe to match on in
 * gates, tooling, and UIs. Consumers should reference these constants rather than repeating
 * the literals. (The engine's own tests deliberately assert the literals, pinning the contract
 * from the outside.)</p>
 */
public final class ChangeRules {

    private ChangeRules() {
    }

    // Types
    public static final String MESSAGE_ADDED = "MESSAGE_ADDED";
    public static final String MESSAGE_REMOVED = "MESSAGE_REMOVED";
    public static final String ENUM_ADDED = "ENUM_ADDED";
    public static final String ENUM_REMOVED = "ENUM_REMOVED";

    // Fields
    public static final String FIELD_ADDED = "FIELD_ADDED";
    public static final String FIELD_REMOVED = "FIELD_REMOVED";
    public static final String FIELD_REMOVED_NOT_RESERVED = "FIELD_REMOVED_NOT_RESERVED";
    public static final String FIELD_REQUIRED_ADDED = "FIELD_REQUIRED_ADDED";
    public static final String FIELD_TYPE_CHANGED = "FIELD_TYPE_CHANGED";
    public static final String FIELD_TYPE_CHANGED_COMPATIBLE = "FIELD_TYPE_CHANGED_COMPATIBLE";
    public static final String FIELD_MESSAGE_TYPE_CHANGED = "FIELD_MESSAGE_TYPE_CHANGED";
    public static final String FIELD_ENUM_TYPE_CHANGED = "FIELD_ENUM_TYPE_CHANGED";
    public static final String FIELD_NAME_CHANGED = "FIELD_NAME_CHANGED";
    public static final String FIELD_JSON_NAME_CHANGED = "FIELD_JSON_NAME_CHANGED";
    public static final String FIELD_LABEL_CHANGED = "FIELD_LABEL_CHANGED";
    public static final String FIELD_PRESENCE_CHANGED = "FIELD_PRESENCE_CHANGED";
    public static final String FIELD_MAP_ENTRY_CHANGED = "FIELD_MAP_ENTRY_CHANGED";
    public static final String FIELD_MOVED_INTO_ONEOF = "FIELD_MOVED_INTO_ONEOF";
    public static final String FIELD_MOVED_OUT_OF_ONEOF = "FIELD_MOVED_OUT_OF_ONEOF";

    // Oneofs
    public static final String ONEOF_ADDED = "ONEOF_ADDED";
    public static final String ONEOF_REMOVED = "ONEOF_REMOVED";

    // Reserved
    public static final String RESERVED_NUMBER_REUSED = "RESERVED_NUMBER_REUSED";
    public static final String RESERVED_NAME_REUSED = "RESERVED_NAME_REUSED";
    public static final String RESERVED_RANGE_REMOVED = "RESERVED_RANGE_REMOVED";

    // Enum values
    public static final String ENUM_VALUE_ADDED = "ENUM_VALUE_ADDED";
    public static final String ENUM_VALUE_REMOVED = "ENUM_VALUE_REMOVED";
    public static final String ENUM_VALUE_NUMBER_CHANGED = "ENUM_VALUE_NUMBER_CHANGED";
    public static final String ENUM_VALUE_NAME_CHANGED = "ENUM_VALUE_NAME_CHANGED";

    // Services
    public static final String SERVICE_ADDED = "SERVICE_ADDED";
    public static final String SERVICE_REMOVED = "SERVICE_REMOVED";
    public static final String METHOD_ADDED = "METHOD_ADDED";
    public static final String METHOD_REMOVED = "METHOD_REMOVED";
    public static final String METHOD_REQUEST_TYPE_CHANGED = "METHOD_REQUEST_TYPE_CHANGED";
    public static final String METHOD_RESPONSE_TYPE_CHANGED = "METHOD_RESPONSE_TYPE_CHANGED";
    public static final String METHOD_STREAMING_CHANGED = "METHOD_STREAMING_CHANGED";
}
