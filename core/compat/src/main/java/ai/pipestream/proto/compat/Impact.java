package ai.pipestream.proto.compat;

/**
 * The compatibility dimensions a {@link SchemaChange} can break. A change carries the impacts
 * that apply to it; an empty impact set marks a purely informational change (still reported,
 * never a violation).
 *
 * <p>Direction terminology follows Confluent Schema Registry: <em>backward</em> asks whether a
 * consumer built against the NEW schema can read data written with the OLD schema;
 * <em>forward</em> asks whether a consumer built against the OLD schema can read data written
 * with the NEW schema. Wire and JSON are tracked separately because protobuf's binary format
 * keys fields by number while canonical proto3 JSON keys them by name.</p>
 */
public enum Impact {

    /**
     * Binary wire incompatibility in the backward direction: a consumer of the NEW schema
     * cannot correctly read data written with the OLD schema.
     */
    WIRE_BACKWARD,

    /**
     * Binary wire incompatibility in the forward direction: a consumer of the OLD schema
     * cannot correctly read data written with the NEW schema.
     */
    WIRE_FORWARD,

    /**
     * Canonical proto3 JSON incompatibility in the backward direction: a consumer of the NEW
     * schema cannot correctly read JSON data written with the OLD schema. In proto3 JSON the
     * field names (or {@code json_name}) and enum value names are the payload, and default
     * parsers reject unknown JSON fields.
     */
    JSON_BACKWARD,

    /**
     * Canonical proto3 JSON incompatibility in the forward direction: a consumer of the OLD
     * schema cannot correctly read JSON data written with the NEW schema. In proto3 JSON the
     * field names (or {@code json_name}) and enum value names are the payload, and default
     * parsers reject unknown JSON fields.
     */
    JSON_FORWARD,

    /**
     * Breaks generated code or the RPC surface: callers of generated classes, stubs or gRPC
     * method descriptors must change even when the encoded payloads remain readable.
     */
    SOURCE
}
