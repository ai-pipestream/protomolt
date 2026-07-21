/**
 * A Kafka protobuf serializer and deserializer that writes the Confluent wire format and enforces
 * the schema's own declared rules on the way out.
 *
 * <p>{@link ai.pipestream.proto.kafka.serde.ProtoMoltProtobufSerializer} and
 * {@link ai.pipestream.proto.kafka.serde.ProtoMoltProtobufDeserializer} are the two entry points,
 * paired as one {@link ai.pipestream.proto.kafka.serde.ProtoMoltSerde} for Kafka Streams. Both
 * halves read the same {@link ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig}: the schema
 * comes from a descriptor set the deployment packages, so there is no per-record registry lookup,
 * and a Confluent-compatible registry — when one is configured — resolves ids and subjects on top
 * of that rather than underneath it. Framing follows Confluent's published specification, which
 * {@link ai.pipestream.proto.kafka.wire.ConfluentWireFormat} implements in both directions.</p>
 *
 * <p>Validation is the serde's code path rather than the application's. Rules already declared on
 * the descriptor are checked before a message is framed and after a frame is parsed, per the
 * {@code protomolt.validate.on.write} and {@code protomolt.validate.on.read} settings, and
 * mapping rules can reshape a record on either path.</p>
 *
 * <p>{@link ai.pipestream.proto.kafka.serde.SerdeMetricsListener} is the extension point.
 * Implementations are discovered with {@link java.util.ServiceLoader}, so a listener on the
 * classpath observes every record the serde handles without configuration; the
 * {@code protomolt-serde-micrometer} module ships one that reports to Micrometer.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/kafka-serde.md">Kafka
 * serde guide</a> for configuration and the wire-format details.</p>
 */
package ai.pipestream.proto.kafka.serde;
