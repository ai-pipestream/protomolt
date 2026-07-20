/**
 * The Confluent protobuf wire format, framework-neutral: a frame reader and writer with no Kafka
 * or Connect dependency.
 *
 * <p>{@link ai.pipestream.proto.kafka.wire.ConfluentWireFormat} frames a payload and reads a
 * frame's schema id, message-index path, and payload back, following Confluent's published
 * specification rather than any implementation. A byte sequence that is not a valid frame surfaces
 * as {@link ai.pipestream.proto.kafka.wire.ConfluentWireFormatException}, which callers translate
 * to their framework's error type — {@code SerializationException} in a Kafka serde,
 * {@code DataException} in a Kafka Connect plugin — so the same reader serves both without pinning
 * either framework.</p>
 */
package ai.pipestream.proto.kafka.wire;
