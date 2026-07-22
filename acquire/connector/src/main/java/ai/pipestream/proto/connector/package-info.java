/**
 * Push-style streaming inputs behind one bounded, pausable SPI.
 *
 * <p>{@link ai.pipestream.proto.connector.StreamSource} is the contract: open it with a plan and
 * messages arrive on a listener until the stream ends, fails, or is closed. Each source kind
 * declares its own {@link ai.pipestream.proto.connector.SourcePlan} record carrying exactly what
 * it needs, so plans stay type-safe rather than collapsing into a property bag. Two
 * implementations ship here — {@link ai.pipestream.proto.connector.GrpcStreamSource} over a
 * server-streaming method described by {@link ai.pipestream.proto.connector.GrpcSourcePlan}, and
 * {@link ai.pipestream.proto.connector.KafkaSource} over a topic described by
 * {@link ai.pipestream.proto.connector.KafkaSourcePlan}. Failures surface as
 * {@link ai.pipestream.proto.connector.SourceException}.</p>
 *
 * <p>{@link ai.pipestream.proto.connector.SourcePump} bridges a source to a synchronous consumer:
 * the source fills a bounded queue, the consumer drains it one message at a time, and the pump
 * pauses and resumes the source to keep a fast producer from outrunning a slow pipeline. Flow
 * control is the handle's {@code pause} and {@code resume}; there is no cursor and no pull method,
 * so offset and resume-token ownership stays in the deployment layer.</p>
 *
 * <p>Byte-oriented sources delegate framing to a {@link ai.pipestream.proto.connector.MessageParser},
 * whose factory methods cover a fixed message type, length-free protobuf bytes, and the Confluent
 * wire format by way of {@code protomolt-serde}. The Kafka Connect runtime in
 * {@code ai.pipestream.proto.kafka.connect} is the production path when a deployment wants managed
 * offsets and rebalance-safe delivery.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/connector.md">Stream
 * connectors guide</a> for the SPI walkthrough.</p>
 */
package ai.pipestream.proto.connector;
