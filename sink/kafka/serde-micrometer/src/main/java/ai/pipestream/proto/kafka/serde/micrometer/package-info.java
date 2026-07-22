/**
 * A Micrometer binding for the Kafka serde's metrics events.
 *
 * <p>{@link ai.pipestream.proto.kafka.serde.micrometer.MicrometerSerdeMetrics} implements
 * {@link ai.pipestream.proto.kafka.serde.SerdeMetricsListener} and turns each event into a
 * Micrometer meter — records written and read, records refused for violating their schema's
 * declared rules, individual rule violations, type refusals, registry fallbacks, and quality
 * scores — tagged by topic, type, and rule.</p>
 *
 * <p>The listener is registered as a {@link java.util.ServiceLoader} provider, so putting this
 * module on the classpath is the whole configuration: the serde discovers it and the no-arg
 * constructor binds to {@link io.micrometer.core.instrument.Metrics#globalRegistry}. A second
 * constructor takes an explicit registry for applications that do not use the global one. This
 * module is one implementation of the SPI declared in
 * {@code ai.pipestream.proto.kafka.serde}; other metrics systems are bound the same way.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/kafka-serde.md">Kafka
 * serde guide</a> for the meter names and their tags.</p>
 */
package ai.pipestream.proto.kafka.serde.micrometer;
