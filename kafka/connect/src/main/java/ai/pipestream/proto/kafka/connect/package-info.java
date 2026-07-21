/**
 * A Kafka Connect plugin that joins topics and gRPC services in both directions, descriptor-native
 * with no generated stubs.
 *
 * <p>{@link ai.pipestream.proto.kafka.connect.GrpcSinkConnector} turns records from the subscribed
 * topics into request messages on a configured gRPC method, and
 * {@link ai.pipestream.proto.kafka.connect.GrpcSourceConnector} feeds a topic from a
 * server-streaming method, resuming from a CEL-extracted token stored as the Connect offset. Their
 * configuration lives in {@link ai.pipestream.proto.kafka.connect.GrpcSinkConfig} and
 * {@link ai.pipestream.proto.kafka.connect.GrpcSourceConfig}; the work is done by
 * {@link ai.pipestream.proto.kafka.connect.GrpcSinkTask} and
 * {@link ai.pipestream.proto.kafka.connect.GrpcSourceTask}.</p>
 *
 * <p>The package also carries four transforms that drop into any connector's pipeline, not only
 * these two: {@link ai.pipestream.proto.kafka.connect.ValidateMessage} checks a record value
 * against the rules its schema declares,
 * {@link ai.pipestream.proto.kafka.connect.MapMessage} reshapes it with field-mapping and CEL
 * rules, {@link ai.pipestream.proto.kafka.connect.RedactMessage} masks fields by their declared
 * sensitivity class, and {@link ai.pipestream.proto.kafka.connect.CelFilter} keeps or drops
 * records by a CEL predicate. Each reads record values as raw protobuf, Confluent-framed bytes,
 * or proto3 JSON.</p>
 *
 * <p>Every type is resolved from a serialized {@code google.protobuf.FileDescriptorSet} supplied
 * in configuration, so the worker needs no proto files and no rebuild when a schema changes. The
 * Iceberg sink ships separately in {@code ai.pipestream.proto.kafka.connect.iceberg}, and the
 * lower-level streaming SPI these connectors sit above is {@code ai.pipestream.proto.connector}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/kafka-connect.md">Kafka
 * Connect guide</a> for connector and transform configuration.</p>
 */
package ai.pipestream.proto.kafka.connect;
