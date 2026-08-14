/**
 * Kafka Connect OpenSearch sink: topic records become OpenSearch documents shaped by the
 * schema's declared indexing hints, through the same search-index SPI the rest of the
 * platform uses.
 *
 * <p>{@link ai.pipestream.proto.kafka.connect.opensearch.OpenSearchSinkConnector} registers
 * the plugin; {@link ai.pipestream.proto.kafka.connect.opensearch.OpenSearchSinkTask}
 * decodes each record (raw protobuf, Confluent wire format, or proto3 JSON) into the
 * configured message type, maps it with
 * {@code ai.pipestream.proto.index.opensearch.OpenSearchDocumentMapper} against the plan
 * built from the descriptor's {@code (ai.pipestream.proto.index.hints.v1.index)} options,
 * and writes each batch as one bulk request through
 * {@code ai.pipestream.proto.index.opensearch.OpenSearchSink}. The index can be created
 * from the plan-generated mappings at task start.
 *
 * <p>Because the mapping runs through the shared write path, {@code google.protobuf.Any}
 * fields expand against the descriptor set and every unpacked payload passes the
 * declared-rules gate ({@code skip_when}, per-field {@code ignore}, and the
 * {@code validate_payloads: false} schema opt-out honored); top-level messages are
 * likewise validated against their declared rules unless {@code validate=false}. Invalid
 * or undecodable records are {@code DataException}s routed by the worker's
 * {@code errors.tolerance}; a failed bulk write retries. Document ids are deterministic
 * (a configured message path or topic-partition-offset), so at-least-once redelivery
 * overwrites rather than duplicates.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/sink/kafka-connect-opensearch.md">
 * OpenSearch sink guide</a> for configuration and installation.
 */
package ai.pipestream.proto.kafka.connect.opensearch;
