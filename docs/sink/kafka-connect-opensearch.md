# Kafka Connect: OpenSearch sink

`protomolt-connect-opensearch` lands topic records in OpenSearch as documents
shaped by the schema itself: the `(ai.pipestream.proto.index.hints.v1.index)`
field options compiled into the configured descriptor set drive field kinds,
engine names, analyzers, vectors, and `google.protobuf.Any` handling. There
are no per-field connector keys; the connector is a thin Kafka Connect shell
around the same [search indexing](../search/indexing.md) write path every
other ProtoMolt surface uses.

Because the mapping runs through the shared plan:

- `google.protobuf.Any` fields expand against the descriptor set, and every
  unpacked payload passes the declared-rules gate (`skip_when`, per-field
  `ignore`, and the `validate_payloads: false` schema opt-out honored).
- Top-level messages are validated against their declared
  `ai.pipestream.proto.validate.v1` rules before indexing (`validate=false`
  turns this off). Violations are data errors, routed by the worker's
  `errors.tolerance` (fail, skip, or dead-letter).
- The target index can be created at task start from the plan-generated
  mappings, `knn_vector` fields included.

Delivery is at-least-once with deterministic document ids: either a value
read from each message (`document.id.path`) or `topic-partition-offset`.
Either way a redelivered record overwrites its own document, so retries
converge instead of duplicating.

## Installation

```bash
./gradlew :protomolt-connect-opensearch:connectPluginZip
unzip sink/kafka/connect-opensearch/build/distributions/protomolt-connect-opensearch-plugin-*.zip \
      -d /opt/connect/plugins
```

Point the worker's `plugin.path` at `/opt/connect/plugins` and use the
`ByteArrayConverter` for record values (the connector parses protobuf bytes
itself; there is no Connect schema involved).

## The sink

```json
{
  "name": "orders-to-opensearch",
  "config": {
    "connector.class": "ai.pipestream.proto.kafka.connect.opensearch.OpenSearchSinkConnector",
    "topics": "orders",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "schema.descriptor.set.base64": "CrgCCg5zaG9wL29yZGVyLnBy...",
    "message.type": "shop.v1.Order",
    "opensearch.url": "http://opensearch:9200",
    "opensearch.index": "orders",
    "document.id.path": "order_id"
  }
}
```

| Key | Default | Meaning |
|---|---|---|
| `schema.descriptor.set.base64` | required | Base64 `FileDescriptorSet` declaring the document type, hint options compiled in (from the `compile` or `reflect` verbs, or a registry descriptor-set endpoint) |
| `message.type` | required | Fully qualified message type of the record values |
| `value.format` | `protobuf` | `protobuf` raw bytes, `confluent` wire format, or proto3 `json` text |
| `opensearch.url` | required | Cluster base URL |
| `opensearch.index` | required | Target index name |
| `opensearch.ensure.index` | `true` | Create the index from the plan-generated mappings at task start (idempotent) |
| `opensearch.refresh` | `false` | Refresh on every bulk write (immediately searchable, slower) |
| `document.id.path` | empty | Dotted proto path read as the document id; empty derives ids from topic-partition-offset |
| `validate` | `true` | Enforce declared validation rules — packed `Any` payloads included — before indexing; `false` suspends both (the schema's `validate_payloads` opt-out stays per-field) |

Error handling follows the platform's Connect conventions: an undecodable,
invalid, or unmappable record (an unknown `Any` type URL included) is a
`DataException`; a failed bulk write is a `RetriableException` and the batch
redelivers.

## Getting the descriptor set

The same recipe as [the gRPC connectors](kafka-connect.md): compile your
sources with the `compile` verb, reflect a live service, or fetch a registry
descriptor-set endpoint — anything that yields a serialized
`FileDescriptorSet` whose file protos carry the hint options. The connector
reads hints even from descriptor sets linked without the hint extensions
registered (they survive as unknown fields and are reparsed), so no special
tooling is required on the producing side.
