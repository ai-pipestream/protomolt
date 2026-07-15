# Kafka Connect

`protomolt-connect` is a Kafka Connect plugin that joins topics and gRPC
services in both directions, descriptor-native with no generated stubs:

- **`GrpcSinkConnector`** — records from subscribed topics become request
  messages on a configured gRPC method. Unary methods are called once per
  record; client-streaming methods receive each delivered batch as one
  stream.
- **`GrpcSourceConnector`** — a server-streaming method feeds a topic. The
  stream's flow control is the poll loop's pace, so an open-ended
  (Watch-style) stream is consumed indefinitely with a bounded buffer, and a
  CEL-extracted resume token stored as the Connect offset lets the
  subscription pick up where Kafka left off after a restart.
- **Three protobuf-aware transforms** — `ValidateMessage`, `MapMessage`,
  and `CelFilter` — that drop into *any* connector's pipeline, not just
  these two, and understand the records where stock SMTs see bytes.

Everything is configured with a serialized
`google.protobuf.FileDescriptorSet` — no proto files on the worker, no code
generation, no rebuild when the schema changes. The classes live in
`ai.pipestream.proto.kafka.connect`.

## Getting the descriptor set

Any of ProtoMolt's surfaces produces the base64 descriptor set the
connectors take:

- the `compile` verb returns `descriptorSetBase64` for inline or gathered
  sources;
- the `reflect` verb captures it from a live server's reflection service;
- the registry serves it per subject:
  `GET {nativePrefix}/subjects/{subject}/descriptor-set` (base64-encode the
  binary response).

## Installation

Kafka Connect loads plugins from directories on the worker's `plugin.path`.
The build packages a ready-made layout — the connector jar and its runtime
dependencies, with the worker-provided Connect framework kept out:

```bash
./gradlew :protomolt-connect:connectPluginZip
unzip kafka/connect/build/distributions/protomolt-connect-plugin-*.zip \
      -d /opt/kafka/plugins/
```

Releases attach the same zip as `protomolt-connect-plugin-<version>.zip`.

## The sink

```json
{
  "name": "orders-to-grpc",
  "config": {
    "connector.class": "ai.pipestream.proto.kafka.connect.GrpcSinkConnector",
    "topics": "orders",
    "grpc.target": "order-service:9090",
    "grpc.method": "shop.v1.OrderService/Record",
    "schema.descriptor.set.base64": "CvQBCg9zaG9wL3YxL29yZGVy...",
    "value.format": "protobuf",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
  }
}
```

| Key | Default | Meaning |
|---|---|---|
| `grpc.target` | — | gRPC target, e.g. `host:9090` or `dns:///svc:443` |
| `grpc.method` | — | `package.Service/Method`; unary or client-streaming |
| `schema.descriptor.set.base64` | — | Serialized `FileDescriptorSet` declaring the service |
| `value.format` | `protobuf` | `protobuf` (raw message bytes), `confluent` (framed with a schema id, as Confluent serializers write), or `json` (canonical proto3 JSON text) |
| `grpc.deadline.ms` | `30000` | Per call (unary) or per delivered batch (client-streaming) |
| `grpc.api.token` | — | Optional shared secret sent as `api_token` metadata |
| `grpc.plaintext` | `true` | Set `false` for TLS |

Unary methods are invoked once per record. Client-streaming methods receive
the whole delivered batch as one stream and complete it, so the service
acknowledges per batch — the natural fit when the service aggregates.

Error semantics follow Connect conventions: transient gRPC statuses
(`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED`) raise
`RetriableException` so the framework redelivers; any other status fails the
task. A record value that does not decode as the request message raises
`DataException`, which the worker routes by its configured error tolerance —
fail, skip, or dead-letter queue.

## The source

```json
{
  "name": "grpc-to-ticks",
  "config": {
    "connector.class": "ai.pipestream.proto.kafka.connect.GrpcSourceConnector",
    "topic": "ticks",
    "grpc.target": "feed-service:9090",
    "grpc.method": "feed.v1.FeedService/Watch",
    "schema.descriptor.set.base64": "CvQBCg9mZWVkL3YxL2ZlZWQ...",
    "grpc.request.json": "{\"shard\": \"us-east\"}",
    "resume.token.cel": "input.cursor",
    "resume.token.request.field": "resume_token",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
  }
}
```

| Key | Default | Meaning |
|---|---|---|
| `grpc.target` | — | gRPC target |
| `grpc.method` | — | `package.Service/Method`; must be server-streaming |
| `schema.descriptor.set.base64` | — | Serialized `FileDescriptorSet` declaring the service |
| `topic` | — | Topic the streamed messages are written to |
| `grpc.request.json` | `{}` | The subscribe request, as canonical proto3 JSON |
| `resume.token.cel` | — | CEL over each streamed message (bound as `input`) yielding its resume token |
| `resume.token.request.field` | — | Dotted path of a string field in the request where the stored token is injected on (re)subscribe |
| `record.key.cel` | — | Optional CEL yielding the record key as a string |
| `value.format` | `protobuf` | `protobuf` bytes (pair with the `ByteArrayConverter`) or proto3 `json` text (pair with the `StringConverter`) |
| `poll.max.records` | `500` | Maximum records per poll |
| `poll.timeout.ms` | `1000` | How long a poll waits for the stream before returning what it has |
| `reconnect.backoff.ms` | `1000` | Pause before resubscribing after the stream ends or fails transiently |
| `grpc.api.token` | — | Optional shared secret sent as `api_token` metadata |
| `grpc.plaintext` | `true` | Set `false` for TLS |

### Resume tokens

Server streams have no Kafka-style offsets, so the source manufactures
them: `resume.token.cel` is evaluated over each streamed message and the
result rides along as the record's Connect offset. On task (re)start the
last committed token is read back and injected into the subscribe request
at `resume.token.request.field`, so a server designed for resumption (a
cursor, a change-stream token, a sequence number rendered as a string)
continues where Kafka left off.

Delivery is at-least-once. After a mid-stream failure the task resubscribes
from the latest token it *emitted*, which may replay messages Kafka already
committed; consumers should treat the token (or the message identity) as
their deduplication key. Without `resume.token.cel` the source has no
offsets and every restart subscribes exactly as `grpc.request.json`
configures.

The stream is one subscription, so the connector always runs a single task
regardless of `tasks.max` — a second subscriber would duplicate every
message. Transient statuses and graceful stream completions resubscribe
after `reconnect.backoff.ms`; any other status fails the task. Everything
that can be validated — the method's shape, the request template, the token
field path, the CEL expressions (type-checked against the stream's message
type) — is validated at connector start, not first poll.

## The transforms

Three Single Message Transforms make protobuf records first-class in any
Connect pipeline — theirs or ours. All three share the schema configuration
(`schema.descriptor.set.base64`, `message.type`, and `value.format`:
`protobuf`, `confluent`, or `json`), pass tombstones through untouched, and
validate everything validatable — including compiling and type-checking the
CEL expressions against the message type — at configure time.

```json
"transforms": "validate,route",
"transforms.validate.type": "ai.pipestream.proto.kafka.connect.ValidateMessage",
"transforms.validate.schema.descriptor.set.base64": "CvQBCg9zaG9w...",
"transforms.validate.message.type": "shop.v1.Order",
"transforms.route.type": "ai.pipestream.proto.kafka.connect.CelFilter",
"transforms.route.schema.descriptor.set.base64": "CvQBCg9zaG9w...",
"transforms.route.message.type": "shop.v1.Order",
"transforms.route.expression": "input.region == 'us-east' && input.qty > 0"
```

**`ValidateMessage`** checks each value against the validation rules
declared on its schema (`ai.pipestream.proto.validate.v1` options) — the
rules travel inside the descriptor set, so the worker enforces exactly what
the schema authors declared. Valid records pass through untouched. For
invalid ones, `on.invalid` selects: `fail` (the default — the worker's
`errors.tolerance` then decides between failing, skipping, and the
dead-letter queue), `drop`, or `header` — pass the record through with the
violations as a JSON array (field, rule, ruleId, message) in the
`header.name` header (default `protomolt.violations`), ready for a
downstream router.

**`MapMessage`** reshapes values in place with ProtoMolt's mapping rules:
`rules` takes text rules (`note = details.summary`, `-internal_field`), and
`cel.rules.json` takes a JSON array of CEL rules
`{"filter"?, "selector"?, "target", "fallback"?}` evaluated with the
current message bound as `input`. The message type is unchanged, so
Confluent-framed values keep their original frame — the schema id stays
true — and JSON values come back as the same Java type they arrived as.

**`CelFilter`** keeps records for which a boolean CEL expression over the
decoded message is true and drops the rest. `on.error` decides what an
undecodable value or a runtime evaluation failure does: `fail` (default),
`keep`, or `drop`.

**`RedactMessage`** masks record values by their schema-declared
sensitivity classes (`ai.pipestream.proto.meta.v1.field.sensitivity`):
declare `pii` once in the proto and every topic this transform touches
honors it — `classes` (default `pii`) picks what to mask, `strategy`
picks `remove`, `redact` (strings become `***`), `encrypt`, or `decrypt`
(AES-GCM with the base64 `key`, a `PASSWORD`-typed config; ciphertext is
bound to its field, so values pasted elsewhere refuse to decrypt).
Recursion covers nested and repeated messages and message-valued map
entries; a class on a map field masks every entry's value.
