# Kafka serde

`protomolt-serde` is a Kafka protobuf serializer and deserializer that writes
the Confluent wire format and enforces the schema's own declared rules on the
way out.

The distinction is worth being precise about. Other protobuf serdes check that
the *schema* matches the registry, which is a schema-shape check. This one
checks that the *data* matches the schema, which is what the schema declares.
The rules already live on the descriptor as options, so the serializer
validates before it frames and a message that violates its own contract is
rejected rather than written. Validation stops being something a producer has
to remember to call, because it stops being the application's code path.

## Wire format

Confluent's, implemented from its published specification:

```
[0x00 magic][4-byte big-endian schema id][message-index array][payload]
```

The message-index array locates the message within its schema's file: the
index of the top-level type, then one index per nesting step. Its varints are
zigzag-encoded, and the common single index `[0]` is written as a lone zero
byte. Records written by this serializer are readable by any Confluent-
compatible consumer, and records written by a Confluent producer are readable
by this deserializer.

## Where the schema comes from

A descriptor set the deployment already packages, on the classpath or inline:

```properties
value.serializer=ai.pipestream.proto.kafka.serde.ProtoMoltProtobufSerializer
protomolt.descriptor.set.resource=schemas/orders.desc
protomolt.message.type=acme.orders.v1.Order
```

That alone is a working serde, with no registry involved and no per-record
network hop.

Point it at a Confluent-compatible registry and it will use one:

```properties
protomolt.registry.url=http://localhost:8081
```

On write, the id registered for the subject (`<topic>-value` by default) is
looked up once and stamped into the frame — paired with the message's index
in the *registry's* schema, because an index path is a position in the file
the id names, and the packaged file may lay the same types out differently.
On read, the frame's id is resolved to the registry's schema, which is how a
consumer follows a topic whose writers have moved on.

The registry answer also settles type *identity* by name. A frame's
message-index array is a position in the writer's file, not a name, and two
files can declare the same message at different positions; with a registry
answer, the deserializer checks that the resolved type's full name is the
configured one, so a writer whose file lays its messages out differently is
still read correctly — and a frame that genuinely carries another type is
refused by name. Only when no registry can answer does the check fall back to
comparing the frame's index path against the configured type's position in
the packaged file.

## What comes back

The application's own generated classes, when they are on the classpath. The
descriptor set records each file's Java options — `java_package`,
`java_multiple_files`, `java_outer_classname` — which is exactly what protoc
used to name the generated classes, so the deserializer derives the class
name, looks it up once per type, and parses straight into it. A consumer gets
its `Order` back and calls `getId()`. Nothing is configured; a type with no
generated class (descriptor-set-only deployments, types resolved from a
registry this application never compiled) comes back as a `DynamicMessage`,
and `protomolt.generated.classes=false` turns the whole behavior off.

For Kafka Streams, `ProtoMoltSerde` packages both halves as one `Serde`:

```java
Consumed.with(Serdes.String(), new ProtoMoltSerde())
```

## Several types on one topic

Leave `protomolt.message.type` unset and the serde is unpinned. The
serializer accepts any type the descriptor set declares — the set stays the
producer's contract, and a type outside it is refused — looking each type's
id up under its own subject, which is what the record-name strategies are
for:

```properties
protomolt.subject.strategy=record
```

`topic` is the default (`<topic>-value`, Confluent's TopicNameStrategy);
`record` uses the message's full name and `topic-record` uses
`<topic>-<full name>`, matching Confluent's other two strategies byte for
byte.

An unpinned deserializer resolves each frame's type through the registry,
and therefore requires one: an index path is a position, not a name, and
without a registry (or a pinned type) there is nothing to turn it into one.
Resolved ids are cached for the serde's lifetime, so a registry outage only
affects ids the consumer has never seen before.

### When the registry cannot answer

The packaged descriptor set is used instead, and the record is still processed.
This is deliberate: a serde that fails when the registry does turns a metadata
service into a hard runtime dependency of every producer and consumer on the
cluster. An unreachable registry should not stop a producer whose schema has
not changed.

Falling back supplies a schema; it does not suspend the contract that schema
declares. Messages are still validated against the packaged schema's rules, and
a frame carrying a type other than the configured one is still refused rather
than parsed as the wrong message. The fallback is logged once per serde, not
once per record, because a warning on every message during an outage is a
second outage.

Nor does the outage cost every record a connection attempt. A lookup that
failed is not retried until `protomolt.registry.retry.backoff.ms` (default
30 seconds) has passed, and a lookup that succeeded is cached for the life of
the serde. The backoff cuts both ways: a registry that comes back — or a
subject registered after the producer started — is noticed at the next window,
so the stamped id repairs itself without a restart.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `protomolt.descriptor.set.resource` | | Classpath resource holding a serialized `FileDescriptorSet`. Exactly one of this or the base64 form is required |
| `protomolt.descriptor.set.base64` | | The same, inline |
| `protomolt.message.type` | unset | Pin the serde to one fully qualified type; unset accepts any packaged type |
| `protomolt.registry.url` | none | A Confluent-compatible registry, if there is one |
| `protomolt.subject` | per strategy | Explicit subject, overriding the strategy |
| `protomolt.subject.strategy` | `topic` | `topic`, `record`, or `topic-record` |
| `protomolt.schema.id` | `0` | Id stamped when no registry answers |
| `protomolt.registry.retry.backoff.ms` | `30000` | How long a failed registry lookup stands before asking again |
| `protomolt.generated.classes` | `true` | Return generated Java classes when they are on the classpath |
| `protomolt.validate.on.write` | `true` | Reject invalid messages instead of writing them |
| `protomolt.validate.on.read` | `false` | Validate after deserializing |
| `protomolt.quality.on.write` | `true` | Score declared quality dimensions before writing |
| `protomolt.quality.on.read` | `false` | Score after deserializing (measure only) |
| `protomolt.quality.min` | unset | Reject writes whose composite score falls below this |
| `protomolt.map.on.write` | none | Mapping rules applied before validating and writing |
| `protomolt.map.on.read` | none | Mapping rules applied right after parsing |

Validating on read is off by default. A consumer usually cannot fix what a
producer already wrote, and one that starts rejecting history on upgrade is
worse than one that does not. Turn it on for topics whose producers do not all
come through this serde, which is the only way invalid data gets in.

## Mapping: the serde as a transformer

An ordered list of mapping rules can run inside the serde — what Confluent's
data contracts call migration rules, done with ProtoMolt's own
[mapping machinery](mapping.md) instead of a second expression language:

```properties
protomolt.map.on.write=id = legacy_name, -scratch
protomolt.map.on.read=display_name := input.legacy_name + ' (legacy)' if input.display_name == ''
```

Two rule forms share one ordered list. Text rules are exactly the mapping
docs' (`target = source`, `target += source`, `-field`); CEL rules are
`target := <selector>`, optionally followed by ` if <filter>`, with the
current message bound as `input` (the filter separator is the last ` if ` in
the entry).

One flat-properties caveat: these are Kafka LIST configs, so entries in a
properties file split on commas. A CEL expression that itself contains commas
— `clamp(x, 0.0, 1.0)`, say — needs the config passed as an actual list
(programmatic configuration, or any format that preserves list structure).

Write-side rules run *before* validation and quality: a producer normalizes,
and the contract judges the normalized message — which is also what reaches
the topic. Read-side rules run right after parsing: a consumer reshapes
records written before a schema moved, without waiting for producers to
upgrade. Rules are the deployment's, not the schema's; a rule that cannot
apply to a record fails that record loudly rather than being skipped, and a
malformed rule fails at configure time.

## Metrics: the validator as a data-quality sensor

The serde is the choke point every record passes through, and it is already
deciding what a data-quality dashboard wants to know. Drop
`protomolt-serde-micrometer` on the classpath — nothing to configure, the
serde discovers it — and every serde reports counters to Micrometer's global
registry, which is where an instrumented application already points its
Prometheus (or other) backend:

| Meter | Tags | Counts |
|---|---|---|
| `protomolt.serde.records` | direction, topic, type | records written and read |
| `protomolt.serde.rejections` | direction, topic, type | records refused for violating their rules |
| `protomolt.serde.violations` | topic, type, rule | individual rule violations |
| `protomolt.serde.refusals` | topic, reason | records refused on type identity |
| `protomolt.serde.registry.fallbacks` | | lookups the packaged set answered instead |

A rising `violations{rule="string.min_len"}` on one topic is a producer
regression announcing itself — from inside the producer, with no sidecar and
no extra pass over the data.

Other metrics systems plug in the same way: implement `SerdeMetricsListener`
(all methods defaulted) and register it via `META-INF/services`. Listeners
observe, never participate — one that throws is logged once and costs no
records.

## Quality scoring

Schemas can declare [quality dimensions](quality.md) — CEL expressions
returning scores — and the serializer measures every record against them by
default (`protomolt.quality.on.write`), reporting composites and per-dimension
scores through the same metrics listeners
(`protomolt.serde.quality.score` / `.dimension` distributions in Micrometer).
Types that declare no dimensions cost nothing. Set `protomolt.quality.min`
to turn the measurement into a write-side gate; reads only ever measure.

## How this compares

Against the two protobuf serdes people actually deploy:

| | ProtoMolt | Confluent | Apicurio |
|---|---|---|---|
| License | Apache-2.0 | Apache-2.0 jar, CCL compile-scope dependency | Apache-2.0 |
| Data validated against the schema's declared rules | serializer and deserializer, on by default | via Data Contracts: CEL rule strings in registry metadata, registry required | no — its validation option is a schema-shape diff, cached per type, that never looks at field values |
| Runs with no registry at all | yes — the descriptor set is sufficient | no | no |
| Registry outage | packaged fallback; paced retries; still validating | fails unresolved lookups | fails by default; two opt-in fallbacks, each needing per-serde configuration |
| Generated-class return | automatic, on by default, derived from the descriptor set's java options | `derive.type` / `specific.protobuf.value.type` | explicit return class, or opt-in `derive.class` |
| Multi-type topics | unpinned mode + record-name strategies | record-name strategies | record-name strategies |
| Auto-registration | no, deliberately (see below) | yes (default on) | yes (option) |

Auto-registration is the one their column wins, and it is declined on
purpose: an id is something the schema's owner publishes, not something a
producer decides, which is why registries are routinely deployed with
auto-registration off. A subject the registry does not know falls back to
the configured id.

## Current limitations

- **A registry-resolved schema carries only its own rules.** The validator
  reads rules from descriptor options even when the descriptor was built
  without the extensions registered (the annotations survive as unknown fields
  and are reparsed), so a descriptor set from any source keeps its rules. But a
  schema resolved from a registry declares whatever its `.proto` text declares,
  which for a schema registered by another tool is typically no rules at all.
