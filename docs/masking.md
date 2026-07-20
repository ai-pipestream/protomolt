# Field masking

Sensitivity is declared once, on the field, in the schema — and every
surface that handles the message reads the same declaration. There is no
per-deployment field list to keep in step with the contract.

The masking primitive is `SensitivityMasker` in
`protomolt-protobuf-metadata`. It is exposed as the `mask-message`
[verb](actions.md) and as the `RedactMessage`
[Kafka Connect transform](kafka-connect.md).

## Declaring sensitivity

Masking reads the `sensitivity` field of the
`ai.pipestream.proto.meta.v1.field` option (extension number `59100491` on
`FieldOptions`) — the same [metadata](metadata.md) option that carries
descriptions, ownership, and labels.

```protobuf
import "ai/pipestream/proto/meta/v1/metadata.proto";

message Order {
  string id = 1;
  string email = 2 [(ai.pipestream.proto.meta.v1.field) = {sensitivity: "pii"}];
  bytes  token = 3 [(ai.pipestream.proto.meta.v1.field) = {sensitivity: "secret"}];
}
```

The class is a free-form string; `public`, `internal`, `pii`, and `secret`
are the conventional values but nothing enforces them. A field is masked
when its declared class is present, as an exact string, in the set of
classes the caller asked for. `MessageMeta` also has a `sensitivity` field;
the masker does not read it — masking is field-level only.

Descriptors must have been parsed with the metadata extension registered
(`DescriptorMetadata.registerExtensions`), or the option stays an unknown
field and matches nothing. The action layer, the Kafka Connect transforms,
and the Kafka serde all register it when they parse a descriptor set. A
descriptor set parsed without it produces a successful call that masks
nothing.

## Strategies

`remove` is the default. `encrypt` and `decrypt` require a key.

| Strategy | `string` fields | `bytes` fields | Every other type |
|---|---|---|---|
| `remove` | Cleared | Cleared | Cleared |
| `redact` | Replaced with `***` | Cleared | Cleared |
| `encrypt` | Base64 of the encrypted envelope | The encrypted envelope | Cleared |
| `decrypt` | Reverses `encrypt` | Reverses `encrypt` | Cleared |

Non-string types clear rather than take a placeholder: a redacted number or
boolean would still be a plausible value, and would read as data.

## Traversal

The walk descends through singular and repeated message fields, and through
message-valued map entries; a `pii` field three levels down, or inside a map
value, is found. Map entry paths are reported as `field[key].nested`. A
sensitivity class on a map field itself applies to every entry's value, and
clears the whole field when the value type cannot take the strategy's
transform.

A `google.protobuf.Any` hides its payload in opaque bytes, so the walk has
to resolve the packed type before it can see anything classed inside. When
it can, the payload is unpacked, masked as any other message, and repacked
under the same type URL; the bytes are left byte-for-byte alone when nothing
inside was classed. The default resolver searches the message's own file and
its transitive imports. `mask-message` supplies a wider one: the call's
schema first, then the context's descriptor registry.

A payload whose type cannot be resolved cannot be masked. The masker does
not fail and does not pass it over silently — the field path is returned in
`MaskResult.unresolvedPaths()`, and the caller decides what an unopened
payload means. See [what masking does not do](#what-masking-does-not-do)
for how far that report travels on each surface.

## The library

```java
var result = SensitivityMasker.mask(order, Set.of("pii"), Strategy.REDACT);
result.message();          // the masked message
result.maskedPaths();      // ["email", "contacts[home].email"]
result.unresolvedPaths();  // packed payloads that could not be opened
```

Overloads take a key (`byte[]`, required for `ENCRYPT`/`DECRYPT`) and a
`PayloadResolver` for `Any` payloads. Calling `mask` with `ENCRYPT` or
`DECRYPT` and no key throws; so does a key that is not 16, 24, or 32 bytes.

## The `mask-message` verb

| Field | Required | Means |
|---|---|---|
| `schema` | yes | `{"type": …}`, inline `{"sources": …}`, or `{"descriptorSetBase64": …}` |
| `type` | when the schema does not identify one | Fully qualified message type |
| `message` | yes | The message, as canonical proto3 JSON |
| `classes` | yes, non-empty | Sensitivity classes to mask, e.g. `["pii"]` |
| `strategy` | no | `remove` (default), `redact`, `encrypt`, `decrypt` |
| `key` | for `encrypt`/`decrypt` | Base64 AES key of 16, 24, or 32 bytes |

The result is `{"message": …, "maskedFields": [...]}`, plus
`"unresolvedPayloads": [...]` only when there were any. `maskedFields`
holds proto field paths, using field names rather than JSON names.

```shell
protomolt-cli mask-message --input-file mask.json
```

```json
{
  "message": { "id": "A-1", "email": "***" },
  "maskedFields": [ "email" ]
}
```

The typed `MaskMessage` RPC on `ProtoMoltService`, and its REST projection,
carry a narrower envelope than the verb: `MaskMessageRequest` has no `key`
field and `MaskMessageResponse` has no `unresolved_payloads` field. Over
gRPC and REST, therefore, only `remove` and `redact` are reachable, and
unresolved payloads are not reported. The [CLI](cli.md) and the
[MCP server](mcp.md) pass the JSON envelope through unchanged and have both.

## The `RedactMessage` transform

`ai.pipestream.proto.kafka.connect.RedactMessage` masks record values in
any Connect pipeline, source or sink. It shares the descriptor-driven
config every ProtoMolt transform uses.

| Config | Default | Means |
|---|---|---|
| `schema.descriptor.set.base64` | — | Base64 `FileDescriptorSet` declaring the type |
| `message.type` | — | Fully qualified message type of the record values |
| `value.format` | `protobuf` | `protobuf`, `confluent`, or `json` |
| `classes` | `pii` | Comma-separated sensitivity classes to mask |
| `strategy` | `remove` | `remove`, `redact`, `encrypt`, or `decrypt` (case-insensitive) |
| `key` | — | Base64 AES key; a `PASSWORD`-typed config, required for `encrypt`/`decrypt` |

```json
"transforms": "redact",
"transforms.redact.type": "ai.pipestream.proto.kafka.connect.RedactMessage",
"transforms.redact.schema.descriptor.set.base64": "CvQBCg9zaG9w...",
"transforms.redact.message.type": "shop.v1.Order",
"transforms.redact.classes": "pii,secret",
"transforms.redact.strategy": "redact"
```

Tombstones (a null value) pass through untouched, and a record with nothing
masked is returned as the original object rather than re-encoded.
Confluent-framed values keep their frame, since the message type does not
change. The transform uses the default `Any` resolver — the descriptor set
it was configured with — and does not surface unresolved payloads.

## Encryption

`encrypt` seals string and bytes values with **AES-GCM** (`AES/GCM/NoPadding`,
128-bit authentication tag) in a versioned envelope: a one-byte format
version, a 12-byte nonce drawn from `SecureRandom` per value, then the
ciphertext and tag. String fields carry that envelope base64-encoded; bytes
fields carry it raw.

The key is the caller's, always: 16, 24, or 32 raw bytes supplied base64 in
the request or the connector config. It is never read from the schema and
never stored — the contract declares what is sensitive, the operator holds
the means. ProtoMolt provides no key store, no key derivation, and no
rotation mechanism; the envelope's version byte reserves room for a second
algorithm or key generation without guessing which is in play.

The value's identity — its containing message's full name and its field
number — is bound as AES-GCM additional authenticated data. A ciphertext
moved to a different field, or to a field of a different message type,
fails to decrypt rather than silently opening. Ciphertext is therefore not
portable across fields; re-encrypt under the destination field to move a
value. `decrypt` with the wrong key, the wrong field, or a tampered value
fails loudly (`invalid-input` on the verb); it never returns garbage.

## `json_name` round-trip

Descriptor `json_name` does not survive every descriptor round-trip — some
encoders drop it — so `FieldMeta` carries a `json_name` of its own, set by
`infer-schema` when it sanitizes an original key (`user-name` → `user_name`).
Loaders call `DescriptorMetadata.materializeJsonNames` after parsing a
descriptor set and copy that annotation back into the descriptor's
`json_name` where the descriptor has none. The action layer, the Kafka
Connect transforms, and the Kafka serde all do this.

For masking, the effect is that a masked message printed back as proto3
JSON uses the document's original keys, so it can be compared against, or
substituted for, the input it came from. Field paths in `maskedFields` are
unaffected: they always use proto field names.

## What masking does not do

Masking is a schema-driven transform, not a security boundary in itself.
Being precise about the edges:

- It masks only fields whose declared class is in the requested set. A
  field carrying the same data without an annotation, or with a class the
  caller did not ask for, passes through. The output is only as good as the
  annotations on the schema.
- It runs where it is invoked. Copies made upstream, log lines written
  before the transform, and the bytes on the wire ahead of it are untouched.
- `remove` and `redact` are one-way. There is no recovery path, and `***`
  is a fixed literal that says nothing about the original.
- Removal and redaction do not hide that the field exists or that it was
  masked; the field path is in the result, and a redacted string is visibly
  a redacted string.
- An `Any` payload whose type cannot be resolved is not masked. The verb
  reports it in `unresolvedPayloads`; the gRPC/REST surface and the
  Connect transform do not. A caller treating masking as a boundary must
  resolve the payload type, or reject payloads it cannot open, rather than
  assume the output is clean.
- `encrypt` protects the value's confidentiality and integrity under the
  key, and nothing else. Envelope length reveals plaintext length. The
  field's presence is still visible. Because every value gets a fresh
  nonce, two equal plaintexts encrypt to different ciphertexts, so
  encrypted fields cannot be matched, grouped, or joined on.
- Field binding stops a ciphertext being replayed into a different field or
  message type. It does not stop one being replayed into the same field of
  a different record; nothing in the envelope identifies the record.
- The key travels in the request. For `mask-message` it is base64 inside
  the JSON envelope, and so appears in anything that records that request.
  For `RedactMessage` it is a `PASSWORD` config, which Connect masks in its
  REST responses and logs.
