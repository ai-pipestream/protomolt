# Core utilities

`protomolt-helpers` collects small, dependency-light utilities used across
the toolkit and useful on their own.

## Payloads and conversion

- `AnyHandler` — pack, unpack, and inspect `google.protobuf.Any` values,
  including type-URL handling.
- `TypeConverter` — conversions among messages, primitives,
  `Struct`/`Value`, and plain Java types.
- `PayloadCodec` — `Any`/`Struct` payload packing built on the two above,
  for pipelines that move loosely-typed payloads.

## Schema tooling

- `MappingHelper` (with `MappingHelperJsonSupport`) — walks descriptors to
  enumerate messages and fields, with JSON export; intended for building
  mapping and schema-browsing UIs.
- `MessageDiff` — field-level difference between two messages of the same
  type.

## Schema hygiene

- `ProtoFqnConflictDetector` — detects fully-qualified-name conflicts across
  a set of `FileDescriptorProto`s where two definitions would disagree on
  wire shape.
- `BinaryProtobufIdentifierValidator` — rejects illegal identifiers inside
  binary descriptors before they reach a registry or compiler.

Both throw `ProtoSchemaValidationException` with precise context, and both
are intended as pre-flight gates for anything that accepts schema uploads.
