# Projections: self-describing message-to-message mappings

A projection is a mapping that lives **on the target message itself**, as
ordinary protobuf descriptor options. The target's `.proto` is the mapping
definition: compliant proto, parseable by any `protoc`, versionable in the
registry like any other schema, and executable at runtime from descriptors
alone, with no generated classes required.

```proto
import "ai/protomolt/proto/projection/v1/projection.proto";

message SearchDoc {
  option (ai.protomolt.proto.projection.v1.sources) = {
    source: "acme.court.v1.Case"
    source: "acme.billing.v1.Matter"
  };

  string doc_id = 1 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "case_id" path: "matter_no"}
  }];
  string title = 2 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "style_of_cause" path: "caption"}
  }];
  string region = 3 [(ai.protomolt.proto.projection.v1.from) = {
    cel: "source.address.state.upperAscii()"
  }];
  string source_system = 4 [(ai.protomolt.proto.projection.v1.from) = {
    literal: {string_value: "filing-index"}
  }];
  optional string category = 5 [
    (ai.protomolt.proto.projection.v1.from) = {
      paths: {path: "category"}
    },
    (ai.protomolt.proto.projection.v1.default_from) = {
      cel: "source.kind.lowerAscii()"
    }
  ];
}
```

Two shards holding different message types can each project into `SearchDoc`
at index (or query) time and be searched as one. The "join" is the shared
target shape, and the mapping is data, not glue code.

## Provenance kinds

Each mapped field declares primary provenance with `(from)`, fallback
provenance with `(default_from)`, or both. When both are present, `from` is
resolved first and `default_from` runs only when the primary result is absent.
The fallback never overwrites a primary value. A field with neither option is
never populated.

- **`paths`**: candidate dotted source paths, tried in order; the first that
  resolves to a present value wins. A path that does not resolve against the
  source type counts as absent, which is what makes one candidate list serve
  several source shapes.
- **`cel`**: a CEL expression evaluated with the source message bound as
  `source`, type-checked per source type at construction. An expression that
  does not compile against a source type counts as absent for that type;
  guard presence-dependent logic with `has()`. An expression that compiles
  against *no* declared source fails projection construction (that is a typo,
  not a join), and one that fails at evaluation fails the projection.
- **`literal`**: a constant `google.protobuf.Value`, independent of the
  source (provenance tags, fixed classifications).

`default_from` accepts the same three forms. In particular, a CEL default is
materialized into the projected protobuf before indexing, so every search
backend sees the same stored value and no backend needs its own expression
runtime. Runtime CEL failures remain projection failures; a calculated default
does not silently disappear.

Values are coerced to the target field type with the usual rules
(`TypeConverter`): exact types pass through, scalars widen, messages re-parse
across descriptor instances of the same type, repeated fields copy
element-wise. An enum value of another enum type converts by number, then by
name, and a number (a CEL enum, or a `number_value` literal) converts to the
enum value with that number; a value the target enum lacks fails. A message
of a different type goes through a nested projection, described next.

## Nested projections

A mapped message field whose own type declares `(sources)` gets a child
projection, compiled together with the parent. A source value of a declared
child source type is projected through the child: a singular value once, a
repeated value element by element. This is how a list of one message type
builds a list of another:

```proto
message Rendition {
  option (ai.protomolt.proto.projection.v1.sources) = {
    source: "acme.store.v1.Blob"
  };

  string name = 1 [(ai.protomolt.proto.projection.v1.from) = {
    cel: "has(source.filename) ? source.filename : source.blob_id"
  }];
  string mime_type = 2 [
    (ai.protomolt.proto.projection.v1.from) = {paths: {path: "mime_type"}},
    (ai.protomolt.proto.projection.v1.default_from) = {
      literal: {string_value: "application/octet-stream"}
    }
  ];
  int64 size_bytes = 3 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "size_bytes"}
  }];
}

message EntryView {
  option (ai.protomolt.proto.projection.v1.sources) = {
    source: "acme.store.v1.StoredDocument"
  };

  // Each Blob of the list becomes one Rendition.
  repeated Rendition renditions = 1 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "blob_bag.blobs.blob"}
  }];
  // A CEL list feeds the child the same way.
  repeated Rendition large = 2 [(ai.protomolt.proto.projection.v1.from) = {
    cel: "source.blob_bag.blobs.blob.filter(b, b.size_bytes > 1024)"
  }];
  // The singular arm becomes one Rendition.
  Rendition primary = 3 [(ai.protomolt.proto.projection.v1.from) = {
    paths: {path: "blob_bag.blob"}
  }];
}
```

For each message value the field receives:

- **The field's own type** (by full name) is copied as is. It is not
  projected again, so a value already in view shape keeps its fields.
- **A declared source of the child** is projected through the child, with
  the child's own `from` and `default_from` rules and its CEL bound to that
  element as `source`.
- **Any other message type** fails the projection, naming the field, the
  value's type and the child's declared sources.

A field whose message type declares no `(sources)` keeps the coercion rules
above. Child CEL rules are validated when the parent is built, so a child
typo fails `forTarget` for the parent. Recursive types work: a target that
contains itself reuses its own projection as the child.

In `sourceMask`, a path that feeds a child stays as written (here
`blob_bag.blobs.blob`). A FieldMask path names the whole subtree below it, and
a path ending in a repeated field cannot be extended, so the parent path
already covers every field the child reads; child CEL never makes the parent
mask incomplete. The child's own `sourceMask` over its source type reports
its reads in detail.

## Usage

```java
// Descriptors from a registry need the extensions registered once per
// ExtensionRegistry; generated classes need nothing.
MessageProjection.registerExtensions(registry);

MessageProjection projection =
    MessageProjection.forTarget(SearchDoc.getDescriptor(), SourceResolver.of(registry))
        .orElseThrow();

DynamicMessage doc = projection.project(caseMessage);   // or a Matter
```

`forTarget` returns empty for messages that are not projection targets. The
`SourceResolver` feeds eager CEL validation only. Projection itself takes the
source type from the message, so sources that were not resolvable at build
time still project. Instances are immutable and thread-safe; CEL programs are
compiled once per source type and cached.

## Deriving FieldMasks

The mapping is the single source of truth, so the two masks consumers usually
hand-maintain fall out of it:

```java
FieldMask writes = projection.targetMask();               // every populated target field
SourceMask reads = projection.sourceMask(Case.getDescriptor());
reads.fieldMask();   // paths the projection reads from Case
reads.complete();    // false: CEL rules apply to Case, mask is a lower bound
```

- **`targetMask()`** names every field the projection populates: the
  partial-response or update mask for APIs serving the target type, always
  exact and always in sync with the `.proto`.
- **`sourceMask(sourceType)`** names the candidate paths that resolve against
  one source type: the read-pruning set when fetching sources. It is exact
  unless a CEL rule compiles against that source type (CEL field references
  are not statically enumerable); check `complete()` before pruning on it.
  A CEL rule that does not compile against a source type reads nothing from
  it, so masks for that type stay complete.

This is the bridge to `google.protobuf.FieldMask` tooling: projections replace
hand-written masks for the mapping itself, and the derived masks feed the
request-time field-selection APIs where FieldMask is the right mechanism.

## Limits and sharp edges (v1)

- **Source masks take plain dotted names.** `sourceMask` ignores whitespace
  around a path and leaves out a path with an empty segment (a leading,
  trailing or doubled dot), since it names no field.
- **Map fields are not supported.** Projection fails with a deliberate error
  naming the field rather than guessing at generated-vs-dynamic runtime shapes.
- **Proto3 implicit defaults count as absent.** A source field at its default
  (`0`, `""`, `false`) does not project, even if application code set it
  explicitly, because proto3 cannot tell the difference. Use a CEL rule
  (`source.count`) when the zero value is meaningful.
- **Coercion follows `TypeConverter`.** Widening (`int32`→`int64`, numbers to
  strings, string parsing back to numbers) is safe; narrowing a fractional
  double into an integer target truncates. One failed field fails the whole
  projection with the field named in the error.
- **CEL sees real values**, including proto3 defaults, so path fallback and CEL
  can legitimately disagree about "absent" for the same field.
- **Defaults apply during projection, not mutation.** `default_from` fills a
  newly projected target only when its primary provenance is absent. It is not
  an instruction to overwrite an already stored message during an update.

## Relationship to the rest of ProtoMolt

Projections sit on top of [field mapping](mapping.md) (`ProtoFieldMapper`
paths, `CelEvaluator`) and complement
[derived shapes](join-shapes.md): shapes synthesize new message types
from joins; projections make an existing, hand-authored type the declared join
target. Because the mapping is a registry-versioned schema artifact,
[compat](../schema/compatibility.md) can flag when a source schema evolution breaks a
projection that names it.
