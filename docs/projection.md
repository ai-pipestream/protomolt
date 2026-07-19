# Projections: self-describing message-to-message mappings

A projection is a mapping that lives **on the target message itself**, as
ordinary protobuf descriptor options. The target's `.proto` is the mapping
definition: compliant proto, parseable by any `protoc`, versionable in the
registry like any other schema, and executable at runtime from descriptors
alone, with no generated classes required.

```proto
import "ai/pipestream/proto/projection/v1/projection.proto";

message SearchDoc {
  option (ai.pipestream.proto.projection.v1.sources) = {
    source: "acme.court.v1.Case"
    source: "acme.billing.v1.Matter"
  };

  string doc_id = 1 [(ai.pipestream.proto.projection.v1.from) = {
    paths: {path: "case_id" path: "matter_no"}
  }];
  string title = 2 [(ai.pipestream.proto.projection.v1.from) = {
    paths: {path: "style_of_cause" path: "caption"}
  }];
  string region = 3 [(ai.pipestream.proto.projection.v1.from) = {
    cel: "source.address.state.upperAscii()"
  }];
  string source_system = 4 [(ai.pipestream.proto.projection.v1.from) = {
    literal: {string_value: "filing-index"}
  }];
}
```

Two shards holding different message types can each project into `SearchDoc`
at index (or query) time and be searched as one. The "join" is the shared
target shape, and the mapping is data, not glue code.

## Provenance kinds

Each mapped field declares exactly one provenance. Fields without the
`(from)` option are never populated.

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

Values are coerced to the target field type with the usual rules
(`TypeConverter`): exact types pass through, scalars widen, messages re-parse
across descriptor instances of the same type, repeated fields copy
element-wise.

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

## Relationship to the rest of ProtoMolt

Projections sit on top of [field mapping](mapping.md) (`ProtoFieldMapper`
paths, `CelEvaluator`) and complement
[derived shapes](design/join-shapes.md): shapes synthesize new message types
from joins; projections make an existing, hand-authored type the declared join
target. Because the mapping is a registry-versioned schema artifact,
[compat](compatibility.md) can flag when a source schema evolution breaks a
projection that names it.
