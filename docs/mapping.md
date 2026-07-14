# Field mapping

The mapper modules reshape protobuf messages at runtime: copying and moving
fields, appending to repeated fields, clearing scratch data, and — with CEL —
doing all of that conditionally, computing values from expressions. Mapping
is deliberately separate from [validation](validation.md): reshaping a
message and judging it are different concerns.

Two layers are involved. `protomolt-mapper-core` provides path-based field
access and a small text rule language; `protomolt-mapper-cel` wraps it with
CEL filters and selectors.

## Text rules

`ProtoFieldMapper` (implemented by `ProtoFieldMapperImpl`) applies rules to a
message builder:

```java
var registry = DescriptorRegistry.create();
var mapper = new ProtoFieldMapperImpl(registry);

mapper.mapInPlace(builder, List.of(
    "title = body",
    "tags += \"proto\"",
    "-scratch"
));
```

| Rule | Form | Effect |
|---|---|---|
| Assign | `target = source` | Copy the value at `source` to `target` |
| Append | `target += source` | Append to a repeated `target` |
| Clear | `-field` | Clear the field |

Paths are dotted field paths; sources may be paths or literals. The mapper
also exposes the underlying path operations directly (`getValue`,
`setValue`, `appendValue`, `clearField`) for programmatic use.

## CEL mapping

`CelProtoMapper` evaluates [CEL](https://cel.dev) expressions against the
message, bound by default as `input`. Each `CelMappingRule` combines an
optional boolean filter (should this rule fire?), an optional selector (what
value to write?), a target path, and an optional list of text rules to fall
back on when no selector is given.

```java
var cel = new CelEvaluator(CelEnvironmentFactory.builder()
    .addMessageType(builder.getDescriptorForType())
    .addVar("input")
    .build());

new CelProtoMapper(mapper, cel).map(builder, List.of(
    new CelMappingRule(
        "input.lang == 'en'",   // filter
        "input.title",          // selector
        "search_title",         // target path
        List.of())              // text-rule fallback
));
```

`CelEnvironmentFactory` builds the CEL environment: message types, extra
variables, and custom function bindings. `CelEvaluator` compiles and caches
programs — successful compilations and compile failures alike, so a bad
expression fails fast on every use instead of recompiling. Compilation and
evaluation problems surface as `CelCompilationException` and
`CelEvaluationException`.

Beyond `map`, the mapper supports progressive builder state (later rules see
earlier writes), extra per-call bindings, and candidate fallback via
`tryMap` / `mapFirstCandidate` — try a list of rules and keep the first that
applies.

Any message type works, including `google.protobuf.Struct` for
schema-loose payloads. The `:samples` module contains a runnable example
(`CelMappingSample`).

## Where mapping sits

The REST gateway and the indexing pipeline both accept mapped messages —
a common arrangement is shape/filter with the mapper, then
[validate](validation.md), then [index](indexing.md). For extracting a
metadata bag from messages (rather than reshaping them), see
[Schema metadata](metadata.md).
