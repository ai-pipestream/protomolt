# Validation

`protomolt-protobuf-validation` validates messages against rules carried on
their descriptors as protobuf options — the same `FieldOptions` /
`MessageOptions` mechanism protovalidate uses. The engine is
dialect-neutral: it evaluates an internal constraint model, and pluggable
rule sources translate annotation dialects into that model. The built-in
dialect is ProtoMolt's own `validate.v1` options; protovalidate's
`buf.validate` options are supported through an optional module and pass the
full protovalidate conformance suite.

## Declaring rules

```protobuf
import "ai/pipestream/proto/validate/v1/validate.proto";

message Person {
  string name = 1 [(ai.pipestream.proto.validate.v1.field) = {
    required: true
    string: { min_len: 2 }
  }];
  string email = 2 [(ai.pipestream.proto.validate.v1.field) = {
    cel: { id: "email.not_localhost"
           expression: "!this.endsWith('@localhost')" }
  }];
}
```

## Validating

```java
var result = ProtoValidator.forMessageType(Person.getDescriptor()).validate(person);
result.throwIfInvalid();
```

`ProtoValidator` compiles the rules for a message type once — regular
expressions and CEL programs eagerly, so malformed rules fail
deterministically at construction (`RuleCompilationException`) rather than
at some later validation call. Failures while evaluating a rule against a
value surface as `RuleEvaluationException`. `ValidationResult` carries
structured violations: field path, rule path, rule ID, and message.

## Rule surface

| Category | Rules |
|---|---|
| Any field | `required`, `cel` (custom CEL with `this` bound to the value) |
| `string` | `const`, `len`, `min_len`, `max_len` (code points), `pattern`, `prefix`, `suffix`, `contains`, `not_contains`, `in`, `not_in`, `email`, `uuid`, `hostname`, `uri`, `ip`, `ipv4`, `ipv6` |
| `int32` / `int64` (and `sint*`, `sfixed*`) | `const`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in` |
| `uint32` / `uint64` (and `fixed*`) | The same, with unsigned comparison semantics |
| `double` / `float` | `const`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `finite` |
| `bool` | `const` |
| `bytes` | `len`, `min_len`, `max_len`, `prefix`, `suffix`, `contains` |
| `enum` | `const`, `defined_only`, `in`, `not_in` |
| `repeated` | `min_items`, `max_items`, `unique`, `items` (nested rules per element) |
| `map` | `min_pairs`, `max_pairs`, `keys` / `values` (nested rules per entry) |
| `google.protobuf.Timestamp` | `gt`, `gte`, `lt`, `lte`, `lt_now`, `gt_now`, `within` |
| `google.protobuf.Duration` | `gt`, `gte`, `lt`, `lte` |

The string format rules (`email`, `hostname`, `ip`, `uri`, and friends) are
backed by `protomolt-formats`, a standalone zero-dependency library of RFC
validators that exposes the same names CEL's standard library uses.

### Semantics

Violation rule IDs are stable and deliberately align with protovalidate's
naming (`string.min_len`, `repeated.unique`, `timestamp.within`, …), so
results interoperate across annotation dialects. Paths use `[i]` for
repeated elements and `["key"]` for map entries, with a key marker when the
map key itself is in violation. Standard rules run only when the field is
present (proto3 semantics); repeated and map size rules also apply to empty
collections. Message-typed values — including repeated elements and map
values — are validated recursively.

## Rule sources

`ProtoValidator` never reads a specific annotation dialect directly. Each
dialect implements `ValidationRuleSource`, reading its own options off the
descriptor and translating them into the neutral constraint model
(`FieldConstraints`, `MessageConstraints`, and per-category records):

```java
public interface ValidationRuleSource {
    Optional<FieldConstraints>   fieldConstraints(FieldDescriptor field);
    Optional<MessageConstraints> messageConstraints(Descriptor message);
}
```

Every configured source is consulted for every field and message, and all
violations are merged — no source silently wins. The default chain,
`ValidationRuleSources.defaults()`, is the built-in `validate.v1` reader
plus any `ValidationRuleSource` found on the classpath via `ServiceLoader`,
so a dialect module takes effect just by being present and is removed
cleanly by dropping the dependency. The chain can also be pinned explicitly:

```java
// Built-in dialect only, ignoring classpath extensions
ProtoValidator.forMessageType(desc, ValidationRuleSources.pipestreamOnly());

// Explicit multi-dialect chain
ProtoValidator.forMessageType(desc,
    List.of(new AiPipestreamRuleSource(), new ProtovalidateRuleSource()));
```

## Protovalidate interoperability

`protomolt-protobuf-validation-protovalidate` provides
`ProtovalidateRuleSource`, which reads `(buf.validate.field)` and
`(buf.validate.message)` options — including predefined-rule extensions,
custom CEL, `Any`/`FieldMask` rules, and the well-known string and bytes
formats. Schemas annotated for protovalidate validate through
`ProtoValidator` unchanged; adding the module to the classpath is enough.
The module vendors `buf/validate/validate.proto` (pinned at v1.2.2,
Apache-2.0, attributed in its `NOTICE`). Rules are recovered even when
descriptors were built without the protovalidate extension registry, so
schemas loaded from a registry or reflection are enforced rather than
silently passed.

Compatibility is measured rather than claimed: the implementation passes
the complete protovalidate v1.2.2 conformance suite, 2872 of 2872 cases,
and CI re-scores the full suite with buf's own runner on every push.

## Conformance harness

`protomolt-protobuf-validation-conformance` drives `ProtoValidator` against
protovalidate's own conformance suite in two modes sharing one runner:

- **In-build.** A JUnit harness validates a curated set of
  `buf.validate`-annotated cases as part of `./gradlew build`, comparing the
  structured field and rule paths, rule IDs, and key flags of every
  violation exactly as the suite does. It gates at 100%, so any drift in
  emitted rule IDs or paths fails the build.
- **Authoritative.** `ConformanceMain` implements the language-agnostic
  executor protocol (a `TestConformanceRequest` on stdin, a
  `TestConformanceResponse` on stdout), so buf's own
  `protovalidate-conformance` binary can score the full suite:

  ```shell
  ./gradlew :protomolt-protobuf-validation-conformance:installDist
  protovalidate-conformance \
    protobuf/validation-conformance/build/install/protomolt-protobuf-validation-conformance/bin/protomolt-protobuf-validation-conformance
  ```

Structured field and rule paths are reconstructed with a faithful port of
the suite's own field-path algorithm, so a match reflects semantic
agreement, not formatting luck.

## At the gRPC boundary

`protomolt-grpc-validation` enforces the same rules at the call boundary —
the guarantee the [Kafka serde](kafka-serde.md) gives a topic, given to a
service:

```java
Server server = ServerBuilder.forPort(port)
    .addService(ServerInterceptors.intercept(service,
        ValidatingServerInterceptor.create()))
    .build();
```

Every inbound request message — unary or streamed — is validated before the
handler sees it; a violation is refused as `INVALID_ARGUMENT` naming every
violated rule. Malformed rules are the server's problem and surface as
`INTERNAL`, with the detail kept in server logs rather than on the wire.
The builder can also measure [quality dimensions](quality.md) per request
(`onQuality` callback) and gate on a floor (`qualityFloor`, refusing with
`FAILED_PRECONDITION`).

`ValidatingClientInterceptor` is the outbound half: an invalid request fails
locally with the same `INVALID_ARGUMENT` a validating server would send back,
without paying the network for an answer the descriptor in hand already knew.

## Related

- [JSON Schema generation](json-schema.md) renders the same constraint
  model as JSON Schema keywords.
- The [indexing facade](indexing.md) can run validation before emitting
  index documents.
- [Quality scoring](quality.md) measures where validation gates.
