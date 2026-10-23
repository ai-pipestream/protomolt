# Validation

`protomolt-protobuf-validation` validates messages against rules carried on
their descriptors as protobuf options: the same `FieldOptions` /
`MessageOptions` mechanism protovalidate uses. The engine is
dialect-neutral: it evaluates an internal constraint model, and pluggable
rule sources translate annotation dialects into that model. The built-in
dialect is ProtoMolt's own `validate.v1` options; protovalidate's
`buf.validate` options are supported through an optional module and pass the
full protovalidate conformance suite.

## Declaring rules

```protobuf
import "ai/protomolt/proto/validate/v1/validate.proto";

message Person {
  string name = 1 [(ai.protomolt.proto.validate.v1.field) = {
    required: true
    string: { min_len: 2 }
  }];
  string email = 2 [(ai.protomolt.proto.validate.v1.field) = {
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

`ProtoValidator` compiles the rules for a message type once: regular
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
collections. Message-typed values: including repeated elements and map
values: are validated recursively.

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
violations are merged: no source silently wins. The default chain,
`ValidationRuleSources.defaults()`, is the built-in `validate.v1` reader
plus any `ValidationRuleSource` found on the classpath via `ServiceLoader`,
so a dialect module takes effect just by being present and is removed
cleanly by dropping the dependency. The chain can also be pinned explicitly:

```java
// Built-in dialect only, ignoring classpath extensions
ProtoValidator.forMessageType(desc, ValidationRuleSources.protomoltOnly());

// Explicit multi-dialect chain
ProtoValidator.forMessageType(desc,
    List.of(new ProtomoltRuleSource(), new ProtovalidateRuleSource()));
```

## Protovalidate interoperability

`protomolt-protobuf-validation-protovalidate` provides
`ProtovalidateRuleSource`, which reads `(buf.validate.field)` and
`(buf.validate.message)` options: including predefined-rule extensions,
custom CEL, `Any`/`FieldMask` rules, and the well-known string and bytes
formats. Schemas annotated for protovalidate validate through
`ProtoValidator` unchanged; adding the module to the classpath is enough.
The module vendors `buf/validate/validate.proto` (pinned at v1.2.2,
Apache-2.0, attributed in its `NOTICE`). Rules are recovered even when
descriptors were built without the protovalidate extension registry, so
schemas loaded from a registry or reflection are enforced rather than
silently passed.

Compatibility is measured against a pinned upstream suite. On 2026-10-23,
ProtoMolt `193ef86dde3a9493795f0ac29aa99a03c72dbf2d` passed all **2880** cases
at upstream `63347b841120e916b4cd29de64b88548326c9a4f`, with **0 failed and
0 skipped**, using the default comparison mode described below. This runner
is newer than the vendored v1.2.2 schemas; the old 2872-case result is not the
current CI baseline. Passing this pin does not establish compatibility with
later upstream changes.

## Conformance harness

`protomolt-protobuf-validation-conformance` drives `ProtoValidator` against
protovalidate's conformance protocol in two modes sharing one runner:

- **In-build.** A JUnit harness validates a curated set of
  `buf.validate`-annotated cases as part of `./gradlew build`, comparing the
  structured field and rule paths, rule IDs, and key flags of every
  violation exactly as the suite does. It gates at 100%, so any drift in
  emitted rule IDs or paths fails the build. `ConformanceHarnessTest` has
  15 locally authored cases, not the full upstream corpus. The other local
  tests cover the executor protocol and field-path conversion.
- **Authoritative.** `ConformanceMain` implements the language-agnostic
  executor protocol (a `TestConformanceRequest` on stdin, a
  `TestConformanceResponse` on stdout), so buf's own
  `protovalidate-conformance` binary can score the full upstream suite.
  The upstream tool owns the cases and expected results; ProtoMolt supplies
  the executor under test. This does not invoke the upstream Java validator
  as ProtoMolt's validation implementation.

Structured field and rule paths are reconstructed with a faithful port of
the suite's own field-path algorithm, so a match reflects semantic
agreement, not formatting luck.

### What is compared

The upstream tool sends a binary protobuf request containing a descriptor
set and named `Any` test payloads. `ConformanceMain` registers the
`buf.validate` extensions before parsing, links the descriptors with
`DescriptorSets`, and parses each payload as a `DynamicMessage`.
`ConformanceRunner` calls the production `ProtoValidator`; the classpath
registers `ProtovalidateRuleSource` through `ServiceLoader`. Per-message
validators support descriptor-aware CEL. `FieldPaths` converts violations
to the structured field and rule paths expected by Buf. The response stays
binary protobuf on stdout; diagnostics belong on stderr.

The default upstream comparison checks success/failure and, for validation
violations, count, field path, rule path, rule ID and map-key flag. CI does
**not** enable `--strict_message` or `--strict_error`: it does not require
identical violation prose, and an expected compilation error may match a
runtime error. A claim about either strict mode needs a separate recorded
run. See the pinned upstream [comparison implementation](https://github.com/bufbuild/protovalidate/blob/63347b841120e916b4cd29de64b88548326c9a4f/tools/protovalidate-conformance/internal/results/result.go).

This is separate from `buf lint`, which applies the `STANDARD` and
`COMMENTS` schema-style rules configured in `buf.yaml`, and from
`buf breaking`, which checks schema compatibility. Neither establishes
runtime validation conformance. ProtoMolt-only rules such as taxonomy and
postal rules also need their own tests; the Buf corpus does not cover them.

### Pins and sources of truth

| Component | Pin | Maintained in |
|---|---|---|
| Full upstream conformance runner and corpus | `63347b841120e916b4cd29de64b88548326c9a4f` (CI uses `63347b8`) | [CI conformance job](../../.github/workflows/ci.yml) |
| Vendored `buf/validate/validate.proto` | v1.2.2, `4baed765070e580226a0a30bf2a357cc027dc1f7` | [Dialect NOTICE](../../protobuf/validation-protovalidate/NOTICE) |
| Vendored `buf/validate/conformance/harness/harness.proto` and field-path port provenance | v1.2.2, same commit | [Harness NOTICE](../../protobuf/validation-conformance/NOTICE) |
| Buf CLI for lint and breaking checks | `1.70.0` | [CI](../../.github/workflows/ci.yml) and [build prerequisites](../operations/building.md#prerequisites) |

These are independent pins. Installing a newer Buf CLI does not update the
validation suite or the vendored annotations. The runner resolves to Go
module version `v1.2.3-0.20260903145109-63347b841120`; it is not a released
v1.2.3 tag. The CI comment records why the runner advanced beyond v1.2.2.

### Reproduce the CI baseline locally

Run from the repository root with JDK 25 and Go on `PATH`, and network
access for uncached Gradle and Go dependencies. The upstream tool's
`tools/go.mod` determines its Go requirement; CI currently selects stable
Go. Use a dedicated binary directory so an unrelated installed runner
cannot silently supply a different suite:

```bash
set -euo pipefail
conformance_tools="$(mktemp -d)"
GOBIN="$conformance_tools" go install \
  github.com/bufbuild/protovalidate/tools/protovalidate-conformance@63347b8
go version -m "$conformance_tools/protovalidate-conformance"

./gradlew :protomolt-protobuf-validation:test \
  :protomolt-protobuf-validation-protovalidate:test \
  :protomolt-protobuf-validation-conformance:test \
  :protomolt-protobuf-validation-conformance:installDist \
  --max-workers=2 --console=plain --rerun-tasks

"$conformance_tools/protovalidate-conformance" \
  protobuf/validation-conformance/build/install/protomolt-protobuf-validation-conformance/bin/protomolt-protobuf-validation-conformance \
  2>&1 | tee protovalidate-conformance.log
```

At the current pin, the final summary must be
`PASS (failed: 0, skipped: 0, passed: 2880, total: 2880)` and the command
must exit successfully. Keep `pipefail` when using `tee`. Check both the
summary and exit status; an empty or filtered run is not evidence. Preserve
the log outside committed source, with the ProtoMolt commit, upstream full
commit/module version, JDK/Go versions, command and flags. Local JUnit XML
and HTML reports are under each module's `build/test-results/test` and
`build/reports/tests/test` directories.

The GitHub `conformance` job separately builds `installDist`, installs the
pinned upstream runner, and invokes it without filters or expected-failure
files. It runs on pull requests, pushes to `main`, and manual dispatches;
an arbitrary branch push without a PR does not trigger this workflow.
`./gradlew build` alone runs the local curated harness, not this external
full-suite command. Review the conformance job for the exact candidate
commit, not a previous green run.

### Assistant maintenance procedure

1. For changes to validation, the Buf dialect, descriptor/option handling,
   CEL, shared format rules, the harness, or their dependencies, run the
   relevant local tests and the complete pinned suite above. Preserve a
   small permanent regression test for each behavior fixed. Diagnose the
   production validator and its adapter separately; never hard-code a
   case name or move enforcement into the harness to make the score green.
2. Before a release or a validation dependency/pin update, compare the
   pinned commit with upstream [releases](https://github.com/bufbuild/protovalidate/releases)
   and [changes since the pin](https://github.com/bufbuild/protovalidate/compare/63347b841120e916b4cd29de64b88548326c9a4f...main).
   Record the check date and exact candidate SHA. A passing pinned suite
   does not detect new upstream cases; CI has no automatic pin-update job.
3. Build a candidate runner into a separate `GOBIN` using that exact SHA
   and run its complete corpus against the same executor. Keep the current
   baseline run as well. Investigate changed expectations and new failures;
   fix their cause and add regression tests before advancing the pin.
   `--suite` and `--case` are useful for diagnosis only; rerun unfiltered
   before claiming conformance.
4. When adopting upstream changes, update the CI runner pin and rationale.
   Inspect changes to both vendored schemas and the field-path algorithm.
   If those need updating, copy the schemas verbatim, retain licensing,
   and update each module's `NOTICE` and its `src/main/resources/META-INF/NOTICE`
   copy. Regenerate affected outputs and run lint/breaking checks when
   contracts change. Do not edit vendored schemas to suppress failures.
5. Require zero failures and zero skipped/expected failures. Do not add
   `--expected_failures`, exclusions, disabled assertions, `continue-on-error`,
   or lower thresholds to obtain green. Report infrastructure/toolchain
   failures as unverified runs, with their cause; they are not passes.
6. Record old and new pins, full-suite totals, default/strict flags, test
   results and logs in the review. Update the dated result in this guide
   and current public claims in `docs/website-feature-inventory.md` only
   from the new evidence. A changed total must be explained by upstream
   corpus changes, not filtered cases. Do not infer strict-mode or newer
   release compliance from the default pinned run.

This procedure is also linked from `AGENTS.md` so future coding assistants
encounter it before changing the validation path.

## At the gRPC boundary

`protomolt-grpc-validation` enforces the same rules at the call boundary,
the guarantee the [Kafka serde](../sink/kafka-serde.md) gives a topic, given to a
service:

```java
Server server = ServerBuilder.forPort(port)
    .addService(ServerInterceptors.intercept(service,
        ValidatingServerInterceptor.create()))
    .build();
```

Every inbound request message, unary or streamed, is validated before the
handler sees it; a violation is refused as `INVALID_ARGUMENT` naming every
violated rule. Malformed rules are the server's problem and surface as
`INTERNAL`, with the detail kept in server logs rather than on the wire.
The builder can also measure [quality dimensions](quality.md) per request
(`onQuality` callback) and gate on a floor (`qualityFloor`, refusing with
`FAILED_PRECONDITION`).

`ValidatingClientInterceptor` is the outbound half: an invalid request fails
locally with the same `INVALID_ARGUMENT` a validating server would send back,
without paying the network for an answer the descriptor in hand already knew.

Everything the interceptors decide is observable, the same way the
[serde](../sink/kafka-serde.md) is: `GrpcValidationMetricsListener` implementations
found via `ServiceLoader` hear every validation, rejection (with the violated
rule ids) and quality score, on both sides of the wire. Drop
`protomolt-grpc-validation-micrometer` on the classpath and those events are
Micrometer meters: `protomolt.grpc.validation.requests` / `.rejections` /
`.violations` tagged by side, method, type (and rule), plus
`protomolt.grpc.quality.score` / `.dimension` distributions: on the global
registry, with nothing configured. Listeners observe, never participate: one
that throws is logged once and costs no calls.

## Related

- [JSON Schema generation](../schema/json-schema.md) renders the same constraint
  model as JSON Schema keywords.
- The [indexing facade](../search/indexing.md) can run validation before emitting
  index documents.
- [Quality scoring](quality.md) measures where validation gates.
