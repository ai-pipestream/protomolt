# Compatibility checking

`protomolt-compat` detects breaking changes between two versions of a
protobuf schema — programmatically, as a library. It is the write-gate a
schema registry needs, a CI check for schema repositories, and a
pre-publish guard in front of the [schema publishers](publishing.md).

The design separates *what changed* from *what you tolerate*: a diff engine
produces typed changes with precise impact classification, and a policy
layer evaluates them under a compatibility mode.

## Checking

```java
var checker = CompatibilityChecker.create();   // wire-level rules

CompatibilityResult result = checker.check(oldSources, newSources, CompatibilityMode.BACKWARD);
result.throwIfIncompatible();
```

Overloads accept `FileDescriptorSet`s, single `FileDescriptor`s,
`ProtoSourceSet`s (compiled through the shared
[compiler](descriptor-sources.md)), and the registry-loader-friendly shape
`check(List<FileDescriptor> current, ProtoSourceSet incoming, mode)` — so
"load what the registry serves, check the incoming change" is one call.

`CompatibilityResult` reports `violations()` (changes the mode forbids) and
`changes()` (everything, including informational ones);
`throwIfIncompatible()` raises `IncompatibleSchemaException` enumerating
every violation's rule, path, and message.

## Modes

The modes follow schema-registry convention:

| Mode | Guarantee |
|---|---|
| `BACKWARD` | Consumers on the new schema can read data written with the old schema |
| `FORWARD` | Consumers on the old schema can read data written with the new schema |
| `FULL` | Both directions |
| `NONE` | Nothing enforced; changes are still reported |
| `*_TRANSITIVE` | The same guarantee against every historical version, not just the latest |

Transitive modes use `checkAgainstHistory(history, newSet, mode)`; the
non-transitive variants check only the most recent entry.

## Rule layers

By default the checker enforces **wire** semantics only — what protobuf
binary encoding actually tolerates — which matches how Confluent Schema
Registry judges protobuf schemas. Two stricter layers are opt-in:

```java
var checker = CompatibilityChecker.builder()
    .includeJsonRules(true)     // canonical proto3 JSON compatibility
    .includeSourceRules(false)  // generated-code / RPC surface stability
    .build();
```

JSON rules matter when payloads travel as proto3 JSON (as they do through
the [REST gateway](rest-gateway.md)): renaming a field is invisible on the
wire but breaks every JSON consumer, and strict JSON parsers reject unknown
fields that removal leaves behind in old payloads. Source rules treat
generated-code breakage (removed services, renamed types) as violations in
any non-`NONE` mode.

The engine understands protobuf's real wire-compatibility groups — the
varint-interchangeable integers, `sint*` zigzag as its own family,
fixed-width groups, `string`/`bytes` asymmetry (`string → bytes` is safe,
`bytes → string` is not), open enums interchanging with integers — plus
oneof moves, map key/value changes, reserved-number reuse, proto2
`required` semantics, and gRPC method signature and streaming changes.
Types are matched by fully-qualified name across the whole set, so moving a
message between files is not a change.

## With the publishers

The registries enforce their own compatibility server-side; the local
checker lets you fail earlier and with better diagnostics, or enforce
policies a registry does not offer (JSON rules, transitive checks):

```java
List<FileDescriptor> current = loader.loadDescriptors();
checker.check(current, gathered, CompatibilityMode.FULL).throwIfIncompatible();
publisher.publish(gathered, PublishOptions.defaults()).throwIfFailed();
```
