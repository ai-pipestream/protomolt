# Compatibility checking and release baselines

`protomolt-compat` detects breaking changes across ProtoMolt's contract surfaces
programmatically. It serves three roles:

1. **Protobuf schema write-gate** for schema registries, CI pipelines, and
   pre-publish checks in front of the [schema publishers](publishing.md).
2. **Stability tier model** (`StabilityTier`, `@ApiStability`, and
   `ai.protomolt.proto.meta.v1` stability options) classifying declarations
   across Java APIs, protobuf contracts, retained stored formats, REST routes,
   and action envelopes.
3. **Five-surface release-baseline gate** (`ReleaseBaselineGate`) comparing live
   repository surfaces against the recorded snapshot under
   `compatibility/baseline/`.

The design separates *what changed* from *what policy permits*: diff engines
observe typed changes with stable rule identifiers, and policy layers evaluate
them against compatibility modes, stability tiers, and reviewed waivers.

## Protobuf schema checking

```java
var checker = CompatibilityChecker.create();   // wire-level rules

CompatibilityResult result = checker.check(oldSources, newSources, CompatibilityMode.BACKWARD);
result.throwIfIncompatible();
```

Overloads accept `FileDescriptorSet`s, single `FileDescriptor`s,
`ProtoSourceSet`s (compiled through the shared
[compiler](../core/descriptor-sources.md)), and the registry-loader-friendly shape
`check(List<FileDescriptor> current, ProtoSourceSet incoming, mode)`. Therefore,
"load what the registry serves, check the incoming change" is one call.

`CompatibilityResult` reports `violations()` (changes the mode forbids) and
`changes()` (everything, including informational ones);
`throwIfIncompatible()` raises `IncompatibleSchemaException` enumerating
every violation's rule, path, and message.

### Modes

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

### Rule layers

By default the checker enforces **wire** semantics only: what protobuf
binary encoding actually tolerates, matching how Confluent Schema
Registry judges protobuf schemas. Two stricter layers are opt-in:

```java
var checker = CompatibilityChecker.builder()
    .includeJsonRules(true)     // canonical proto3 JSON compatibility
    .includeSourceRules(false)  // generated-code / RPC surface stability
    .build();
```

JSON rules matter when payloads travel as proto3 JSON (as they do through
the [REST gateway](../surface/rest-gateway.md)): renaming a field is invisible on the
wire but breaks every JSON consumer, and strict JSON parsers reject unknown
fields that removal leaves behind in old payloads. Source rules treat
generated-code breakage (removed services, renamed types) as violations in
any non-`NONE` mode.

The engine understands protobuf's real wire-compatibility groups: the
varint-interchangeable integers, `sint*` zigzag as its own family,
fixed-width groups, `string`/`bytes` asymmetry (`string → bytes` is safe,
`bytes → string` is not), open enums interchanging with integers, plus
oneof moves, map key/value changes, reserved-number reuse, proto2
`required` semantics, and gRPC method signature and streaming changes.
Types are matched by fully-qualified name across the whole set, so moving a
message between files is not a change.

### With the publishers

The registries enforce their own compatibility server-side; the local
checker lets you fail earlier and with better diagnostics, or enforce
policies a registry does not offer (JSON rules, transitive checks):

```java
List<FileDescriptor> current = loader.loadDescriptors();
checker.check(current, gathered, CompatibilityMode.FULL).throwIfIncompatible();
publisher.publish(gathered, PublishOptions.defaults()).throwIfFailed();
```

## Stability tiers across all five surfaces

Every externally reachable or retained contract in ProtoMolt resolves to a
`StabilityTier` (`ai.protomolt.proto.descriptors.StabilityTier`):

| Tier | Gate enforcement | Meaning |
|---|---|---|
| `STABLE` | Enforced (`violatesGate = true`) | Supported public contract. Breaking changes fail closed unless covered by an explicit reviewed waiver or major release boundary. |
| `DEPRECATED` | Enforced (`violatesGate = true`) | Scheduled for removal. Remains protected by compatibility gates until retired with a reviewed waiver or major release boundary. |
| `EVOLVING` | Informational (`violatesGate = false`) | Public contract under active refinement; additive evolution is expected and breaking changes are surfaced in reports. |
| `EXPERIMENTAL` | Exempt (`violatesGate = false`) | Incubating or opt-in surface; may change or be removed without a waiver. |
| `INTERNAL` | Exempt (`violatesGate = false`) | Implementation detail (including `.internal` packages); excluded from external compatibility guarantees. |

Transitioning a `STABLE` declaration to `DEPRECATED` is allowed without a waiver
because `DEPRECATED` continues to enforce compatibility gates. Downgrading a
`STABLE` or `DEPRECATED` declaration to `EVOLVING`, `EXPERIMENTAL`, or
`INTERNAL` is treated as a breaking change (`*_STABILITY_DOWNGRADED`) so a
protected contract cannot silently shed gate enforcement.

### How stability is declared per surface

1. **Java APIs (`CompatibilitySurface.JAVA_API`)**:
   Annotate packages (`package-info.java`), classes, interfaces, records, enums,
   constructors, methods, or fields with `@ApiStability(StabilityTier.STABLE)`
   (retained at `RUNTIME`). `ApiStabilityResolver` resolves member stability
   from member → declaring type → enclosing types → package → `.internal`
   package convention. `JavaApiSnapshot` discovers all exported top-level and
   recursively declared nested `public`/`protected` types across every published
   module in `bom/build.gradle` (excluding unpublished `samples/`, `examples/`,
   `tests/`, and internal demo modules). `JavaApiCompatibilityChecker` detects
   removed types, constructors, methods, and fields; return-type and field-type
   changes; visibility reductions; added `final` or `abstract` modifiers on
   extensible types/methods; removed supertypes; and added abstract methods on
   interfaces or abstract classes.
2. **Protobuf contracts (`CompatibilitySurface.PROTOBUF`)**:
   Declare file, message, field, enum, enum-value, service, or RPC method
   stability with `(ai.protomolt.proto.meta.v1.file_stability)` through
   `(ai.protomolt.proto.meta.v1.method_stability)` in
   `ai/protomolt/proto/meta/v1/stability.proto`, or via
   `labels: { key: "stability", value: "..." }` on
   `(ai.protomolt.proto.meta.v1.message)` / `(ai.protomolt.proto.meta.v1.field)`.
   `DescriptorMetadata.stability(...)` resolves the effective tier with parent
   and file inheritance, failing closed with `IllegalArgumentException` if
   present stability wire bytes cannot be parsed, if `StabilityLevel` carries an
   unrecognized enum wire value, or if `labels["stability"]` contains an
   unrecognized non-blank tier name (while preserving forward-compatible unknown
   protobuf fields). `ProtobufSurfaceCompatibilityChecker` combines `SchemaDiff`
   and `CompatibilityChecker` with element-level stability tiers.
3. **Stored formats (`CompatibilitySurface.STORED_FORMAT`)**:
   `StoredFormatSnapshot` records retained persistence, repository, archive,
   authorization, receipt, gRPC profile, mesh, delegation, and workflow schemas
   (21 root formats, including `repo.document.v1` pinned to
   `BuiltinDocumentSchema`, `repo.drive.v1`, `repo.manifest.accession.v2`,
   `repo.manifest.document.v2`, `repo.catalog.entry.v2`, `repo.archive.v1`,
   `authz.access-policy.v1`, `receipt.signed-work-record.v1`,
   `receipt.trust-snapshot.v1`, `grpc.service-profile.v1`,
   `mesh.cluster.event-log.v1`, `delegation.encrypted-repository-state.v1`, and
   `workflow.definition.v1`). `StoredFormatCompatibilityChecker` enforces format
   presence, root message type identity, non-regressing `formatVersion`, optional
   exact closure SHA-256 pins, wire/JSON schema compatibility, and reservation of
   any removed field number/name (`FIELD_REMOVED_NOT_RESERVED`).
4. **REST routes (`CompatibilitySurface.REST_ROUTE`)**:
   Declare route stability via `@ProtoRestExposed(stability = ...)`,
   `ProtoRestMethod.Builder#stability(...)`, or live server route contracts
   (`ProtoMoltRestMount.registerAllContributed`,
   `SchemaRegistryServer.routeContracts`, `UploadHttpServer.routeContracts`, and
   `IntakeHttpServer.routeContracts`). Because each standalone HTTP server
   dispatches requests from the same route-contract definitions read by the
   baseline generator, any route change perturbs the snapshot directly.
   `RestRouteCompatibilityChecker` checks route presence, path templates,
   `{param}` path parameter sequences, accepted HTTP verbs, required query
   parameters, request/response media types and message types, 2xx success status
   codes, and authentication requirements.
5. **Action envelopes (`CompatibilitySurface.ACTION_ENVELOPE`)**:
   `ProtoAction#stability()` resolves the action's stability tier from
   `@ApiStability` on the action class (defaulting to `STABLE`) across
   `ProtoMoltCatalog.full(...)`, delegation, metric, mesh/cluster, registry
   federation/config, search/replay, JDBC/S3 pull, and workflow-authoring action
   suites. `ActionEnvelopeCompatibilityChecker` checks action presence,
   `requiredScope`, streaming mode, request/response message types, required JSON
   Schema input properties, and input/output field types.

## Release baseline lifecycle and CI gate

The recorded baseline lives under `compatibility/baseline/`:

- `manifest.json`: `ReleaseBaselineManifest` (`status`, `releaseVersion`,
  `releaseTag`, `baselineNote`, and reviewed `waivers`)
- `java-api.json`: `JavaApiSnapshot`
- `protobuf-descriptors.binpb` and `protobuf-stability.json`:
  `ProtobufSurfaceSnapshot` (`ReleaseBaselineGate.load` verifies that
  `protobuf-stability.json` matches the stability map derived from
  `protobuf-descriptors.binpb` and fails closed on any drift)
- `stored-formats.json`: `StoredFormatSnapshot` (including encoded descriptor
  closures and fingerprints)
- `rest-routes.json`: `RestRouteSnapshot`
- `action-envelopes.json`: `ActionEnvelopeSnapshot`

### Pre-release snapshot vs. released baseline

`ReleaseBaselineStatus` enforces a strict boundary between in-repository
development gating and external release guarantees:

- **`UNRELEASED_INITIAL_SNAPSHOT`** (current repository state):
  Enforces compatibility rules across all five surfaces in CI while explicitly
  recording `releaseTag: ""`. ProtoMolt does **not** claim an external released
  stability guarantee while the baseline status is `UNRELEASED_INITIAL_SNAPSHOT`.
  `ReleaseBaselineManifest` rejects any non-empty `releaseTag` in this state.
- **`RELEASED_BASELINE`**:
  Cut when an official external release tag is published. Requires a non-blank
  `releaseTag` and `releaseVersion`.

### Running and advancing the baseline

Both the Java API classpath and the protobuf descriptor snapshot are derived
from the published module inventory in `bom/build.gradle`, compiling every
published `.proto` file under `src/main/proto` or `src/main/resources` (excluding
unpublished sample, fixture, and conformance modules) through
`ProtoSourceCompiler` and asserting non-empty snapshots before writing. New
published modules and contracts enter the snapshot automatically instead of
depending on a hand-maintained generated-class list.

Verify current repository surfaces against `compatibility/baseline/`:

```bash
./gradlew :protomolt-system-tests:checkCompatibilityBaseline
```

Additive compatible changes (new optional protobuf fields, new Java classes or
methods, new REST routes, new action verbs, or changes to `EXPERIMENTAL` /
`INTERNAL` surfaces) pass `checkCompatibilityBaseline` without modifying
`compatibility/baseline/`.

When advancing the baseline snapshot (for example, to record newly stabilized
contracts or after adding an explicit reviewed `CompatibilityWaiver` in
`compatibility/baseline/manifest.json`):

```bash
./gradlew :protomolt-system-tests:updateCompatibilityBaseline
```

`updateCompatibilityBaseline` runs `ReleaseBaselineGate.advanceBaseline(...)`,
which evaluates the new snapshot against the existing baseline first, refuses to
overwrite the baseline if unwaived breaking changes or stale unused waivers
exist, and retires consumed waivers from `manifest.json` once the baseline
advances past the waived break. Each `CompatibilityWaiver` in `manifest.json`
must name an exact `id`, `surface`, `ruleId`, `targetPath`, `approvalRef`, and
`reason` (wildcards such as `*` are rejected, and unused waivers fail closed).
