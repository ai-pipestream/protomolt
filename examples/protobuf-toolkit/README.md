# Protobuf toolkit in your own application

This standalone Gradle build uses published ProtoMolt libraries. It does not
include the parent build, use `mavenLocal()`, start a server, or require Docker
or an LLM account. Its Java application exercises mappings, CEL selectors,
descriptor-declared projections, runtime validation, an embedded schema registry,
and OpenAPI generation.

## Run

Prerequisites: JDK 25 and network access to Maven Central, the Gradle plugin
portal, and Maven Central's snapshot repository. From the repository root:

```sh
./gradlew -p examples/protobuf-toolkit run --refresh-dependencies
```

You can also copy this directory outside the repository and run it with Gradle
9.6.1: `gradle run --refresh-dependencies`. The root wrapper above supplies Gradle
only; all ProtoMolt dependencies are resolved from Maven artifacts.

The current artifacts use `0.1.0-SNAPSHOT`. They are mutable, not an immutable
release. Refresh dependencies on first use: an existing Gradle cache may contain
older snapshots with earlier Java package names. A successful source build does
not prove that your cached published artifacts match it.

Machine-wide Gradle initialization scripts can override a build's repositories,
including adding `mavenLocal()`. For a clean consumer check on a developer machine,
run with a new Gradle home:

```sh
GRADLE_USER_HOME="$(mktemp -d)" ./gradlew -p examples/protobuf-toolkit run
```

This downloads Gradle and dependencies again, without reading your usual Gradle
initialization scripts or dependency cache. The hosted consumer check likewise
uses the example's declared public repositories.

Expected application output:

```text
mapping: copied Ada; selector: selected Ada; false filter: skipped
projection: Ada Lovelace; validation: accepted valid contact
validation: rejected invalid email, short name, and mismatched confirmation
registry: reused version 1; added version 2; rejected incompatible change
OpenAPI: wrote build/contact-openapi.json; name minLength = 2
Runtime validation remains required for the cross-field CEL rule.
```

The program fails if any of those outcomes is missing. Gradle may also report
deprecation warnings, and SLF4J reports that this example has no logging provider.
Neither warning is an application result.

## What to reuse

To connect these libraries to a running platform, follow
[Project data and run an accepted workflow](../../docs/tutorials/projected-workflow.md).
Its optional `workflowInput` task validates a projected contact and writes JSON
for the starter's launch form without contacting a server itself.

Read [ToolkitExample.java](src/main/java/example/ToolkitExample.java) alongside
[contact.proto](src/main/proto/contact.proto):

- `mappingAndSelection()` copies a field, selects a value with CEL, and checks
  that a false filter makes no write. For Struct, CEL exposes native map values.
- `projectionAndValidation()` projects `full_name` into `name` using descriptor
  annotations. ProtoMolt's validator reads Buf validation annotations and checks
  a valid contact plus three invalid cases. Projection alone does not validate.
- `registry()` embeds a compatibility-gated store, reuses an identical schema's
  identity, accepts a compatible addition, and refuses a changed wire type. The
  in-memory store loses its data when the process exits. This example does not
  exercise the registry's HTTP or Confluent protocol surfaces.
- `openApi()` generates an OpenAPI 3.0.3 file from the service descriptor without
  serving it. It checks the generated name-length and email constraints and the
  disclosure of the cross-field CEL rule. No Check RPC is started by this example.

The generated OpenAPI document preserves the cross-field expression under
`x-protomolt-cel`. Standard OpenAPI clients do not execute that expression.
Continue running the validator at the application boundary. Format enforcement
also depends on the OpenAPI consumer; a generated schema does not replace the
runtime validator.

The five direct library dependencies are listed in [build.gradle](build.gradle).
Remove the registry or OpenAPI dependency when your application does not use it;
the sample combines several independent use cases for convenience. Transitive
dependencies are resolved by Gradle, not copied from a running platform.

## Failures to expect

- Compilation errors mentioning missing `ai.protomolt` classes after an earlier
  checkout: refresh the mutable snapshots, then inspect the resolved dependencies.
- Artifact download failure: check the repository URL and network access. This
  example does not require a repository token and intentionally has no local-cache
  repository fallback.
- Invalid candidate: inspect `ValidationResult.violations()` for field paths,
  rule IDs, and messages. `throwIfInvalid()` converts that result into an exception.
- Incompatible schema: handle `IncompatibleRegistrationException`. Do not bypass
  the write gate to make an incompatible change appear accepted.

For more depth, see [mapping](../../docs/transform/mapping.md),
[projections](../../docs/transform/projection.md),
[validation](../../docs/transform/validation.md),
[registry](../../docs/schema/registry.md), and
[JSON Schema](../../docs/schema/json-schema.md).
