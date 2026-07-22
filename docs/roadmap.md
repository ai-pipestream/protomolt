# Roadmap: a schema registry over Git and Maven

ProtoMolt today is a consumption toolkit: given compiled descriptors, it can
validate, map, index, transcode, and serve messages. The longer-term goal is to
close the loop on where those descriptors come from — a protobuf schema
registry whose storage and distribution ride on infrastructure teams already
run (Git repositories and Maven repositories), rather than a bespoke database.
The registry is its own product: git-native storage fronted by standard
registry protocols, federating with the registries teams already operate, and
serving compiled descriptors first-class for gRPC consumers.

This document records the target architecture, what exists today, and the gap
between the two. It is a working document, not a commitment to dates.

## The target

A team should be able to:

1. Keep `.proto` sources in Git — one repository or many, monorepo or not.
2. Publish compiled schema artifacts (descriptor sets, generated stubs) to
   Maven, versioned like any other dependency.
3. Point ProtoMolt at either — a Git ref, a Maven coordinate, a directory, a
   jar, or an existing registry (Confluent, Apicurio) — and get runtime
   descriptors with no bespoke tooling in between.
4. Run a registry server that fronts that storage with standard APIs
   (Confluent-compatible subjects API at minimum), so existing serializers and
   clients in any language work unchanged.
5. Start a proto project that is cross-language out of the box: the registry
   serves canonical `.proto` text and binary descriptor sets, which every
   language's protoc toolchain can consume; JVM consumers additionally get
   the ProtoMolt runtime (validation, mapping, indexing, REST).

Git supplies history, review, and branching; Maven supplies immutable versioned
distribution and dependency resolution. The registry is the coordination layer
over both, not a new source of truth.

## What exists today

### Acquisition (read path) — partial

The `DescriptorLoader` SPI (`core/descriptors`) is the seam every source plugs
into, and `DescriptorRegistry` aggregates loaders with caching and on-demand
type resolution. Six loaders ship:

| Source | Loader | Notes |
|---|---|---|
| Generated classes on the classpath | `ClasspathDescriptorLoader` | On-demand by class name; no enumeration |
| Descriptor set as a classpath resource | `GoogleDescriptorLoader` | Default path `META-INF/grpc/services.dsc` |
| Confluent Schema Registry (subjects API) | `ConfluentSchemaRegistryLoader` | Fetches `.proto` text + references, compiles at runtime via Square Wire |
| Descriptor set over HTTP | `ConfluentDescriptorSource` | Binary `FileDescriptorSet` payloads |
| Apicurio Registry v3 | `ApicurioDescriptorLoader` | Native v3 API, reference resolution |
| Any gatherer source (filesystem, jar, Git, Maven) | `GatheringDescriptorLoader` | Adapts the `ProtoGatherer` SPI; see item 1 below |

Two properties of this list matter for the roadmap. First,
`ConfluentSchemaRegistryLoader` already proves the hard part — compiling
`.proto` source to runtime descriptors inside the JVM, including reference
graphs — so new text-based sources are a matter of plumbing, not research.
Second, every loader here is read-only: acquisition and publication are
separate seams. Publishing is `SchemaPublisher` (item 2 below), not the
loader SPI.

### Acquisition at build time — solved in a sibling project

[`quarkus-grpc-gatherer`](https://github.com/ai-pipestream/quarkus-grpc-gatherer)
already implements the full source model we want, but as a Gradle build-time
stage for Quarkus codegen:

- filesystem directories (with `scanRoot` discovery of nested proto trees)
- jars resolved from Maven dependencies (explicit list or scan-all)
- Git repositories, with single-subdir, explicit-paths, and multi-module
  monorepo modes, token/basic auth, and persistent per-repo caching
- Google well-known types

Its output contract — a staged proto tree plus a `services.dsc` descriptor set
packaged at a stable classpath location — is exactly what
`GoogleDescriptorLoader` consumes. The two projects already compose at build
time. What the gatherer does not do: run at runtime, work outside Gradle
(Maven support is explicitly deferred), or serve anything.

### Serving and consumption — largely built

The pieces a registry *server* needs already exist as libraries: a
framework-agnostic HTTP gateway (`surface/http/rest`), six server hosts
(`host/servers/*`), JSON transcoding, OpenAPI and JSON Schema generation, and
schema hygiene checks (`ProtoFqnConflictDetector`,
`BinaryProtobufIdentifierValidator`) that reject conflicting or malformed
uploads before they reach storage. Validation, mapping, and indexing consume
whatever descriptors the registry resolves.

## The gap

In rough dependency order:

**1. Runtime loaders with gatherer parity — done.** The `acquire/gather/` modules
implement the gatherer's source model behind the `ProtoGatherer` SPI —
filesystem (with scan-root discovery), jars, Git (all three layout modes,
auth, persistent clone caching), and Maven coordinates — with
`GatheringDescriptorLoader` adapting any gatherer to the `DescriptorLoader`
SPI. The Wire compilation pipeline was extracted from
`ConfluentSchemaRegistryLoader` into `protomolt-proto-sources`
(`ProtoSourceCompiler`), so every text-based source shares one
implementation. See [Gathering proto sources](gathering.md).

**2. A write path to existing registries — done.** `SchemaPublisher`
(`protomolt-proto-sources`) with Confluent and Apicurio implementations:
reverse-topological reference registration, idempotent re-publish, dry runs,
and server-side compatibility rejections surfaced per file. Verified by
round-trip integration tests against live registries. See
[Publishing schemas](publishing.md). Still open from the original scope:
publishing descriptor-set artifacts to Maven repositories and committing to
a Git backend — both belong to the registry milestone below.

**3. Compatibility checking as a library — done.** `protomolt-compat`
implements breaking-change detection over descriptor diffs: a typed change
model with wire, JSON, and source impact classification, evaluated under
the standard registry modes (backward, forward, full, and their transitive
variants against version history). Wire semantics are the default, matching
how Confluent judges protobuf schemas; JSON and source rule layers are
opt-in. This is the write-gate for the registry server below. See
[Compatibility checking](compatibility.md).

**4. The registry server — first cut done.** The `schema/registry/` modules store
schemas in a Git repository (one commit per registration, per-subject
compatibility configuration, cross-process locking) behind the Confluent
subjects protocol, with writes gated by `CompatibilityWriteGate` and a
native endpoint serving compiled `FileDescriptorSet`s — the gRPC
differentiator. The acceptance test is the dogfood: our own publisher and
loader round-trip through it. See [The registry](registry.md). Still open
in this milestone: authentication, federation (mirroring subjects out to
Apicurio/Confluent through the publishers), branch-per-scope workflows,
Maven-repository artifact publication, and — keeping JSON strictly an edge
dialect — a binary registration endpoint (upload a `FileDescriptorSet`
natively) plus protobuf envelopes for the action catalog so gRPC clients
drive the verbs without JSON.

**4a. An indexing shape language.** Per-field hints cover the common case,
but two problems are structurally beyond field annotations: one-to-many
projection (one message producing multiple index documents, e.g. per element
of a repeated field) and map carving. The plan is an `IndexShape` standard —
itself a protobuf message, so shapes are schemas: versioned, diffed, and
served by the registry like anything else. A shape names its target index,
declares field definitions using the same metadata the hint standard carries
(types, analyzers, vector parameters, doc values), and binds each field to a
value source that is either a proto field path or a CEL expression. A
document-split rule expresses one-to-many (each engine lowers it natively:
Lucene block joins, OpenSearch nested/parent-join, Solr child documents), and
maps are handled the only way they ever are in practice — a conservative
default policy at the hint level, explicit CEL carving of named entries at
the shape level. This reuses the validation standard's architecture (neutral
model, pluggable sources, CEL where it matters) as a separate standard with
its own lifecycle: validation says what data may enter; a shape says how data
comes to rest in an index. The pipeline reads gather → validate → shape →
engine.

**4b. The MCP surface — first slice done.** `protomolt-mcp` serves the
action catalog as MCP tools (the manifest is already the MCP tool shape)
and a git-backed registry as MCP resources, over a hand-rolled JSON-RPC
2.0 stdio transport: plain Java, Jackson, no framework. The Streamable
HTTP mount is done too — `protomolt-serve` serves the same catalog at
`/mcp` through `McpHttpHandler`, with registry resources when a registry
is mounted. Thin Spring AI / Quarkus MCP host adapters over the same
catalog remain open; nothing in `protomolt-mcp` is framework-aware, so a
host registers the catalog through its own APIs today.
`generate-stubs` is also done: `protomolt-codegen` bundles every
libprotoc generator (Java, Kotlin, Python, C++, C#, Ruby, PHP,
Objective-C) and the grpc-java plugin compiled to WebAssembly, run
in-JVM via Chicory, so descriptors become compilable client code in
eight languages as a live call with no native toolchain. The wasm
module is built by our fork of protobuf4j (Apache-2.0,
github.com/ai-pipestream/protobuf4j), where the generator series
landed as one PR per language. Still open here: registry-backed type
resolution in the MCP action context, per-language gRPC plugins
(grpc_python and friends, vendored the way grpc-java was), and Rust
once protobuf's Rust runtime story settles. The `grpc-invoke` and `reflect` verbs are done: `protomolt-grpc-invoke`
calls unary and server-streaming methods on live servers straight from
descriptors (dynamic marshallers, no stubs), with metadata, deadlines,
and a response cap that cancels open-ended streams cleanly; and `reflect`
pulls a server's own descriptors over the gRPC server-reflection
protocol, so a service can be operated given only its address. Together
they make any reflection-enabled gRPC service an MCP integration with
zero registration, and any service with a registered or reflectable
schema operable by an agent. Verified live against a real OpenVINO Model
Server (KServe v2 gRPC): server and model metadata introspected over
MCP, tensor contracts and all.

The catalog is now also a gRPC service of itself:
`protomolt-grpc-service` defines every verb as a typed RPC
(`ai.pipestream.protomolt.v1.ProtoMoltService`), compiles that `.proto`
with the runtime's own compiler at startup, and serves it over dynamic
messages with server reflection on — grpcurl and ProtoMolt's own
`reflect` verb both discover it, and its `GrpcInvoke` RPC can operate
another ProtoMolt server (the test suite pins this). `protomolt-serve`
mounts the same descriptors on the JSON/REST gateway with a generated
OpenAPI document and bundled Swagger UI, and optionally the git-backed
registry, in one process. See [The gRPC service](grpc-service.md).

The dynamic-invocation layer also grew true streaming primitives — a
flow-controlled, poll-shaped handle on server streams and a
readiness-aware client-streaming call — and `protomolt-connect` builds a
Kafka Connect plugin on them: a sink that drives any unary or
client-streaming gRPC method from topics, and a source that feeds topics
from a server stream, resumable across restarts via CEL-extracted tokens
stored as Connect offsets. Alongside them, four protobuf-aware Single
Message Transforms (validate against declared rules with DLQ-ready
violation headers, reshape with mapping rules, filter by CEL predicate,
redact sensitive fields) drop into any Connect pipeline, and a plugin zip
rides along with releases. See [Kafka Connect](kafka-connect.md).

The first slice of the ETL story shipped alongside its design
([joins and derived shapes](design/join-shapes.md)): `protomolt-shapes`
synthesizes join/union output types at runtime — envelopes, projections
whose field types are inferred from scoped source paths, and oneof tagged
unions — emitted as registrable proto source with true import paths, and
joins named messages through multi-source mapping scopes (text rules and
CEL reading from every source at once). Exposed as the `synthesize-shape`
and `join-messages` verbs on all surfaces. `merge-schemas` completes the
schema-level story: merge the fields of two or more types into one new
type through a validate-resolve-emit flow — the clash report (same name,
different type or cardinality) is computed from descriptors alone, the
caller decides each clash (rename with source prefixes, prefer one side,
or override a coalesce), and one move then emits the merged proto with
its defined-join and defined-union rulesets. The static half of that
story is `check-rules`: mapping rules and CEL checked against descriptors
without executing (filters must type-check to bool), with an optional
dry run over sample messages.

The [chain manager](design/chain-manager.md) is the composition layer over
those invocation primitives, and it is built: configured, type-checked
compositions of gRPC calls (invoke, reshape with mapping rules and CEL,
gate, validate) exposed as `run-chain`/`check-chain` verbs and versioned in
the registry — a sidecar to whatever pipeline people already run,
deliberately not a pipeline product. `protomolt-chain` runs inline chains —
serial unary gRPC calls whose requests are mapped from the chain input and
every prior step's response, with boolean CEL gates, nested deadlines,
optional response validation, and fail-fast errors carrying the step name —
and `check-chain` verifies all of it statically, so a chain that checks
clean cannot fail on a type error at run time. Named chains resolve through
the `ChainRepository` seam, which `protomolt-serve` backs with the git
registry store, so `run-chain` can take a stored chain by name.
`StreamJoiner` covers the streaming half: two live gRPC server streams
joined into a target type, paired by arrival order (`ZIP`) or matched on a
key field path (`KEYED`), both sides flow-controlled with bounded per-side
buffers that drop oldest on overflow. The console gained the merge
workbench at `/console/schema-registry/merge`: pick two types, decide
clashes, and register the merged schema with its mappings in one move.
Still open from the designs: a schema-declared key option, so a join key
travels with the schema rather than being named per call; and step kinds
beyond the serial gated call (the runner has no repeat construct).

**5. A web console — done.** Every server host serves `openapi.json`, and
`MappingHelper` feeds schema-browsing UIs. The console ships as static
assets: `ConsoleHandler` serves it from the classpath at `/console`, and
five views cover the surface — a subject browser, a subject detail page
with version history and a version diff panel, the merge workbench, a
chains page, and the connect-service wizard. The try-it panel is Swagger
UI, bundled by `protomolt-serve` at `/docs` over the generated OpenAPI
document. The console is built with Vite outside the Gradle build and its
`dist` output is bundled when present, so the Gradle build itself still
carries no frontend toolchain and the server builds and explains itself
without the assets. Still open: the console is disabled in token mode
until the session flow in the hardening backlog lands.

**6. Cross-language project bootstrap.** With 1–4 in place, "cross-language
out of the box" is mostly packaging: a project template (or plugin) where
`.proto` lives in Git, CI publishes descriptor sets to Maven and registers
subjects, and consumers in any language pull from the registry API or from
Git directly. JVM consumers get the full toolkit; others get standard
protobuf artifacts.

## Hardening backlog

The 2026-07-15 architecture review (disposition in
`docs/reviews/2026-07-15-architecture-review-response.md`) fixed the concrete
defects the same day; these are the remaining design efforts, in rough priority
order. They strengthen contracts the toolkit already implies rather than adding
features.

1. **Authorization scopes.** The shared token now covers every listener; the
   next step is separating authentication from authorization: scopes such as
   schema-read, schema-write, chain-execute, and outbound-action-execute, plus
   a route-enumeration test proving each route's requirement.
2. **Console session flow.** A server-side session (or backend-for-frontend)
   so the console works in token mode without a browser ever holding the
   process secret. Until then the console is disabled in token mode.
3. **Metadata propagation contract for shapes.** Classify field options as
   transferable, contextual, derived, or prohibited for every shape operation
   (projection, merge, join, inference); detect conflicting sensitivity,
   validation, and indexing policy during merges; record provenance for
   derived fields. Cross-module tests prove a sensitive field stays sensitive
   through every derivation.
4. **Execution and outbound-network budgets.** Per-action timeouts and
   concurrency quotas; Git clone size/depth/wall-clock limits under policy;
   outbound scheme/host/port allowlists with resolved-address re-checks; and
   one `ChannelPolicy` shared by chains, reflection, invocation, and gather so
   every outbound channel obeys the same trust configuration
   (`ChainRunner.ChannelFactory` is the seam to generalize).
5. **Transactional Git registry writes.** Build commits through JGit plumbing
   (blobs, trees, commit, atomic ref advance) instead of working-tree writes,
   with fault-injection tests at each boundary and documented crash-recovery
   expectations.
6. **MCP protocol conformance.** A connection/session state machine
   (initialize, initialized, cancellation, shutdown), fixtures per protocol
   revision, and regular runs against the official MCP inspector.
7. **API lifecycle.** Stability annotations (stable / evolving / experimental /
   internal), package docs on stable entry points, a published compatibility
   policy for Java APIs, protobuf options, stored registry formats, REST
   routes, and action envelopes — enforced with japicmp or Revapi once a
   first release exists to compare against.
8. **Hermetic WASM protoc supply chain.** Source provenance, the exact
   protobuf4j build commit, a rebuild script, component versions, and a
   build-enforced binary checksum now live under `core/codegen/provenance`. Finish
   this by digest-pinning the build image and downloaded archives, then audit
   and package the required LICENSE/NOTICE attribution for every component in
   the combined binary.
9. **Sensitive-field vectorization policy.** An explicit opt-in before an
   `encrypt`-classed field's content may feed an embedding pipeline, so the
   documented neighborhood/inversion trade-off is a deliberate deployment
   decision rather than a default.

## Assessment

Items 1 through 4 are complete, and 4b and 5 with them. ProtoMolt reads from
and writes to every registry and source it targets, the gather-from-Git →
publish-to-registry bridge works end to end, compatibility checking gates
writes, and the git-backed subject/version store serves the Confluent
subjects protocol alongside a native descriptor-set endpoint and a console
over both. The registry is demoable; the acceptance test is the dogfood
described in item 4.

What remains is depth rather than foundation. The registry milestone still
owes authentication, federation, branch-per-scope workflows, Maven-repository
artifact publication, and the binary registration and protobuf action
envelopes that keep JSON an edge dialect. The shape language (4a) is designed
but not built, and follows the expanded hint standard. Project bootstrap (6)
is packaging over everything above. Running underneath all of it is the
hardening backlog, whose first four items — authorization scopes, the console
session flow, the metadata propagation contract, and execution budgets —
gate a production deployment more than any remaining feature does.
