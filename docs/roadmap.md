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
type resolution. Five loaders ship:

| Source | Loader | Notes |
|---|---|---|
| Generated classes on the classpath | `ClasspathDescriptorLoader` | On-demand by class name; no enumeration |
| Descriptor set as a classpath resource | `GoogleDescriptorLoader` | Default path `META-INF/grpc/services.dsc` |
| Confluent Schema Registry (subjects API) | `ConfluentSchemaRegistryLoader` | Fetches `.proto` text + references, compiles at runtime via Square Wire |
| Descriptor set over HTTP | `ConfluentDescriptorSource` | Binary `FileDescriptorSet` payloads |
| Apicurio Registry v3 | `ApicurioDescriptorLoader` | Native v3 API, reference resolution |

Two properties of this list matter for the roadmap. First,
`ConfluentSchemaRegistryLoader` already proves the hard part — compiling
`.proto` source to runtime descriptors inside the JVM, including reference
graphs — so new text-based sources are a matter of plumbing, not research.
Second, everything here is read-only; nothing in ProtoMolt can publish a
schema anywhere.

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
framework-agnostic HTTP gateway (`http/rest`), six server hosts
(`servers/*`), JSON transcoding, OpenAPI and JSON Schema generation, and
schema hygiene checks (`ProtoFqnConflictDetector`,
`BinaryProtobufIdentifierValidator`) that reject conflicting or malformed
uploads before they reach storage. Validation, mapping, and indexing consume
whatever descriptors the registry resolves.

## The gap

In rough dependency order:

**1. Runtime loaders with gatherer parity — done.** The `gather/` modules
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

**4. The registry server — first cut done.** The `registry/` modules store
schemas in a Git repository (one commit per registration, per-subject
compatibility configuration, cross-process locking) behind the Confluent
subjects protocol, with writes gated by `CompatibilityWriteGate` and a
native endpoint serving compiled `FileDescriptorSet`s — the gRPC
differentiator. The acceptance test is the dogfood: our own publisher and
loader round-trip through it. See [The registry](registry.md). Still open
in this milestone: authentication, federation (mirroring subjects out to
Apicurio/Confluent through the publishers), branch-per-scope workflows, and
Maven-repository artifact publication.

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

**5. A web console.** Every server host already serves `openapi.json`, and
`MappingHelper` exists specifically to feed schema-browsing UIs. A bundled,
build-free console — schema browser, subject/version history, a try-it
request panel — served as static assets from the same hosts would give the
registry and gateway a common front end without adding a frontend toolchain
to the build.

**6. Cross-language project bootstrap.** With 1–4 in place, "cross-language
out of the box" is mostly packaging: a project template (or plugin) where
`.proto` lives in Git, CI publishes descriptor sets to Maven and registers
subjects, and consumers in any language pull from the registry API or from
Git directly. JVM consumers get the full toolkit; others get standard
protobuf artifacts.

## Assessment

With items 1 and 2 complete, ProtoMolt reads from and writes to every
registry and source it targets, and the gather-from-Git → publish-to-registry
bridge works end to end. What stands between here and a demoable registry is
items 3 and 4 — compatibility checking as a library and the git-backed
subject/version store with its API facade — both bounded, well-understood
problems. The shape language (4a) follows the expanded hint standard; the
console (5) and project bootstrap (6) build on all of it.
