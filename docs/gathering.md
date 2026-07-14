# Gathering proto sources

The gather modules acquire `.proto` source files from wherever they live —
local directories, jars, Git repositories, Maven artifacts — and stage them
as a `ProtoSourceSet`: an import-path-keyed set of sources that the shared
compiler (`protomolt-proto-sources`) turns into runtime descriptors, and
that the [schema publishers](publishing.md) can register with a registry.

The semantics deliberately match the
[quarkus-grpc-gatherer](https://github.com/ai-pipestream/quarkus-grpc-gatherer)
build plugin, which does the same job at build time for Quarkus code
generation. These modules are the runtime, framework-free equivalent.

| Artifact | Provides |
|---|---|
| `protomolt-gather` | The `ProtoGatherer` SPI; filesystem and jar gatherers; composition; the `DescriptorLoader` adapter |
| `protomolt-gather-git` | Git repositories via JGit, with persistent clone caching |
| `protomolt-gather-maven` | Maven artifacts by coordinate, via Maven Resolver |

## The SPI

```java
public interface ProtoGatherer {
    ProtoSourceSet gather() throws GatherException;
    String origin();
    default boolean isAvailable() { return true; }
}
```

Every staged file carries its origin (`git:<repo>@<ref>`, `jar:<file>`,
`maven:<coordinate>`), so conflicts and compile errors name their source.

## Filesystem

```java
var gatherer = FilesystemProtoGatherer.builder()
    .root(Path.of("src/main/proto"))
    .scanRoot(Path.of(".."))          // optional: discover nested src/main/proto trees
    .failIfMissing(true)              // default; false skips absent roots
    .build();
```

Each root's `*.proto` files are staged with import paths relative to that
root. `scanRoot` walks a directory tree and picks up every nested
`src/main/proto` it finds, skipping hidden and `build` directories.

## Jars

```java
var gatherer = JarProtoGatherer.builder()
    .jar(Path.of("libs/common-protos-1.2.0.jar"))
    .includeEntries("common/**")               // optional glob filters
    .excludeEntries("**/internal/**")
    .includeGoogleWellKnownTypes(false)        // default: the compiler supplies WKTs
    .build();
```

Every `*.proto` entry is staged under its in-jar path. `google/protobuf/**`
entries are skipped by default because the compiler provides the well-known
types; including them from a jar risks shadowing the runtime's versions.

## Git

```java
var gatherer = GitProtoGatherer.builder()
    .repo("https://github.com/example/schemas.git")
    .ref("main")                                   // branch, tag, or commit SHA
    .subdir("proto")                               // default
    .token(System.getenv("GH_TOKEN"))              // or username/password
    .build();
```

Three layout modes, mutually exclusive and checked in this priority order:

- **`modules(List)`** — a monorepo where each named top-level module has its
  own `<module>/<subdir>` proto root; the per-module trees are flattened onto
  one shared root so cross-module imports resolve. A module without the
  subdir uses the module directory itself. Flatten collisions with identical
  content are tolerated; differing content is an error.
- **`paths(List)`** — specific files or directories relative to `subdir`.
- **Single subdir** (default) — one proto tree under `subdir`.

Checkouts are cached persistently (default
`~/.cache/protomolt/gather/git/<repo-hash>`, override with `cacheDir`): the
first gather clones, later gathers fetch and hard-reset to the resolved ref.
`offline(true)` uses the cache only and fails when it is cold; in online
mode, a fetch failure over a warm cache logs a warning and falls back to the
cached checkout.

## Maven

```java
var gatherer = MavenProtoGatherer.builder()
    .coordinate("com.example:common-protos:1.4.0")   // g:a:v, optional :classifier
    .repositories(List.of("https://repo1.maven.org/maven2/"))  // default: Central
    .transitive(false)                               // true scans the runtime graph
    .build();
```

Artifacts are resolved through Maven Resolver (using the standard local
repository at `~/.m2/repository` unless overridden) and their jars are
scanned with the same extraction rules as `JarProtoGatherer`.

## Composition and descriptors

`CompositeProtoGatherer` merges any number of gatherers in order; the same
import path arriving from two sources with different content fails with both
origins named.

`GatheringDescriptorLoader` adapts any gatherer to the
[`DescriptorLoader` SPI](descriptor-sources.md): it gathers and compiles
lazily on first use, caches the result, resolves types by file name,
fully-qualified name, or simple name, and re-gathers on `refresh()`.

```java
var loader = new GatheringDescriptorLoader(gatherer);
registry.addLoader(loader);
```

In Spring and Quarkus, defining the loader as a bean is enough — both
integrations aggregate every `DescriptorLoader` bean into the registry (see
[Framework integrations](framework-integrations.md)).
