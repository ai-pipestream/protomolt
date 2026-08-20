# The registry

`protomolt-registry` and `protomolt-registry-service` are a protobuf schema
registry whose storage is a Git repository: every registration is a commit,
history is `git log`, review and replication are whatever your Git hosting
already does. The server fronts that storage with the Confluent subjects
protocol, so existing serializers and clients: including ProtoMolt's own
[loader and publisher](publishing.md): work against it unchanged.

## The store

`SchemaRegistryStore` is the storage SPI: subjects, ascending versions,
global IDs, content-identity lookup, and per-subject compatibility
configuration falling back to a global default (`BACKWARD`). Two
implementations ship: `GitSchemaRegistryStore` and an in-memory store for
tests and embedding.

The same store owns compiled descriptor sets by lowercase SHA-256 fingerprint.
The Git implementation writes them under `descriptors/sha256/`. Writes verify
the hash, enforce a 16 MiB bound, parse the bytes as a non-empty
`FileDescriptorSet`, and are idempotent. Service profiles pin this fingerprint,
so reflection and registration send the descriptor once while later inspection
and invocation resolve it inside ProtoMolt.

```java
var store = GitSchemaRegistryStore.builder()
    .repositoryDir(Path.of("/var/lib/protomolt/registry"))
    .writeGate(new CompatibilityWriteGate())
    .build();
```

The Git layout is deliberately plain: `subjects/<subject>/vN.proto` with a
small metadata file per version. The repository is legible without any
tooling. Writes take a file lock (safe across processes sharing a
repository), reads come from an in-memory index, and `refresh()` picks up
commits made out of band, e.g. by a `git pull`.

Registration is gated three ways, in order: every schema reference must
already exist in the store; the schema must compile (with its transitive
references, through the shared [compiler](../core/descriptor-sources.md)); and the
write gate must pass.

## Compatibility gating

`CompatibilityWriteGate` connects [`protomolt-compat`](compatibility.md)
into the write path. The subject's effective mode decides the check:
non-transitive modes compare against the latest version, `*_TRANSITIVE`
modes against the whole history: which catches what latest-only checking
cannot, such as re-using a removed field number with a different type two
versions later. Violations reject the registration with the rule, path, and
reason, surfaced to HTTP clients as the standard 409. Wire rules are the
default; construct the gate with a configured `CompatibilityChecker` to
enforce JSON or source rules as well.

## The server

`SchemaRegistryServer` serves the store over HTTP (JDK `HttpServer`,
virtual threads) speaking the Confluent subjects protocol: subject and
version listing, version envelopes with references, registration,
content lookup, `/schemas/ids/{id}`, and global and per-subject config,
including the protocol's quirks (`compatibility` in PUT bodies,
`compatibilityLevel` in GET responses), verified against Confluent's error
codes. Four groups of native routes go beyond the protocol, all under the
configured native path prefix (`/protomolt` by default):

- `GET /protomolt/subjects/{subject}/descriptor-set`: the subject's latest
  version and its transitive references compiled to a binary
  `FileDescriptorSet`. This is the gRPC path: build-time consumers, runtime
  loaders, and reflection all speak descriptor sets, in any language.
- `GET /protomolt/subjects/{subject}/parquet-schema?message={fqn}[&version={n}]`:
  the Parquet schema of one message of the subject as canonical schema
  text (`text/plain`). The schema is a pure function of the descriptor, so
  it is derived on read and never stored; `message` (a fully qualified
  message name) is required because a subject's descriptor set can hold
  many messages, and `version` pins the schema version, defaulting to
  latest.
- `GET /protomolt/workflows` and `GET/PUT /protomolt/workflows/{name}`: named
  workflow definitions, versioned by Git commits, with `check-workflow` as the
  write gate when the action catalog is mounted.
- `GET /protomolt/actions` and `POST /protomolt/actions/{name}`: the
  [action catalog](../surface/actions.md) mounted on the registry: the list route
  returns each action with its input schema, the execute route runs one
  from a JSON body. Both routes exist only when a catalog is passed to the
  server; without one they 404 like any unknown path.

`GET /health` sits outside the prefix and is the only route an API token
does not guard.

```java
var server = new SchemaRegistryServer(config, store);
server.start();
```

## The acceptance test is the dogfood

The server's round-trip suite is `ConfluentSchemaPublisher` publishing a
reference-linked source set into the registry and
`ConfluentSchemaRegistryLoader` resolving the types back out: creation,
idempotent re-publish, and update discrimination all verified through the
same client code that talks to Confluent and Apicurio. If those pass
against us, we speak the protocol.

## Federation

A registry federates by treating another mesh's registry repository as an
ordinary git remote of its own. `RegistryFederation` (over the Git store)
manages the remotes and runs sync; the composed registry role exposes it as
two [actions](../surface/actions.md): `registry-remotes` (list/add/remove;
remotes are node-local git config, never a commit) and `registry-sync`.

Sync is strictly pull: fetch the remote, read its `subjects/` and
`descriptors/sha256/` straight out of the fetched git objects (nothing is
merged into the working tree), and import through the normal registration
pipeline. That gives every imported version a local global ID, a compile
check, and a compatibility gate: the sync path is always gated by a
`CompatibilityWriteGate` under the target subject's effective local mode,
even when the store's own write gate is absent. A version that fails the
gate is reported with its violations and stops that subject's import;
other subjects continue.

Subjects carry their origin in the name: remote subject `s` imports as
`<remote>:<s>`, so a subject federated through two meshes reads as a
provenance chain (`b:a:s`). References between remote subjects are
rewritten to the namespaced names; the schema text (whose import paths are
reference *names*) is untouched. Descriptor artifacts are content-addressed
and import verbatim when missing. Sync is idempotent, and a remote whose
history diverged from what was already imported is refused at the
divergence point: local registry history is append-only, federation
included. The remote's global IDs, compatibility modes, and workflows never
sync: those are local concerns on both sides.
