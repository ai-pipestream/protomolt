# The registry

`protomolt-registry` and `protomolt-registry-server` are a protobuf schema
registry whose storage is a Git repository: every registration is a commit,
history is `git log`, review and replication are whatever your Git hosting
already does. The server fronts that storage with the Confluent subjects
protocol, so existing serializers and clients — including ProtoMolt's own
[loader and publisher](publishing.md) — work against it unchanged.

## The store

`SchemaRegistryStore` is the storage SPI: subjects, ascending versions,
global IDs, content-identity lookup, and per-subject compatibility
configuration falling back to a global default (`BACKWARD`). Two
implementations ship: `GitSchemaRegistryStore` and an in-memory store for
tests and embedding.

```java
var store = GitSchemaRegistryStore.builder()
    .repositoryDir(Path.of("/var/lib/protomolt/registry"))
    .writeGate(new CompatibilityWriteGate())
    .build();
```

The Git layout is deliberately plain — `subjects/<subject>/vN.proto` with a
small metadata file per version — so the repository is legible without any
tooling. Writes take a file lock (safe across processes sharing a
repository), reads come from an in-memory index, and `refresh()` picks up
commits made out of band, e.g. by a `git pull`.

Registration is gated three ways, in order: every schema reference must
already exist in the store; the schema must compile (with its transitive
references, through the shared [compiler](descriptor-sources.md)); and the
write gate must pass.

## Compatibility gating

`CompatibilityWriteGate` connects [`protomolt-compat`](compatibility.md)
into the write path. The subject's effective mode decides the check:
non-transitive modes compare against the latest version, `*_TRANSITIVE`
modes against the whole history — which catches what latest-only checking
cannot, such as re-using a removed field number with a different type two
versions later. Violations reject the registration with the rule, path, and
reason, surfaced to HTTP clients as the standard 409. Wire rules are the
default; construct the gate with a configured `CompatibilityChecker` to
enforce JSON or source rules as well.

## The server

`SchemaRegistryServer` serves the store over HTTP (JDK `HttpServer`,
virtual threads) speaking the Confluent subjects protocol: subject and
version listing, version envelopes with references, registration,
content lookup, `/schemas/ids/{id}`, and global and per-subject config —
including the protocol's quirks (`compatibility` in PUT bodies,
`compatibilityLevel` in GET responses), verified against Confluent's error
codes. Three groups of native routes go beyond the protocol, all under the
configured native path prefix (`/protomolt` by default):

- `GET /protomolt/subjects/{subject}/descriptor-set` — the subject's latest
  version and its transitive references compiled to a binary
  `FileDescriptorSet`. This is the gRPC path: build-time consumers, runtime
  loaders, and reflection all speak descriptor sets, in any language.
- `GET /protomolt/chains` and `GET/PUT /protomolt/chains/{name}` — named
  chain definitions, versioned by Git commits, with `check-chain` as the
  write gate when the action catalog is mounted.
- `GET /protomolt/actions` and `POST /protomolt/actions/{name}` — the
  [action catalog](actions.md) mounted on the registry: the list route
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
`ConfluentSchemaRegistryLoader` resolving the types back out — creation,
idempotent re-publish, and update discrimination all verified through the
same client code that talks to Confluent and Apicurio. If those pass
against us, we speak the protocol.
