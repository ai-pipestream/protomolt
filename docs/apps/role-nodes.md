# Role nodes

One binary, many roles. The document platform binary boots whatever
`PROTOMOLT_ROLES` names (comma-separated); the default, with the variable
absent, is the full one-container preset. A different role list is a
specialized node — a repo node, a search node, a connector node — never a
different program, and configuration is only demanded for what a node
actually mounts: a repo-only node needs no jobs database, no registry
repository, no search index.

## The mechanism

Every service ships as a `ServiceModule` (the composer SPI). The composer
orders selected roles by their requirements and wires them over in-process
channels; a required role *outside* the selection is reached remotely
through `PROTOMOLT_<ROLE>_TARGET` (e.g. `PROTOMOLT_REPO_TARGET=repo-node:9090`),
opened as a plaintext gRPC channel. The same rule the golden path proved
in-process holds across the wire: intake authenticates, saves land in the
repository, the door indexes and serves — wherever each role happens to run.

The roles the platform binary knows
(`DocumentPlatformConfig.KNOWN_ROLES`):

| Role | Serves | Notes |
| --- | --- | --- |
| `repo` | document store gRPC | needs the ledger + object store config |
| `parser-text` | reference parser (in-process) | |
| `registry` | schema registry HTTP + the actions route | hosts every contributed verb |
| `parse` | parsing coordinator gRPC | |
| `jobs` | durable workflow runs | needs the jobs database |
| `intake` | authenticated ingest gRPC | needs a key store |
| `playground` | parser playground page | |
| `search` | search door gRPC | needs the index directory |
| `metrics` | metric door gRPC (`MetricService`) | needs `search` on the same node: it aggregates over the search index in process |
| `search-console` | the search page + operations panel | panel needs a local registry or `DOCUMENT_PLATFORM_ACTIONS_URL` |
| `acquire-s3` | the `pull-s3` verb | opt-in; `PROTOMOLT_ACQUIRE_*` config |
| `acquire-jdbc` | the `pull-jdbc` verb | opt-in; `PROTOMOLT_ACQUIRE_*` config |

## What must stay together

Contributions travel in-JVM, not over the wire: the registry builds its
actions catalog and workflow store from what co-mounted modules contribute
at wire time. So `registry`, `jobs` and `parse` belong on one node (the
jobs verbs, the parse-and-index workflow and the replay action all ride the
registry's route), and the acquire roles belong with `intake` (they feed
the door in-process). `metrics` belongs with `search`: the metric
executor borrows the search door's live store, so a node claiming
`metrics` without `search` refuses to boot; its `describe-mapping` and
`query-metrics` verbs additionally need a co-mounted `registry` to be
served. `repo` splits off cleanly today — that is the proven topology —
and the same target mechanism carries further splits as the
contribution surfaces grow wire equivalents.

## The remote metrics node

`PROTOMOLT_ROLES=search,metrics` with
`DOCUMENT_PLATFORM_SEARCH_READ_ONLY=true` and the snapshot family
(`DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_*`) pointing at the writer's
bucket is a self-contained analytics node: it restores the index from
the bucket on boot, needs no repo role or target, mounts no indexing
surface (`SearchIndexService` answers UNIMPLEMENTED), and never writes
to the bucket, so the writer's snapshots stay the writer's.
`PlatformSnapshotIT` proves the composition end to end.

`DOCUMENT_PLATFORM_SEARCH_REFRESH_SECONDS` makes the reader follow the
writer live: on that interval it pulls newer snapshots into its serving
index — missing segment files first, the new `segments_N` marker last,
the refreshed commit verified before it swaps in — so a pull that fails
half-way (a writer pruning mid-download, a network fault) leaves the
serving commit untouched and the next tick retries. The pull never
uploads or prunes a blob, locally initiated deletes included, so the
writer's snapshots stay the writer's. Absent means restart-only: a
reader picks up newer snapshots by restarting over an empty directory.

## The worked example

[`deploy/document-platform/compose-roles.yml`](../../deploy/document-platform/compose-roles.yml)
boots the one image twice: a `repo-node` (`PROTOMOLT_ROLES=repo`) and an
`app-node` with everything else and
`PROTOMOLT_REPO_TARGET=repo-node:9090`. `PlatformRoleNodeIT` proves the
same split in-tree: a document ingested through the intake node's door is
read back from the repo node's store, surfaces outside a node's role list
refuse by role name, and configuration is only demanded for selected roles.
