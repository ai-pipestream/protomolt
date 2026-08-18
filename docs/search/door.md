# The search door

`protomolt-search-door` is the user-facing query surface and its indexing
RPC (`protomolt-search-proto`, `ai.pipestream.proto.search.v1`). It mounts
as the `search` role over a co-mounted or remote `repo` role, owns a
Lucene index per served subject, and is the first consumer of the
[embedding lane](embeddings.md): indexing a document runs chunk-and-embed
under the subject's [chunking policy](chunking.md).

## Mapping subjects

The door serves a fixed set of *mapping subjects* (`ServedMapping`): each
names an index mapping (the queryable surface), the document identity
field, and optionally a chunk lane — a chunking policy plus the source
text field it derives from. Every request is gated by membership:

- an unknown subject is refused with the list of served subjects,
- a lexical field outside the subject's mapping is refused by name with
  the mapping's text fields,
- a vector query against a subject with no chunking policy is refused as a
  failed precondition,
- an unset lane and a non-positive `k` are refused rather than defaulted.

Before any of that, the proto's own declared rules answer at the boundary:
both door servers mount the gRPC validating interceptor, so a request that
violates `validate.v1` annotations (an empty query, an out-of-range `k`, a
missing subject) is refused with the schema's wording before a handler
runs. Handlers keep only the judgments a schema cannot make, like mapping
membership.

`RepoDocumentMapping` is the out-of-the-box subject (`repo-document`):
identity plus the folded search metadata, storage and provenance planes
skipped, with an optional chunk lane over the folded body.

Chunk-lane embedding providers resolve through the ServiceLoader when the
door mounts (`EmbeddingProviders.forSpec`), so a subject naming an absent
or misconfigured model fails the mount, not the first document.

## Indexing

`SearchIndexService/IndexDocument` is workflow-facing: a durable run's
index step calls it after the parse step lands, so what is searchable is
exactly what the durable pipeline has processed. The door fetches
CORE+PARSED through the repo wire contract, maps the parent under the
subject's mapping, derives chunk children (chunk identity
`<doc_id>#<policy-digest-12>#<ordinal>`, vectors under the engine
convention `"<sourceField>#<model>"`), and writes the block children-first
parent-last. Re-indexing atomically replaces the document's previous block
by identity term, so replays never duplicate.

Visibility and durability are decoupled. Every write refreshes the
near-real-time searcher, so a document answers queries the moment its
index call returns; durability commits (fsyncs) batch instead — per 64
writes, on a 2-second flush, and on close — so a corpus-wide replay fsyncs
per batch rather than per document. The honest trade: a crash loses at
most the last interval's writes, and a replay re-derives them.

Every repo read the door makes (the indexing fetch and replay's listing)
carries a 30-second call deadline: a hung repository fails the calling
workflow step, which requeues with backoff, instead of parking a worker
forever.

## Querying

Queries name a subject, a lane, a query text, and `k` — all required, and
`k` is bounded (1000): one query cannot ask for the whole index.

`SearchService/Search` runs one of three lanes against a subject:

| Lane | What runs |
|---|---|
| `SEARCH_LANE_LEXICAL` | Analyzed terms over the mapping's text fields (or the requested subset) |
| `SEARCH_LANE_VECTOR` | KNN over the subject's chunk vectors, the query embedded by the same provider that indexed |
| `SEARCH_LANE_HYBRID` | Both lanes, fused by reciprocal rank (k=60) |

Hits carry the document identity, the chunk identity for chunk hits, the
lane score, and the stored fields of the matching document or chunk as
typed cells (`StoredValue`): the mapping's declared kind decides the arm,
so a keyword arrives as a string, an `INT64` as an integer, a `DATE` as a
timestamp, and a caller never re-parses rendered strings. Every stored
field the engine returns appears, numerics and dates included.

## Index snapshots

The store can snapshot each subject's index to a blob store
(`SnapshotStore`; `protomolt-search-snapshot-s3` is the S3
implementation). Snapshots happen at commit points, never as a live
directory over object storage: the commit is held open, files the bucket
does not already have upload (segment immutability makes this
incremental), and the commit's `segments_N` writes last as the atomic
marker that the snapshot exists, after which unreferenced blobs prune.
The cadence is the store's durability-commit cadence plus close.

Identity keys a snapshot to what produced it,
`{subject}/{mapping-digest}/{policy-digest}`: change the mapping or the
chunking policy and the old snapshot is not this mount's to restore.
Restore runs on boot into an empty directory only, and a restore that
fails verification wipes and mounts empty. The repository stays the
source of truth and a snapshot is a cache: losing the bucket loses
time, never data, because `replay-documents` re-derives any subject.
This is also what makes a read-only analytics node cheap: the indexing
node commits and uploads, readers restore and serve. A read-only door
(`SearchDoorConfig.readOnly`) mounts only the query surface, needs no
document fetcher, opens no `IndexWriter` at all (no write lock, no
commits), and demands restore-only snapshots, so a reader can never
overwrite or prune the writer's blobs. A reader with a refresh interval
(`SearchDoorConfig.refreshSeconds`) follows the writer live: each tick
pulls a newer commit's missing files, lands the `segments_N` marker
last, verifies, and swaps the searchers — a failed pull keeps the
serving commit. A reader born before the first snapshot serves an empty
index and swaps the real one in when a restore lands. See
[Role nodes](../apps/role-nodes.md#the-remote-metrics-node) for the
composed form.

The document platform wires this through the
`DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_*` family (see
[Document platform](../apps/document-platform.md)); a bare
`SearchDoorModule.Config` takes an `IndexSnapshots` directly.

## The parse-and-index workflow

`SearchWorkflows.parseAndIndexWorkflow` builds the two-step durable
workflow (`parse-and-index`): the coordinator parses the stored document,
then the door indexes it under the request's mapping subject
(`ParseAndIndexRequest` carries both identities explicitly). When the
`search` role co-mounts with `parse` and `registry`, the module registers
the workflow at wire time, so operators submit it by name through the jobs
verbs. The [document platform](../apps/document-platform.md) mounts all of
this by default and its smoke IT proves ingest-to-search-hit over real TCP.

## Deleting

`SearchIndexService/DeleteDocument` removes one document's block — parent
and chunk children in one term delete — from a mapping subject's index.
It is idempotent (deleting an absent id succeeds: "not searchable" already
holds) and refuses an unknown subject or a blank `doc_id` by name, like
every door request. The response reports `chunks_deleted`, counted across
every policy digest, so a caller can tell a real removal from a no-op —
a never-indexed id answers zero, not an error.

Removal is durable through `delete-and-unindex`, parse-and-index's mirror
(`SearchWorkflows.deleteAndUnindexWorkflow`): the repository deletes the
stored document (`DeleteDocumentRequest.purge_storage` chooses immediate
purge over the default two-phase tombstone), then the door un-indexes it.
A completed run means the document neither reads back nor answers queries;
either step's transient failure requeues with backoff, so a repo-deleted
document cannot stay searchable because the un-index step was lost. The
module registers the workflow at wire time whenever a registry co-mounts —
it rides the repo channel the door already requires, so it needs no parse
coordinator.

## Replay

`replay-documents` (contributed by the door when `jobs` co-mounts) re-runs
a stored workflow over every document a repository listing matches: one
durable run per document, riding the jobs module's own `submit-workflow`
action. Input: `workflowName`, `mappingSubject`, `drive`, optional
`accountId`, optional `replayId` — every identity explicit. With `replayId`
set, each document's run is submitted under a deterministic job id derived
from it, so a replay retried after a mid-replay failure resumes instead of
duplicating runs; without it every run gets a fresh id, as before. This is
the operation behind a chunking-policy or mapping change: a changed policy
is a different digest and a different chunk generation, and the door's
atomic replace-by-identity means replays re-derive and never duplicate
(`PolicyChangeReindexTest` pins the generation swap; the platform smoke IT
replays the corpus and asserts a single hit survives).

With `prune` set, the replay is a reconcile: it runs over the whole
repository listing and removes indexed documents the listing no longer
contains — the cleanup for anything deleted outside `delete-and-unindex`.
Prune is unscoped by design (`drive` and `accountId` are refused by name:
a scoped listing would prune other scopes' documents), and the indexed set
is captured before the listing pages, so a concurrently indexed document
is never a prune candidate. This works because the repository listing
serves only AVAILABLE rows: a tombstoned document drops out of the listing
the moment it is deleted, so replays never resubmit it and prune removes
its index entry.

## The console

`protomolt-search-console` is the product page over the door: one pure-JDK
HTTP server, one page, no build step (the playground idiom). The page's
subject and lane pickers are populated from the door's `ListSubjects` RPC,
so what the page offers is exactly what the door serves, and refusals from
the door render verbatim — the door writes them for humans. The server
bridges `POST /search` (proto3-JSON `SearchRequest` in, hits out) and
`GET /subjects` onto the door's gRPC surface, and proxies
`POST /actions/<name>` same-origin onto the registry's actions route, which
gives the operations panel replay (`replay-documents`), job inspection
(`list-jobs`), and connector pulls without CORS. Mounted as the
`search-console` role (requires `search`); the platform serves it on port
8096 by default (`DOCUMENT_PLATFORM_SEARCH_CONSOLE_PORT`).
