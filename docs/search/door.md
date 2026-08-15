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

## Querying

`SearchService/Search` runs one of three lanes against a subject:

| Lane | What runs |
|---|---|
| `SEARCH_LANE_LEXICAL` | Analyzed terms over the mapping's text fields (or the requested subset) |
| `SEARCH_LANE_VECTOR` | KNN over the subject's chunk vectors, the query embedded by the same provider that indexed |
| `SEARCH_LANE_HYBRID` | Both lanes, fused by reciprocal rank (k=60) |

Hits carry the document identity, the chunk identity for chunk hits, the
lane score, and the stored fields of the matching document or chunk.

## The parse-and-index workflow

`SearchWorkflows.parseAndIndexWorkflow` builds the two-step durable
workflow (`parse-and-index`): the coordinator parses the stored document,
then the door indexes it under the request's mapping subject
(`ParseAndIndexRequest` carries both identities explicitly). When the
`search` role co-mounts with `parse` and `registry`, the module registers
the workflow at wire time, so operators submit it by name through the jobs
verbs. The [document platform](../apps/document-platform.md) mounts all of
this by default and its smoke IT proves ingest-to-search-hit over real TCP.

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
