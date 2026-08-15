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
