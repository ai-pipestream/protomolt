# Intake and parsing — the front door and the routing coordinator

Status: wire contracts landed (`intake/proto`, `parse/proto`), pre-implementation.
2026-07-29. Builds on the reshape tenets in
[document-platform-reshape.md](document-platform-reshape.md); nothing here
revisits them.

Two services sit between the outside world and the pipeline graphs:
**intake-service**, the only public ingest surface, and the **parsing
coordinator**, which turns stored raw documents into parser results and
arbitrated search metadata. This round lands their proto contracts only —
no service implementations.

## Intake

Intake-service is the platform front door. Every byte that enters the
platform crosses it; everything downstream reads from repo. Intake
authenticates the caller, narrows the request to the caller's scope, wraps
the payload into a repo `Document`, and saves it to the account's intake
drive via repo-service's `DocumentService`.

### Three lanes, one service identity

1. **Unary** — `IntakeService.IngestDocument(IngestDocumentRequest)`. Small
   documents, and the typed-crawler lane: connectors that already speak the
   repo model pass a `repo.v1.Document` on the `document` arm of the
   content oneof; everyone else passes `RawPayload` (bytes + filename +
   declared mime) and intake does the wrapping.
2. **Client-streaming** — `IntakeService.IngestStream(stream IngestStreamRequest)`.
   Large payloads that should not fit one message. Frame discipline: the
   first frame carries `IngestMetadata` (the same targeting/mime/filename
   fields as the unary lane), every later frame carries bytes; the payload
   is the concatenation of the data frames in stream order.
3. **HTTP POST raw binary** — deliberately NOT a gRPC rpc. It mirrors
   repo-service's existing raw-upload route shape (raw body, filename and
   drive as headers, `X-Content-Sha256` verification, content-addressed
   blob ids) with `x-api-key` replacing the raw account headers
   (`x-account-id`/`x-datasource-id`/`x-drive-name`): the caller proves who
   they are, the server decides where the bytes may land. grpc-web clients
   and simple `curl` producers both get a lane that fits.

All three lanes return the same receipt shape (`IngestDocumentResponse`/`IngestStreamResponse` — one type per rpc): doc_id, node_id,
the repo `NodeAddress`, sha256, size, and the dedupe flag — repo's
save/upload vocabulary, so a receipt reads the same no matter which lane
produced it.

### The API-key model

The API key is a **credential that resolves to a scope server-side**;
it is never itself the targeting. The scope is

```
{ account_id, allowed datasource_ids, allowed drives, permissions }
```

- Keys are stored **hashed (SHA-256)** — the database holds digests, so a
  leaked table leaks no usable keys.
- **Rotation without re-targeting**: a key maps to a stable key id; rotating
  the secret re-points the digest at the same scope, and callers keep their
  datasource/drive targeting unchanged.
- **Request targeting narrows within scope, never widens it.** The proto
  carries `datasource_id` and an optional `drive`; both must fall inside
  the key's scope. Auth itself is not a proto field — it rides gRPC
  metadata (`x-api-key`, or `authorization`).
- The error split is contractual:
  - `UNAUTHENTICATED` — the key is missing, malformed, or unknown (the
    server could not establish WHO is calling).
  - `PERMISSION_DENIED` — the key is valid, but the requested
    datasource/drive is outside its scope.

Key→identity resolution rides account-service's `IdentityResolver` seam.
Intake-service is a **peer** of account-service (per the reshape's resolved
decision), so the open question is the shape of the coupling — see Open
decisions below.

### Where intake ends and repo begins

Intake owns authentication, scope enforcement, and wrapping. Repo owns
durability. Concretely: raw bytes become a `BlobBag` entry whose content is
a `FileStorageReference` to a **content-addressed blob** (the same
`blobs/<uuid-of-sha256>` layout `PutBlob` writes); the wrapped `Document` is
saved with `use_datasource_id` on the account's intake graph
(`intake:<accountId>`) to the account's `intake` drive. Intake dedupe
(SHA-256 match → `deduplicated=true` with the existing coordinates) is
repo's, not intake's — intake surfaces it, it does not reimplement it.
Nothing but repo-service speaks S3; intake sees drives and references.

## The parsing coordinator

The coordinator consumes repo documents and produces parser results. Its
contract is two rpcs: `RouteDocument` (dry-run) and `ParseDocument`
(execute).

### Consumption: events or on-demand — open decision

Two trigger shapes, both consistent with the contract:

- **Event-driven**: consume repo's `document-events` topic (the
  transactional outbox relay) and route+parse every newly stored intake row.
- **On-demand**: `ParseDocument` is called by whatever drives the pipeline —
  an operator, a graph step, or a chain.

The proto supports both (the request is a bare `NodeAddress` plus an
optional override list); the trigger is deployment wiring, not wire shape.
Leaning event-driven for the intake graph with on-demand as the operator
tool.

### Part-masked reads

The coordinator reads **CORE+BLOBS only**. PARSED and CHUNKS are its
outputs, not its inputs; a re-parse re-derives them from the raw bytes.
This is exactly what repo's part masks exist for — multi-megabyte blobs
never transit when the coordinator only needs metadata, and parser exhaust
never transits at all.

### Sniffing beats declaring

Routing's content type comes from **magic-byte sniffing** on the first
bytes of the blob — the routing source of truth. The mime type the intake
caller declared (`RawPayload.mime_type`, `search_metadata.source_mime_type`)
is a hint, surfaced to rules as `declared_mime_type` but never trusted
alone: callers lie, proxies re-encode, and extensions get renamed.
`RouteDocumentResponse.content_type` reports what routing actually used,
with `content_type_sniffed` saying whether the bytes or the declaration
supplied it.

### CEL routing rules

Parser selection is a set of `RoutingRule`s: `rule_id`, a CEL `when` guard,
`parser_name`, a `Struct parser_config`, and a `priority`. CEL is the
platform rule language (mapper-cel, chain when-gates); routing rides the
same engine. The guard's scope: `mime_type` (sniffed),
`declared_mime_type`, `filename`, `extension`, `size_bytes`, `account_id`.
Rules ship as **service config, not RPC CRUD** — there is deliberately no
rule-management rpc. Operators test rules with `RouteDocument`, the
dry-run: give it a `NodeAddress` or an inline `Document`, get back the
`PlannedParse` list (parser, config, `matched_rule_id`) the current rule
set would produce. Routing is a set, not first-match: every matching rule
contributes one plan entry, ordered by priority.

### Scatter-gather and result assembly

`ParseDocument` executes the plan: fan out to the parser gRPC services in
parallel (virtual threads, one per parser — tenet 2), gather, and assemble
one repo `ParserResult` per parser. A failed parse is **recorded** (status
FAILED, error set), never silently absent — the response's
`parser_results` map is the same shape the document stores. Each result's
`config_fingerprint` hashes the parser config **and the routing-rule
version** (rule_id + config), so a rule edit cleanly invalidates prior
results: a stale fingerprint is the re-run token, no separate versioning
scheme.

### The extracted_fields → SearchMetadata fold

Each parser returns doc-level claims in `ParserDocument.extracted_fields`
(title, author, language, page_count, ...). The coordinator **arbitrates** —
one winner per field — and repo stores the result: `SearchMetadata` stays
the arbitrated winner, `parser_results` keeps every parser's claims intact
for audit. `ParseDocumentResponse.search_metadata_fold` is the fold
summary, kept deliberately flat: which fields were folded, and which parser
won each. Arbitration order follows the plan order (priority) until a
richer policy earns its keep.

### The known gap: PARSED-part write granularity

Repo's partial-save granularity is **whole-part plus CHUNKS sub-keys**
(`parts_written` + `chunk_sets_written`). Parser results live in one PARSED
part, so two parsers' writes for the same document race: scatter-gather
means concurrent coordinator fan-ins, and each partial save rewrites the
whole `parser_results` map. The planned fix is a
**`parser_results_written` refinement on `SaveDocumentRequest`**, mirroring
`chunk_sets_written`: name the parser keys this save writes, copy-forward
the siblings like unwritten parts. Until it lands, the coordinator
serializes a document's parser writes (one writer per document) — correct,
but it caps fan-in parallelism at the save step.

### Adjacent serial infra: chains

Not every composition is scatter-gather. Where parsers must run serially
(output of one feeds the next), protomolt's chain manager
(`check-chain`/`run-chain`) is the existing infra: typed, statically
verified compositions of gRPC calls with CEL when-gates and deadlines. The
coordinator's rule set is the natural consumer of the static-verification
model — routing rules type-check against the same descriptors the chains
verify against — but chains are adjacent, not load-bearing: the coordinator
fans out by itself.

## Phasing

1. **Intake proto + pass-through impl.** The `intake/proto` contract
   (landed here) plus an intake-service that wraps and saves via the repo
   stub, with a single static key for development.
2. **API-key scope resolution.** Hashed-key store, scope resolution,
   rotation; the IdentityResolver coupling decided (below).
3. **Coordinator with `RouteDocument` dry-run.** Rule config loading, the
   routing context, sniffing; no execution yet — operators test rules
   against real documents immediately.
4. **Scatter-gather + fold.** Parser fan-out, `ParserResult` assembly, the
   extracted_fields fold, PARSED-part writes with serialized per-document
   saves.
5. **`parser_results_written`.** The repo partial-save refinement that
   removes the PARSED-part write race; coordinator drops the serialization.

## Open decisions

- **How intake resolves keys to identities.** Leaning: SPI-in-process —
  intake-service loads the `IdentityResolver` SPI directly with an
  account-service-backed adapter (the same Postgres store account-service
  serves), rather than a per-request RPC to account-service. The SPI keeps
  the hot path in-process and GraalVM-safe (tenet 10); the RPC fallback
  stays available if account data ever stops being shareable at the store
  level.
- **Coordinator trigger: event-driven off `document-events` vs on-demand
  RPC.** Leaning event-driven for the intake graph, on-demand retained as
  the operator surface. The proto supports both; this is deployment wiring.
- **Fold arbitration policy.** Plan order (priority) for now; per-field
  parser precedence tables or CEL selectors are the obvious refinement once
  real parsers disagree in practice.
