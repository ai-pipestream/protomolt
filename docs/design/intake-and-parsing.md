# Intake and parsing

Intake and parsing define the boundary between external content and document
pipelines. The contracts live in `intake/proto` and `parse/proto`. The intake
runtime lives in `intake/service`, the parsing coordinator's in
`parse/service`; `parse/text` is the reference parser and `parse/playground`
the streaming front end.

## Intake service

Intake is the public ingest surface. It authenticates the caller, resolves an
account and drive, wraps the payload in a repository `Document`, and saves it
through `DocumentService`. Downstream services read from repository service
instead of accepting another copy of the original payload.

The contract supports three lanes:

- unary ingest for small or already-typed documents;
- client-streaming ingest for payloads whose bytes should not fit one
  message; and
- an HTTP POST raw-binary route (`POST /v1/intake:upload`) mirroring
  repo-service's upload route with `x-api-key` replacing the raw account
  headers.

Every lane answers the same receipt vocabulary: document ID, node ID, the
canonical `NodeAddress`, drive, payload size, the payload's SHA-256, and the
deduplication result.

API keys resolve to an `IntakeScope`: the owning account plus optional
datasource, drive, and content-type restrictions and a payload cap. The
request cannot widen those host-owned limits — targeting outside the scope is
`PERMISSION_DENIED`, an unknown key is `UNAUTHENTICATED`. Resolution runs
through the `ApiKeyIdentityResolver` key-store SPI
(`intake/service`): the default production store is an external IdP treating
API keys as client credentials (rotation-with-grace falls out of several keys
resolving to one scope), with in-memory and env-seeded stores for tests,
demos, and single-tenant deployments. Only repository service accesses object
storage.

## Parsing coordinator

The parsing contract has two operations:

- `RouteDocument` evaluates routing without invoking a parser;
- `ParseDocument` routes, invokes parsers, and assembles results.

The coordinator reads the `CORE` and `BLOBS` document parts. `PARSED` and
`CHUNKS` are outputs and are not loaded as parser input.

### Content detection

Magic-byte inspection is the routing source of truth. A caller-provided MIME
type is a hint exposed to routing rules. The response records the content type
used and whether it came from sniffing or the declaration.

### Routing rules

Each `RoutingRule` contains an ID, CEL guard, parser name, parser
configuration, and priority. CEL can read:

- sniffed and declared MIME type;
- filename and extension;
- payload size; and
- account ID.

Every matching rule contributes a parse plan entry. Priority determines
ordering. Rules are service configuration, not mutable RPC resources.
`RouteDocument` provides the dry-run surface for operators and tests.

### Parser execution

The coordinator invokes selected parser services concurrently on bounded
virtual threads. Each parser produces a named `ParserResult`. Failure is
stored as a failed result with an error instead of being omitted.

The result fingerprint covers parser configuration and routing-rule identity.
A changed fingerprint identifies stale output that needs another parse.

### Search metadata fold

Parsers may make document-level claims such as title, author, language, or
page count. The coordinator keeps every claim in the parser results and folds
one winner per field into `SearchMetadata`. The fold response records which
parser won each field.

Priority order is the initial arbitration policy. A future policy may define
per-field precedence or CEL selectors.

When no parser claims a `body`, the coordinator derives one from the parse
stream's page events: the highest-priority non-failed parser that streamed
page text contributes its pages, joined in stream order. An explicit body
claim always outranks derived text, whatever the claiming parser's
priority. This is how a parser that speaks a native document model but
volunteers no claims (such as the gRParse adapter) still yields a lexical
body for search.

## Parser plugin contract

`parse/proto`'s `plugin/v1` package defines `ParserPluginService`, the
contract every parser service implements — the formalization of the idiom
the parser fleet converged on:

- `GetParserInfo` returns the parser's identity (`parser_name` is THE
  identity: routing rules match it, `parser_results` keys on it, service
  profiles register under it), version, capabilities, and limits;
- `Parse` takes an options-first chunked request stream and returns a typed
  event stream: progress, per-page content, page preview images, document
  claims for the metadata fold, and the final `ParserOutput` emitted exactly
  once before the stream closes.

Streaming honesty is contractual: events fire when the underlying work
completes, never faked from a finished result. A parse that produces nothing
fails the stream with a gRPC status (stored as a FAILED result); a degraded
parse reports its losses through `ParserOutput.warnings` (stored PARTIAL).

gRParse, the C++ fleet parser, joins through the `parse/grparse` sidecar
adapter with zero C++ changes: the adapter implements `ParserPluginService`,
bridges each parse onto gRParse's `StreamProcessDocument` page stream
(vendored wire surface), maps page events and collector output back onto the
plugin envelope, and reports collector failures as warnings. It is deployed
next to gRParse and registered via a service profile under the name
`grparse`. The streaming wire carries no processing options, so the adapter
cannot request page renders; when the gRParse fleet is built to render page
images into `PageData.page_meta.image` (a data URI), the adapter decodes
and forwards each one live as a `PagePreview`. `emits_previews` advertises
that deployment fact, set with `PARSE_GRPARSE_EMITS_PREVIEWS`.

## Trigger model

The contract supports event-driven and on-demand execution. An event-driven
deployment consumes repository `document-events`; an operator or pipeline can
call `ParseDocument` directly. Trigger choice does not change the wire shape.

## Required repository refinement

Parser results share the `PARSED` document part. Concurrent partial saves need
a `parser_results_written` selector, equivalent to the existing chunk-set
selector, so one parser update preserves sibling entries. Until that contract
exists, a coordinator must serialize parser-result writes per document.

## Open runtime choices

- Whether event-driven parsing is enabled by default.
- Whether metadata arbitration needs per-field policies beyond parser
  priority.

See [document platform architecture](document-platform.md) for service,
identity, storage, and eventing boundaries.

## Intake revision expectations and deletes (D8)

Unary ingestion and streaming metadata accept the existing repository v2
`ExpectedRevision`. Omitted means create (`none=true`); an update must carry the
revision previously observed by the caller. The HTTP lane accepts
`expected_revision` (or `x-expected-revision`). Retrying a request keeps the same
expectation and typed intent. Intake does not replace a stale expectation with
the latest revision. A changed intent at a stale revision fails `ABORTED`.

Intake's `DeleteDocument` delegates the existing v2 request and response to the
repository, after checking the authenticated account, intake graph and datasource
scope. It requires a positive observed revision. It creates a tombstone, not a
physical purge. A delayed delete cannot delete a newer revision: the repository
checks the expectation in its commit transaction. A retry retains its operation
ID and expectation and returns the recorded tombstone receipt.

The older `DocumentOperations.saveFull` used the root SHA of the split document
for intake deduplication. `S3Purger.eligible` rejected obsolete physical purge
work using the repository's body-write timestamp, or the purge generation. That
was not a source `last_modified_date` ordering policy. Those implementations were
not deleted by D8. The v2 migration must preserve their safety properties through
revision checks, immutable objects and reference-aware purge, without mounting
that legacy service.

Binary reuse and operation replay are distinct. The save path can reuse identical
SHA-256/length objects from the authorized base revision in the selected stack.
It still validates the candidate and records a new accession for a fresh update;
metadata changes are not discarded. It does not claim global cross-document or
cross-stack deduplication. Intake's existing `deduplicated` receipt field denotes
operation replay, as its contract comment states; it is not an object-upload count.

Both intake and parse fixtures mount the integrated v2 upload/read, delete, list
and purge coordinators. The full intake and parse checks include real-repository
delete/replay/history and parsing through ReadBlob; no replacement test RPC is
used. Production-host cutover and streaming parser inputs remain separate work.
