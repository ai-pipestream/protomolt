# Graph standards contracts and provider SPI

Status: proposal for review (#816). Nothing here is implemented beyond what
the "What exists" section names, and nothing here claims standards
conformance or provider performance. Once reviewed, the decisions move into a
design of record and the slices at the end become tickets under #776.

## Layers

Five contracts stay separate, because each one changes for different reasons
and a provider can satisfy some without the others.

| Layer | What it fixes | Owner |
|---|---|---|
| Standard semantics | What an operation means: SPARQL 1.1 Query and Update, the SPARQL 1.1 Graph Store HTTP Protocol, RDF 1.1 or 1.2 terms | W3C; this repo only selects a subset and states it |
| Payload encoding | How RDF terms, quads and result rows travel | Jena PB_RDF (`binary-rdf.proto`) for the Jena provider; Jelly is a candidate, see [Encodings](#encodings) |
| Service operations | The RPCs a graph service exposes | The provider's own gRPC contract (Jena `RdfDatasetService`) |
| Provider implementation | The engine behind the RPCs | Jena reference; Oxigraph is a candidate only |
| Repository-aware publication | Binding a graph write to a source revision, retained evidence, mapping version, operation identity and receipt | ProtoMolt; an adapter supplies it for a provider that lacks it |

ProtoMolt coordinates workflows and does descriptor-based mapping and
projection. protomolt-search owns query planning that combines graph, lexical
and vector retrieval. Graph providers execute authorized operations. The
minimal repository service depends on none of them.

## What exists

Verified at main `c1456ebd5`:

- `ConceptScheme`, `Concept`, `ConceptRef`, `LangLabel`, `ConceptNotation`
  (`protobuf/types/.../types/v1/concepts.proto`), with house validation rules.
- `SkosConverter` (`surface/skos`): strict SKOS read and write between
  `ConceptScheme` and Turtle or RDF/XML through Jena RIOT.
- `SnapshotProjector` (`sink/graph`, Jena 6.2.0): projects document links and
  concept schemes into one named graph per full document address. One Jena
  `WRITE` transaction replaces the document graph and updates a control graph
  holding the revision, a deleted flag and the mapping version. A lower
  revision is refused as stale; an equal revision is a replay only when the
  flag, mapping version and graph are isomorphic, otherwise a conflict. A
  tombstone empties the graph and keeps the guard, so a delayed retry cannot
  restore a deleted projection. A required `Authorization` callback checks the
  source, snapshot and retained evidence (`PartSchemaBinding`,
  `ResolvedOccurrence`) on every call, replays and deletes included; there is
  no permissive default. Evidence protobufs are kept in the graph as base64
  literals of their deterministic bytes.

Finding: `SnapshotProjector.replaceScheme` writes the scheme to Turtle with
`SkosConverter` and parses the text back into a Jena model. That is the
"through Turtle for internal convenience" hop this ticket rules out. Slice 1
below removes it.

## The Jena gRPC contract

Inspected at ai-pipestream/jena PR #3 head `b8d5081a5`
(`jena-grpc/src/main/proto/org/apache/jena/grpc/v1/rdf_service.proto`, on
`binary-rdf.proto` from the stacked PR #2). The PR description lists an older
RPC set (`QuerySelect`, `BeginTxn`, and so on); the proto below is the
contract.

`RdfDatasetService`:

| RPC | Shape | Covers |
|---|---|---|
| `StreamRdf` | unary in, server stream | Read a quad pattern, one graph (default or named), or the whole dataset |
| `WriteRdf` | client stream, header first | Append, replace or clear a graph or the dataset |
| `ExecuteQuery` | unary in, server stream | SELECT (header then row batches), CONSTRUCT/DESCRIBE (`RDF_Stream` batches), ASK, or a JSON result string |
| `ExecuteUpdate` | client stream | SPARQL Update with `using_graph_uris` / `using_named_graph_uris` |
| `ApplyPatch` | client stream | RDF Patch rows |
| `SubscribePatches` | server stream | Committed patches, optionally filtered to one graph |
| `Transaction` | bidirectional | Begin, promote, commit, abort, end; `READ`, `WRITE`, `READ_PROMOTE`, `READ_COMMITTED_PROMOTE` |

PB_RDF terms cover IRIs, blank nodes, literals (simple, language tag,
language tag with base direction, datatype), RDF 1.2 triple terms, variables,
and value forms for integer, double and decimal. That is enough for RDF 1.2
term fidelity, and ProtoMolt should reuse it rather than define another RDF
term format.

Points to settle with the Jena side before ProtoMolt depends on it:

- `QUERY_RESULT_FORM_JSON` returns SPARQL JSON results inside a protobuf
  string. ProtoMolt code must not use that form; select rows travel as
  `RDF_VarTuple` and `RDF_DataTuple`.
- `txn_id` is a server-assigned string carried in request fields or
  `x-jena-txn-id` metadata, tied to one connection's `Transaction` stream.
  Promotion modes are Jena semantics. They stay optional and Jena-specific,
  not a requirement every provider must meet.
- `SubscribePatches` and `ApplyPatch` are a change feed, not a query or
  publication API.

## Capabilities

A provider declares what it supports; ProtoMolt never assumes more and never
downgrades silently. An operation the provider has not declared fails with
`UNIMPLEMENTED` naming the capability. A declared operation the request cannot
satisfy (an RDF 1.2 term sent to an RDF 1.1 provider, a result over the
declared byte limit) fails with `FAILED_PRECONDITION` or `RESOURCE_EXHAUSTED`,
never by dropping data.

| Capability | Meaning | Jena `RdfDatasetService` |
|---|---|---|
| `graph-read` | Stream one graph, a quad pattern or the dataset | `StreamRdf` |
| `graph-store-write` | Replace, append or clear one graph | `WriteRdf` |
| `sparql-query` | SELECT, ASK, CONSTRUCT, DESCRIBE with a declared SPARQL version | `ExecuteQuery` |
| `sparql-update` | SPARQL Update | `ExecuteUpdate` |
| `transactions` (optional) | Multi-request isolation | `Transaction` |
| `patch-feed` (optional) | Committed change stream | `SubscribePatches`, `ApplyPatch` |
| `repository-publication` | See [Repository-aware publication](#repository-aware-publication) | Not provided; needs an adapter |

Each declaration also carries the RDF version, SPARQL version, maximum
request and response bytes, maximum rows per batch, and a provider name and
version. Provider discovery reuses the existing service-profile and descriptor
machinery: a provider is a service profile whose reflected descriptor names
the RPCs, plus a capability declaration. The declaration message is a proto
addition for slice 2, with house `validate.v1` rules and a closure repin; it
is not added by this document.

## Parity with the W3C protocols

A gRPC binding is not the SPARQL Protocol or the Graph Store HTTP Protocol.
What ProtoMolt can promise is that the operations mean the same thing. This
table is the parity target; the "Open" column lists what the shared tests must
pin down because the Jena contract does not state it.

| W3C operation | gRPC operation | Same meaning requires | Open |
|---|---|---|---|
| Query, `query` with `default-graph-uri` / `named-graph-uri` | `ExecuteQuery` with `default_graph_uris` / `named_graph_uris` | Protocol dataset overrides any `FROM` / `FROM NAMED` in the query, as the SPARQL Protocol states | Whether the Jena service applies that override |
| Update, `update` with `using-graph-uri` / `using-named-graph-uri` | `ExecuteUpdate` with `using_graph_uris` / `using_named_graph_uris` | Same override rule for `USING` | As above |
| Query result forms | SELECT rows, ASK boolean, CONSTRUCT/DESCRIBE triples | An empty SELECT has a header and zero rows; an ASK false is a result, not an absence | Whether an empty SELECT still sends the header frame |
| GSP `GET ?graph=` / `?default` | `StreamRdf` with `GraphTarget` | A missing named graph is distinguishable from an empty one (GSP answers 404) | Whether `StreamRdf` reports absence or streams nothing |
| GSP `PUT` | `WriteRdf` `REPLACE` | Replace is atomic for the graph | Atomicity of a client-streamed replace that is cancelled midway |
| GSP `POST` | `WriteRdf` `APPEND` | Append-only | Blank-node scope across batches of one stream |
| GSP `DELETE` | `WriteRdf` `CLEAR` | GSP removes the graph and answers 404 when it is absent | Whether `CLEAR` removes the graph or leaves it empty |
| Malformed query or update | `INVALID_ARGUMENT` | No partial execution | Error detail format |
| Client disconnect | gRPC cancellation | Server stops work and releases the transaction | Cancellation of a running query in Jena |
| Server limits | `RESOURCE_EXHAUSTED` | Declared limits, enforced before results stream | Where the Jena service enforces them |

Provider-specific query extensions and transaction modes are not part of the
parity target.

## Encodings

PB_RDF is the encoding of the Jena provider and stays so. Jelly (protobuf RDF
streams, with its own gRPC publish and subscribe protocol) is evaluated, not
adopted:

- Jelly encodes RDF streams. It is not a SPARQL result encoding and not a
  query or publication API, so it could at most replace PB_RDF for
  `graph-read`, `graph-store-write` and patch payloads.
- Evaluation uses the same fixtures in Java and Rust: RDF 1.2 triple terms,
  language tags with direction, datatype literals, blank-node scope across
  frames, default and named graphs, large literals, and malformed or truncated
  streams. It records the exact Jelly spec and library versions, and whether
  each implementation passes its own conformance suite.
- Without a measured gain on those fixtures, PB_RDF stays the only encoding.

Neither path goes through Turtle or JSON for ProtoMolt's own convenience.
Turtle and RDF/XML stay what `SkosConverter` already is: interchange formats
at the edge.

## Repository-aware publication

This capability binds one graph write to:

- the source address and exact revision (`SourceRevision`),
- the retained schema and evidence (`PartSchemaBinding`, `ResolvedOccurrence`),
- the mapping version,
- a stable operation identity for the exact publication intent,
- the provider's receipt for the write.

`SnapshotProjector` already has the semantics in process: stale revisions
refused, exact replays recognized, conflicting replays refused, tombstones
that outlive the graph, all in one Jena transaction. The atomicity boundary is
that transaction: the document graph and its control-graph guard change
together or not at all.

A remote provider keeps that boundary only if it can apply the graph replace
and the guard check atomically. With the Jena service that means one
`Transaction` stream covering a guard read, the `WriteRdf` replace and the
guard write. A generic SPARQL endpoint without transactions cannot advertise
`repository-publication`. An adapter may supply it only by proving the same
outcomes in the shared tests: stale refusal, replay detection, conflict
refusal, and deletion fencing under retry and restart.

The control graph is derived internal state. It is not repository authority
and not an authorization mechanism. Publication receipts and external object
identity follow the external-binding work in #814. Operation identity and the
acceptance versus publication distinction follow #777. A changed source or
mapping under the same operation identity is refused. Projection can be
inspected and replayed, and publication retried, without rerunning NLP,
because its inputs are the retained artifacts, not a new model run.

Large outputs use bounded streams (`WriteRdf` batches) or a retained
artifact, never one unbounded message.

## Authorization

A caller's visible datasets are enforced before a provider evaluates joins,
aggregates or property paths. Filtering final rows, or trusting a `FROM NAMED`
the caller wrote, is not a boundary: a property path or an aggregate over the
default graph can carry information out of graphs the caller cannot see.

- ProtoMolt computes the caller's visible graph set per request (scopes are
  resolved per request, as on the MCP transport) and the provider evaluates
  the query over exactly that dataset, set through the protocol dataset
  fields, which override the query text.
- Queries naming a graph outside the set fail with `PERMISSION_DENIED` before
  execution. `SERVICE` (federated query) is refused unless a separate,
  explicit capability allows it.
- The default graph is never the union of all named graphs for a scoped
  caller.
- Revocation applies from the next request. A running query keeps the dataset
  it started with; the shared tests state this explicitly.

## Mapping profiles

Each profile is versioned, names the vocabulary version it maps to, and fails
explicitly on input it cannot represent. Registered `Any` descriptors expose
structure, not an inferred ontology.

| Profile | Scope | Status |
|---|---|---|
| SKOS | `ConceptScheme` to and from SKOS | Exists (`SkosConverter`) |
| Web Annotation | Source spans: target IRI, selector, offsets | Candidate. Offsets need an explicit unit (UTF-8 bytes, UTF-16 code units or code points) and the root-source text they index; chunk-relative offsets are not converted silently |
| DPV | Governance vocabulary for screening and disclosure categories | Candidate. Categories inform policy; they grant no access |
| CoNLL-U / UD | Syntactic layers (tokens, lemmas, dependencies) for interchange | Candidate, only where a parse layer needs to leave the platform |

## Provider qualification

Jena stays the reference implementation. Oxigraph (behind a Tonic gRPC
service) is a candidate. A comparison uses the same semantics, dataset,
transport, durability setting, transaction sizes and resource limits, and
reports throughput, tail latency, memory and recovery time. No size or speed
advantage is claimed before those numbers exist.

The shared test kit runs against every provider under `:integrationTest`:

- RDF term fidelity: IRIs, language tags with and without direction, datatype
  literals, numeric value forms, triple terms, blank-node scope.
- Default versus named graphs, empty versus absent graphs.
- Result shapes for SELECT, ASK, CONSTRUCT, DESCRIBE, including empty results.
- Malformed and truncated streams, backpressure, cancellation, byte and row
  limits.
- Authorization: dataset restriction, property paths and aggregates across a
  hidden graph, `SERVICE` refusal, revocation between requests.
- Repository publication: atomic replace, replay, conflict, deletion fencing,
  restart.

Exact provider and protocol versions are pinned in the kit.

## Implementation slices

Each slice links to #776 rather than opening a competing graph worker.

1. Remove the Turtle round trip from `SnapshotProjector.replaceScheme`: give
   `SkosConverter` a writer onto a Jena `Graph` or `StreamRDF`. Pure refactor;
   the isomorphism-based replay check keeps its meaning.
2. Provider capability declaration proto and discovery through service
   profiles.
3. Jena gRPC provider adapter: a Java client over `RdfDatasetService`.
4. Shared conformance kit module, first run against an embedded Jena dataset
   and the Jena gRPC service.
5. Repository-aware publication over a remote provider, with the guard in a
   provider transaction (#776, #777, #814).
6. Dataset-restricted query authorization.
7. Jelly fixture evaluation.
8. Oxigraph qualification.

Slices 1, 2 and 4 have no external dependency. Slice 3 depends on the Jena PR
landing or on pinning its head.

## Questions for review

- Is RDF 1.2 (triple terms, base direction) required from the first provider,
  or declared and tested as optional?
- Should `repository-publication` be refused for any provider without
  multi-request transactions, or is a per-graph compare-and-set RPC on the
  Jena side preferable to the `Transaction` stream?
- Which Web Annotation offset unit is canonical for ProtoMolt spans?
