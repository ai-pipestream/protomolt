# Processor placement and locality

## Goal

Give the mesh a vocabulary for why a processor runs where it runs, so
placement is part of the contract rather than a deployment convention.

Three distinct reasons exist today. The advertisement contract expresses
one of them.

## Current state

`nano1-tei` advertises `provider=tei`, `model=BAAI/bge-small-en-v1.5`, and
capabilities `[embedding, dimensions-384, cuda, cuda-sm87,
grpc-reflection]`. That describes what the processor does. It does not
describe why the processor has to be on nano1.

Two more embedding runtimes are already deployed and healthy but absent
from the directory: `protomolt-embedder-tei` on krick, backed by an RTX
4080 SUPER, and `protomolt-embedder-ovms` on krick-1. Neither needs new
provider code. The `EmbeddingProvider` SPI and its `tei`, `ovms`, and
`model2vec` implementations already exist in `search/embedding`.

What is missing is the vocabulary to say what those nodes are, and a
publisher to say it. The publisher generalizes
`scripts/nano1-mesh-publisher.py`.

## The placement taxonomy

| Class | Bound by | Examples |
|---|---|---|
| Hardware-pinned | Specific silicon | TEI on the krick RTX 4080 SUPER, OVMS on krick-1, TEI on the nano1 Orin at sm87 |
| Data-pinned | The bytes the node holds | A node owning Lucene segments for one shard |
| Movable | Nothing; the model travels to the caller | Model2Vec static tables |

The classes are not a property of the model or the engine. They are a
property of the deployment: the same provider is hardware-pinned on a GPU
node and movable on a node that holds the weights locally.

## Movable providers

`Model2VecEmbeddingProvider` takes a model directory and resolves it from
`protomolt.embeddings.model2vec.path`. The provider is already local
first. What is absent is distribution, not a provider.

The flow:

1. A node declares it can host a named model.
2. The mesh ships the model directory once.
3. The node re-advertises with the model resident.

Model directories are content-addressed and cached on disk, which is the
claim-check discipline the mesh already applies to entity payloads. The
existing artifact stores carry the bytes.

The remote variant stays available. `EmbeddingProvider` hides which side
answered, so a caller writes `embed(text)` and never learns whether the
vector came from a table in local memory or a gRPC hop. Keep the remote
path for cold start, for nodes too small to hold a model, and for models
whose license forbids redistribution.

## Proportionality

A network hop is justified when its cost is small relative to the work it
dispatches.

TEI serving bge-m3 on krick measures about 1400 embeddings per second,
roughly 0.7 ms of GPU work each, recorded in
`deploy/krick/compose.embeddings.yml` on 2026-08-21. A tailnet round trip
is the same order of magnitude. Dispatching that work is proportionate.

Model2Vec is a subword table lookup and a mean pool, with no forward
pass. The work is microseconds. A round trip is orders of magnitude
larger than the operation it carries, so a remote Model2Vec provider
spends almost all of its time on transport.

The rule that follows: pin what needs the silicon, ship what does not.

This also settles small nodes. A four-core arm64 board with a resident
Model2Vec table is a first-class embedding node. Without the movable
class the same board is only a slow proxy to somebody else's CPU.

## Data-pinned processors

Lucene is a library, not a service. A node holding segments is the
search. Modelling it as a remote search service adds a hop and describes
the deployment incorrectly.

The primitive is shard ownership rather than service reachability: a node
advertises that it owns a named shard of a named index, queries fan out
to the owners, each scores in process, and the coordinator merges.

## The document-frequency decision

BM25 scores depend on document frequency. When each shard scores against
its own local df, cross-shard scores are not comparable and score-based
pruning loses its bound.

This is the decision to settle before any shard-owning node exists,
because it is invisible in normal use. Ranking still looks plausible.
The failure appears only when results are compared against a
single-shard baseline over the same corpus, and a distributed lexical
engine that gets this wrong can forfeit most of its pruning across every
code path that consults a frequency.

Three options, to be chosen rather than inherited:

1. **Shared term statistics.** Shard owners score against a common df
   source. Exact, and adds a dependency to the query path.
2. **Two-phase query.** Collect df from the owners, then score with the
   global values. Exact, and costs a second round trip.
3. **Declared approximation.** Score with local df and state it in the
   contract, so a caller can decide whether the ranking is good enough.

Option 3 is what a shard fan-out does by default. It is a defensible
choice and an indefensible accident, so the contract has to name it.

## Contract gaps

`ProcessorAdvertisement` carries `processor_id`, `node_id`, `kind`,
`capabilities`, `accepted_schemas`, `provider`, `model`, `model_version`,
`capability_details`, and the lease fields, with a CEL rule that a
`model` requires a `provider`.

Two things have no vocabulary:

- **Data locality.** No field names the index and shard a processor owns,
  so an eligibility query cannot route to the node holding the data.
- **Materializable capability.** No field separates what a node serves
  now from what it could serve once provisioned, which is the
  distinction the movable class depends on.

Both can start as capability slugs, since `capabilities` is a repeated
slug field: `shard-documents-3` for ownership, `model2vec-capable`
against `model2vec-resident` for provisioning state. That is a
convention, not a contract, and it puts structure inside strings.

Promote them to typed fields when eligibility queries need to filter and
compare rather than string-match, and when a wrong match becomes a
correctness problem rather than a missed route.

## Tests

Use in-process nodes and a fake clock, as the sibling packages do.

Cover advertisement and eligibility for each placement class; a movable
provider before and after the model is resident; equivalence between the
resident and remote answers for one model, through the existing
`search/embedding/harness` cosine certification; shard-ownership routing
to the owning node; a query whose shard owner is absent; and each of the
three document-frequency options producing the ranking its contract
declares.

## Acceptance criteria

- krick and krick-1 appear in the directory with their existing embedding
  runtimes, without new provider code.
- An eligibility query distinguishes a hardware-pinned processor from a
  movable one and refuses to route work to silicon that is absent.
- A node with no model resident acquires one, re-advertises, and answers
  locally, with vectors certified equivalent to the remote provider.
- A shard query reaches the node owning the shard rather than a proxy.
- The document-frequency behavior a deployment provides is the one its
  contract declares, and a caller can read which it is.

## Exclusions

Do not implement model training or distillation, cross-cluster model
distribution, automatic shard rebalancing, replica placement or failover,
or query planning beyond selecting the owner of a shard.

Model licensing and redistribution policy is a separate decision and is
not settled here.
