# Config distribution

Status: design of record for configuration distribution between
protomolt nodes in a scaled deployment. Decided 2026-08-18 with the
project owner; landing changes to this shape means updating this
chapter first.

## The decision

A node does not need a distribution protocol; it needs a **config
reader**. So the architecture is a pluggable seam, not a system:

- **The contract is typed and validated, not stringly.** A config
  document is a protobuf message of a declared type. Before anything
  applies, the document parses strictly as that type and passes the
  type's own declared validate.v1 rules — the same enforcement every
  wire door mounts. Exact or refused: an invalid document never
  applies, the node keeps serving the config it already runs, and the
  refusal names the violations.
- **`ConfigSource` is the plug** (`protomolt-config`): one subject
  resolves to at most one versioned payload. No watches, no sessions,
  no membership. `DistributedConfig` is the consumer over any source:
  subscribe typed, refresh on the host's cadence, verify-then-swap
  atomically, notify listeners, report the outcome.
- **The version is evidence.** Every applied config carries the
  source's version (a git commit, a topic offset), so a node can
  always say exactly which config it runs — the same evidence stance
  as `physical_plan` and snapshot ids.
- **There are no coordinator nodes.** Every node is a reader of its
  role-scoped subjects; the writer is whoever publishes to the source
  (an operator, CI, a workflow). Scaling adds readers, never a
  membership event.

## The plugs

Two ship; others can exist without touching the consumer:

1. **Registry/git (the default).** Config documents live in the
   git-backed registry beside schemas and workflows: versioned by
   commit, compat-gated at the type level, federated between meshes by
   the machinery that already exists, air-gap friendly. This is the
   GitOps idiom with typed protobuf instead of YAML. The write side is
   the `publish-config` verb on the registry's action catalog: the same
   gate as the HTTP config door (strict parse as the declared type, its
   declared rules enforced), the commit id back as the version.
2. **Kafka (the signal plug).** A compacted topic: key = subject,
   value = the typed config message through the **house serde**
   (`protomolt-kafka-serde`) against the protomolt registry — schema-id
   framed, `validate.on.write` for publishers and `validate.on.read`
   for consumers both ON. Eat-our-own-lunch is a requirement, not a
   nicety: the config lane exercises the same serde, registry, and
   validation stack the data lanes ship. Compaction keeps the latest
   document per subject; replay-to-latest is bootstrap. This gives the
   replicated-log properties (the KRaft instinct) without this
   codebase writing consensus. The platform mounts it by environment:
   the `DOCUMENT_PLATFORM_CONFIG_KAFKA_*` family (bootstrap servers,
   topic defaulting to `protomolt-config`, and the serde's schema
   registry URL, required because the lane is verify-then-swap or
   nothing); naming it alongside `DOCUMENT_PLATFORM_CONFIG_URL` is a
   contradiction, refused rather than resolved by preference.

## Rejected, with reasons

- **ZooKeeper / any quorum coordination service** — explicitly ruled
  out by the project owner. A second stateful system with its own
  operational life (the tax Kafka spent years removing); protomolt has
  chosen "no cluster protocol" repeatedly and keeps choosing it.
- **Gossip** — no authoritative version, convergence-under-stress
  doubts, and config wants exactly what gossip cannot give: a
  versioned authority and evidence of which version each node runs.
  Gossip's honest domain is liveness/membership, and the
  `mesh/cluster` presence directory already owns that seam.
- **NiFi-style elected coordinator** — too central; the pull model
  needs no election because there is nothing to elect.

## Fit

The consumption semantics are the reader-refresh idiom already proven
on the search reader: pull on an interval, verify, swap atomically,
keep serving the old state when a pull fails, and say so. Bootstrap
environment shrinks toward two facts — where the source is and who
this node is — with everything else eligible to move into distributed
config, subject by subject, starting with the parse routing rules.

The long-run ambition (an Apache-governed project) shapes this too:
no hard infrastructure dependency in the contract, the Kafka plug as
the ecosystem-integration story, and no-ZooKeeper as a feature.
