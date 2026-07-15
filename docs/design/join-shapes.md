# Joins, unions, and derived shapes (design)

Status: design settled; implemented through stream joins — multi-source
resolution, shape synthesis, `synthesize-shape` / `join-messages` /
`merge-schemas` / `infer-schema` (struct-to-proto: reverse-engineer a
message type from data-rich JSON samples), and `StreamJoiner`.

## Motivation

ETL systems live and die by their in-between steps, and two of those steps
are non-negotiable: **join** (two inputs, one enriched output) and
**union** (many similar inputs, one stream). ProtoMolt already has the
manipulation half — mapping rules, CEL, validation. This design adds the
combination half, protobuf-natively: what is the *shape* of a join's
output, how are two messages resolved into it, and where does the derived
schema live?

## The shape problem

In SQL a join's output shape is ephemeral — the SELECT list projects
columns and the "type" evaporates after the query. Protobuf refuses that:
anything downstream needs a real descriptor. So a join's output must be a
declared message type, and there are exactly three honest ways to get one:

1. **Authored.** The output message is written by hand in a `.proto`, and
   mapping rules populate it from the sources. The rules are the SELECT
   list. Explicit, reviewed, versioned — the right choice for contracts
   that outlive the join.
2. **Synthesized envelope.** A generated wrapper holding each source
   intact: `message Joined { shop.v1.Order order = 1; crm.v1.Customer
   customer = 2; }`. Lossless and zero-authoring; the shape is machine-made
   and consumers navigate nesting.
3. **Synthesized projection.** A generated flat message whose fields are
   *derived from source paths*: `customer_name` gets its type from
   `customer.name`'s field descriptor. The SELECT list becomes the schema.

Union splits the same way:

- **Structural union** (SQL `UNION`): N similar types projected onto one
  common target — an authored or synthesized target plus one ruleset per
  source type.
- **Tagged union**: protobuf's native answer, a synthesized `oneof` over
  the source types. Lossless, and consumers dispatch on the case.

## Derived schemas are real schemas

The move that makes this ProtoMolt-shaped rather than generic-ETL-shaped:
synthesized shapes are built as descriptors (a `FileDescriptorProto`
depending on the sources' files), compiled in-process, and emitted as
`.proto` source whose imports are the sources' true import paths. That
text registers in the Git-backed registry like any hand-written schema —
with references, history, diffs, and compatibility gates. A join's output
contract stops being implicit in a transformation config and becomes a
governed schema. When the join definition changes, `check-compat` says
whether downstream consumers survive it.

## Schema merging: validate, resolve, emit

Where projections pick fields, a **merge** carries *every* top-level field
of two or more types into one new type — the schema-level join/union. The
flow has three steps because clash analysis is pure descriptor work:

1. **Validate.** The clash report is computed before anything is
   generated. Same name + same type is `coalesced` — an info entry, and
   the natural join keys. Same name + different type (`order.status:
   string` vs `ticket.status: enum`) or different cardinality is a hard
   clash that blocks emission.
2. **Resolve.** The caller decides each hard clash: `rename` (each
   source's field kept under a new name, defaulting to
   `<source>_<field>` — the suggested default, since prefixing is almost
   always what people want), `prefer` (one source's field wins), or an
   override of a default coalesce.
3. **Emit.** One move produces the merged proto source (registrable, true
   import paths), the compiled descriptor set, and both relationships to
   the new type: the **defined join** — one ruleset reading every source
   at once (coalesced singular fields let later sources overwrite,
   absent values skip; coalesced repeated fields accumulate) — and the
   **defined union** — one ruleset per source, mapping it alone onto the
   merged shape, the structural `UNION`.

The `merge-schemas` verb carries the flow on every surface; `reportOnly`
runs the validation step standalone. Map-typed fields are not yet
mergeable (rejected with a clear error); they need synthesized map-entry
types and are deferred.

## Multi-source resolution

One resolution model powers everything: a **scope** of named messages.
Text rules read `target.path = source.path` where the first segment of the
source path names a scope entry; CEL expressions see each scope entry as a
variable. The same scope shape appears everywhere combination happens:

| Context | Scope entries |
|---|---|
| Join | `order`, `customer`, … (the named sources) |
| Chain step | `input`, `steps.<name>` |
| Enrich transform | `input`, `response` |
| Key+value record join | `key`, `value` |

Learned once, used everywhere. Synthesized projections carry their implied
rules (field name ← source path), so projecting needs no hand-written
ruleset; authored targets take explicit rules, exactly the `map-message`
surface with scoped source paths.

## Execution stories

- **The verbs** — `synthesize-shape` (sources + mode + name → proto
  source, descriptor set, implied rules) and `join-messages` (sources with
  messages + a target or shape spec + rules → the joined message). Verbs
  mean every surface: typed RPCs, REST, Swagger, MCP tools for agents.
- **Chains** — the chain variable scope *is* a multi-source scope, so a
  chain's output mapping is already a join of every step's response.
- **gRPC streams — our streaming turf.** Stateful topic-to-topic joins
  belong to Kafka Streams. But when both sides are *gRPC server streams*,
  the join is ours: open two flow-controlled `DynamicGrpcStream`s, match
  pairwise (zip) or by key within a bounded buffer, emit the joined shape.
  Flow control comes free — each side is only requested as it drains, so
  a fast stream cannot flood a slow one, and bounded buffers make memory
  explicit. Implemented as `StreamJoiner` (exact semantics under Phasing
  below); richer unmatched-entry policies (emit-partial, fail-on-drop)
  remain future options.
- **Connect transforms** — a record's key and value messages joined into
  the value (the smallest useful join); `GrpcEnrich` as the lookup join
  where the other side lives behind a service.

## Keys declared in the schema

Joins need keys, and the schema should say what they are. A declared-key
field option in the metadata standards (`ai.pipestream.proto.meta.v1`)
lets a message state its identity once; key-based stream joins, dedup,
Kafka record keys, and upsert semantics all derive from it instead of
being configured per deployment. Same philosophy as validation rules and
indexing hints: declare in the contract, honor on every surface. Planned
alongside the stream-join phase.

## Non-goals

- **Stateful topic-to-topic joins** — windows, co-partitioning, state
  stores. Kafka Streams does this well; a sidecar should not.
- **Unbounded buffering** — stream joins always carry explicit bounds and
  an expiry policy. If a join needs unbounded state, it needs a database.
- **A query language** — rules and CEL are the surface. If a SQL-ish
  layer ever makes sense, it compiles down to these primitives (the same
  posture as the chain manager's contract-as-language direction).

## Phasing

1. **Shapes and verbs** (implemented): `protomolt-shapes` — scope,
   scoped mapper, synthesizer (envelope, projection, tagged union),
   joiner — plus the `synthesize-shape` / `join-messages` verbs on every
   surface.
2. **Stream joins** (implemented): `StreamJoiner` — zip and keyed joins
   over two flow-controlled `DynamicGrpcStream`s. Exact semantics:
   matching is symmetric and immediate (each arrival probes the other
   side's buffer); completed joins wait in their own queue, so a caller
   taking few results never strands a matched pair; per-side buffers are
   bounded and evict the oldest *unmatched* entry by true arrival order;
   duplicate keys queue FIFO; a message whose key path cannot be read is
   dropped (it can never match); key paths are validated at construction
   (existence, singular scalar, same type on both sides). Output is built
   through the standard scoped rules. A Connect source emitting a joined
   stream remains open.
3. **Schema-declared keys**: the metadata option and its adoption by
   stream joins and the connectors.
4. **Registry registration of derived shapes** as a first-class flow
   (register the synthesized source with references in one call).
