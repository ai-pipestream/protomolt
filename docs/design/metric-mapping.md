# Metric mappings and the Cube comparison

Status: design record, 2026-08-17. Rewritten from the first-draft notes,
which underread both sides: they treated Cube as "a semantic layer plus
out-of-scope chrome" and ProtoMolt as "a toolkit missing a query." The
grounded position is different. ProtoMolt already carries most of what
Cube's platform does, several pieces more rigorously; Cube's real edge is
the analytics half (aggregate compiler, pre-aggregations, BI surfaces).
That half is deliberately sequenced **after search lands fully**. Nothing
in this note is implemented; feature guides belong under `docs/search/`
once code exists.

## The playing field

ProtoMolt's product goal, stated as the frame for every comparison here:

- **Separated tools.** Every module stands alone; no use case requires
  the full stack. A team that wants schema compatibility checking, or
  masking, or an Iceberg sink, takes that jar and nothing else.
- **Composed, a full text-processing platform.** Parser and datasource
  integrations on the way in, search-engine outputs on the way out, and
  out-of-the-box RAG (chunking, embeddings, hybrid retrieval, rerank) as
  the result.
- **Native gRPC fluency and multiple agentic protocols.** The platform
  understands gRPC as a first-class thing (reflection, descriptor-native
  serving, checked workflows over live services) and meets agents on
  their own protocols: MCP over stdio and streamable HTTP, ACP, and the
  delegation mesh.

Cube's goal is different: govern the query surface over a warehouse
somebody else filled. The two overlap on one strip of ground, the
governed model and its agent-facing surfaces, and that overlap is what
this note maps.

## What Cube is in 2026

Two halves, one brand (the "D3" name was retired October 2025 when
"agentic analytics" went GA under the unified Cube brand):

**Cube Core** (Apache 2.0, self-hosted) is the semantic layer and query
engine. Data model in YAML/JS/Python over warehouse tables: cubes,
views, measures (count, sum, avg, min, max, count-distinct incl.
approximate, filtered, calculated, rolling-window, time-shift,
multi-stage), dimensions with time granularities and custom calendars,
joins with declared relationships, hierarchies, segments, folders.
Access policies combine row-level filters, member allow/deny lists, and
data masking, driven by a JWT security context with programmatic query
rewrite. The performance layer is pre-aggregations (rollup,
original-sql, rollup-join, lambda for streaming) stored in Cube Store, a
refresh worker, and aggregate awareness that matches queries to rollups.
The v1.7 "Tesseract" engine (GA July 2026) added multi-fact views,
multi-stage calculations, and columnar transport. (Their Tesseract is a
SQL-generation engine; it has no relation to Tesseract OCR, which sits
on our parse path below. Keep the names apart.) APIs: REST, GraphQL,
and a Postgres-wire SQL API with `MEASURE()`.

**Cube Cloud** (commercial) is now a full BI and agentic-analytics
product on top: Analytics Chat, workbook/dashboard/semantic-model
agents, agent skills, scheduled agent tasks, rules, memories,
certified queries, evals, bring-your-own-model, MCP connectors for
outbound tool use; workbooks with Python analysis tabs, 13+ chart
types, dashboards-as-code, embedding SDKs and signed iframes with
Creator Mode; DAX/MDX APIs and Excel/Sheets add-ins for the Microsoft
estate; SSO/SCIM, audit, usage analytics; per-developer seat pricing.
Cube's own MCP server is a Cloud feature gated to paid tiers. The
former Semantic Catalog and the standalone AI API are deprecated,
absorbed into the agentic platform.

Cube does not parse, ingest, index, or store raw facts. It starts where
dbt ends: structured tables in a warehouse. ProtoMolt starts at raw
documents and produces the structured projections. That asymmetry runs
through everything below.

## Vocabulary (ADR-001)

The metric work is a **mapping** (the queryable surface) plus, later, a
**workflow** (optional refresh). Do not introduce semantic layer, cube,
rollup, pre-aggregation, or metric store as product nouns.

| Word | Means here | Does not mean |
|---|---|---|
| mapping subject | Named queryable surface, same idea as the search door's `ServedMapping` | A Cube "view" product object |
| member | A named field on that mapping, with role `DIMENSION` or `MEASURE` | A quality dimension (`quality.v1`) |
| metric query | Request: subject, measures, group-by members, filters, grain, limit | Ad-hoc warehouse SQL |
| grain | Time truncation on a `DATE` member (`DAY`, `WEEK`, `MONTH`, ...) | A Cube time dimension object |
| backend | Physical executor (Lucene or Iceberg/DuckDB) | A second database product |

`measure` and `dimension` appear only as **member roles** on the wire.
Quality's `QualityDimension` stays a per-message CEL score; the two
words do not share a type.

## Where ProtoMolt already stands

The first draft's accounting was wrong in one direction: it granted Cube
the whole agentic column and called it chrome. Grounded against what is
in this repository today, most Cube capabilities have a shipped
counterpart, and the counterparts are frequently stronger because they
hang off the descriptor instead of a parallel YAML model.

### The model itself: ahead

Cube's model is authored YAML beside the warehouse schema. ProtoMolt's
model **is** the source schema: one descriptor drives validation
(`validate.v1` plus a protovalidate reader passing 2872/2872 of the
v1.2.2 conformance suite), quality scoring (weighted CEL dimensions),
metadata (`meta.v1` description, owner, sensitivity, labels; Cube's
`ai_context` analog), masking (remove/redact/encrypt on sensitivity
classes), index shape (hints), projection provenance, JSON Schema,
OpenAPI, MCP tool schemas, structured-generation response formats,
Iceberg table schemas, and Parquet field ids. Changes run through a
Git-backed registry with three-gate registration and transitive
compatibility modes, federated pull-only across meshes, speaking the
Confluent subjects protocol verbatim. Cube's dev-mode branches and
content validation are Cloud tooling; here the equivalent gates are the
registry's write path, available to every consumer of the jar.

Cube's views (consumer facades) map to mapping subjects, descriptor
projections, and synthesized shapes; join-shapes registers a join's
output contract as a real `.proto` with history and compatibility
checks, which is the structural answer to a SQL join's SELECT list
evaporating. What ProtoMolt does not have is Cube's aggregate-member
grammar; that is the gap, treated below.

### Serving surfaces: equivalent by different means

Cube serves REST, GraphQL, and a Postgres wire. ProtoMolt serves gRPC
natively (descriptor-native, reflection on), REST transcoding with
OpenAPI and Swagger UI, JSON Schema rendering, a CLI, and two consoles.
No SQL wire, no GraphQL, no DAX/MDX: deliberate, unchanged. The
Microsoft estate and Postgres-tool ecosystems are Cube's distribution
channels, not ours; external engines reach ProtoMolt data through the
Iceberg tables it writes, which any warehouse or DuckDB/Trino/Spark
can read without ProtoMolt in the path.

### The agentic surface: ahead on substance, different on packaging

This is where the first draft was most wrong. Cube's agentic platform
is Cloud-gated and chat-first; its MCP server is a paid-tier feature.
ProtoMolt's agent surface is core, free, and protocol-plural:

- A self-describing action catalog: 17 descriptor-only defaults, 31 in
  the stdio MCP binary, 41 in the full catalog, plus host-contributed
  verbs (delegation, mesh, federation, door replay, connector pulls) to
  roughly sixty on a composed platform. Every verb declares name,
  tool-grade description, and JSON Schema input, so the catalog **is**
  the MCP manifest with no translation layer. Stable kebab-case error
  codes are distinct per repair strategy, so a model knows whether to
  fix its input or change its plan.
- MCP resources (workspace identity with a fingerprinted tool list,
  registry subjects, service profiles, delegation transcripts) that
  agents read without spending tool calls.
- The **workflow workbench**, which is the honest counter to Analytics
  Chat and certified queries combined. There is no bundled chat UI and
  that is the point: the chat is whatever MCP/ACP client the user
  already lives in. An agent registers a service, inspects it, gets
  mapping suggestions, statically checks a workflow (every method and
  type resolves, mappings and CEL type-check, projections and
  validation rules match), compiles it, records a run, replays it
  offline, and promotes it as an immutable version in the Git registry.
  Requests are proto-validated before any provider is invoked; replies
  are validated before they bind downstream (`TypedEdge.validate`,
  `WorkflowStep.validate_response`); structured generation renders a
  strict JSON Schema from the target descriptor and uses rendered
  validation feedback for bounded repair. Promoted workflows are the
  "certified query" analog with provenance; offline replay over
  sensitivity-masked, content-addressed evidence is the "evals" analog,
  except it verifies hashes and recorded validation verdicts rather
  than scoring answer text.
- The ACP agent is literally a chat-window REPL over the catalog for
  ACP clients, and the delegation mesh plus task console cover the
  multi-agent coordination Cube schedules as "agent tasks."

What Cube's agent suite has that ProtoMolt does not: tenant/user agent
memories, markdown behavior rules, BYO-LLM routing as a product
feature, and above all agents that can answer **aggregate** questions.
The last one is the analytics gap, not an agentic gap.

### The substrate: not comparable, and ours

Cube has 28 warehouse drivers and no ingest. ProtoMolt ingests (S3,
JDBC, Microsoft Graph, Confluence, Kafka Connect, stream connectors
through the intake door), parses (coordinator plus plugin SPI plus the
streaming gRParse adapter; the gRParse service behind it speaks a
docling-shaped contract with selectable OCR engines including
Tesseract, PDF backends, table-structure extraction, and a VLM
pipeline, which is the docling-parity parsing story), transforms (mapping, masking, projections, joins,
quality), indexes (Lucene, OpenSearch, Solr renderers), retrieves
(search door: lexical, vector, hybrid lanes with chunk identity and
membership-gated refusals), and lands lake tables (descriptor-driven
Iceberg, Hadoop-free, with Parquet column metrics for external
engines). The same message can be parsed, validated, projected,
indexed, appended to Iceberg, and retrieved. Cube cannot see any of
this layer; it is the part of the platform that exists whether or not
the metric work ever lands, and it is why the comparison is one strip
of overlap rather than product-versus-product.

### The honest ledger

| Cube capability | ProtoMolt today | Verdict |
|---|---|---|
| Code-first governed model | Descriptor + Git registry + compatibility gates | Ahead |
| Views / consumer facades | Mapping subjects, projections, synthesized shapes | Match |
| Member metadata / `ai_context` | `meta.v1` + `extract-metadata` + `render-prompt` | Match |
| Masking / member security | `mask-message` on sensitivity classes | Match (authorization scopes landed; the caller-derived policy rewrite is not built) |
| Row-level derived values | CEL mapping, quality CEL, projection CEL | Match, not aggregate-aware |
| Measures / dimensions / group-by compiler | Nothing computes a measure | **Gap** |
| Time grain queries | `DATE` hints exist; no grain execution | **Gap** |
| Pre-aggregations / Cube Store / aggregate awareness | Parquet column metrics are file-skipping stats only | **Gap** (mostly stays out of scope) |
| REST / metadata APIs | REST gateway, OpenAPI, JSON Schema, `list-types` | Match |
| SQL / GraphQL / DAX / MDX wires | None | Deliberate non-goal |
| MCP | Core and free, 41+ verbs, resources, stdio + HTTP | Ahead (theirs is paid Cloud) |
| Analytics Chat / certified queries / evals | Workbench: check, record, replay, promote with proto-validated requests and replies | Ahead on rigor; no aggregate answers until the gap closes |
| Agent memories, rules, BYO-LLM product | Inference module has provider config; no memory/rules product | Behind (low priority) |
| Workbooks, dashboards, charts, embeds | Search console (8096) and registry console only | Behind, out of scope |
| Row-level security with JWT query rewrite | Authorization scopes landed with a caller model; the compile-time rewrite is not built | Behind (planned, v1.1 here) |
| Ingest / parse / RAG / lake write | The whole acquire-to-sink platform | Ours alone |

One asterisk on our own column: the door's `validate.v1` annotations on
`search.v1` are machine-readable contract, but the validating server
interceptors are not yet installed in any production server; live
enforcement is the door's hand-written refusals. Closing that is part
of landing search, not part of the metric work.

## The gap, precisely

Aggregation. `facetable` hints are honored all the way into the index
(SortedSet/SortedNumeric doc values, exercised by the SEO schemas), and
the Iceberg sink stamps column metrics, but nothing reads any of it
back as an answer: no faceting collector runs at query time,
`SearchRequest` has no group-by, filter, or aggregation field, and
`describe-mapping` / `query-metrics` appear in zero Java files. A
storage layer that is aggregation-ready and a query surface that cannot
sum: that is the whole gap, and it is exactly shaped like the thin v1
below.

## Sequencing: search lands first

Decision of record: the metric work does not start until the search
surface is finished. "Finished" means the open door items in
[planned-work](planned-work.md), at minimum: typed `SearchHit.stored`
values (aggregating over stringified values is how metric layers rot),
validating interceptors installed so `validate.v1` is enforced where it
is declared, coordinator-side body derivation, and retrieval evidence.
Those items make the door trustworthy; the metric layer then reuses its
nouns (subjects, refusal voice, mount pattern) and its hardened store.
Starting metrics before that just builds a second door with the same
unfinished edges.

## Decisions of record

1. **Stay in-grain.** Descriptor options, a typed query, subject-gated
   refusals, catalog verbs. No Postgres wire, no GraphQL, no DAX, no
   chart builder, no bundled chat UI: agents get aggregates through the
   same catalog and protocols they already use for everything else.
2. **One mapping, engines behind an interface.** The mapping is the
   contract; execution is a `MetricExecutor` SPI, and **Lucene is the
   shipped default executor** (it reads doc values the door already
   writes). The subject's mount chooses the engine. A query only names
   a backend to disambiguate: on a single-engine mount an unset backend
   means the mount's engine, which is configuration, not a guess; on a
   multi-engine mount an unset backend is refused naming the mounted
   engines. A backend the subject was not mounted with is refused. The
   response's `physical_plan` always says what actually ran.
3. **Refuse, never guess.** Unknown subject, unknown member, grain on a
   non-`DATE` member, measure used as group-by, empty measures, `limit`
   over the bound: refused by name with the legal set, same voice as
   [the search door](../search/door.md).
4. **Do not overload `SearchService`.** Retrieval stays `Search` /
   `IndexDocument`. Aggregation is a sibling service and sibling
   actions. Hybrid "search then aggregate the hits" is a later
   composition, not v1.
5. **Do not extend `FieldIndexHint` with aggregates.** Indexing says
   how a field is stored. Aggregation says how it is reduced. Sibling
   options, same FieldOptions pattern. A measure field will usually
   also carry an index hint (`INT64` + `sortable`); that is composition,
   not one option doing two jobs.
6. **Calculated measures are CEL over other members**, not over raw
   rows. `paying / total` is in-grain (quality already evaluates CEL on
   descriptors). Window functions, rankings, time-shifts, and Cube's
   multi-stage measures are out of v1.
7. **No metric joins in v1.** One message type per subject. Fan-out
   (`orders join line_items`) is how Cube metrics historically went
   wrong, which is why Tesseract needed multi-fact views; skip it until
   a declared grain exists. Use an authored or synthesized projection
   as the subject if two sources must already be one row.
8. **Row/member security is v1.1, not a blocker for the compiler.**
   Sensitivity metadata is already on the field. Compile-time rewrite
   (drop members, inject filters from a caller context) needs the
   planned authorization scopes. v1 may refuse a member whose
   sensitivity is above a mount-time ceiling; it does not invent a
   policy language.
9. **Physical SQL is evidence, not an API.** A response may carry the
   Lucene collector plan or the DuckDB statement so a human or agent
   can see what ran. Clients do not submit SQL.
10. **Separation survives the feature.** The metric modules follow the
    platform rule: option reader, SPI, each backend, and the service
    are separate artifacts; `describe-mapping` must work with no index
    and no table on the classpath.

## Thin v1

### Option dialect

New package `ai.pipestream.proto.metric.v1`, living next to the other
descriptor-option standards (proposed extension ids `59100541` field,
`59100542` message; confirm unused at implementation time).

```protobuf
syntax = "proto3";

package ai.pipestream.proto.metric.v1;

import "google/protobuf/descriptor.proto";

// Role of this field on a metric mapping. UNSPECIFIED is a schema error
// when the option is present: the option is an explicit declaration.
enum MemberRole {
  MEMBER_ROLE_UNSPECIFIED = 0;
  MEMBER_ROLE_DIMENSION = 1;
  MEMBER_ROLE_MEASURE = 2;
}

enum Aggregate {
  AGGREGATE_UNSPECIFIED = 0;
  AGGREGATE_COUNT = 1;
  AGGREGATE_SUM = 2;
  AGGREGATE_AVG = 3;
  AGGREGATE_MIN = 4;
  AGGREGATE_MAX = 5;
  AGGREGATE_COUNT_DISTINCT = 6;
}

enum TimeGrain {
  TIME_GRAIN_UNSPECIFIED = 0;
  TIME_GRAIN_DAY = 1;
  TIME_GRAIN_WEEK = 2;
  TIME_GRAIN_MONTH = 3;
  TIME_GRAIN_QUARTER = 4;
  TIME_GRAIN_YEAR = 5;
}

message FieldMetric {
  MemberRole role = 1;
  // Required when role is MEASURE and cel is empty. Forbidden on DIMENSION.
  Aggregate aggregate = 2;
  // Public member name; empty = proto field name.
  string name = 3;
  // Row filter for this measure only, CEL over `this` (the message),
  // must be bool. Empty = all rows. Forbidden on DIMENSION.
  string filter_cel = 4;
  // Calculated measure: CEL over sibling member names (not raw fields).
  // When set, aggregate and filter_cel are forbidden. Role must be MEASURE.
  string cel = 5;
  // Default grain when this DATE dimension is selected without one.
  TimeGrain default_grain = 6;
}

message MessageMetric {
  // Default subject name when a host serves this type without an override.
  string subject = 1;
  // Identity field used to count rows / dedupe; empty = no default COUNT(*).
  string identity_field = 2;
}

extend google.protobuf.FieldOptions {
  FieldMetric metric = 59100541;
}

extend google.protobuf.MessageOptions {
  MessageMetric metric = 59100542;
}
```

Worked example (not a shipped schema):

```protobuf
import "ai/pipestream/proto/index/hints/v1/indexing_hints.proto";
import "ai/pipestream/proto/metric/v1/metric.proto";
import "ai/pipestream/proto/meta/v1/metadata.proto";
import "google/protobuf/timestamp.proto";

message Order {
  option (ai.pipestream.proto.metric.v1.metric) = {
    subject: "orders"
    identity_field: "id"
  };

  string id = 1 [
    (ai.pipestream.proto.index.hints.v1.index) = { type: INDEX_FIELD_TYPE_KEYWORD }
  ];
  string segment = 2 [
    (ai.pipestream.proto.index.hints.v1.index) = {
      type: INDEX_FIELD_TYPE_KEYWORD
      facetable: true
    },
    (ai.pipestream.proto.metric.v1.metric) = { role: MEMBER_ROLE_DIMENSION },
    (ai.pipestream.proto.meta.v1.field) = { description: "Sales segment" }
  ];
  google.protobuf.Timestamp created_at = 3 [
    (ai.pipestream.proto.index.hints.v1.index) = { type: INDEX_FIELD_TYPE_DATE },
    (ai.pipestream.proto.metric.v1.metric) = {
      role: MEMBER_ROLE_DIMENSION
      default_grain: TIME_GRAIN_MONTH
    }
  ];
  int64 amount_cents = 4 [
    (ai.pipestream.proto.index.hints.v1.index) = {
      type: INDEX_FIELD_TYPE_INT64
      sortable: true
    },
    (ai.pipestream.proto.metric.v1.metric) = {
      role: MEMBER_ROLE_MEASURE
      aggregate: AGGREGATE_SUM
      name: "revenue"
    }
  ];
  bool paying = 5 [
    (ai.pipestream.proto.index.hints.v1.index) = { type: INDEX_FIELD_TYPE_BOOLEAN }
  ];
}
```

A filtered or calculated measure that is not a physical field is a
**synthetic member** declared on the message option, not a phantom
field. v1 can start without synthetics and add them as a repeated
`MessageMetric.members` list if the first implementation needs
`paying_count` without a dedicated proto field. Prefer a real field
when the value already exists.

Schema errors (fail the mapping build, not the first query):

- option present with `role` unset
- `DIMENSION` with `aggregate`, `filter_cel`, or `cel`
- `MEASURE` with neither `aggregate` nor `cel`
- `MEASURE` with both `cel` and (`aggregate` or `filter_cel`)
- `default_grain` on a non-`DATE` field
- `cel` that does not type-check against sibling member names
- `filter_cel` that is not bool
- `name` colliding with another member on the same message
- `COUNT_DISTINCT` or `AVG` on a non-numeric / non-keyword type where
  the backend cannot execute it

Programmatic side-car, same as indexing: a `MetricHintSource` SPI with
`ProtoOptionsMetricHintSource` and `CatalogMetricHintSource`, so schemas
you cannot annotate still serve.

### Query contract

New package `ai.pipestream.proto.metric.v1` (service file, may live in
`search/metrics/proto` the way `search.v1` lives in `search/door/proto`).

```protobuf
enum MetricBackend {
  // Unset = the mount's engine on a single-engine mount; refused with
  // the mounted set on a multi-engine mount. Never a silent pick.
  METRIC_BACKEND_UNSPECIFIED = 0;
  METRIC_BACKEND_LUCENE = 1;
  METRIC_BACKEND_ICEBERG = 2;
}

message MemberRef {
  string name = 1;           // required, mapping member name
  TimeGrain grain = 2;       // DATE dimensions only
}

message MetricFilter {
  string member = 1;
  // Equality set. Empty values refused. RANGE / CEL filters are v1.1.
  repeated string equals = 2;
}

message QueryMetricsRequest {
  string mapping_subject = 1;          // required
  // Explicit engine selector. Unset on a single-engine mount means the
  // mount's engine (configuration, not a guess). Unset on a multi-engine
  // mount is refused naming the mounted engines. A backend the subject
  // was not mounted with is refused.
  MetricBackend backend = 2;
  repeated string measures = 3;        // required, min 1
  repeated MemberRef dimensions = 4;   // group-by, may be empty
  repeated MetricFilter filters = 5;
  int32 limit = 6;                     // required, 1..1000
}

message MetricRow {
  map<string, string> dimensions = 1;  // member name -> rendered value
  map<string, double> measures = 2;    // member name -> number
}

message QueryMetricsResponse {
  string mapping_subject = 1;
  MetricBackend backend = 2;
  repeated MetricRow rows = 3;
  int32 row_count = 4;
  // Human/agent evidence. Never a source of truth for a later query.
  string physical_plan = 5;
}

message DescribeMappingRequest {
  string mapping_subject = 1;          // required
}

message MappingMember {
  string name = 1;
  MemberRole role = 2;
  Aggregate aggregate = 3;             // MEASURE only
  string field_path = 4;               // proto path; empty if synthetic
  string description = 5;              // from meta.v1 when present
  string sensitivity = 6;              // from meta.v1 when present
  TimeGrain default_grain = 7;
}

message DescribeMappingResponse {
  string mapping_subject = 1;
  string message_type = 2;
  repeated MappingMember members = 3;
  repeated MetricBackend backends = 4; // what this mount can run
}
```

The request messages carry `validate.v1` annotations from day one, and
the metric service mounts the validating interceptor from day one; the
door's annotate-now-enforce-later split is a debt this surface does not
inherit.

`ListSubjects` already exists on the search door. Metric mounts either
reuse it (same subject names) or add `ListMetricSubjects` if the served
sets diverge. Prefer reuse: a subject that is indexed and metric-mapped
is one name.

gRPC service (sibling, not a new RPC on `SearchService`):

```protobuf
service MetricService {
  rpc DescribeMapping(DescribeMappingRequest) returns (DescribeMappingResponse);
  rpc QueryMetrics(QueryMetricsRequest) returns (QueryMetricsResponse);
}
```

### Actions and MCP

Two verbs, same envelope as the rest of the catalog. They become MCP
tools with no translation layer, which also means they land in every
agent surface at once: stdio MCP, streamable HTTP, ACP, CLI, gRPC,
REST. This is the moment the workbench chat can answer an aggregate
question, and it costs zero chat-specific code.

| Action | Does |
|---|---|
| `describe-mapping` | Members, roles, descriptions, sensitivity, backends for one subject |
| `query-metrics` | Run a `QueryMetricsRequest` (proto3 JSON) and return rows plus plan |

Refuse with stable kebab-case codes: `unknown-subject`, `unknown-member`,
`unknown-backend`, `ambiguous-backend` (unset on a multi-engine mount),
`unsupported-aggregate` (executor capability), `invalid-grain`,
`invalid-limit`, `empty-measures`, `role-mismatch`. The legal set goes
in `details`.

### Executor SPI

Execution sits behind one interface in `protomolt-metric-spi`, loaded
via ServiceLoader the same way chunker, embeddings, and rerank
providers are. The SPI owns mapping build, member resolution, and every
schema and query refusal; an executor receives a **compiled,
already-validated** query and returns rows plus its physical plan:

- `MetricExecutor.capabilities()` declares what the engine can run
  (which aggregates, whether `COUNT_DISTINCT` stays bounded, grain
  support). A query needing a capability the mounted executor lacks is
  refused by name with the executor's legal set; capabilities differ
  per engine and are never flattened to a common denominator.
- `MetricExecutor.execute(CompiledMetricQuery)` runs the reduction.
  Executors never see raw request JSON and never make policy choices.

Because everything is gRPC, the SPI seam can be crossed by a wire: an
executor implementation that is a gRPC client pointed at a remote
metric node is indistinguishable from the in-process one, the same move
parse made with `ParserPluginService` and embeddings made with TEI. A
`metrics` role node then slots into the existing role pattern
(`PROTOMOLT_ROLES`, `PROTOMOLT_METRICS_TARGET`). Swapping or scaling
the analytics engine never touches the contract.

### Execution

**Lucene (interactive, document-native, the shipped default).** Requires the subject already
indexed by the search door. Group-by members must be `facetable` (or
`sortable` for single-valued numerics); the doc values are already
written today, so this backend is a read path over existing storage.
`COUNT` is document count in the filter. `SUM`/`AVG`/`MIN`/`MAX` need
numeric doc values. `COUNT_DISTINCT` is v1 only if a collector exists
that stays bounded; otherwise refuse that aggregate on the Lucene
backend by name.

**Iceberg + DuckDB (lake-native).** Requires a table the Iceberg sink
already wrote from the same descriptor. Compiler emits a single `SELECT
... GROUP BY` against that table. Partition identity / day fields should
line up with group-by members so the existing column metrics do their
file-skipping work. DuckDB is an in-process reader over the table's
files, not a warehouse product we operate. Trino/Spark stay external
consumers of the same table; they are not v1 backends.

A subject may mount one or both backends. On a single-engine mount an
unset backend resolves to that engine; on a multi-engine mount it is a
failed precondition naming the mounted set, not "pick whichever
exists."

`physical_plan` for Lucene is the collector description; for Iceberg it
is the DuckDB SQL. Both are evidence.

### Mounting

Follow the search door: a host lists metric subjects at boot
(`ServedMetricMapping`: subject, message type, backends, optional
Iceberg table identifier). Unknown configuration fails the mount, not
the first query. The document platform is the first host that should
grow a `metrics` role; `apps/serve` does not need it for v1.

No durable refresh workflow in v1. Lucene is already NRT from
`IndexDocument`. Iceberg is already snapshot-append. A later workflow
that rebuilds a declared aggregate table is a **workflow**, and only
when a subject is too large for on-the-fly GROUP BY. That later
workflow, not Cube Store, is this platform's answer to
pre-aggregations: declared, durable, evidenced, and optional.

### Index snapshots to S3

A door/store feature that metrics inherits, not a metric module. Per
the separation rule it lands with the search store and is useful with
no metric code on the classpath.

- **The repository stays the source of truth; a snapshot is a cache.**
  `replay-documents` already rebuilds any subject's index from the
  repo. Restore from S3 when a snapshot exists; fall through to replay
  when it does not, or when it is stale or fails verification. Losing
  the bucket loses time, never data.
- **Snapshot at commit points, never a live S3 directory.** The store
  already batches durability into explicit commits. A commit is an
  immutable set of segment files: hold the commit point open
  (`SnapshotDeletionPolicy`), upload files the bucket does not already
  have, and write the new `segments_N` last as the atomic marker that
  the snapshot exists. Segment immutability makes uploads incremental
  for free. Running Lucene's `Directory` live over object storage is
  refused as a design: S3 is not a filesystem, and locking over it is
  unsafe.
- **Identity keys the snapshot to what produced it**:
  `{subject}/{mapping-digest}/{policy-digest}/...`. Change the mapping
  or the chunking policy and the old snapshot is automatically not
  yours to restore; the mount falls through to replay instead of
  serving a stale shape. No assumed compatibility.
- **Session lifecycle**: a mount may declare an S3 location; it
  restores on boot and snapshots on the commit cadence and on close.
- **This is what makes the remote metrics role cheap.** The indexing
  node commits and uploads; analytics nodes restore the snapshot and
  serve `QueryMetrics` read-only, refreshing on the next snapshot. A
  reader/writer split over object storage with no cluster protocol,
  behind the same `MetricExecutor` interface. Freshness is the
  snapshot cadence, and `physical_plan` plus the snapshot id say what
  was served.

## Out of scope (do not sneak in)

- Cube Cloud BI surfaces: workbooks, dashboards, chart types, Slack,
  Excel, Sheets, embedded iframes, Creator Mode. A bundled chat UI
  stays out for the same reason: our chat is the client the user
  already has.
- Postgres wire / Semantic SQL / GraphQL / DAX / MDX
- Cube Store, aggregate awareness, refresh workers, lambda pre-aggs
- Multi-fact views, join paths, fan-out grain, multi-stage measures
- Window functions, running totals, time-shift measures
- Ad-hoc calculated members in the query (only schema-declared `cel`)
- Caller-submitted SQL
- Agent memories, behavior rules, BYO-LLM routing as product features
- Row-level security language and JWT query rewrite (v1.1, after
  authorization scopes)
- RANGE / CEL query filters (v1.1; v1 is equality sets)
- Mixing retrieval hits into an aggregate in one RPC
- Default subject, default limit, and any backend guess on a
  multi-engine mount (a single-engine mount resolving an unset backend
  is configuration, not a default)

## Suggested module layout

Sketch for the scoping pass, not a commitment. Each artifact stands
alone per the platform rule; none requires the others at runtime.

| Path | Artifact | Role |
|---|---|---|
| `protobuf/metric/` or `search/metrics/options/` | `protomolt-protobuf-metric` | Option proto + reader, sibling to `protobuf-metadata` |
| `search/metrics/spi/` | `protomolt-metric-spi` | Member resolution, `MetricHintSource`, mapping build, schema errors, `MetricExecutor` SPI |
| `search/metrics/lucene/` | `protomolt-metric-lucene` | Collector backend |
| `search/metrics/iceberg/` | `protomolt-metric-iceberg` | DuckDB/Iceberg backend |
| `search/metrics/door/` or next to `search/door/` | `protomolt-metric-door` | `MetricService`, subject mount, refusals |
| actions registered by the door module | `describe-mapping`, `query-metrics` | Catalog + MCP |

Keep option reading independent of any backend so `describe-mapping`
works in unit tests with no index and no table.

## Acceptance for v1

A later implementation is done when all of the following hold:

1. A descriptor with valid `metric.v1` options builds a mapping; each
   schema error in the list above fails the build with the field path.
2. `DescribeMapping` returns exactly the declared members and the
   mounted backends; `meta.v1` description and sensitivity flow through.
3. `QueryMetrics` on Lucene over a search-door subject returns the same
   `SUM` / `COUNT` / group-by as a hand-checked fixture, including a
   filtered measure (`filter_cel`).
4. `QueryMetrics` on Iceberg/DuckDB over a table written by
   `IcebergSink` from the same descriptor returns the same numbers.
5. Unknown subject, unknown member, grain on a keyword, empty measures,
   `limit` 0 or 1001, a backend the subject was not mounted with, an
   unset backend on a multi-engine mount, and an aggregate the mounted
   executor's capabilities exclude: each refused by name with the
   legal set. An unset backend on a single-engine mount executes on the
   mount's engine. Covered by tests, not comments.
6. The validating interceptor is mounted on `MetricService` and the
   `validate.v1` annotations on the request messages are what enforces
   the shape rules, with the hand-written refusals covering only what
   annotations cannot express (membership, role checks).
7. `describe-mapping` and `query-metrics` appear in the action
   inventory and answer on the MCP catalog when the door module is
   mounted.
8. No new noun appears in user-facing strings except mapping, member,
   measure/dimension as roles, grain, backend, and the two action names.

## Open questions for the scoping pass

These are the only product choices left. Do not reopen the out-of-scope
list to answer them.

1. **Synthetic members in v1 or v1.1?** Needed for `paying_count` and
   `paying_percentage` without extra proto fields. If v1, add
   `repeated FieldMetric members` on `MessageMetric`.
2. **Where does `MetricService` live?** Own module vs. second service
   inside `protomolt-search-door`. Own module keeps retrieval and
   aggregation independently mountable; the door already owns subjects.
3. **DuckDB dependency.** Acceptable in `protomolt-metric-iceberg` if
   it stays Hadoop-free and isolated the way the Iceberg module already
   tests. Alternative: generate SQL only and require a caller-supplied
   `SqlBackend` SPI, with DuckDB as one implementation.
4. **Result types.** v1 uses `map<string, double>` for measures.
   `COUNT` that exceeds a double's integer exactness, or money that
   must stay integer cents, may want `google.protobuf.Value` or a
   typed cell. The door's typed-`stored` work should settle the cell
   representation first; decide before the proto freezes.
5. **Shared subjects with the search door.** Same name and same
   descriptor, or a parallel metric-subject list? Same name is simpler
   for `parse-and-index` then `query-metrics`.
6. **`COUNT(*)` without a measure field.** `MessageMetric.identity_field`
   plus a reserved member name (`count`) vs. requiring an explicit
   measure field. Prefer explicit: a field or a synthetic member named
   `count` with `AGGREGATE_COUNT`.
7. **Extension id confirmation** at implementation: `59100541` /
   `59100542` must still be free.

## What not to write in the first PR

Do not land a console page, a default demo cube, a SQL proxy, or a
"semantic layer" README section. Land the option proto, the SPI, one
backend, the two actions, and the refusal tests. The document platform
can mount it in a follow-up the way it mounted the search door.
