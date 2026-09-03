# Metric mappings and the Cube comparison

Status: design of record. The framing it argues is unchanged: ProtoMolt
carries most of what Cube's platform does, several pieces more
rigorously, and the analytics half is where Cube led.

The v1 described here **is implemented**, and further than the sketch:
the option dialect, the query contract, the SPI, both executors, the
three catalog verbs, declared rollups with rebuild-time enrichment, and
the `metric` role all ship. What still separates the two is the BI
surface and automatic aggregate awareness, both deliberate non-goals.
Every section below carries its own state.

| Section | State |
|---|---|
| The playing field, What Cube is in 2026, Vocabulary | Position; unchanged |
| Where ProtoMolt already stands | Comparison, refreshed against shipped code |
| The gap, precisely | Closed; kept as the record of what the work answered |
| Sequencing: search lands first | Honored; history |
| Decisions of record | In force; the no-joins decision superseded and marked as such |
| Thin v1 (options, query contract, actions, SPI, execution, mounting, snapshots) | Shipped, with the deltas named per section |
| Out of scope | In force, minus the joins line that [metric joins](metric-joins.md) answered |
| Module layout | Shipped; the table is the built layout |
| Acceptance for v1 | Met |
| Open questions for the scoping pass | Settled; answers recorded inline |

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

The metric work is a **mapping** (the queryable surface) plus a
**workflow** (optional rebuild). Do not introduce semantic layer, cube,
pre-aggregation, or metric store as product nouns. **Rollup** is the
one exception this list originally refused: it is the shipped name for
a declared, durably materialized aggregate table, and it appears on
the wire (`RebuildRollup`, `rollup:<table>`). Nothing else joins it.

| Word | Means here | Does not mean |
|---|---|---|
| mapping subject | Named queryable surface, same idea as the search service's `ServedMapping` | A Cube "view" product object |
| member | A named field on that mapping, with role `DIMENSION` or `MEASURE` | A quality dimension (`quality.v1`) |
| metric query | Request: subject, measures, group-by members, filters, grain, limit | Ad-hoc warehouse SQL |
| grain | Time truncation on a `DATE` member (`DAY`, `WEEK`, `MONTH`, ...) | A Cube time dimension object |
| backend | Physical executor (Lucene or Iceberg/DuckDB) | A second database product |
| rollup | A lake table an explicit rebuild replaced with a complete aggregate | A Cube pre-aggregation matched to queries automatically |

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
evaporating. The aggregate-member grammar this note treated as the gap
is now `metric.v1` on the same descriptors.

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

- A self-describing action catalog: descriptor-only defaults, a larger
  standalone MCP catalog, the full catalog
  ([counts](../generated/action-inventory.json)), plus host-contributed
  verbs (delegation, mesh, metrics, federation, search replay, connector
  pulls) on a composed platform. Every verb declares name,
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
memories, markdown behavior rules, and BYO-LLM routing as a product
feature. Aggregate answers were the fourth item on that list until
`describe-mapping`, `query-metrics`, and `rebuild-rollup` joined the
catalog; an agent on any of these protocols now asks aggregate
questions with no chat-specific code.

### The substrate: not comparable, and ours

Cube has 28 warehouse drivers and no ingest. ProtoMolt ingests (S3,
JDBC, Microsoft Graph, Confluence, Kafka Connect, stream connectors
through the intake service), parses (coordinator plus plugin SPI plus the
streaming gRParse adapter; the gRParse service behind it speaks a
docling-shaped contract with selectable OCR engines including
Tesseract, PDF backends, table-structure extraction, and a VLM
pipeline, which is the docling-parity parsing story), transforms (mapping, masking, projections, joins,
quality), indexes (Lucene, OpenSearch, Solr renderers), retrieves
(search service: lexical, vector, hybrid lanes with chunk identity and
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
| Row-level derived values | CEL mapping, quality CEL, projection CEL | Match |
| Measures / dimensions / group-by compiler | `metric.v1` members, the SPI's compiler, `QueryMetrics` on Lucene and Iceberg/DuckDB | Match |
| Time grain queries | `TimeGrain` on `DATE` dimensions, executed by both engines | Match |
| Pre-aggregations / Cube Store / aggregate awareness | Declared rollups: `rebuild-rollup` writes a complete aggregate to a lake table, served back as `rollup:<table>` | Partial by design (declared and durable; no automatic query rewriting onto rollups, and no refresh worker) |
| REST / metadata APIs | REST gateway, OpenAPI, JSON Schema, `list-types` | Match |
| SQL / GraphQL / DAX / MDX wires | None | Deliberate non-goal |
| MCP | Core and free, resources, stdio + HTTP | Ahead (theirs is paid Cloud) |
| Analytics Chat / certified queries / evals | Workbench: check, record, replay, promote with proto-validated requests and replies, over a catalog that now aggregates | Ahead on rigor |
| Agent memories, rules, BYO-LLM product | Inference module has provider config; no memory/rules product | Behind (low priority) |
| Workbooks, dashboards, charts, embeds | Search console (8096) and registry console only | Behind, out of scope |
| Row-level security with JWT query rewrite | The access policy's `metric_access` rewrites at compile time: denied members and injected row filters per principal, fail-closed rollups | Match ([authorization scopes](authorization-scopes.md)) |
| Ingest / parse / RAG / lake write | The whole acquire-to-sink platform | Ours alone |

## The gap, precisely (closed)

The gap this note was written around: `facetable` hints reached the
index (SortedSet/SortedNumeric doc values) and the Iceberg sink stamped
column metrics, but nothing read any of it back as an answer. A storage
layer that was aggregation-ready under a query surface that could not
sum.

The metric service closes it. `SearchRequest` still has no group-by,
filter, or aggregation field, by decision 4 below: aggregation is a
sibling service reading the same doc values, not an overload of
retrieval.

## Sequencing: search lands first (honored)

Decision of record, kept as the record of why the metric work waited:
it did not start until the search surface carried typed
`SearchHit.stored` values (aggregating over stringified values is how
metric layers rot) and had the validating interceptor installed so
`validate.v1` is enforced where it is declared. Both landed first, and
the metric layer reuses the search service's nouns (subjects, refusal
voice, mount pattern) and its store. Retrieval evidence remains open in
[planned-work](planned-work.md); it is a search-service item, not a
metric prerequisite.

## Decisions of record

1. **Stay in-grain.** Descriptor options, a typed query, subject-gated
   refusals, catalog verbs. No Postgres wire, no GraphQL, no DAX, no
   chart builder, no bundled chat UI: agents get aggregates through the
   same catalog and protocols they already use for everything else.
2. **One mapping, engines behind an interface.** The mapping is the
   contract; execution is a `MetricExecutor` SPI, and **Lucene is the
   shipped default executor** (it reads doc values the search service already
   writes). The subject's mount chooses the engine. A query only names
   a backend to disambiguate: on a single-engine mount an unset backend
   means the mount's engine, which is configuration, not a guess; on a
   multi-engine mount an unset backend is refused naming the mounted
   engines. A backend the subject was not mounted with is refused. The
   response's `physical_plan` always says what actually ran.
3. **Refuse, never guess.** Unknown subject, unknown member, grain on a
   non-`DATE` member, measure used as group-by, empty measures, `limit`
   over the bound: refused by name with the legal set, same voice as
   [the search service](../search/service.md).
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
7. **No joins at query time.** One message type per subject. Fan-out
   (`orders join line_items`) is how Cube metrics historically went
   wrong, which is why Tesseract needed multi-fact views. Superseding
   the original "no metric joins in v1": joins land at **rebuild**
   time, as `RollupEnrichment` on `rebuild-rollup`, strictly
   one-to-at-most-one and refused on fan-out. The query surface stays
   single-subject. See [metric joins](metric-joins.md). An authored or
   synthesized projection is still the answer when two sources must
   already be one row before aggregation.
8. **Row/member security is the access policy's, not a query
   feature.** The policy document's `metric_access` section declares
   per-principal denied members and injected equality row filters (the
   metric layer's own filter shape, no new policy language); the metric
   service rewrites at compile time from the resolved caller, with
   rollup subjects and the rebuild verb fail-closed for restricted
   principals. See [authorization scopes](authorization-scopes.md).
9. **Physical SQL is evidence, not an API.** A response may carry the
   Lucene collector plan or the DuckDB statement so a human or agent
   can see what ran. Clients do not submit SQL.
10. **Separation survives the feature.** The metric modules follow the
    platform rule: option reader, SPI, each backend, and the service
    are separate artifacts; `describe-mapping` must work with no index
    and no table on the classpath.

## Thin v1 (shipped)

Everything under this heading is built. The sketches below are kept as
the reasoning; where the shipped shape differs, the difference is named
in the section. The proto of record is
`protobuf/metric/src/main/proto/ai/protomolt/proto/metric/v1/metric.proto`
for the options and
`metric/proto/src/main/proto/ai/protomolt/proto/metric/v1/metric_service.proto`
for the service.

### Option dialect

Package `ai.protomolt.proto.metric.v1`, beside the other
descriptor-option standards, on extension ids `59100541` (field) and
`59100542` (message). Two deltas from the sketch: the message extension
is named `metric_message` (a `MessageOptions` extension cannot share
the `metric` name with the `FieldOptions` one), and synthetic members
shipped in v1 as `repeated FieldMetric MessageMetric.members` rather
than waiting for v1.1.

```protobuf
syntax = "proto3";

package ai.protomolt.proto.metric.v1;

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
  // Synthetic measures with no backing field.
  repeated FieldMetric members = 3;
}

extend google.protobuf.FieldOptions {
  FieldMetric metric = 59100541;
}

extend google.protobuf.MessageOptions {
  MessageMetric metric_message = 59100542;
}
```

Worked example (not a shipped schema):

```protobuf
import "ai/protomolt/proto/index/hints/v1/indexing_hints.proto";
import "ai/protomolt/proto/metric/v1/metric.proto";
import "ai/protomolt/proto/meta/v1/metadata.proto";
import "google/protobuf/timestamp.proto";

message Order {
  option (ai.protomolt.proto.metric.v1.metric_message) = {
    subject: "orders"
    identity_field: "id"
  };

  string id = 1 [
    (ai.protomolt.proto.index.hints.v1.index) = { type: INDEX_FIELD_TYPE_KEYWORD }
  ];
  string segment = 2 [
    (ai.protomolt.proto.index.hints.v1.index) = {
      type: INDEX_FIELD_TYPE_KEYWORD
      facetable: true
    },
    (ai.protomolt.proto.metric.v1.metric) = { role: MEMBER_ROLE_DIMENSION },
    (ai.protomolt.proto.meta.v1.field) = { description: "Sales segment" }
  ];
  google.protobuf.Timestamp created_at = 3 [
    (ai.protomolt.proto.index.hints.v1.index) = { type: INDEX_FIELD_TYPE_DATE },
    (ai.protomolt.proto.metric.v1.metric) = {
      role: MEMBER_ROLE_DIMENSION
      default_grain: TIME_GRAIN_MONTH
    }
  ];
  int64 amount_cents = 4 [
    (ai.protomolt.proto.index.hints.v1.index) = {
      type: INDEX_FIELD_TYPE_INT64
      sortable: true
    },
    (ai.protomolt.proto.metric.v1.metric) = {
      role: MEMBER_ROLE_MEASURE
      aggregate: AGGREGATE_SUM
      name: "revenue"
    }
  ];
  bool paying = 5 [
    (ai.protomolt.proto.index.hints.v1.index) = { type: INDEX_FIELD_TYPE_BOOLEAN }
  ];
}
```

A filtered or calculated measure that is not a physical field is a
**synthetic member** declared on the message option, not a phantom
field. These shipped in v1 as `repeated FieldMetric
MessageMetric.members`: each needs an explicit name and the `MEASURE`
role and is either a `COUNT` (optionally row-filtered by `filter_cel`)
or a calculated `cel` over sibling member names. Anything needing
storage stays a field; prefer a real field when the value already
exists.

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

Package `ai.protomolt.proto.metric.v1`, in `metric/proto` the way
`search.v1` lives in `search/proto`. The sketch below is what shipped,
with three additions: `MetricFilter` grew a `DateRange range` and a
`TreePath prefix` beside the equality set (exactly one form per
filter), and `RebuildRollup` joined the service with
`RebuildRollupRequest` / `RollupEnrichment` /
`RebuildRollupResponse`. Read the proto for those.

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
  // Equality set. Empty values refused. Exactly one of these three forms.
  repeated string equals = 2;
  ai.protomolt.proto.types.v1.DateRange range = 3;   // DATE dimensions
  ai.protomolt.proto.types.v1.TreePath prefix = 4;   // TREE_PATH dimensions
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

The request messages carry `validate.v1` annotations and the metric
service mounts the validating interceptor; the search service's
annotate-now-enforce-later split is a debt this surface does not
inherit.

Subjects are shared with the search service by name: the metric role's
subject keys name mapping subjects the co-mounted search role serves,
so `ListSubjects` covers both and no `ListMetricSubjects` exists.
Rollup tables are the one addition, resolved as `rollup:<table>`.

gRPC service (sibling, not a new RPC on `SearchService`):

```protobuf
service MetricService {
  rpc DescribeMapping(DescribeMappingRequest) returns (DescribeMappingResponse);
  rpc QueryMetrics(QueryMetricsRequest) returns (QueryMetricsResponse);
  rpc RebuildRollup(RebuildRollupRequest) returns (RebuildRollupResponse);
}
```

### Actions and MCP

Three verbs, same envelope as the rest of the catalog. They become MCP
tools with no translation layer, which also means they land in every
agent surface at once: stdio MCP, streamable HTTP, ACP, CLI, and the
registry's actions route. This is the moment the workbench chat can
answer an aggregate question, and it costs zero chat-specific code.
They are contributed by the metric service module, so they are not on
the typed gRPC or REST surface; see
[the action surface](../surface/actions.md).

| Action | Scope | Does |
|---|---|---|
| `describe-mapping` | `metrics-query` | Members, roles, descriptions, sensitivity, backends for one subject |
| `query-metrics` | `metrics-query` | Run a `QueryMetricsRequest` (proto3 JSON) and return rows plus plan |
| `rebuild-rollup` | `metrics-rebuild` | Run a complete aggregate and atomically replace a declared lake table |

Refuse with stable kebab-case codes, the legal set in `details`. The
sketch named `unknown-subject`, `unknown-member`, `unknown-backend`,
`ambiguous-backend` (unset on a multi-engine mount),
`unsupported-aggregate` (executor capability), `invalid-grain`,
`invalid-limit`, `empty-measures`, and `role-mismatch`. Shipped
alongside them: `unsupported-filter`, `missing-table`,
`distinct-bound`, `missing-sink`, `rollup-budget`, `join-fanout`, and
`invalid-enrichment`.

### Executor SPI

Execution sits behind one interface in `protomolt-metric-spi`. The SPI
owns mapping build, member resolution, and every schema and query
refusal; an executor receives a **compiled, already-validated** query
and returns rows plus its physical plan. Executors are wired by the
mount rather than discovered by ServiceLoader as this sketch expected:
the metric role builds the Lucene executor over the co-mounted search
role's store itself, and the host passes any other engine in under its
own named backend.

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
parse made with `ParserPluginService` and embeddings made with TEI. No
such client executor is built; the shipped remote node is the whole
`metric` role beside a read-only search role instead (see index
snapshots below). The role slots into the existing pattern
(`PROTOMOLT_ROLES`, `PROTOMOLT_METRIC_TARGET`). Swapping or scaling
the analytics engine never touches the contract.

### Execution

**Lucene (interactive, document-native, the shipped default).** Requires the subject already
indexed by the search service. Group-by members must be `facetable` (or
`sortable` for single-valued numerics); the doc values are already
written today, so this backend is a read path over existing storage.
`COUNT` is document count in the filter. `SUM`/`AVG`/`MIN`/`MAX` need
numeric doc values. `COUNT_DISTINCT` shipped: it counts exactly up to a
per-measure distinct bound and refuses as `distinct-bound` rather than
approximating past it.

**Iceberg + DuckDB (lake-native).** Requires a table the Iceberg sink
already wrote from the same descriptor. Compiler emits a single `SELECT
... GROUP BY` against that table. Partition identity / day fields should
line up with group-by members so the existing column metrics do their
file-skipping work. DuckDB is an in-process reader over the table's
files, not a warehouse product we operate. Trino/Spark stay external
consumers of the same table; they are not backends. `COUNT_DISTINCT`
is supported here because the reduction spills.

A subject may mount one or both backends. On a single-engine mount an
unset backend resolves to that engine; on a multi-engine mount it is a
failed precondition naming the mounted set, not "pick whichever
exists."

`physical_plan` for Lucene is the collector description; for Iceberg it
is the DuckDB SQL. Both are evidence.

### Mounting

Follow the search service: a host lists metric subjects at boot
(`ServedMetricSubject`: the built mapping and the engines mounted for
it, keyed by the search mapping subject they aggregate over). Unknown
configuration fails the mount, not the first query. The document
platform is the host that grew the `metric` role; `apps/serve` does
not mount it. The role refuses to wire without the search role on the
same node, because the executor borrows that role's store in process.

No continuous refresh worker: Lucene is NRT from `IndexDocument` and
Iceberg is snapshot-append. The declared-rollup answer landed as the
`rebuild-rollup` verb and its `MetricWorkflows.rebuildRollupWorkflow`,
for subjects too large for on-the-fly GROUP BY. That workflow, not
Cube Store, is this platform's answer to pre-aggregations: declared,
durable, evidenced, and optional. Rebuilt tables are self-describing
(`rollup:<table>` subjects carrying each column's source aggregate),
so a rollup serves back through the ordinary query path and
re-aggregation over it is only offered where it is honest.

### Index snapshots to S3

A search-store feature that metrics inherits, not a metric module. Per
the separation rule it landed with the search store
(`SnapshotStore`, `IndexSnapshots`, and `protomolt-search-snapshot-s3`)
and is useful with no metric code on the classpath.

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
- Cube Store, aggregate awareness, refresh workers, lambda pre-aggs.
  Declared rollups are the exception this list already anticipated:
  they are written by an explicit verb, never matched to a query
  behind the caller's back.
- Multi-fact views, join paths, fan-out grain, multi-stage measures.
  Rebuild-time enrichment is the answer to the join question and is
  none of these; see [metric joins](metric-joins.md), which keeps all
  four out with reasons.
- Window functions, running totals, time-shift measures
- Ad-hoc calculated members in the query (only schema-declared `cel`)
- Caller-submitted SQL
- Agent memories, behavior rules, BYO-LLM routing as product features
- A row-level policy language beyond the access policy's per-principal
  deny lists and equality row filters (JWT claim mapping stays the
  OIDC resolver's job)
- CEL query filters. Date ranges and tree-path prefixes shipped as
  typed filter forms; arbitrary caller CEL over rows did not.
- Mixing retrieval hits into an aggregate in one RPC
- Default subject, default limit, and any backend guess on a
  multi-engine mount (a single-engine mount resolving an unset backend
  is configuration, not a default)

## Module layout

The built layout. Each artifact stands alone per the platform rule;
none requires the others at runtime.

| Path | Artifact | Role |
|---|---|---|
| `protobuf/metric/` | `protomolt-protobuf-metric` | Option proto, sibling to `protobuf-metadata` |
| `metric/proto/` | `protomolt-metric-proto` | `MetricService` contract and its messages |
| `metric/spi/` | `protomolt-metric-spi` | Member resolution, `MetricHintSource`, mapping build, schema errors, `MetricExecutor` and `RollupSink` SPIs, `MetricSubjectResolver` |
| `metric/lucene/` | `protomolt-metric-lucene` | Collector backend and the `metric` role module |
| `metric/iceberg/` | `protomolt-metric-iceberg` | DuckDB/Iceberg backend and the Iceberg rollup sink |
| `metric/service/` | `protomolt-metric-service` | Service implementation, subject mount, refusals, the catalog verbs |

The role module lives in `metric/lucene` rather than `metric/service`
because it builds the Lucene executor over the co-mounted search
role's store. Option reading stays independent of any backend, so
`describe-mapping` works in unit tests with no index and no table.

## Acceptance for v1 (met)

The bar the implementation was held to. All of it holds:

1. A descriptor with valid `metric.v1` options builds a mapping; each
   schema error in the list above fails the build with the field path.
2. `DescribeMapping` returns exactly the declared members and the
   mounted backends; `meta.v1` description and sensitivity flow through.
3. `QueryMetrics` on Lucene over a search-service subject returns the same
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
7. The metric verbs answer on the MCP catalog when the metric service
   module is mounted. They are contributed, so they are absent from
   `docs/generated/action-inventory.json`, which covers the static
   catalogs only.
8. No new noun appears in user-facing strings except mapping, member,
   measure/dimension as roles, grain, backend, rollup, and the action
   names.

## Open questions for the scoping pass (settled)

Kept with their answers; do not reopen them, and do not reopen the
out-of-scope list.

1. **Synthetic members in v1 or v1.1?** v1. `repeated FieldMetric
   members` on `MessageMetric`.
2. **Where does `MetricService` live?** Its own modules
   (`protomolt-metric-proto`, `-spi`, `-service`), independently
   mountable from retrieval.
3. **DuckDB dependency.** Taken, Hadoop-free, isolated in
   `protomolt-metric-iceberg`. No caller-supplied `SqlBackend` SPI.
4. **Result types.** `map<string, double>` stands: counts stay exact
   through 2^53, and the typed-cell question was settled for stored
   search fields, not for aggregates.
5. **Shared subjects with the search service.** Same name and same
   descriptor. Rollup tables are the one added namespace,
   `rollup:<table>`.
6. **`COUNT(*)` without a measure field.** Explicit, as preferred: a
   real field or a synthetic member carrying `AGGREGATE_COUNT`.
7. **Extension ids.** `59100541` / `59100542` were free and are taken.

## What the surface still does not carry

No console page, no default demo cube, no SQL proxy, and no "semantic
layer" README section. The rule stands for anything added here later.
