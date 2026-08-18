# Metrics

The metric surface answers aggregate questions over a mapping subject:
the descriptor is the semantic layer, so measures and dimensions are
`metric.v1` declarations on the subject's message type, never a parallel
model. Design record: [metric mappings](../design/metric-mapping.md).

## The modules

| Artifact | Role |
|---|---|
| `protomolt-protobuf-metric` | The option dialect: `FieldMetric` (role, aggregate, name, filter_cel, cel, default_grain) and `MessageMetric` (subject, identity_field, synthetic members) |
| `protomolt-metric-proto` | The query contract: `MetricService` with `DescribeMapping` and `QueryMetrics`, validate.v1 rules on every request |
| `protomolt-metric-spi` | Mapping build, the query compiler and its refusals, the `MetricExecutor` seam |
| `protomolt-metric-lucene` | The shipped default engine: collectors over the search door's doc values |
| `protomolt-metric-door` | The gRPC service over served subjects, plus the `describe-mapping` and `query-metrics` verbs |

Each stands alone per the platform rule: option reading needs no index,
the SPI needs no engine, and describing a mapping works in a unit test
with no infrastructure.

## The mapping build

`MetricMappings.build` turns one message type's declarations into a
`MetricMapping` at mount time; every schema error fails the build naming
the field path, never the first query. Declarations come from a
`MetricHintSource`: the schema's own options
(`ProtoOptionsMetricHintSource`, unknown-field reparse included) with a
programmatic catalog (`CatalogMetricHintSource`) for schemas that cannot
be annotated.

Two CEL forms compile at build time. A measure's `filter_cel` (bool over
`this`, the message) is translated into an engine-neutral equality form,
equality over string or bool fields joined by `&&`; a valid but
untranslatable filter fails the build loudly. A calculated measure's
`cel` compiles over its sibling physical measure names and records what
it reads; at query time the compiler pulls those inputs into the engine
query, evaluates the calculation per row, and trims what was not
requested.

A measure that is not a physical field is a **synthetic member**,
declared on the message (`MessageMetric.members`) instead of a phantom
field: `paying_count` is a COUNT with a `filter_cel` over `this` and no
field at all, and a calculated `cel` works the same way. Each needs an
explicit name and the MEASURE role; anything needing storage — every
dimension, every SUM — stays a field, and a synthetic declaring one
refuses at build time naming the member. Prefer a real field when the
value already exists.

## Querying and refusing

`MetricQueries.query` compiles a request against the mapping, refuses
everything refusable with a stable kebab-case code and the legal set
(`unknown-subject`, `unknown-member`, `unknown-backend`,
`ambiguous-backend`, `unsupported-aggregate`, `unsupported-filter`,
`invalid-grain`, `invalid-limit`, `empty-measures`, `role-mismatch`),
and hands the executor a fully-resolved `CompiledMetricQuery`. `backend`
is an explicit selector: unset on a single-engine mount resolves to the
mount's engine (configuration, not a guess); unset on a multi-engine
mount is refused naming the mounted set. The response's `physical_plan`
is evidence for humans and agents, never input to a later query.

A filter takes exactly one of two forms. An equality set matches a
keyword or bool dimension against its rendered values (the same strings
result rows carry, schema-bounded in count and length). A **date
range** matches a DATE dimension against an inclusive UTC calendar
window: the bounds are ISO-8601 dates, schema-bounded to exactly that
shape (an unset side is unbounded), and the compiler resolves them once
to inclusive epoch-millis bounds both engines compare identically — the
Lucene backend over the date's doc values, the Iceberg backend in the
rendered SQL. Both-empty, inverted, non-date bounds, a range on a
non-DATE dimension, and both forms at once each refuse by name; an
equality set on a DATE dimension points at the range form. Joins are
deliberately absent from the query surface — see
[metric joins](../design/metric-joins.md) for the design of record.

The door mounts the validating interceptor from day one: the request
protos' validate.v1 rules enforce the shape rules (required subject,
bounded limit, non-empty measures), and handlers add only what a schema
cannot express. `describe-mapping` and `query-metrics` are catalog
actions, so any host that registers them serves them on every agent
surface at once.

## The Lucene engine

`LuceneMetricExecutor` is a single-pass collector over the doc values
the search door already writes: aggregation is a read path over existing
storage. Group-by members need `facetable` (or `sortable`) on their
indexing hint; a field present without the needed doc values fails
loudly naming the hint to declare. FLOAT and DOUBLE doc values decode
the mapper's sortable encoding; date dimensions bucket UTC under the
resolved grain honoring the hint's resolution. `COUNT_DISTINCT` counts
exactly, over keyword terms or raw numeric values, up to a per-measure
bound (100000 tracked values by default; the sets live in memory for
the query's duration): a query that passes the bound is refused with
`distinct-bound` naming the Iceberg backend as the engine that spills,
never an estimate, never a silently truncated count. The executor
reads through the door store's `withSearcher` borrow seam, so acquire
and release never leave the store.

## The Iceberg engine

`IcebergMetricExecutor` (`protomolt-metric-iceberg`) is the lake-native
backend: one `SELECT ... GROUP BY` rendered from the compiled query and
run by DuckDB over the Parquet files of the table the
[Iceberg sink](../sink/iceberg.md) wrote from the same descriptor.
DuckDB is an in-process reader, never a warehouse we operate;
Trino and Spark stay external consumers of the same table. An
object-store lake works the same way: scanned files materialize
locally through the table's own `FileIO` for the query's duration, so
the reader reaches exactly what the catalog reaches — no second
credential path, no DuckDB extension — and the physical plan says how
many files moved. Columns are
addressed by each member's `fieldPath` (the table keeps the message's
nesting as structs, where the search index flattens), date buckets
label themselves in UTC with exactly the Lucene backend's formats, and
the rendered SQL is the `physical_plan`. `COUNT_DISTINCT` runs
unbounded here (the reduction spills), where the Lucene backend counts
exactly only up to its in-memory bound; rows missing a
dimension value (NULL, or the empty string on term dimensions) are
excluded from group-by, matching the doc-values backend; tables
carrying delete files are refused loudly, because the sink appends and
this reader trusts that. The document platform mounts this engine
beside Lucene through the `DOCUMENT_PLATFORM_METRICS_ICEBERG_*` family
(see [the platform's lake metrics section](../apps/document-platform.md#lake-metrics));
on such a two-engine mount a query that leaves the backend unset is
refused with `ambiguous-backend` naming both, per the design.

## Rollups

`RebuildRollup` is the platform's answer to pre-aggregations, and it is
a workflow, not a second database: the aggregate query runs on the
named engine and its complete result atomically replaces one lake table
(`<namespace>.<table>`), so later reads scan the rollup instead of
grouping the subject on the fly. The rebuild is exact or refused, never
truncated: a result that fills the group budget (the query surface's
own limit cap) refuses with `rollup-budget`, because a rollup that
might be missing groups is worse than no rollup.

The write side is a plugin seam: `RollupSink` in the SPI, with
`IcebergRollupSink` as the shipped implementation — the rollup's schema
is a flat protobuf message synthesized from the member names (dimension
columns are the rendered strings, measure columns are doubles), written
as Parquet through the same emitter every lake table here takes, so the
rollup is scannable by DuckDB, Trino, or Spark and indexable into a
search subject later. A mount without any sink refuses with
`missing-sink`.

The declared, durable form is the `rebuild-rollup` workflow: one
checkpointed step under the jobs executor calling
`MetricService/RebuildRollup`, registered with a co-mounted registry so
operators submit it by name, with the run's output carrying the
physical plan and the lake snapshot the replace committed — the
evidence of what the rollup holds. Nothing runs until submitted:
optional, as designed. The same rebuild is also a catalog verb
(`rebuild-rollup`, beside `describe-mapping` and `query-metrics`), so
an agent surface can refresh a rollup directly without the jobs role —
one shared code path behind both doors, refusing identically.

A rebuilt rollup is itself a queryable subject: the sink stamps the
declaration (source subject, dimension columns, measure columns with
their source aggregates) onto the table as Iceberg properties, and
`rollup:<table>` resolves against the lake at request time — any
rebuilt rollup is instantly queryable and describable with no
side-channel configuration. Re-aggregation is honest or absent: COUNT
and SUM columns re-serve as SUM (summing counts is counting), MIN as
MIN and MAX as MAX, while AVG and COUNT_DISTINCT columns are not
members at all — an average of averages is a wrong answer, so those
columns stay scan-only outside the door. Date dimensions arrive as
their rendered bucket labels and serve as keyword dimensions: a rollup
cannot re-bucket time below the grain it was built at. A lake table
the sink did not write refuses instead of guessing a shape.

## The platform mount

`MetricDoorModule` (in `protomolt-metric-lucene`) is the composer role:
`metrics` in `PROTOMOLT_ROLES`. Wiring borrows the `LuceneSearchStore`
the co-mounted `search` role contributes, so the two roles must share a
node; a `metrics` role without `search` refuses to wire instead of
serving an empty corpus, and a metric subject must name a served search
mapping subject. The document platform mounts it by default on port 9095
(`DOCUMENT_PLATFORM_METRICS_GRPC_PORT`), serving `repo-document` with a
`documents` COUNT measure, dimensions over the folded search metadata's
document type, language, and category, and a processed-date time
dimension defaulting to daily grain; the three verbs ride the registry's
actions route. Each dimension field carries a facetable hint in the
search mapping, because the executor reads the doc values that hint
writes: the two declarations move together.

Declarations on nested fields work: the mapping build descends singular
message fields (Timestamp stays a DATE leaf), and a nested member's
field name is the flattened engine name (`parent_child`, the index
mapping's own naming; an index-side name override puts a field out of
metric reach). A nested `filter_cel` speaks about its declaring
message's siblings and its translated filters take the member's own
prefix. Repeated paths cannot carry members and refuse loudly, and
recursive types stop the descent.
