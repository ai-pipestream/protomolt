# Metrics

The metric surface answers aggregate questions over a mapping subject:
the descriptor is the semantic layer, so measures and dimensions are
`metric.v1` declarations on the subject's message type, never a parallel
model. Design record: [metric mappings](../design/metric-mapping.md).

## The modules

| Artifact | Role |
|---|---|
| `protomolt-protobuf-metric` | The option dialect: `FieldMetric` (role, aggregate, name, filter_cel, cel, default_grain) and `MessageMetric` (subject, identity_field) |
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
resolved grain honoring the hint's resolution. `COUNT_DISTINCT` is
deliberately absent until a bounded collector exists. The executor reads
through the door store's `withSearcher` borrow seam, so acquire and
release never leave the store.
