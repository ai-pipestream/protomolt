# metric/

Descriptor-declared measures and dimensions over indexed data: the mapping
builder and query compiler, the gRPC service, and two execution backends
(interactive Lucene collectors and lake-native DuckDB over Iceberg).

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-metric-proto` | `metric/proto` | The metric contract: `MetricService` with `DescribeMapping`, `QueryMetrics` and `RebuildRollup` |
| `:protomolt-metric-spi` | `metric/spi` | Mapping build from `metric.v1` declarations, the query compiler and its refusals, the executor seam |
| `:protomolt-metric-service` | `metric/service` | The `MetricService` over served subjects plus the `describe-mapping`, `query-metrics` and `rebuild-rollup` verbs |
| `:protomolt-metric-lucene` | `metric/lucene` | The interactive executor: aggregate collectors over the doc values the search service writes |
| `:protomolt-metric-iceberg` | `metric/iceberg` | The lake-native executor: one DuckDB `SELECT` over the Parquet files the Iceberg sink wrote |

Docs: [metrics](../docs/metric/metrics.md),
[metric mappings](../docs/design/metric-mapping.md),
[metric joins](../docs/design/metric-joins.md).
