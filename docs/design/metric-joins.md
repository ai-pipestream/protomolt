# Metric joins

Status: design of record for joins on the metric surface, landed. The
[metric mapping design](metric-mapping.md) lists "multi-fact views, join
paths, fan-out grain, multi-stage measures" as out of scope and warns
"do not sneak in"; this chapter is the considered answer that replaces
sneaking. Changing the landed shape means updating this chapter first.

## The position

Joins happen at **rebuild time, not query time**. The rollup is the join
surface: `rebuild-rollup` already runs a complete aggregate and
atomically replaces a lake table, so an enriched rollup — the aggregate
joined against another subject's dimensions — is a rebuild that writes
denormalized rows, and querying it afterwards is the ordinary
single-subject path with all its refusals intact. This is the same
division Cube draws between pre-aggregations and its query planner, kept
on the side of it that protomolt already owns: declared, durable,
evidenced, optional.

What this buys:

- **The query surface stays single-subject.** One mapping, one engine,
  one refusal vocabulary. No join planner, no cardinality estimation,
  no query whose cost depends on a second subject's size.
- **Fan-out cannot happen at query time.** The classic join wound —
  a one-to-many join silently multiplying measure rows — is impossible
  when the joined shape was materialized by a rebuild that declared its
  grain and was refused if it could not attest completeness.
- **The evidence model survives.** A rollup's physical plan and lake
  snapshot already say what it holds; an enriched rollup's evidence
  additionally names the enrichment source and its snapshot.

## The shape (as landed)

`RebuildRollupRequest` carries an optional `RollupEnrichment`: a second
subject, a join key (a dimension member present in both the primary
request's dimensions and the enrichment subject's mapping), the
dimension members to pull, and an optional engine selector for the
lookup. The rebuild runs the primary aggregate exactly as today, then
resolves each result row's key against the enrichment subject — a
**lookup, strictly one-to-at-most-one**:

- The lookup is a dimensions-only aggregate on the enrichment subject
  (`MetricQueries.lookup`: the distinct rendered combinations of the
  join key and the pulled members under a synthetic COUNT), so the
  enrichment subject needs no declared measure. It runs under the same
  group budget as the rebuild and refuses rather than truncates when it
  fills — an incomplete lookup cannot attest the join.
- A key matching more than one enrichment combination refuses the whole
  rebuild (`join-fanout`), because a fan-out would multiply measures
  and a rollup is exact or refused. This is the load-bearing rule.
- A key matching nothing leaves the enrichment columns empty; the
  response counts them (`rows_unenriched`) and names the source
  (`enriched_from`), evidence not silence.
- A malformed enrichment — the key absent from the primary dimensions,
  or a pulled member colliding with an existing rollup column —
  refuses as `invalid-enrichment`; membership and role errors on the
  enrichment subject refuse with the ordinary vocabulary.
- The enrichment values land as ordinary dimension entries on the
  result rows, and the pulled members join the rollup's dimension
  columns, so the rebuilt table serves back through `rollup:<table>`
  with no sink or resolver changes (as plain keywords, like every
  rollup dimension).

## Stays out, with reasons

- **Query-time joins, multi-fact views, join paths.** A second database
  grows here; the design's first rule is that one does not.
- **Fan-out grain** (deliberately multiplying to a finer grain):
  incompatible with "exact or refused"; a use case that truly needs it
  models the finer grain as its own subject and rolls up from there.
- **Multi-stage measures** (measures over other rollups' measures):
  compose by rebuilding from a rollup subject instead — `rollup:x` is
  already a legal source for `rebuild-rollup`.
