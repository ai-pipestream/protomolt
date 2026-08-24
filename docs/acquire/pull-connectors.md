# Pull connectors

`protomolt-acquire-pull`, `protomolt-acquire-s3` and `protomolt-acquire-jdbc`
are the pull side of acquisition: a connector reads a source of record (a
bucket, a database) and feeds what changed through the **intake service** — never
into repo-service directly. Account identity keeps riding the API key, and
every intake rule (scope narrowing, payload caps, the save shape) applies to
pulled documents exactly as to pushed ones.

## Stable identity

Every pulled item wraps into a document whose id is a deterministic
name-based UUID over `connector : datasource : source key`
(`PullDocuments.docId`). The same S3 object or database row always lands on
the same document: an updated item **re-saves its own doc id** instead of
accumulating a duplicate, unchanged content dedupes at the repository, and
the search service's replace-by-identity keeps exactly one live document per
source item. The ownership context carries the connector id (`s3-pull`,
`jdbc-pull`) as provenance; account and datasource are stamped by intake
from the key's scope, never by the connector.

## Stateless, watermark in and out

Connectors hold no state and run no schedule. A pull is one pass, triggered
by its verb; the watermark travels in with the request and out in the
report, owned by the caller. Stopping a connector is just not calling it —
stop is pause, and there is no hidden cursor to leak or orphan. A failed
item freezes the watermark (the report still processes later items), so
failures stay ahead of the watermark and retry on the next pull instead of
being silently lost.

## The S3 connector

`pull-s3` lists the bucket (optionally under a prefix), takes every object
strictly past the watermark in `(lastModified, key)` order, fetches it, and
submits it with source key `s3://bucket/key`. The watermark is
`<epochMillis>/<key>` of the newest processed object. Listing is
metadata-only; content transfers only for what is new or changed. Module
config (the `acquire-s3` role): `PROTOMOLT_ACQUIRE_S3_REGION` (required),
`PROTOMOLT_ACQUIRE_S3_ENDPOINT` / `_ACCESS_KEY` / `_SECRET_KEY` (optional,
defaults to AWS and the default credential chain), and
`PROTOMOLT_ACQUIRE_API_KEY` (required — the connector's identity rides the
intake key).

## The JDBC connector

`pull-jdbc` runs the caller's query against the configured source database
and wraps each row as a JSON document (`<idColumn>=<id>` as the source key,
all result-set columns as one JSON object). **The query owns its SQL and its
types**: an incremental query carries exactly one `?` placeholder bound with
the watermark string (cast in SQL as the column needs, e.g.
`updated_at > ?::timestamptz`); a first pull uses a placeholder-free query.
A watermark without a placeholder, or a placeholder without a watermark, is
a contradiction refused by name. The query must order by the watermark
column ascending — out-of-order rows refuse the pull, because a wrong
watermark silently loses rows. Watermark values compare numerically when
both sides are numbers (auto-increment ids), lexically otherwise. Module
config (the `acquire-jdbc` role): `PROTOMOLT_ACQUIRE_JDBC_URL` (required),
`_USERNAME` / `_PASSWORD`, and `PROTOMOLT_ACQUIRE_API_KEY`.

## The verbs

Both connectors are [actions](../surface/actions.md) contributed by their
role modules, so a mounted registry serves them on the actions route:
`POST /protomolt/actions/pull-s3` and `POST /protomolt/actions/pull-jdbc`.
The request and the answer are declared in `pull.proto`
(`PullFromS3Request`, `PullFromJdbcRequest`, `PullReport`), and each verb's
published input schema is derived from its request message, so the bounds a
caller reads are the bounds the verb enforces. The answer is the pull report:
`submitted`, `deduplicated`, `failed`, per-item `errors`, and the `watermark`
to hand back next time. For recurring
pulls, drive the verb from whatever owns operations cadence (a durable
workflow, cron, a pipeline) and persist the watermark there.
