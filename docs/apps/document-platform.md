# Document platform

The document platform is the one-container deployment of the document
pipeline: `apps/document-platform` wires repo-service, the authenticated
intake service, the parsing coordinator, the durable jobs worker, the
schema registry, the search service with its console page and the metric
service, and the streaming parser playground into one JVM over the
in-process transport. It is the productized form of what
`GoldenPathSystemTest` proves. The same binary boots as specialized nodes
via `PROTOMOLT_ROLES` — see [Role nodes](role-nodes.md).

## Running it

```shell
./gradlew :protomolt-document-platform:installDist
docker compose -f deploy/document-platform/compose.yml up --build
```

The compose file brings PostgreSQL (two databases: the repository ledger and
the jobs store) and RustFS for object storage. Ports:

| Port | Surface |
| --- | --- |
| 9090 | repo-service gRPC (DocumentService, DriveService, health, reflection) |
| 9092 | intake gRPC (`IntakeService`, API-key authenticated) |
| 9093 | parsing coordinator gRPC (`ParseCoordinatorService`) |
| 9094 | search service gRPC (`SearchService`, `SearchIndexService`) |
| 9095 | metric service gRPC (`MetricService`) |
| 8081 | schema registry HTTP, with the jobs verbs on `/protomolt/actions` |
| 8095 | parser playground |
| 8096 | search console (the search page + operations panel) |

## What first boot does

- The git-backed registry initializes at `DOCUMENT_PLATFORM_REGISTRY_GIT`
  (default `/data/registry.git`) and **publishes the fleet document model**
  (`ai/pipestream/document/v1/document.proto`) from the build's own
  classpath, so the registry serves it as a subject from the start. The
  registry is not optional here: the platform runs it by default.
- The `parse-document` workflow registers under that name, so a durable parse
  is one `submit-workflow` call:
  `POST /protomolt/actions/submit-workflow` with
  `{"workflowName": "parse-document", "input": {"address": {...}}}`; poll with
  `get-job`. The platform's own worker fleet claims and completes it.
- The `parse-and-index` workflow registers too: the same submission with
  `{"workflowName": "parse-and-index", "input": {"address": {...},
  "mappingSubject": "repo-document"}}` parses the document and indexes it
  under the [search service](../search/service.md)'s `repo-document`
  subject, so a completed run means the document answers queries on the
  search port.
- The `replay-documents` action re-runs a stored workflow over a drive's
  documents (one durable run each): the operation behind a chunking-policy
  or mapping change, with the service's atomic replace-by-identity keeping
  replays duplicate-free.
- The search service serves the `repo-document` mapping subject over the
  index at `DOCUMENT_PLATFORM_SEARCH_INDEX_DIR` (default `/data/search-index`).
  The lexical lane always works; the vector lane activates when a Model2Vec
  model directory is configured (`PROTOMOLT_MODEL2VEC_PATH`), with the
  policy's dims read from the loaded model.
- The metric service answers over the same live index: `repo-document`
  serves a `documents` COUNT measure, group-by dimensions on document type,
  language, and category, and a processed-date time dimension (daily grain
  by default) through `MetricService` on the metrics port, with the
  `describe-mapping` and `query-metrics` verbs riding the registry's
  actions route.
- The embedded reference text parser serves text and markdown. A fleet of
  external parsers replaces it by pointing
  `DOCUMENT_PLATFORM_PARSE_PROFILES` (+ `..._PROFILE_ENDPOINT`) at a
  service-profile store; routing rules come from
  `DOCUMENT_PLATFORM_PARSE_RULES_JSON` when the defaults (text/markdown to
  the reference parser) are not enough.
- With `DOCUMENT_PLATFORM_SEED_ACCOUNT_ID` set, the account's `intake` and
  `pipeline` drives provision at boot; the compose file seeds account
  `demo` with API key `demo-key`.

Key stores follow the intake service's convention: OIDC introspection
(`DOCUMENT_PLATFORM_INTAKE_OIDC_INTROSPECTION_URL` + client id/secret) or
the env-seeded table (`DOCUMENT_PLATFORM_INTAKE_KEYS`).

## Index snapshots

The search index can snapshot to S3 at commit points and restore on boot
(see [the search service's snapshot
section](../search/service.md#index-snapshots)).
The `DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_*` family turns it on:

| Variable | Meaning |
| --- | --- |
| `..._S3_BUCKET` | the snapshot bucket; absent means snapshots are off, and any other family member set without it refuses by name |
| `..._S3_REGION` | the bucket's region; required with the bucket |
| `..._S3_PREFIX` | key prefix inside the bucket (default `search-snapshots`) |
| `..._S3_ENDPOINT` | endpoint override (LocalStack, RustFS); implies path-style addressing |
| `..._S3_ACCESS_KEY`, `..._S3_SECRET_KEY` | static credentials as a pair, or neither for the AWS default provider chain |

The bucket is infrastructure the operator provides; the platform never
creates it. A node booting over an empty index directory with the family
set restores each subject from the bucket before serving, which is what
`PlatformSnapshotIT` proves end to end.

`DOCUMENT_PLATFORM_SEARCH_READ_ONLY=true` makes the search role a
reader: no repo channel, no `SearchIndexService`, and restore-only
snapshots (a reader never uploads a commit or prunes the writer's
blobs). `DOCUMENT_PLATFORM_SEARCH_REFRESH_SECONDS` makes the reader
follow the writer live, pulling newer snapshots into its serving index
on that interval; absent means restart-only, and setting it on a
writable node refuses — refresh is the reader's pull. Combined with the
`metrics` role this is the remote metrics node — see
[Role nodes](role-nodes.md).

## Distributed config

`DOCUMENT_PLATFORM_CONFIG_REFRESH_SECONDS` switches the
[config lane](../core/config.md) on: the node pulls its config subjects
from the co-mounted registry (or `DOCUMENT_PLATFORM_CONFIG_URL` for a
node without one) on that interval, verify-then-swap, refusing it on a
node with no config consumer or no registry to pull from. Absent means
environment-only configuration, exactly as before.

The first consumer is the parse role's routing rules: the routing
contract publishes to the registry at boot beside the document model,
so the registry's config gate checks a `parse-routing` document (an
`ai.pipestream.proto.parse.v1.RoutingConfig`) against it — an empty
rule set refuses at the gate, because a coordinator with no rules
routes nothing and that must be said, not configured. A valid document
swaps the live rules on the next interval with no reboot and no CRUD
surface (the reload the routing contract always promised), and a
rebooted node reads the distributed config ahead of its environment
defaults. The applied version — the document's git commit — is the
evidence in the log line.

## Lake metrics

The metrics role can mount the [Iceberg engine](../search/metrics.md#the-iceberg-engine)
beside its Lucene executor. The `DOCUMENT_PLATFORM_METRICS_ICEBERG_*`
family turns it on:

| Variable | Meaning |
| --- | --- |
| `..._ICEBERG_CATALOG_URI` | the catalog; absent means the lake engine is off, and any other family member set without it refuses by name. A `jdbc:` URI is a JDBC catalog (`jdbc:sqlite:` works out of the box, the one-container lake); `http(s)` is a REST catalog service. Any other scheme refuses. |
| `..._ICEBERG_WAREHOUSE` | the warehouse location; required with a `jdbc:` URI, optional pass-through for REST |
| `..._ICEBERG_NAMESPACE` | the namespace metric tables live under (default `protomolt`) |
| `..._ICEBERG_S3_REGION` | the object store's region; setting any `S3_*` member puts the lake's file plane on S3 through Iceberg's `S3FileIO`, and this member is required with the group |
| `..._ICEBERG_S3_ENDPOINT` | endpoint override (LocalStack, RustFS); implies path-style addressing |
| `..._ICEBERG_S3_ACCESS_KEY`, `..._ICEBERG_S3_SECRET_KEY` | static credentials as a pair, or neither for the AWS default provider chain |

Each metric subject reads the lake table named exactly like it —
`repo-document` reads `<namespace>.repo-document` — loaded per query, so
the [Iceberg sink](../sink/iceberg.md) can write the table after the node
boots. A subject whose table does not exist refuses with `missing-table`
instead of answering zero: the sink writes tables, this reader never
creates one. Two details keep writer and reader on the same lake: without
the `S3_*` group both shapes read a local-filesystem warehouse through
the sink's `LocalFileIO` (this build is Hadoop-free), while with it the
warehouse lives on the object store and the metric reader reaches it
through the table's own `FileIO` — scanned files materialize locally for
the query's duration, so there is no second credential path and no
DuckDB extension; and a JDBC catalog scopes its table records by catalog
*name*, so a writer sharing the catalog database must initialize with
this node's name, `protomolt`.

With the family set the subject serves two engines, and per the design a
query that leaves `backend` unset is refused with `ambiguous-backend`
naming both — a caller picks `METRIC_BACKEND_LUCENE` (the live index) or
`METRIC_BACKEND_ICEBERG` (the lake table) explicitly. Without the family
nothing changes: the single-engine mount keeps answering unset-backend
queries with Lucene, which is configuration, not a guess.

Rollups always have somewhere to land (see
[Rollups](../search/metrics.md#rollups)): with the family set,
`RebuildRollup` replaces tables in that lake; without it, rollups land
in a default local lake — a sqlite catalog plus Parquet under
`DOCUMENT_PLATFORM_METRICS_LAKE_DIR` (default `/data/metrics-lake`),
created lazily on the first rebuild, so a node that never rebuilds
never touches the directory. The default mounts the rollup sink only,
never the Iceberg query backend, so the single-engine query surface is
unchanged. The `rebuild-rollup` workflow registers with the co-mounted
registry and submits through the jobs role like any other workflow.

## Configuration

The repository half of the platform reads the `DOCUMENT_PLATFORM_*` family
exactly as [`repo/README.md`](../../repo/README.md) documents it. The
platform's own variables are javadoc'd on `DocumentPlatformConfig`; the
compose file is the worked example.

`DocumentPlatformSmokeIT` drives every external surface over real TCP:
registry subjects, authenticated ingest, submit-workflow to completion, the
parsed result read back, parse-and-index to a lexical search hit on the
search port, replay without duplication, the playground page, the
search console (page, subjects, a search hit through the JSON bridge, and
the operations proxy), and the metric service counting the corpus over its
own port and through the catalog verbs.
