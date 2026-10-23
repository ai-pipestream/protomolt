# Environment variables

The single reference of every environment variable the main source reads,
grouped by the service or binary that reads it. Each entry is taken from the
reading class on this commit; `scripts/check-env-vars-documented.sh` (run by
`scripts/check-deployment-statics.sh` and CI) fails the build when a variable
appears in `*/src/main/java` but not on this page.

Reading conventions, stated once:

- "unset" below also covers a blank value unless a row says otherwise.
- "fail fast" means the reading class throws `IllegalArgumentException`
  naming the variable at configuration time, before the service boots.
- Booleans parsed by the repository environment helper accept
  `true/false`, `1/0`, `yes/no`, `on/off` (case-insensitive); anything else
  fails fast. Rows below say "strict flag" for that form.
- Two names are dynamic families, not fixed variables:
  `PROTOMOLT_<ROLE>_TARGET` (the composer's remote-role channel for any role
  name, for example `PROTOMOLT_REPO_TARGET`; see
  [role nodes](../apps/role-nodes.md)) and the `env:<NAME>` credential
  references of the inference catalog, where the reference names the
  variable holding the credential (`env:OPENAI_TOKEN` reads
  `OPENAI_TOKEN`); resolution fails on an unset or empty variable without
  naming it, so credential material never leaks into messages.

## Repo service (`repo/service`, `repo/container`)

Read by `RepoServiceConfig.fromEnvironment` through
`RepositoryEnvironment`: defaults apply only to omitted variables; a present
but blank text value, an out-of-range or non-integer number, and a
non-strict-flag boolean all fail fast naming the variable. The console
sign-in variables (`PROTOMOLT_CONSOLE_*`) are read by `OidcConsoleSettings`
when the HTTP port is on: all five or none, and a subset fails the boot
naming the missing ones.

| Variable | Default | Accepted |
|---|---|---|
| `DOCUMENT_PLATFORM_GRPC_PORT` | `9090` | integer 0..65535; 0 = ephemeral |
| `DOCUMENT_PLATFORM_CONSOLE_DIR` | unset = console unavailable (HTTP 503) | path to a built console bundle containing `index.html`; blank or invalid paths fail boot |
| `DOCUMENT_PLATFORM_HTTP_PORT` | `8080` | integer 0..65535 or `off`; 0/`off` disables the HTTP upload server |
| `DOCUMENT_PLATFORM_S3_ENDPOINT` | unset = provider default endpoint | URL |
| `DOCUMENT_PLATFORM_S3_REGION` | `us-east-1` | non-blank text |
| `DOCUMENT_PLATFORM_S3_ACCESS_KEY` | unset = SDK default credentials chain | text; must be set together with the secret key or the record constructor fails fast |
| `DOCUMENT_PLATFORM_S3_SECRET_KEY` | unset | text; paired with the access key |
| `DOCUMENT_PLATFORM_S3_CONDITIONAL_WRITES` | `false` | strict flag; requires an s3 or s3-redis-cache blob store |
| `DOCUMENT_PLATFORM_DEFAULT_BUCKET_BASE` | `documents` | non-blank text |
| `DOCUMENT_PLATFORM_BLOB_STORE` | `s3` | `s3`, `redis` or `s3-redis-cache`; anything else (including the retired `repo` values) fails fast |
| `DOCUMENT_PLATFORM_REDIS_URI` | `redis://localhost:6379` | non-blank text |
| `DOCUMENT_PLATFORM_REDIS_TTL_SECONDS` | `3600` | integer >= 0 |
| `DOCUMENT_PLATFORM_REDIS_MAX_OBJECT_BYTES` | `8388608` | integer >= 0; 0 = unbounded |
| `DOCUMENT_PLATFORM_LIFECYCLE_ENABLED` | `true` | strict flag |
| `DOCUMENT_PLATFORM_PURGE_INTERVAL_MS` | `5000` | integer >= 1 |
| `DOCUMENT_PLATFORM_SWEEP_INTERVAL_MS` | `60000` | integer >= 1 |
| `DOCUMENT_PLATFORM_RECONCILE_ENABLED` | `false` | strict flag |
| `DOCUMENT_PLATFORM_RECONCILE_DRY_RUN` | `true` | strict flag |
| `DOCUMENT_PLATFORM_RECONCILE_MIN_AGE_MS` | `3600000` | integer >= 0 |
| `DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS` | unset = eventing off (no outbox, no relay) | text |
| `DOCUMENT_PLATFORM_KAFKA_TOPIC` | `document-events` | non-blank text |
| `DOCUMENT_PLATFORM_SCHEMA_REGISTRY_URL` | unset = registry-free (frames stamp schema id 0) | URL |
| `DOCUMENT_PLATFORM_SEED_ACCOUNT_ID` | unset = no boot-time drive seeding | text; treat as permanent once used |
| `DOCUMENT_PLATFORM_READ_BUDGET_BYTES` | `268435456` (256 MiB) | integer >= 1 |
| `DOCUMENT_PLATFORM_PURGE_QUEUE` | `jdbc` | `jdbc` or `kafka`; `kafka` requires the bootstrap servers or the config fails fast |
| `DOCUMENT_PLATFORM_KAFKA_PURGE_TOPIC` | `document-purges` | non-blank text; used only with the kafka purge queue |
| `PROTOMOLT_CONSOLE_OIDC_ISSUER` | unset = console sign-in off | the OpenID provider's issuer URL; discovered at `/.well-known/openid-configuration` at boot, and a wrong one fails the boot |
| `PROTOMOLT_CONSOLE_OIDC_CLIENT_ID` | required with the issuer | text |
| `PROTOMOLT_CONSOLE_OIDC_CLIENT_SECRET` | required with the issuer | text |
| `PROTOMOLT_CONSOLE_OIDC_REDIRECT_URI` | required with the issuer | the host's `/console/session/callback` as the provider reaches it, registered at the provider exactly |
| `PROTOMOLT_CONSOLE_SESSION_TTL` | required with the issuer | ISO-8601 duration, one second to seven days (`PT8H`) |

The ledger (read by `LedgerConfig.fromEnvironment` in `repo/container`;
`DOCUMENT_PLATFORM_POOL_SIZE` fails fast on a non-positive or non-integer
value, the text values fail fast when present but blank):

| Variable | Default | Accepted |
|---|---|---|
| `DOCUMENT_PLATFORM_JDBC_URL` | `jdbc:postgresql://localhost:5432/documents` | JDBC URL |
| `DOCUMENT_PLATFORM_USERNAME` | `documents` | text |
| `DOCUMENT_PLATFORM_PASSWORD` | `documents` | text; may be empty but not null when present |
| `DOCUMENT_PLATFORM_POOL_SIZE` | `10` | positive integer |

Managed storage (read by `ManagedStoragePolicy.fromEnvironment`; every
malformed value fails fast):

| Variable | Default | Accepted |
|---|---|---|
| `DOCUMENT_PLATFORM_MANAGED_BACKEND_GENERATION` | unset = managed storage off | nonsecret identifier, 1..128 characters |
| `DOCUMENT_PLATFORM_MANAGED_STORAGE_REALM` | unset | nonsecret identifier, 1..128 characters |
| `DOCUMENT_PLATFORM_MANAGED_RETENTION_QUALIFIED` | unset | exactly `true` or `false`; required `true` when the identifiers are set |

The standalone repo service launcher (`RepoServiceMain`) additionally
requires `PROTOMOLT_API_TOKEN` (see the platform-wide table) and refuses to
start without it.

The bounded Redis archive launcher (`RepoBoundedArchiveMain`, see
[bounded Redis archive](../apps/bounded-archive.md)) reads these through the
same fail-fast helper:

| Variable | Default | Accepted |
|---|---|---|
| `DOCUMENT_PLATFORM_ARCHIVE_ACCOUNT` | required | non-blank text |
| `DOCUMENT_PLATFORM_ARCHIVE_DRIVE` | required | non-blank text |
| `DOCUMENT_PLATFORM_ARCHIVE_MAX_REQUEST_BYTES` | `2097152` | integer >= 1 |
| `DOCUMENT_PLATFORM_ARCHIVE_MAX_OBJECT_BYTES` | `1048576` | integer 1..9437184 |
| `DOCUMENT_PLATFORM_ARCHIVE_MAX_RENDITIONS` | `16` | integer >= 1 |
| `DOCUMENT_PLATFORM_ARCHIVE_PAYLOAD_BUDGET_BYTES` | `14680064` | integer >= 1 |
| `DOCUMENT_PLATFORM_ARCHIVE_MAX_CONCURRENT_REQUESTS` | `4` | integer 1..1024 |
| `DOCUMENT_PLATFORM_ARCHIVE_MAX_RESPONSE_BYTES` | the request limit | integer >= 1 |
| `DOCUMENT_PLATFORM_ARCHIVE_MAX_MANIFEST_BYTES` | the request limit | integer >= 1 |

## Account service (`account/service`)

Read by `AccountServiceConfig.fromEnvironment` and
`AccountStoreConfig.fromEnvironment`. Numbers fail fast on a non-integer;
blank text falls back to the default (unlike the repo service's helper).

| Variable | Default | Accepted |
|---|---|---|
| `DOCUMENT_PLATFORM_ACCOUNT_GRPC_PORT` | `9091` | integer |
| `DOCUMENT_PLATFORM_REPO_GRPC_TARGET` | `localhost:9090` | `host:port` or `inprocess:<name>` |
| `DOCUMENT_PLATFORM_ACCOUNT_KAFKA_BOOTSTRAP_SERVERS` | unset = eventing off | text |
| `DOCUMENT_PLATFORM_ACCOUNT_KAFKA_TOPIC` | `account-events` | text |
| `DOCUMENT_PLATFORM_ACCOUNT_SCHEMA_REGISTRY_URL` | unset = registry-free | URL |
| `DOCUMENT_PLATFORM_ACCOUNT_LIFECYCLE_ENABLED` | `true` | `true`/`false`/`1`/`0`, case-insensitive, trimmed; unset or blank keeps the default, anything else fails fast naming the variable |
| `DOCUMENT_PLATFORM_ACCOUNT_RELAY_INTERVAL_MS` | `5000` | integer; non-positive falls back to the default |
| `DOCUMENT_PLATFORM_ACCOUNT_JDBC_URL` | `jdbc:postgresql://localhost:5432/accounts` | JDBC URL |
| `DOCUMENT_PLATFORM_ACCOUNT_USERNAME` | `accounts` | text |
| `DOCUMENT_PLATFORM_ACCOUNT_PASSWORD` | `accounts` | text |
| `DOCUMENT_PLATFORM_ACCOUNT_POOL_SIZE` | `10` | integer; fails fast on a non-integer, non-positive falls back to the default |

## Intake service (`intake/service`)

Read by `IntakeServiceConfig.fromEnvironment`, `IntakeServiceMain` and
`IntakeKeyStoreConfig.fromEnvironmentMap`. Numbers fail fast naming the
variable; exactly one key source (env-seeded keys, JDBC store or OIDC) must
be configured, and setting both the OIDC URL and the JDBC URL is refused.

| Variable | Default | Accepted |
|---|---|---|
| `DOCUMENT_PLATFORM_INTAKE_REPO_TARGET` | required | `host:port` or `inprocess:<name>`; unset fails fast |
| `DOCUMENT_PLATFORM_INTAKE_GRPC_PORT` | `9092` | integer |
| `DOCUMENT_PLATFORM_INTAKE_HTTP_PORT` | `0` (HTTP lane off) | integer |
| `DOCUMENT_PLATFORM_INTAKE_MAX_PAYLOAD_BYTES` | `67108864` (64 MiB) | positive integer |
| `DOCUMENT_PLATFORM_INTAKE_STEP_WRITERS` | `parse-coordinator` | comma-separated caller names |
| `DOCUMENT_PLATFORM_INTAKE_KEYS` | required for the env-seeded key store | `key=account[:drive...]` entries |
| `DOCUMENT_PLATFORM_INTAKE_KEYS_JDBC_URL` | unset = JDBC key store off | JDBC URL; required for the JDBC store, no default database |
| `DOCUMENT_PLATFORM_INTAKE_KEYS_USERNAME` | required with the JDBC URL | text |
| `DOCUMENT_PLATFORM_INTAKE_KEYS_PASSWORD` | required with the JDBC URL | text |
| `DOCUMENT_PLATFORM_INTAKE_KEYS_POOL_SIZE` | `10` | positive integer; a malformed or non-positive value fails fast naming the variable (#480) |
| `DOCUMENT_PLATFORM_INTAKE_OIDC_INTROSPECTION_URL` | unset = OIDC off | URL |
| `DOCUMENT_PLATFORM_INTAKE_OIDC_CLIENT_ID` | required with the OIDC URL | text |
| `DOCUMENT_PLATFORM_INTAKE_OIDC_CLIENT_SECRET` | required with the OIDC URL | text |

## Parse coordinator (`parse/service`)

Read by `ParseCoordinatorConfig.fromEnvironment`; numbers fail fast naming
the variable.

| Variable | Default | Accepted |
|---|---|---|
| `DOCUMENT_PLATFORM_PARSE_REPO_TARGET` | required | `host:port` or `inprocess:<name>`; unset fails fast |
| `DOCUMENT_PLATFORM_PARSE_GRPC_PORT` | `9093` | integer |
| `DOCUMENT_PLATFORM_PARSE_DRIVE` | `intake` | drive name |
| `DOCUMENT_PLATFORM_PARSE_DEADLINE_SECONDS` | `300` | positive integer |
| `DOCUMENT_PLATFORM_PARSE_RULES_JSON` | unset | CEL routing rules as JSON |
| `DOCUMENT_PLATFORM_PARSE_RULES_FILE` | unset | path to the rules JSON |
| `DOCUMENT_PLATFORM_PARSE_PARSERS` | unset = embedded reference parser | comma-separated parser targets |
| `DOCUMENT_PLATFORM_PARSE_PROFILES` | unset | service-profile store directory for parser discovery |
| `DOCUMENT_PLATFORM_PARSE_PROFILE_ENDPOINT` | unset | endpoint name each profile must carry; required exactly when the profiles directory is set |

## Document platform launcher (`apps/document-platform`)

Read by `DocumentPlatformConfig`, `MetricsIcebergConfig` and
`SearchSnapshotConfig`. Ports fail fast on a non-integer; an unknown role
in `PROTOMOLT_ROLES` fails fast listing the known set; a snapshot or
Iceberg family member set without its anchor variable fails fast naming
both.

| Variable | Default | Accepted |
|---|---|---|
| `PROTOMOLT_ROLES` | the full one-container role set | comma-separated role names (aliases `metrics`, `parser-text` accepted) |
| `DOCUMENT_PLATFORM_REGISTRY_GIT` | `/data/registry.git` | path; required on a registry node |
| `DOCUMENT_PLATFORM_REGISTRY_PORT` | `8081` | integer |
| `DOCUMENT_PLATFORM_INTAKE_GRPC_PORT` | `9092` | integer (same variable the intake service reads; same default) |
| `DOCUMENT_PLATFORM_PARSE_GRPC_PORT` | `9093` | integer (shared with the parse coordinator) |
| `DOCUMENT_PLATFORM_PLAYGROUND_PORT` | `8095` | integer; 0 picks a free port |
| `DOCUMENT_PLATFORM_PARSE_RULES_JSON` | unset | CEL routing rules as JSON (shared with the parse coordinator) |
| `DOCUMENT_PLATFORM_PARSE_PROFILES` | unset | profile store directory (shared) |
| `DOCUMENT_PLATFORM_PARSE_PROFILE_ENDPOINT` | unset | endpoint name (shared) |
| `DOCUMENT_PLATFORM_WORKER_COUNT` | jobs worker default | integer |
| `DOCUMENT_PLATFORM_JOBS_JDBC_URL` | required on a jobs node | JDBC URL |
| `DOCUMENT_PLATFORM_JOBS_USERNAME` | unset | text |
| `DOCUMENT_PLATFORM_JOBS_PASSWORD` | unset | text |
| `DOCUMENT_PLATFORM_ACTIONS_URL` | unset | URL of the registry actions route |
| `DOCUMENT_PLATFORM_SEARCH_GRPC_PORT` | `9094` | integer |
| `DOCUMENT_PLATFORM_SEARCH_INDEX_DIR` | `/data/search-index` | path |
| `DOCUMENT_PLATFORM_SEARCH_CONSOLE_PORT` | `8096` | integer |
| `DOCUMENT_PLATFORM_SEARCH_READ_ONLY` | `false` | boolean; a read-only node restores from the snapshot bucket |
| `DOCUMENT_PLATFORM_SEARCH_REFRESH_SECONDS` | unset | integer; the read-only node's snapshot refresh interval |
| `DOCUMENT_PLATFORM_SEARCH_VECTORIZE_SENSITIVITY` | unset | sensitivity ceiling for the embed lane |
| `DOCUMENT_PLATFORM_METRICS_GRPC_PORT` | `9095` | integer |
| `DOCUMENT_PLATFORM_METRICS_LAKE_DIR` | `/data/metrics-lake` | path; ignored when the Iceberg catalog family is set |
| `DOCUMENT_PLATFORM_CONFIG_REFRESH_SECONDS` | unset = config lane off | integer |
| `DOCUMENT_PLATFORM_CONFIG_URL` | unset | registry config source URL; mutually exclusive with the config Kafka family |
| `DOCUMENT_PLATFORM_CONFIG_KAFKA_BOOTSTRAP_SERVERS` | unset | text |
| `DOCUMENT_PLATFORM_CONFIG_KAFKA_TOPIC` | `protomolt-config` | topic name |
| `DOCUMENT_PLATFORM_CONFIG_KAFKA_SCHEMA_REGISTRY_URL` | required with the config Kafka bootstrap | URL |
| `DOCUMENT_PLATFORM_TAXONOMIES` | unset | config subject; requires the config lane |
| `DOCUMENT_PLATFORM_SCREENING` | unset | config subject; requires the config lane |
| `DOCUMENT_PLATFORM_POSTAL_CODES` | unset | config subject; requires the config lane |

Metrics Iceberg catalog (`MetricsIcebergConfig`; the whole family is
anchored on the catalog URI, any other member set without it fails fast):

| Variable | Default | Accepted |
|---|---|---|
| `DOCUMENT_PLATFORM_METRICS_ICEBERG_CATALOG_URI` | unset = local lake dir | `jdbc:` or HTTP REST catalog URI |
| `DOCUMENT_PLATFORM_METRICS_ICEBERG_WAREHOUSE` | required with a `jdbc:` URI | warehouse location |
| `DOCUMENT_PLATFORM_METRICS_ICEBERG_NAMESPACE` | `protomolt` | namespace |
| `DOCUMENT_PLATFORM_METRICS_ICEBERG_S3_REGION` | required with any S3 member | region |
| `DOCUMENT_PLATFORM_METRICS_ICEBERG_S3_ENDPOINT` | unset | endpoint override |
| `DOCUMENT_PLATFORM_METRICS_ICEBERG_S3_ACCESS_KEY` | unset | set both keys or neither |
| `DOCUMENT_PLATFORM_METRICS_ICEBERG_S3_SECRET_KEY` | unset | paired with the access key |

Search snapshots (`SearchSnapshotConfig`; anchored on the bucket, any other
member set without it fails fast):

| Variable | Default | Accepted |
|---|---|---|
| `DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_BUCKET` | unset = snapshots off | bucket name |
| `DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_REGION` | required with the bucket | region |
| `DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_PREFIX` | `search-snapshots` | key prefix |
| `DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_ENDPOINT` | unset | endpoint override |
| `DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_ACCESS_KEY` | unset | set both keys or neither |
| `DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_SECRET_KEY` | unset | paired with the access key |

## Platform-wide (`PROTOMOLT_API_TOKEN`, access policy, role targets)

| Variable | Read by | Default | Accepted |
|---|---|---|---|
| `PROTOMOLT_API_TOKEN` | `ProtoMoltServe`, `DocumentPlatformConfig`, `RepoServiceMain`, `ProtoMoltAcpAgent` | unset = open surfaces (serve, platform); required by the standalone repo service | token text |
| `PROTOMOLT_ACCESS_POLICY` | `ProtoMoltServe`, `DocumentPlatformConfig` | unset | path to the access-policy document; requires the API token |
| `PROTOMOLT_ACCOUNT_BINDINGS` | `AccountBindingPolicies` | unset = no account bindings | path to a binary protobuf `.binpb` or `.pb` account-binding document; blank, unreadable or invalid documents fail boot |
| `PROTOMOLT_<ROLE>_TARGET` | `Channels` (composer) | unset = in-process or refused | `host:port` of the remote role, for example `PROTOMOLT_REPO_TARGET` |

## protomolt-serve (`apps/serve`)

Read in `ProtoMoltServe`; every value also has a command-line flag that
takes precedence. Numbers and booleans go through helpers that fail fast
naming the variable on a malformed value.

| Variable | Default | Accepted |
|---|---|---|
| `PROTOMOLT_GATHER_CACHE` | `~/.cache/protomolt/gather/git` | path |
| `PROTOMOLT_CONFIG_URL` | unset = config lane off | registry config source URL |
| `PROTOMOLT_CONFIG_REFRESH_SECONDS` | `0` (off) | integer |
| `PROTOMOLT_JOBS_JDBC` | unset = jobs lane off | JDBC URL |
| `PROTOMOLT_JOBS_USER` | unset | text |
| `PROTOMOLT_JOBS_PASSWORD` | unset | text |
| `PROTOMOLT_JOBS_KAFKA` | unset | bootstrap servers |
| `PROTOMOLT_JOBS_REQUEST_TOPIC` | unset | topic name |
| `PROTOMOLT_JOBS_WORKERS` | the jobs default worker count | integer |
| `PROTOMOLT_JOBS_TARGET_CONCURRENCY` | the jobs default | integer |
| `PROTOMOLT_SERVICE_WORKSPACE` | unset = in-memory profiles | profile store directory |
| `PROTOMOLT_WORKFLOW_WORKSPACE` | unset | workflow workspace directory |
| `PROTOMOLT_WORKFLOW_AUTHORING_POLICY_SHA256` | unset | hex digest pinning the authoring policy |
| `PROTOMOLT_WORKFLOW_AUTHORING_AUTHORIZATION_DIR` | unset | directory |
| `PROTOMOLT_WORKFLOW_PREPARATION_INTENT_DIR` | unset | directory |
| `PROTOMOLT_WORKFLOW_PREPARATION_TEMPLATE_PROVIDER` | unset | provider name |
| `PROTOMOLT_GRPC_ALLOWED_SCHEMES` | policy default | comma-separated schemes |
| `PROTOMOLT_GRPC_ALLOWED_HOSTS` | policy default | comma-separated hosts |
| `PROTOMOLT_GRPC_ALLOWED_PORTS` | policy default | comma-separated ports |
| `PROTOMOLT_GRPC_ALLOW_PLAINTEXT` | `true` | boolean, fails fast otherwise |
| `PROTOMOLT_GRPC_ALLOW_TLS` | `true` | boolean |
| `PROTOMOLT_GRPC_MAX_DEADLINE_MS` | `60000` | integer |
| `PROTOMOLT_GRPC_MAX_ACTIVE_CHANNELS` | `64` | integer |
| `PROTOMOLT_INFERENCE_MODELS` | unset = empty catalog | model catalog entries |
| `PROTOMOLT_TASK_CONSOLE_TOKEN` | unset = console sessions off | token text |
| `PROTOMOLT_TASK_CONSOLE_SESSION_SECONDS` | `43200` | integer |
| `PROTOMOLT_DELEGATION_REPO_ENDPOINT` | unset = in-memory transcripts | repo-service `host:port` |
| `PROTOMOLT_DELEGATION_REPO_TLS` | `false` (plaintext) when unset | strictly `true` or `false`, anything else fails fast naming the variable; setting it at all without the repo endpoint is refused at startup |
| `PROTOMOLT_DELEGATION_REPO_DRIVE` | unset | drive name |
| `PROTOMOLT_DELEGATION_TRANSCRIPT_OBJECT` | unset | object key |
| `PROTOMOLT_DELEGATION_STATE_KEY_REF` | unset | `env:<NAME>` credential reference for the AES key |
| `PROTOMOLT_MESH_CLUSTER_ID` | unset | cluster identity |
| `PROTOMOLT_MESH_CLUSTER_NAME` | unset | cluster name |
| `PROTOMOLT_MESH_TRUST_DOMAIN` | unset | trust domain |
| `PROTOMOLT_MESH_CREATED_AT` | unset | timestamp |
| `PROTOMOLT_CORRECTION_TARGET` | unset = correction starter off | `host:port` only; any other URI shape fails fast |
| `PROTOMOLT_CORRECTION_API_TOKEN` | required with the correction target | token; resolved through the `env:` credential scheme |

## Authorization (`surface/authz`, `surface/authz-jdbc`)

`CallerStoreConfig.fromEnvironmentMap` has deliberately NO localhost
fallback: the JDBC URL, username and password are all required by name once
the store is selected, and the pool size fails fast on a malformed or
non-positive value. `OidcCallerResolver` is off while the introspection URL
is unset; a set URL without both client credentials refuses by name.

| Variable | Default | Accepted |
|---|---|---|
| `PROTOMOLT_AUTHZ_KEYS_JDBC_URL` | unset = JDBC caller store off | JDBC URL |
| `PROTOMOLT_AUTHZ_KEYS_USERNAME` | required with the URL | text |
| `PROTOMOLT_AUTHZ_KEYS_PASSWORD` | required with the URL | text |
| `PROTOMOLT_AUTHZ_KEYS_POOL_SIZE` | `10` | positive integer, fails fast otherwise |
| `PROTOMOLT_AUTHZ_OIDC_INTROSPECTION_URL` | unset = OIDC off | URL |
| `PROTOMOLT_AUTHZ_OIDC_CLIENT_ID` | required with the URL | text |
| `PROTOMOLT_AUTHZ_OIDC_CLIENT_SECRET` | required with the URL | text |

## Pull connectors (`acquire/s3`, `acquire/jdbc`, `acquire/pull`)

| Variable | Default | Accepted |
|---|---|---|
| `PROTOMOLT_ACQUIRE_API_KEY` | required by both pull modules | the intake API key; unset fails fast |
| `PROTOMOLT_ACQUIRE_S3_REGION` | required | region; unset fails fast |
| `PROTOMOLT_ACQUIRE_S3_ENDPOINT` | unset | endpoint override |
| `PROTOMOLT_ACQUIRE_S3_ACCESS_KEY` | unset = SDK chain | text |
| `PROTOMOLT_ACQUIRE_S3_SECRET_KEY` | unset | text |
| `PROTOMOLT_ACQUIRE_JDBC_URL` | required | JDBC URL; unset fails fast |
| `PROTOMOLT_ACQUIRE_JDBC_USERNAME` | unset | text |
| `PROTOMOLT_ACQUIRE_JDBC_PASSWORD` | unset | text |
| `PROTOMOLT_ACQUIRE_INTAKE_PLAINTEXT` | `false` (TLS) | `true`/`false`/`1`/`0`, case-insensitive, trimmed; unset or blank keeps TLS, anything else fails fast naming the variable |

## Receipts and trust (`transform/workflow`)

`RecordSigning.fromEnvironment` treats the three signing variables as a
group: all unset = signing off; a partial set fails naming the missing
members. `TrustPin` is off while unset; a set path that cannot be read
fails at load.

| Variable | Default | Accepted |
|---|---|---|
| `PROTOMOLT_RECEIPT_KEY_FILE` | unset | path to the Ed25519 key |
| `PROTOMOLT_RECEIPT_KEY_ID` | unset | key id |
| `PROTOMOLT_RECEIPT_ISSUER` | unset | issuer name |
| `PROTOMOLT_TRUST_SNAPSHOT` | unset = no pinned trust | path to the trust snapshot |

## Search providers (`search/embedding/*`, `search/rerank/*`, `search/index/qdrant`)

Each provider resolves its target lazily on first use, from a system
property first and the environment variable second, so an unconfigured
provider on the classpath never fails until actually used; an unset target
at first use is the failure.

| Variable | Default | Accepted |
|---|---|---|
| `PROTOMOLT_TEI_TARGET` | unset | TEI gRPC `host:port` |
| `PROTOMOLT_TEI_TRUNCATE` | `true` | `false` rejects overlength inputs instead of truncating server side |
| `PROTOMOLT_OVMS_TARGET` | unset | OVMS gRPC `host:port` |
| `PROTOMOLT_OVMS_MODEL` | unset | model name |
| `PROTOMOLT_OVMS_INPUT` | provider default | input tensor name |
| `PROTOMOLT_OVMS_OUTPUT` | provider default | output tensor name |
| `PROTOMOLT_MODEL2VEC_PATH` | unset | path to the Model2Vec model |
| `PROTOMOLT_RERANK_TEI_TARGET` | unset | TEI rerank gRPC `host:port` |
| `PROTOMOLT_RERANK_OVMS_URL` | unset | OVMS rerank REST URL |
| `PROTOMOLT_RERANK_OVMS_MODEL` | unset | model name |
| `PROTOMOLT_QDRANT_TARGET` | unset | Qdrant gRPC `host:port` |
| `PROTOMOLT_QDRANT_COLLECTION` | unset | collection name |
| `PROTOMOLT_QDRANT_API_KEY` | unset | API key |
| `PROTOMOLT_QDRANT_USE_TLS` | provider default | boolean |

## Agent host (`apps/agent-host`)

Read by `AgentHostMain.Options`: each variable supplies the default for the
matching command-line flag (blank counts as unset); the flags document the
required set.

| Variable | What it is |
|---|---|
| `PROTOMOLT_AGENT_MCP_ENDPOINT` | the coordinator MCP endpoint |
| `PROTOMOLT_AGENT_ROLE` | `worker` or `coordinator` |
| `PROTOMOLT_AGENT_ID` | the agent identity |
| `PROTOMOLT_AGENT_PROVIDER` | provider name (`codex`, `kimi`, `openai`, `cursor`, `antigravity`, `muse`) |
| `PROTOMOLT_AGENT_MODEL` | provider model |
| `PROTOMOLT_AGENT_WORKSPACE` | workspace directory |
| `PROTOMOLT_AGENT_STATE` | state directory |
| `PROTOMOLT_AGENT_TOKEN_ENV` | the NAME of another variable holding the MCP token |
| `PROTOMOLT_AGENT_BOOTSTRAP` | bootstrap file path |
| `PROTOMOLT_AGENT_PROVIDER_ENDPOINT` | provider endpoint URI |

## gRPC tools over MCP (`apps/grpc-mcp`)

| Variable | Default | Accepted |
|---|---|---|
| `PROTOMOLT_GRPC_TARGET` | required | `host:port` the stdio MCP server reflects and invokes |
| `PROTOMOLT_GRPC_TRANSPORT` | plaintext unless exactly `tls` | `tls` or `plaintext` |

## Correction sample (`apps/correction`)

| Variable | Default | Accepted |
|---|---|---|
| `PROTOMOLT_CORRECTION_ENDPOINT` | required in live mode | endpoint; unset fails fast |
| `PROTOMOLT_CORRECTION_MODEL` | required in live mode | backend model |
| `PROTOMOLT_CORRECTION_CREDENTIAL_REF` | unset | `env:<NAME>` credential reference |
| `PROTOMOLT_INFERENCE_API_TOKEN` | unset | token for the Kimi inference bridge |
| `PROTOMOLT_KIMI_EXECUTABLE` | `kimi` | executable name |
| `PROTOMOLT_KIMI_MODEL` | `kimi-code/k3` | model name |

## Samples (`samples`)

| Variable | Default | Accepted |
|---|---|---|
| `PROTOMOLT_AUTHOR_TOKEN` | required | the authoring worker's token; unset fails fast |
| `PROTOMOLT_KAFKA_BOOTSTRAP_SERVERS` | required by the Kafka bridge | bootstrap servers |
| `PROTOMOLT_KAFKA_ACCEPTED_LAUNCH_TOPIC` | required | topic |
| `PROTOMOLT_KAFKA_ACCEPTED_LAUNCH_GROUP` | required | consumer group |
| `PROTOMOLT_COORDINATOR_TARGET` | required | coordinator `host:port` |
| `PROTOMOLT_WORKFLOW_LAUNCH_TOKEN_FILE` | required | path to the launch token |
| `PROTOMOLT_LAUNCH_RPC_TIMEOUT_SECONDS` | bridge default | positive integer |
| `PROTOMOLT_COORDINATOR_PLAINTEXT` | `false` (TLS) | `true`/`false`/`1`/`0`, case-insensitive, trimmed; unset or blank keeps TLS, anything else fails fast naming the variable |
| `PROTOMOLT_KAFKA_CLIENT_PROPERTIES_FILE` | unset | path to extra Kafka client properties |

## Confluence connector (`acquire/confluence`)

Read by `ConfluenceConnectorConfig`, `ConfluenceProxyServer`,
`ConfluenceLuceneProjector` and the two Parquet sinks. The three
credentials are required and fail fast by name; numbers in the proxy fail
fast naming the variable.

| Variable | Default | Accepted |
|---|---|---|
| `CONFLUENCE_BASE_URL` | required | the wiki base URL |
| `CONFLUENCE_EMAIL` (alias `CONFLUENCE_USER`) | required | account email; the canonical name wins when both are set |
| `CONFLUENCE_API_TOKEN` (alias `CONFLUENCE_TOKEN`) | required | API token, redacted in logs |
| `CONFLUENCE_SPACES` | all spaces | comma-separated space keys |
| `CONFLUENCE_PAGE_SIZE` | `100` | positive integer; a malformed or non-positive value fails fast naming the variable (#480) |
| `CONFLUENCE_BODY_FORMAT` | `storage` | `storage` or the other supported body format; anything else fails fast |
| `CONFLUENCE_KAFKA_BOOTSTRAP_SERVERS` | unset = Kafka sink off; required by the Lucene projector | bootstrap servers |
| `CONFLUENCE_KAFKA_TOPIC` | `confluence-events` | topic |
| `CONFLUENCE_KAFKA_SNAPSHOTS_TOPIC` | `confluence-snapshots` | topic |
| `CONFLUENCE_SCHEMA_REGISTRY_URL` | unset | URL |
| `CONFLUENCE_REPO_TARGET` | unset = repo sink off | repo-service `host:port` |
| `CONFLUENCE_REPO_DRIVE` | `default` | drive |
| `CONFLUENCE_REPO_ACCOUNT_ID` | `confluence` | account id |
| `CONFLUENCE_REPO_DATASOURCE_ID` | `confluence` | datasource id |
| `CONFLUENCE_GRPC_PORT` | `9095` | integer, fails fast otherwise |
| `CONFLUENCE_ATTACHMENT_MAX_BYTES` | service default | integer, fails fast otherwise |
| `CONFLUENCE_LUCENE_INDEX_DIR` | required by the projector | index directory |
| `CONFLUENCE_LUCENE_GROUP_ID` | `confluence-lucene-projector` | consumer group |
| `CONFLUENCE_PARQUET_S3_BUCKET` | unset = sink off | bucket |
| `CONFLUENCE_PARQUET_S3_REGION` | `us-east-1` | region |
| `CONFLUENCE_PARQUET_S3_ENDPOINT` | unset | endpoint override |
| `CONFLUENCE_PARQUET_S3_ACCESS_KEY_ID` | unset = SDK chain | set both keys or neither |
| `CONFLUENCE_PARQUET_S3_SECRET_ACCESS_KEY` | unset | paired |
| `CONFLUENCE_PARQUET_S3_PREFIX` | `confluence-changes` | key prefix |
| `CONFLUENCE_PARQUET_S3_BATCH_SIZE` | `500` | positive integer; a malformed or non-positive value fails fast naming the variable (#480) |
| `CONFLUENCE_PROJECTED_PARQUET_S3_BUCKET` | unset = sink off | bucket |
| `CONFLUENCE_PROJECTED_PARQUET_S3_REGION` | `us-east-1` | region |
| `CONFLUENCE_PROJECTED_PARQUET_S3_ENDPOINT` | unset | endpoint override |
| `CONFLUENCE_PROJECTED_PARQUET_S3_ACCESS_KEY_ID` | unset | set both keys or neither |
| `CONFLUENCE_PROJECTED_PARQUET_S3_SECRET_ACCESS_KEY` | unset | paired |
| `CONFLUENCE_PROJECTED_PARQUET_S3_PREFIX` | `confluence-content-rows` | key prefix |
| `CONFLUENCE_PROJECTED_PARQUET_S3_BATCH_SIZE` | `500` | positive integer; a malformed or non-positive value fails fast naming the variable (#480) |

## Parser adapters and playground (`parse/grparse`, `parse/playground`)

| Variable | Default | Accepted |
|---|---|---|
| `PARSE_GRPARSE_TARGET` | required | the gRParse endpoint `host:port`; unset fails fast |
| `PARSE_GRPARSE_PORT` | `9096` | port 0..65535, fails fast otherwise |
| `PARSE_GRPARSE_EMITS_PREVIEWS` | adapter default | exactly `true` or `false`, fails fast otherwise |
| `PARSE_PLAYGROUND_PARSER_TARGET` | unset = embedded demo text parser | parser `host:port` |
| `PARSE_PLAYGROUND_HTTP_PORT` | `8095` | port 1..65535; a malformed, non-positive or >65535 value fails fast naming the variable |

## Inference credentials

`OPENAI_TOKEN` is not read directly by any service; it is the conventional
target of the `env:OPENAI_TOKEN` credential reference in the inference
catalog (`EnvCredentialResolver`). Any variable name can be used; the
resolver fails fast on an unset or empty variable without naming it.

## Corpus seeder (`repo/fixtures`)

The dev corpus seeder (`CorpusSeederMain`) reads the repo service family above
for the ledger and object store, plus:

| Variable | Default | Accepted |
|---|---|---|
| `PROTOMOLT_CORPUS_SEED` | unset = the seeder exits with status 2 before touching the store | exactly `dev`; the compose dev profile sets it |
| `PROTOMOLT_CORPUS_SEED_VALUE` | `542` (the default corpus) | integer; the corpus seed |
| `PROTOMOLT_CORPUS_LARGE_BLOB_BYTES` | `3145745` (3 MiB plus 17) | integer >= 1; the length of the streamed large blob |

## Opt-in browser test (`repo/fixtures`)

Read by Gradle, not by any service: `optInSuite` in the root build leaves the
suite out of the `test` and `integrationTest` tasks unless the variable is set,
so a plain run neither runs nor skips it.

| Variable | Default | Accepted |
|---|---|---|
| `PROTOMOLT_CONSOLE_BROWSER_IT` | unset = the console browser leg is left out | exactly `1`; runs `ConsoleBrowserIT`, which boots the seeded repository host with Keycloak sign-in and runs the Playwright specs in `apps/console/e2e` against it. Needs `npm ci && npm run build` in `apps/console` and Playwright's Chromium first |
