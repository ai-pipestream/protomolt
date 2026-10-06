# Run or embed a bounded Redis archive

Use this profile when a Java application needs managed archive entries, retained
versions and optional gRPC access backed by Redis. It uses PostgreSQL for repository
metadata and retention tracking. Redis stores immutable payload objects. It supports
bounded unary writes; document pipelines, bridge generation and streaming uploads
are unavailable in this profile.

The Java API lives in `protomolt-repo-service`. Select it explicitly with
`RepoServices.buildBoundedArchive`; the existing `RepoServiceMain` does not select
this profile from environment settings alone.

## Run a standalone process

`RepoBoundedArchiveMain` starts this profile without an embedding application.
Provide the storage settings below, PostgreSQL connection settings
(`DOCUMENT_PLATFORM_JDBC_URL`, `DOCUMENT_PLATFORM_USERNAME`,
`DOCUMENT_PLATFORM_PASSWORD`), and `PROTOMOLT_API_TOKEN` through your process's
secret configuration. Select an account and drive explicitly:

```sh
export DOCUMENT_PLATFORM_ARCHIVE_ACCOUNT=desktop
export DOCUMENT_PLATFORM_ARCHIVE_DRIVE=storage
export DOCUMENT_PLATFORM_GRPC_PORT=9090
./gradlew :protomolt-repo-service:installDist
java -cp 'repo/service/build/install/protomolt-repo-service/lib/*' \
  ai.protomolt.proto.repo.service.RepoBoundedArchiveMain
```

The launcher provisions the drive through the local repository API before opening
the archive listener. It reports `PROTOMOLT_ARCHIVE_READY port=<bound-port>` after
startup. An existing drive keeps its stored location when host defaults change;
an inactive drive or one belonging to a different selected provider prevents
startup. Clients can then create an archive on that drive through ArchiveService.
The credential has the process-level access described below, not per-account scope.

The standalone limits default to a 1 MiB object, 2 MiB request and read response, 16 renditions,
14 MiB payload budget and four concurrent requests. Override them with
`DOCUMENT_PLATFORM_ARCHIVE_MAX_OBJECT_BYTES`,
`DOCUMENT_PLATFORM_ARCHIVE_MAX_REQUEST_BYTES`,
`DOCUMENT_PLATFORM_ARCHIVE_MAX_RESPONSE_BYTES`,
`DOCUMENT_PLATFORM_ARCHIVE_MAX_RENDITIONS`,
`DOCUMENT_PLATFORM_ARCHIVE_PAYLOAD_BUDGET_BYTES`, and
`DOCUMENT_PLATFORM_ARCHIVE_MAX_CONCURRENT_REQUESTS`. Invalid configured values
fail startup. Redis's configured object cap must cover the archive object limit;
its TTL must explicitly be zero.
When omitted, the response limit follows the configured request limit. It includes
the complete GetEntry metadata, manifest, selected bytes and protobuf framing;
select fewer renditions when a whole entry exceeds that limit.

SIGTERM closes admission and waits for accepted work. A drain timeout keeps the
shutdown hook alive and retries; other shutdown errors are reported as failures.
The process test covers a write blocked at SQL commit beyond ten seconds and reads
the committed version after restart. Delayed Redis I/O and forced-kill recovery
are separate qualification work. Choose an external supervisor's termination grace
period accordingly; a forced kill is not graceful shutdown.

## Configure storage

Build a `RepoServiceConfig` directly or read the existing configuration through
`RepoServiceConfig.fromEnvironment()`. Supply PostgreSQL connection settings and
these storage settings:

The environment parser rejects malformed limits and flags, out-of-range ports,
nonpositive pool sizes, and present-but-blank defaulted service settings. Omit an
optional setting to use its documented default. A blank storage selector does not
select S3, and a misspelled lifecycle flag does not disable recovery. Error messages
identify the setting without echoing numeric, flag or selector values.

```sh
DOCUMENT_PLATFORM_BLOB_STORE=redis
DOCUMENT_PLATFORM_REDIS_URI=redis://localhost:6379
DOCUMENT_PLATFORM_REDIS_TTL_SECONDS=0
DOCUMENT_PLATFORM_REDIS_MAX_OBJECT_BYTES=1048576
DOCUMENT_PLATFORM_MANAGED_BACKEND_GENERATION=desktop-archive-v1
DOCUMENT_PLATFORM_MANAGED_STORAGE_REALM=desktop
DOCUMENT_PLATFORM_MANAGED_RETENTION_QUALIFIED=true
DOCUMENT_PLATFORM_LIFECYCLE_ENABLED=true
```

Retention qualification is an operator decision. Redis must not expire or evict
referenced objects, and persistence must meet the deployment's recovery needs.
The process-crash tests use AOF with `appendfsync always` and `noeviction`; they do
not establish power-loss durability. The backend generation binds permanently to
a physical identity. Changing Redis database or namespace requires a new generation.

## Open the archive host

The limits below allow 1 MiB per object, 2 MiB per complete request, 16 renditions,
a shared 64 MiB input budget and at most four admitted requests per admission gate.

```java
import ai.protomolt.proto.repo.service.BoundedArchiveOptions;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.service.RepoServices;

var limits = new BoundedArchiveOptions(
        1024 * 1024, 2 * 1024 * 1024, 16, 64L * 1024 * 1024, 4);
var config = RepoServiceConfig.fromEnvironment();
try (var host = RepoServices.buildBoundedArchive(config, limits)) {
    var drives = host.driveRepository();
    var archives = host.archiveRepository();
    // Provision a drive, create an archive, then write and read entries through these ports.
}
```

The [embedding test](../../repo/service/src/test/java/ai/protomolt/proto/repo/service/consumer/BoundedArchiveEmbeddingIT.java)
contains the complete drive/archive creation, write, retry and historical-read calls.
It runs against real PostgreSQL and Redis containers using only public composition
methods. Local operations require a trusted `RepositoryCaller`; creating that value
is the embedding application's authentication responsibility.

## Add authenticated gRPC

While the host is open, call:

```java
var server = host.startBoundedArchiveNetty(9090, operatorApiToken);
```

Use a nonblank token supplied through the application's secret configuration.
Clients send it in `api_token` metadata. This operator credential grants access
across accounts. The listener is plaintext, so use a trusted network or TLS
termination for remote access. The host owns the listener: close the host through
the application's shutdown lifecycle, and retry close if a drain timeout preserves
resources for work still running.

Only the ten supported unary ArchiveService methods are enabled. Provision drives
through the local port before remote archive creation. BridgeEntry and UploadRendition
return UNIMPLEMENTED before their handlers run. Document, drive, reflection and
health RPCs are not mounted here. General `startNetty`, `startInProcess`, `services`
and HTTP startup remain unavailable for this profile.

## What the limits cover

All bounded listeners on a host share an ingress gate. Local and remote archive
writes and read construction use shared gates, all reserving from one byte budget.
GetEntry reserves an additional response allowance at RPC admission, held through
the terminal callback. Cancellation does not release reservations while synchronous
provider work is still running.
Retrying after a cancelled successful publication reuses the committed version.

The budget must cover both seven times the request limit (one maximum write) and
two times the request limit plus four times the response limit (one maximum read).
Concurrent work can still receive RESOURCE_EXHAUSTED when slots or bytes are exhausted.
These are payload/copy and serialized-response allowances; they do not measure
decoded object heap, network buffers or protobufs retained by local callers after
return. Metadata/list response bounds remain separate. Bounded reads refuse legacy
renditions without published storage identities before provider access.
Redis objects also remain subject to the provider's 9 MiB create-only ceiling.
