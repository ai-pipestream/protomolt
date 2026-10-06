# Embed a bounded Redis archive

Use this profile when a Java application needs managed archive entries, retained
versions and optional gRPC access backed by Redis. It uses PostgreSQL for repository
metadata and retention tracking. Redis stores immutable payload objects. It supports
bounded unary writes; document pipelines, bridge generation and streaming uploads
are unavailable in this profile.

The Java API lives in `protomolt-repo-service`. Select it explicitly with
`RepoServices.buildBoundedArchive`; the existing `RepoServiceMain` does not select
this profile from environment settings alone.

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
writes use the same write gate, and both gates reserve from one byte budget.
Cancellation does not release reservations while a provider write is still running.
Retrying after a cancelled successful publication reuses the committed version.

The budget must be at least seven times the request limit, enough for one maximum
transport write. Concurrent work can still receive RESOURCE_EXHAUSTED when either
slots or bytes are exhausted. These are serialized-input and copy allowances;
they do not measure decoded object heap, read responses or earlier network buffers.
Redis objects also remain subject to the provider's 9 MiB create-only ceiling.
