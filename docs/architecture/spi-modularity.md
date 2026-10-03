# SPI composition and dependency boundaries

## Repository codec extraction

`protomolt-repo-blob-spi` separately supplies `BlobStore` with JDK-only production
signatures and no production library dependencies. Callers must change imports
from `ai.protomolt.proto.repo.container.blob.BlobStore` to
`ai.protomolt.proto.repo.blob.spi.BlobStore` and recompile. Container adapter APIs
that mention this interface or moved codec types also require recompilation.
Provider implementations live in `protomolt-repo-blob-s3`,
`protomolt-repo-blob-redis` and `protomolt-repo-blob-cache`. Update imports for
`S3BlobStore`, `RedisBlobStore`/`RedisBlobStoreConfig` and `CachingBlobStore`
to `.repo.blob.s3`, `.repo.blob.redis` and `.repo.blob.cache` respectively.
The cache uses the optional `ExpiringBlobStore` capability instead of depending
on Redis. The existing service assembly explicitly composes these providers; provider
factory discovery is available through `BlobStores.discover()` in the byte SPI.
The service assembly uses this factory API for S3 and Redis, including the two
handles in its cache composition. It registers each acquired resource immediately;
failed construction releases earlier resources in reverse order. Shutdown attempts
all owned resources and reports failures with secondary failures suppressed.
`RemoteBlobStore` now lives in `protomolt-repo-blob-grpc`. Update its import
from `ai.protomolt.proto.repo.service.client.RemoteBlobStore` to
`ai.protomolt.proto.repo.blob.grpc.RemoteBlobStore` and recompile. Its public
constructor borrows a generated blocking stub; the caller owns the channel.
The client artifact excludes the repository server, SQL, Kafka and provider SDKs.
The client uses an immutable one-to-one map from local bucket names to remote
drive names. Single-drive constructors bind the drive name as the local bucket
name too; passing arbitrary ignored buckets is no longer supported. Unknown
buckets fail before RPC. Object keys and version IDs are preserved. Uploads accept
at most 9 MiB of data and a 10 MiB serialized request, leaving room for protobuf
fields within the service limit. Stream uploads require an exact nonnegative
length and read at most that length plus one byte; a mismatch, oversized body
or supplied checksum mismatch fails before the RPC. The caller owns the stream.
The existing 9 MiB conditional-write limit is unchanged. This does not add
multi-drive routing or streaming RPCs.
Each RPC has a fresh 30-second timeout by default; a constructor overload accepts
a positive `Duration`. Any shorter deadline on the supplied stub or current gRPC
context remains effective. The client does not retry writes after a timeout,
because a timed-out call may already have committed remotely.
Service remote modes require `DOCUMENT_PLATFORM_REPO_BUCKET_BINDINGS` as a JSON
object, for example `{"documents-acct-input":"upstream-input"}`. Embedders use
`config.withRepoBucketBindings(Map.of(localBucket, remoteDrive))`. The legacy
`repoDrive` field is retained for Java constructor compatibility but no longer
selects routing. Operators must map existing ledger buckets to their existing
remote drives. Each remote drive row must also persist a `RemoteDriveConfig`
with the exact endpoint and remote drive name. The drive provider records the
transport. Loaded rows with absent or mismatched binding metadata fail closed;
changing routing requires an explicit storage migration and verified row update. There is no automatic fallback or key rewriting. Multiple
local buckets cannot map to the same remote drive. Unmapped persisted drives
fail with `FAILED_PRECONDITION`. Remote namespace provisioning remains unsupported.
Startup rejects a remote target matching this process's in-process listener name.
For TCP, supported targets are `host:port` and `dns:///host:port`; the actual bound
port is compared with the target, and matching local/loopback addresses are
rejected. A failed check closes the newly bound listener and its executor.
This checks direct routing at startup, not later DNS changes or cycles through
proxies or other nodes; per-call deadlines still bound those calls.
Remote gRPC storage still uses its explicitly owned channel and is not yet a
discovered provider.

Discovery opens no stores. `open(id, options, requiredCapabilities)` requires an
installed provider and returns an `OpenedBlobStore` whose lifetime belongs to the
caller. Missing or duplicate IDs and invalid options fail explicitly. An
incompatible capability selection closes the acquired handle and fails, retaining
any cleanup failure. Closing a handle attempts cleanup once and forbids further
access through that handle; previously obtained store references must not be used
after closure. Factory-created clients are owned, not borrowed.

The Redis factory requires `uri`, `ttl-seconds`, `max-object-bytes`, and `key-prefix`.
The S3 factory requires `endpoint`, `region`, `access-key`, `secret-key`,
`path-style`, and `conditional-writes`. Booleans accept only `true` or `false`.
The original key-pair option form selects static credentials. Alternatively,
`credentials-mode=static` requires that pair, while `credentials-mode=default-chain`
forbids it and explicitly selects the AWS credential chain. An empty `endpoint`
selects the regional AWS endpoint; it is not a fallback for malformed input.
The factory sets finite timeouts. Optional millisecond settings are
`api-call-timeout-ms` (default 300000), `api-attempt-timeout-ms` (60000),
`connection-timeout-ms` (10000), and `socket-timeout-ms` (60000). Values must be
positive integers no greater than 2147483647, with connection and socket timeouts
no greater than the attempt timeout, and the attempt timeout no greater than the
whole-call timeout. These budgets apply to SDK operations, including writes;
large transfers may need explicit tuning. They are operational settings and do
not change a backend's physical identity. Hosts supplying an already-constructed
client remain responsible for that client's timeouts.
The factory owns the client and any closeable credential provider. Setting
`conditional-writes=true` is an operator assertion that the endpoint has been
qualified; discovery does not establish remote atomicity. The tests qualify the
existing pinned RustFS endpoint with competing conditional writes. Redis does not
advertise conditional writes. The cache remains an explicitly composed decorator.

`protomolt-repo-codec` owns descriptor-driven document splitting, part layouts,
manifest encoding and typed reassembly. It depends on repository protobuf
contracts and protobuf libraries, without storage providers, SQL or Kafka.
`protomolt-repo-container` selects this library for its storage engine.

Java consumers of `DocumentPartCodec`, `PartLayout`, `PartLayouts` and `PartObject`
must update imports from `ai.protomolt.proto.repo.container.codec` to
`ai.protomolt.proto.repo.codec` and recompile. Direct codec consumers should select
`protomolt-repo-codec`. No protobuf identity or part byte format changes.
Existing byte-fidelity and edge-case tests move with the implementation. A runtime
dependency gate rejects repository implementations and storage/database SDKs.

The remaining repository composition work is tracked in
[the design](../design/repository-composition.md); this extraction does not add
authorization, typed admission or progressive hydration.

The service creates an S3 client only for S3 and S3-with-cache modes. Drive
provisioning uses a namespace capability: S3 creates/verifies buckets; Redis
verifies its server and uses logical namespaces. Remote blob storage does not
currently support namespace provisioning and refuses that operation.

This assembly selects one backend. New and resolved drives must match its
provider and effective provider settings. Per-drive credential references and
unimplemented provider options fail explicitly. Existing records describing a
different endpoint/backend need an explicit data/configuration migration; changing
the service backend does not migrate their bytes. The read gate is backend
compatibility checking, not tenant authorization. Raw blob lookup rejects a bare
drive name shared by multiple accounts instead of selecting the first row.

This work separates reusable protocol and dispatch code from optional capabilities.
Java SPI discovers trusted implementations installed in the application. gRPC remains
the boundary for services implemented in other languages or running on other nodes.

## Required outcomes

1. **Actions:** keep public action types, dispatch, authorization, budgets and contract
   validation usable without toolkit implementations. Load action families through
   `ActionProvider`; make schema compilation and named toolkit contracts optional.
2. **MCP:** separate protocol handling from registry, workspace, workflow, delegation,
   code generation and Git capabilities. Preserve a full assembly and provide a
   minimal, usable gRPC integrator assembly.
3. **Service modules:** reuse `ServiceModule`, `NodeContext`, `ServiceMount` and
   `Composer` for configured capability discovery and lifecycle. Remove concrete
   capability construction from general launchers where it prevents independent use.
4. **Registry:** separate store interfaces from Git and workflow implementations so
   applications select their storage without taking unrelated implementation dependencies.
5. **CEL:** extract shared CEL support used by mapping and validation, preserving
   validation semantics and conformance.

## Rules for implementation

- An SPI/core module must not depend on its implementation modules. Application
  assemblies select implementations with runtime dependencies.
- Do not split a production Java package across modules. Preserve existing public
  types where feasible; record required dependency or source migration explicitly.
- Discovery must not start workers, open network connections or load models.
  Configure selected capabilities before starting them. Unselected capabilities must
  not require valid operational configuration.
- Missing selected providers, duplicate identities, invalid configuration and provider
  construction failures must be explicit failures. Do not silently select a different
  provider or replace invalid values with guessed configuration.
- Preserve causes when translating failures. Cleanup failures must remain observable;
  preserve the original failure and attach cleanup failures where appropriate.
- Keep validation and authorization in the dispatch boundary. Providers must not
  advertise capabilities that are not installed or ready to be served.
- Preserve shared budget accounting, cancellation and resource ownership. Shutdown
  and failed startup must release acquired resources in reverse order.
- A module extraction must remove dependencies for a supported consumer or establish
  an independently useful capability. Class splitting alone is not a packaging result.

## Verification

For each boundary, test a minimal consumer without optional implementation JARs and
a full assembly with its existing capabilities. Exercise missing, duplicate and broken
providers. Check generated Maven and Gradle metadata, classpath dependency gates,
existing affected tests and startup/shutdown behavior. Record before/after dependency
counts separately from runtime or performance claims.

The extracted boundaries and their verification paths are described below.
Local checks do not establish publication or deployment; consumers of published
artifacts must use a version containing the relevant modules.

## MCP transport boundary

`protomolt-mcp-transport` contains JSON-RPC handling, MCP sessions, stdio,
resource composition and the catalog-backed workspace resource. Its production
dependencies are the action core and SLF4J. It does not discover or construct
registry, workflow, delegation, Git, indexing or code-generation implementations.
An embedding application supplies its `ActionCatalog` and optional `McpResources`.
The runtime dependency gate rejects those optional implementation modules.

The existing `protomolt-mcp` remains the full distribution. Its public server,
session and resource types delegate to the transport module, retaining the
original resource page type and registry constructor. Existing callers need no
import changes. New minimal embeddings use `ai.protomolt.proto.mcp.transport`.
The `protomolt-grpc-mcp` assembly loads the `grpc-invoke` service module at runtime,
requiring an explicit target and transport. See [gRPC tools over MCP](../apps/grpc-mcp.md)
for its scope and local build instructions.

Unexpected asynchronous response failures are retained by the session even after
the failed task leaves the in-flight map. Subsequent handling, submission,
dispatch, draining and close report the original cause. Additional concurrent
failures are logged. A drain timeout or interruption is explicit, and interruption
preserves the thread flag. Normal tool error responses and explicit request
cancellation do not poison a session. Cancellation cleanup removes only its own
request, preserving a later request that reused the same ID.

Composer shutdown attempts all resources and channels before reporting cleanup
failures. A startup failure retains its original cause and carries cleanup
failures as suppressed exceptions. Channel termination timeout and interruption
are failures; interruption preserves the thread flag.

`PROTOMOLT_ROLES` requires a name between every comma. Leading, trailing or repeated
separators and whitespace-only entries fail before any role is wired. Spaces around
valid names and case normalization remain supported. A comma-only value cannot
start a node with no mounted capabilities.

## Shared CEL foundation

`protomolt-cel` supplies the existing environment factory, evaluator, expression
validation and exception types in `ai.protomolt.proto.cel`. It depends on CEL,
protobuf and SLF4J, with no mapping implementation. Protobuf validation now uses
this foundation directly; a runtime gate prohibits both mapper modules. Quality,
metric and parser routing code also select the foundation where mapping is unused.

`protomolt-mapper-cel` supplies `CelProtoMapper` and `CelMappingRule` in
`ai.protomolt.proto.mapper.cel`, depending on the foundation and field mapper.
**Java migration:** callers of these two classes must update their imports from
`ai.protomolt.proto.cel` and recompile. Public Java signatures referring to them
change with the package move. Keeping wrappers in the old package would split
that package across JARs, against the module rules. Shared evaluator types retain
their names; consumers using only those types can depend on `protomolt-cel`.
No protobuf definitions or stored descriptor identities change in this extraction.

The extraction preserves evaluation behavior in a separate commit. The subsequent
failure-handling change makes `evaluateBoolean`, `tryMap` and `mapFirstCandidate`
propagate compilation and evaluation errors. Only a successfully evaluated false
filter skips a candidate; a malformed expression cannot select a later fallback.
Filtered-out selectors remain unevaluated. Callers relying on error-driven fallback
must express the intended presence or eligibility condition as a filter.

## Registry storage boundary

`protomolt-registry` contains storage contracts, in-memory storage and shared
registration/descriptor-set validation. Its runtime dependency gate rejects
JGit and the Git and workflow implementation modules.

`protomolt-schema-registry-git` contains `GitSchemaRegistryStore`,
`RegistryFederation` and `RegistryWorkflowVersionRepository`. Applications that
use these implementations must select the optional module explicitly. This
module registers a storage provider for the generic registry role described below.
Existing full launchers retain their explicit Git assembly.

**Java migration:** the three implementation classes move from
`ai.protomolt.proto.registry` to `ai.protomolt.proto.schema.registry.git`.
Consumers must add the new dependency, update imports and recompile. Storage
contracts retain their package. Existing persisted formats and protocol definitions
are unchanged by this extraction. `RegistrationSupport` and `DescriptorSetArtifacts`
are public shared helpers so backends reuse the core checks.

A shared test-fixtures variant exercises the same storage contract against both
in-memory and Git implementations. Test dependencies remain separate from the
production runtime. The Git module includes workflow version storage and the
gRPC workflow contracts. It does not depend on the workflow execution engine.
Moving the repository adapter by itself would not remove those contracts: the
store uses their validation and versioned message format for workflow documents.

Registry metadata parsing also rejects malformed stored compatibility policies and
invalid global counters instead of treating them as defaults. Absence of a
subject override still means inheritance; a malformed override does not. A listed
schema version missing from storage fails history assembly so the compatibility
gate cannot evaluate an incomplete history.

Optional `WorkflowDocumentStore` and `ConfigDocumentStore` capabilities extend
the schema store contract for document storage. HTTP routes select these
interfaces rather than checking for Git. Non-Git test backends exercise workflow
put/get/list and typed-config HTTP/action publication. Git implements both.
`PublishConfigAction` now accepts `ConfigDocumentStore`; source callers passing a
Git store still compile, but existing binaries must be recompiled for the changed
constructor signature. Publication errors retain their original cause.

The generic `protomolt-registry-service` no longer assembles Git federation,
workflow runtime or toolkit/index action providers. The optional
`protomolt-schema-registry-git-service` supplies `RegistryModule`,
`RegistryRemotesAction` and `RegistrySyncAction` in
`ai.protomolt.proto.schema.registry.git.service`; consumers of those classes
must add that artifact, update imports and recompile. The document-platform
assembly selects it explicitly. HTTP callers may supply a catalog containing
only the actions they intend to expose.

`protomolt-emit` no longer includes Git delivery. `GitSink` moves to
`protomolt-emit-git`, in `ai.protomolt.proto.emit.git`; callers must add that
artifact and update the import. This also removes JGit from Parquet emission
and the registry HTTP service's Parquet schema endpoint. Runtime gates reject
Git dependencies from emit core and the generic registry HTTP service.

Configuration type resolution reports corrupt stored schemas and missing listed
subjects as storage errors. It does not skip them and return a misleading
"type not found" result. Invalid JSON and protobuf config input retain their
parse causes in `InvalidConfigException`.

## Registry storage provider discovery

`SchemaRegistryStores.discover()` loads `SchemaRegistryStoreProvider` factories
through Java SPI. Discovery does not open stores. `open(id, options, writeGate)`
requires an explicit installed provider ID and returns a caller-owned store.
Duplicate/malformed IDs, missing providers, null results and provider failures
are errors; there is no fallback backend. The caller supplies the compatibility
write gate explicitly, including null when no gate is intended.

Registry core registers the `memory` provider, which accepts no options. Installing
`protomolt-schema-registry-git` also registers `git`. That provider requires all
three options: `directory`, `author-name` and `author-email`. Unknown, missing or
blank options fail before the repository is opened. Existing direct builders keep
their API; this factory path does not choose an implicit commit author.

The factory API supports application wiring through the generic registry role.
The separate Git service assembly also provides federation and workflow integration
for the full document-platform application.

`protomolt-schema-registry-composer` supplies the generic `registry` role through
`ServiceModule` SPI without adding Git or workflow dependencies to the HTTP
library. An embedding application includes this module and uses
`Composer.builder().environment(environment).build().bootFromEnvironment()`.
The existing document-platform continues to select its Git-specific assembly.

The generic role requires these environment settings:

- `PROTOMOLT_ROLES=registry`
- `PROTOMOLT_REGISTRY_STORE=memory` (or another installed provider ID)
- `PROTOMOLT_REGISTRY_HOST=127.0.0.1`
- `PROTOMOLT_REGISTRY_PORT=8081`
- `PROTOMOLT_REGISTRY_AUTH=token` and `PROTOMOLT_REGISTRY_TOKEN` set to the secret;
  `AUTH=none` is an explicit unauthenticated choice and rejects a supplied token.
- `PROTOMOLT_REGISTRY_COMPATIBILITY=wire` for the compatibility write gate;
  `none` explicitly disables that gate. Schema compilation still runs.

Provider settings use `PROTOMOLT_REGISTRY_OPTION_` followed by the uppercase
option name with hyphens replaced by underscores. Git therefore requires
`OPTION_DIRECTORY`, `OPTION_AUTHOR_NAME` and `OPTION_AUTHOR_EMAIL` under that
prefix. Unknown registry settings and unknown provider options fail. The HTTP
protocol uses `/health`, `/protomolt` and the documented 16 MiB request cap.

Wiring opens only the selected store and registers cleanup immediately. HTTP
starts after all selected roles have contributed actions. The catalog includes
those contributions rather than loading every installed action provider. A config
storage capability contributes `publish-config`; the generic role does not supply
Git federation or workflow execution. Multiple action contexts or caller resolvers
fail instead of selecting the first. Startup failure and shutdown release the
server and store through the composer's cleanup stack.

## Explicit registry HTTP configuration

`SchemaRegistryServerConfig` rejects null or blank hosts and endpoint paths.
Supplying an empty host no longer binds every interface, and missing paths no
longer select endpoints implicitly. `defaults()` remains an explicit factory for
the documented conventional configuration; callers can also supply each field.

A non-null authentication token must be nonblank. An empty secret therefore
fails configuration instead of starting an unauthenticated service. Java callers
must pass null deliberately to disable token authentication; the generic registry
role requires `AUTH=none` for that choice. Token values are neither trimmed nor
included in configuration errors. Existing code that supplied missing values to
request defaults must use `defaults()` or pass the intended values explicitly.

The serve and document-platform launchers also reject blank operator tokens.
For an open node, omit `PROTOMOLT_API_TOKEN` and `--api-token` rather than supplying
an empty secret. Java options accept null for that choice. The document-platform
environment parser preserves the exact token value instead of trimming it.

## Explicit workflow worker configuration

`WorkflowRunsConfig` preserves zero retry backoff as an immediate retry. Negative
backoff, nonpositive worker/attempt/concurrency counts, missing or nonpositive
lease and poll durations, and missing or blank event topics fail configuration.
They no longer select replacement values. Java callers wanting the conventional
settings can use `WorkflowRunsConfig.defaults(workerId)` or pass the named
constants explicitly. The serve launcher selects those constants when options
are omitted and rejects supplied nonpositive worker and concurrency counts.

The durable parse regression verifies immediate retry against PostgreSQL without
sleeping or polling: scheduling and claiming both use the database clock.
Lease and poll durations must be at least one millisecond and fit the Java
millisecond representation used by the worker and store. Retry backoff must also
fit that representation at the maximum exponent (20). Overflow is rejected with
the arithmetic cause retained; it cannot wrap into a negative scheduling delay.

## gRPC action adapter

`protomolt-grpc-adapter` binds a supplied `ActionCatalog` and protobuf service
`ServiceDescriptor` to a gRPC `ServerServiceDefinition`. It contains dispatch,
error translation and contract-to-action binding, with no platform catalog,
workspace, workflow engine, jobs, inference, registry, Git, code generation or
optional action providers. It uses gRPC's protobuf marshaller directly rather
than depending on the outbound invocation capability. Runtime dependency gates
protect this boundary.

An embedding application selects its own listener, transport and authentication:

```java
var service = GrpcActionService.bind(catalog, serviceDescriptor,
        Map.of("Process", "process-record"));
serverBuilder.addService(service);
```

The class is in `ai.protomolt.proto.grpc.adapter`. Bindings must cover the service
exactly and reference installed actions with matching protobuf request and response
type names. Missing actions, incomplete or extra mappings, blank names and unsupported
streaming methods fail during binding. This adapter serves unary RPCs; it does not
turn a unary action into a streaming protocol. An empty catalog does not discover
or load optional providers.

The action catalog remains responsible for authorization before request validation,
request/response contract validation and budget accounting. The adapter obtains
caller identity from `CallerContexts`; a host must install the appropriate interceptor
for authenticated use. As with existing embedded catalog calls, absent caller context
means process authority. Merely binding a service does not authenticate its clients.
Successful replies are also checked against the bound RPC output descriptor. Error
codes and structured trailers retain the existing wire format; internal exception
causes are not serialized into gRPC status responses.

`ContractActionBindings` supports contract-based discovery with an explicit eligibility
predicate. The full assembly uses that predicate to exclude restored workspace proxies
from local RPC binding; the adapter has no workspace implementation dependency.

`protomolt-grpc-service` remains the full platform distribution. Its existing
`ProtoMoltGrpcService`, `CatalogBridge` and `ContractActionBindings` APIs forward to
the adapter; `ProtoMoltCatalog` and `ProtoMoltGrpcServer` keep platform assembly and
listener ownership. Existing consumers need no import migration. The fixed platform
service inventory explicitly uses `EXPOSE_UNIMPLEMENTED` for methods whose actions
are absent, preserving its established protocol behavior. The adapter's normal
`bind` path rejects absent actions instead. Neither path selects a fallback provider.
The CLI still selects the full catalog intentionally; small embeddings should depend
on the adapter directly.

## Delegation contracts and optional persistence

Workers depend on `protomolt-delegation-contract` for generated messages and
runtime deliverable validation. `protomolt-delegation-proto` owns the generated
protocol types. Neither brings in coordinator actions, receipt projection, or
repository-service clients. Agent Host selects the contract dependency directly.

`protomolt-delegation-lifecycle` provides replay, review bindings, and the
`TranscriptRepository` storage interface. `protomolt-delegation` adds the
coordinator, worker runtime, and catalog actions. Applications explicitly assemble
these with optional integrations:

- `protomolt-delegation-repository` implements transcript storage using the
  repository-service gRPC client, encrypted envelopes, and conditional writes.
  It depends on repository contracts, not the repository server or its storage
  backends.
- `protomolt-delegation-receipt` projects transcripts into work records.

The serve application selects these integrations. The coordinator's production
classpath excludes them; integration tests add them explicitly. Runtime dependency
gates enforce the worker, lifecycle, coordinator, and integration boundaries.
Storage is currently constructor-wired; this extraction does not introduce a
ServiceLoader provider factory or automatic backend discovery. In-memory storage
is an explicit choice, not a fallback for failed durable storage.

Generated protobuf packages, Java identities, descriptor import paths, field
numbers, and Any URLs are unchanged. Java consumers of handwritten helpers must
recompile and update imports from `ai.protomolt.proto.delegation`:

- `DeliverableContracts` and `DelegationValidation` move to `.delegation.contract`.
- `DelegationReducer`, `DelegationReviewBindings`, `TranscriptRepository`, and
  `InMemoryTranscriptRepository` move to `.delegation.lifecycle`.
- Repository storage and encryption helpers move to `.delegation.repository`.
- `DelegationRecordProjector` moves to `.delegation.receipt`.

Consumers of optional implementations must declare their artifacts explicitly.
`WorkRecords` retains its public fingerprint methods, delegating to the shared
`MessageFingerprints` descriptor utility. A golden serialization and digest test
protects persisted review identities from changes during this extraction.

## Shared raw blob operations

`protomolt-repo-spi` now exposes `BlobRepository`, `RepositoryCaller` and
`RepositoryException`. It carries existing protobuf requests and responses and
has no storage implementation dependency. `protomolt-repo-engine` implements
these five raw blob operations in `BlobOperations`; the gRPC document handler
adapts the authenticated caller and translates domain errors into wire statuses.
The shared key helper takes a prefix string, without exposing a persistence row.

Library callers must supply a caller identity resolved by their trusted host.
Raw byte operations require process authority; a scoped principal receives
`PERMISSION_DENIED`. This prevents the named-principal path from using the raw
byte API to bypass document policy. Existing open listeners still resolve callers
as the operator and must remain inside the documented trusted-network boundary.
This change does not authenticate those listeners or implement document ACLs.

`BlobOperations` moved from the service's internal package to
`ai.protomolt.proto.repo.engine`; library failures use `RepositoryException` codes,
including `FAILED_PRECONDITION` for incompatible drive configuration. Its previous
service-package class was not public. No protobuf wire identity changes in this
extraction. Tests run identical put/get/delete cases locally and over real gRPC
against SQL and object storage, plus scoped-caller denial before writes.

The engine still depends on `repo/container`, including its current ledger and
optional messaging code. Ledger extraction, messaging separation and complete
ownership enforcement remain open.
Do not interpret the raw-byte extraction as full repository conformance.

## Shared document operations

`DocumentRepository` now covers save, node/reference reads, manifest reads, list
and delete. `DocumentOperations` in the engine owns those implementations;
`DocumentGrpcService` adapts calls and errors. Its existing public constructors
remain available. HTTP document uploads invoke the same repository interface,
with `UploadHttpServer.forRepository(...)` available for library composition.
HTTP still stages the body before the document save. It currently supports only
operator-token or trusted open operation; scoped HTTP access must authorize before
staging when account policy is implemented.

Document library calls require an explicit trusted `RepositoryCaller`. Until the
account-policy implementation is installed, scoped principals fail closed rather
than running with ignored identity. This is a temporary process-authority boundary,
not completed per-account ownership enforcement. Open listeners retain their
existing operator default and require the trusted-network deployment boundary.

Remote byte-client transport failures now use `BlobStoreException`, retaining the
original cause without exposing gRPC as the top-level library exception. The
engine translates these into repository errors; gRPC translates them back to the
corresponding status, including transient `UNAVAILABLE` and `DEADLINE_EXCEEDED`.
Clients that previously caught gRPC failures directly from `RemoteBlobStore` must
handle provider-neutral errors instead. Not-found and conditional conflict
specializations on the byte interface remain unchanged.

This translation preserves status codes and descriptions, but does not forward
downstream gRPC trailers. Library callers can inspect the retained cause for
diagnostics; transport-independent structured error details need an explicit
contract before clients can rely on them across a repository proxy.

Tests compare document save, dedupe, both read forms, manifest, list and deletion
through library and real gRPC paths against SQL and object storage. Existing HTTP,
part/version, conditional-write and deletion suites remain regression coverage.
Legacy destructive-operation failure handling is unchanged by extraction and
remains part of the durability work; this does not complete archival admission,
metadata snapshots, ownership policy or progressive hydration.

## Shared archive operations

`ArchiveRepository` exposes the existing archive requests and responses plus a
blocking streamed upload with an `InputStream`. `ArchiveOperations` and its request,
classification and key helpers now live in `repo/engine`. The service adapts gRPC
and HTTP calls to that interface; `RepoServices.archiveRepository()` exposes the
same instance to library callers. Archive workflows remain in the service module.

Every archive operation takes trusted caller identity and currently requires
process authority, matching the interim document boundary. The streaming adapter
captures identity before starting its worker. It reports early worker failure
without waiting for the client to finish filling the bounded queue. This does not
yet guarantee cancellation during a provider call or rollback after commit.

The engine now includes the asset bridge, characterization and formats libraries.
They do not add provider SDKs, but document-only engine consumers also resolve
them. The repository SPI remains independent of these implementations. Hosts can
still supply their own `BridgeEngine` through the existing composition API.

Existing archive versioning, redaction, pruning and HTTP tests remain regression
coverage. Additional cases compare local and gRPC current/historical reads and
version listings over SQL and object storage. These cases do not establish the
future typed-admission, ownership, retry or metadata snapshot guarantees.

## Shared drive operations

`DriveRepository` covers create, lookup by ID/name and account listings, with the
same trusted caller requirement as document and archive operations. The temporary
process-authority boundary rejects scoped callers before provisioning or lookup;
per-account grants remain part of the ownership work.

`DriveOperations` and `DriveProvisioner` live in `repo/engine`. Provisioning uses
the selected provider's `NamespaceProvisioner`; the engine does not import the
S3 implementation. The existing public `DriveGrpcService` constructor accepting
an S3 client adapts it inside the service module and retains caller ownership of
that client. `RepoServices.driveRepository()` exposes the same operations used by
the gRPC adapter. Boot-time seeding continues through the shared provisioner.

The move preserves drive identities, provider compatibility checks, metadata and
name-based continuation tokens. The provisioner's package-private unit tests moved
with it. Conformance cases exercise local/gRPC create retries, lookup and pagination
using the real SQL ledger and object-store namespace provider.
The initial create response now reads the committed row, so its timestamp uses
the same database precision as subsequent reads and retries.
