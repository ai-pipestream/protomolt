# Document platform reshape — architecture of record

Status: agreed tenets, pre-implementation. This document is the bootstrapping
reference for the rewrite; it reshapes the pipestream platform
(`/work/main/core-services`, `/work/main/modules`) onto protomolt
(`/work/main/dev-tools/protomolt`) without redesigning it.

## Tenets (from the architect, non-negotiable)

1. **No framework.** No Quarkus, no Spring. Pure Java (21+), constructor
   wiring, small `main`. Framework tax and dependency churn are rejected;
   the platform already ran blocking-on-virtual-threads inside Quarkus, so
   the framework was scaffolding, not substance.
2. **Virtual threads everywhere.** Blocking style, one virtual thread per
   unit of work (per document part, per upload chunk, per gRPC call).
   `Executors.newVirtualThreadPerTaskExecutor()`, structured fan-out for
   parallel part IO. No reactive types in APIs.
3. **Generic names.** `Document`, `DocumentStream`, `DocumentService` —
   not `PipeDoc`. The platform is going to a client.
4. **Deterministic UUIDs, unchanged.** UUIDv5 over logical coordinates
   (`doc_id | account_id | stream coordinates`). Identity is never random;
   re-saves are idempotent by construction. Cluster is **not** part of
   identity (see Mesh).
5. **Kafka, but protomolt-driven.** Envelopes are protobuf; serde is
   protomolt's descriptor-driven serde (Confluent wire format;
   `sink/kafka/serde`, `sink/kafka/wire`). No Apicurio coupling required.
6. **Account/drive concepts stay; design improves.** Account = tenant root
   = ownership root; every account gets intake + pipeline drives; nothing
   but repo-service speaks S3. Simplify what is over-built
   (`DocumentStorageService`'s 2.4k lines of drive/graph/event glue),
   reshape what doesn't fit.
7. **Indexing pipeline is a custom step, not lifted.** OpenSearch-manager
   and the indexing ledger stay behind. The new search engine (separate
   effort) consumes document streams/replay; indexing is a module-style
   step, not repo-service's job.
8. **Hydration semantics stay.** Intake/pipeline zones, pipeline graphs,
   clusters — engine semantics, unchanged conceptually. But repo-service
   lands first and stands alone.
9. **Frontend is Node + gRPC(-web).** Services must be pleasant to call
   from grpc-web: unary-first, server-streaming where natural, no bidi
   requirement on the client path (bidi demand-pull stays a module/engine
   internal pattern, later).
10. **GraalVM-native-compatible by construction.** SPI not reflection,
    descriptor-driven everything. The protomolt CLI proved the toolchain;
    services should stay native-image-safe (mind the grpc-netty tcnative
    lesson: GraalVM CE, not Oracle).

## The mesh rule (think mesh)

From REQUIREMENTS-identity-and-tenancy.md: accounts mesh M:M with clusters;
**cluster is a routing domain, never an ownership credential**. Concretely:

- Identity and ACLs never carry a cluster id. Claims, drives, ledger rows,
  and the UUIDv5 input are account-scoped. The four identity segments
  (`doc_id | graph_address_id | account_id | graph_id`) are one proto
  message, `NodeAddress` (address.proto), shared by the whole API —
  manifests, by-reference reads, partial-save copy sources and deletes all
  address a row through it.
- Any intake may share with any pipeline (cross-account by design);
  pipelines never cross account boundaries. Sharing is expressed as typed
  grants on the document (see ACL), open-by-default whitelist until the
  architect's consent layer exists. Do **not** build consent/grant policy
  engines yet.
- Drives are per-account namespaces (BYO-bucket allowed); a drive binds
  `(bucket, prefix, region, credentialsRef)`; per-drive clients resolved at
  IO time. Cross-drive copies fall back to get+put; same-drive partial
  saves use server-side copy-forward.
- Engine/cluster routing rides in message envelopes as routing hints only
  (`stream_id`, `cluster` optional), never in storage identity.

## ACL stance (OIS-aligned, from day one)

`OwnershipContext.acls` (bare `repeated string` today) becomes typed:

```proto
message AccessRule {
  string identity = 1;       // SID, email, group id, "public"
  string identity_type = 2;  // windows-sid, oauth-group, user-principal-name,
                             // cmis-user, local-role, public, <custom>
  string display_name = 3;   // audit/debug label
  Access access = 4;         // READ, WRITE, DENY — explicit DENY always wins
}
message DocumentSecurity {
  bool inheritance_enabled = 1;
  repeated AccessRule permissions = 2;
}
```

Extraction rides the document (first-class metadata, per OIS); evaluation
is deny-wins with case-insensitive identity match against principal +
groups; index-time flattening to `security_allowed_read` /
`security_denied_read` keyword fields is the search engine's mapping
concern (protomolt `render-index-mappings` already generates such
artifacts). A standalone `check-access`-style evaluator verb in protomolt
makes the semantics testable once, reusable everywhere.

## Service shape (target)

- **repository-service (repo-service)** — lands first. The document store:
  claim-check, part-decomposed storage, ledger, lifecycle, drives. Pure
  Java, grpc-java + virtual threads, Postgres via plain JDBC + Flyway,
  S3 via AWS SDK v2, Kafka via protomolt serde.
- **account-service** — account CRUD + activation + drive provisioning
  (via repo-service RPC). Adds an `AccountStore`/`IdentityResolver` SPI:
  Postgres default; Salesforce/AD/OAuth adapters later. Emits
  `account-events` (protomolt serde). No users/groups yet — typed
  principals arrive with the ACL proto.
- **engine** — later. Hydration, graphs, clusters, demand-pull module
  work service. Pure Java same pattern. Out of scope for phase 1.
- **search engine** — separate new effort; consumes DocumentStreams /
  replay; owns its index shapes. Not repo-service's concern.
- **frontend** — Node + grpc-web against repo/account services.

## repo-service design (phase 1)

### What lifts from `/work/main/core-services/repository-service` (near-verbatim)

- `parts/PipeDocPartCodec.java` (282 LOC, pure) — **generalized**: part
  boundaries become descriptor-driven (field masks per part), not
  hard-coded `PipeDoc` fields. Any message type can be a Document.
  Round-trip byte-fidelity stays SHA-256-gated.
- `s3/BlobStore.java` port + `s3/S3BlobStore.java` — verbatim minus CDI
  (AWS SDK v2 already in protomolt's `sink/iceberg-s3`; align versions).
- `util/PipeDocUuidGenerator.java` → `DocumentIds` (drop annotations).
- `PartStorage` virtual-thread fan-out (latency-neutral split is a tested
  contract: `StorageTimingComparisonIT` ports as the A/B gate).
- Purge state machine design: tombstone → batched async purge with
  row-lock re-check, `updated_at` staleness guard, revive-before-PUT,
  DLQ, sweeper. Kafka-driven via a thin `PurgeQueue` SPI.
- `StorageReconciler` (orphan rule, min-age guard, dry-run) and the
  coherence probe.
- Test suites as behavioral spec: codec round-trip, dedupe, purge
  lifecycle, copy-forward, cross-graph isolation.

### What is deliberately dropped

Quarkus/Panache/CDI/Mutiny; `DocumentStorageService`'s drive/graph/event
glue (re-plumbed small); indexing ledger; settlement services; filesystem
tree explorer (drives API stays, tree goes); Apicurio serde; OpenSearch
receipts; HTTP raw-upload resource (replaced by `DocumentStream` gRPC).

### Storage formats

- **Of record: protobuf parts** (CORE/BLOBS/CHUNKS/PARSED as today,
  generalized by descriptor). Manifest (JSONB ledger) with per-part
  sha256/size/key/provenance; Merkle root checksum; deleted parts keep
  hash+size as receipts.
- **Parquet**: analytical/TabularDocument projections via protomolt's
  descriptor-driven `sink/emit-parquet` (no Hadoop). A part MAY be stored
  as Parquet when its projection is analytical.
- **Google Document AI format** (`google.cloud.documentai.v1.Document`):
  an *emitter/projection*, never the storage of record — produced
  descriptor-natively through protomolt (load the Document AI descriptor
  set, transcode), so there is no Google client-library dependency.
  Version-churn risk stays outside the service.

### API sketch (grpc, unary-first, grpc-web friendly)

- `DocumentService`: `SaveDocument` (partial saves, `parts_written`,
  `copy_unwritten_parts_from`, `force_save`, `written_by` provenance),
  `GetDocument` (part mask), `GetManifest`, `DeleteDocument`,
  `ListDocuments`, `StreamDocumentsForReplay` (server-stream).
- `DocumentStreamService`: chunked upload (`Initiate`, client-stream
  `UploadChunks`, `Status`, `Cancel`) — replaces the HTTP raw upload.
- `DriveService`: `CreateDrive`, `GetDrive`, `ListDrives` (account-service
  calls this at provisioning).

### Module layout (inside protomolt, `repo/` group — first-class modules)

- `repo/proto` (`protomolt-repo-proto`) — the wire contract: `Document`,
  manifest, and the document/drive gRPC services under
  `ai.pipestream.proto.repo.v1`, with java + grpc stub generation.
- `repo/container` (`protomolt-repo-container`) — the claim-check storage
  engine as sub-packages of `ai.pipestream.proto.repo.container`: `codec`
  (descriptor-driven split/assemble, manifest, root checksum; pure, no IO),
  `blob` (BlobStore port + S3 adapter, virtual-thread part fan-out),
  `ledger` (ledger + JDBC/Postgres impl via Hibernate/HikariCP, Flyway).
  Lifecycle (purge/reclaim/reconcile behind a PurgeQueue SPI + Kafka adapter
  via protomolt serde) lands here later.
- `repo/service` (`protomolt-repo-service`) — the gRPC service set,
  transport-agnostic: `RepoServices.build(config)` wires the whole stack
  (ledger → S3 → part storage → service impls) and serves it either
  in-process (same-JVM embedding: zero-copy, full gRPC semantics) or over
  Netty TCP (standalone, health + reflection; `RepoServiceMain` is that path
  from the environment). Constructor wiring, no framework.
- `repo/blob-store/spi|s3|azure|…` — **when a second provider lands**, the
  `BlobStore` port extracts from `container.blob` to
  `repo/blob-store/spi` and each provider becomes a leaf module
  (`repo/blob-store/s3`, `repo/blob-store/azure`, and `repo/blob-store/repo`
  for the dogfood port-over-repo-service-stub). Pure move, no call-site
  changes: nothing outside the adapters imports a provider SDK. Deliberately
  NOT split yet — one provider does not justify the ceremony.
- `apps/account-service` — CRUD + activation + IdentityResolver SPI +
  drive provisioning via repo-service stub (later).

## Phasing

1. repo-service skeleton: codec (generalized) + store + ledger +
   save/get-with-mask; Testcontainers (Postgres, LocalStack) ports of the
   behavioral suites.
2. Lifecycle: purge queue, reconciler, coherence probe.
3. DocumentStream upload; replay stream; drive API.
4. account-service + IdentityResolver SPI; account-events.
5. Parquet projections; Document AI emitter.
6. Engine/search/frontend (separate tracks).

## Open decisions

- ~~New repo name/home for the platform~~ **Resolved**: the platform lives
  in protomolt as first-class modules under the `repo/` group
  (`repo/proto`, `repo/container`, `repo/service`).
- Whether account-service folds into repo-service as a module or stays a
  peer process (leaning peer: different deploy/scale story).
- gRPC server stack for framework-free services: grpc-java Netty
  (battle-tested, native-image metadata exists) vs JDK HTTP server for
  any REST/health surface (protomolt `host/servers/jdk`).
