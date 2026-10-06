# Remaining repository goal work

This is the working order for the additions to the repository composition goal.
It does not replace the [design](repository-composition.md) or declare unfinished
features available. Recovery is one workstream, not the whole goal.

## Independent work that can advance now

1. **Selected historical reads.** The optional Java SPI, wire contract, response
   verifier and explicit gRPC service exist. Complete malformed-response coverage,
   then the remote client and managed host wiring. Require local/remote parity,
   current authorization at delivery, cancellation and byte-budget release. Test
   exact retained descriptors with the original registry unavailable. Never
   substitute the latest registry definition for historical evidence.
   The client belongs in the `repo/history/grpc` leaf, independent of the service's
   SQL and provider assemblies. Admission exposes a closeable selected-response
   decode result using the already verified descriptor. Bound inbound messages,
   shared reservations and open results, and test the actual in-process service
   before adding managed-host wiring.
   The remote adapter must preserve current READ authorization at each exposure:
   cached decoded bytes and local cancellation checks alone cannot satisfy the
   repository SPI. Bind the caller to transport credentials explicitly; a request
   account ID or locally asserted process authority is not a remote grant.
   `HistoricalOccurrenceClient` provides one-shot verified reads, with fresh server
   authorization for every RPC. That explicit API advances remote use without
   claiming cached results implement the SPI's current-READ semantics. Explicit
   managed mounting now shares native reader lifetime and response capacity with
   ordinary history; selected reads match the local SPI across fresh in-process
   and Netty hosts. Full local/remote repository parity remains unfinished.
2. **Schema resolution and cache.** Reuse the existing resolver and registry
   abstractions. Separate authorized discovery from immutable artifact lookup.
   Follow the [bounded cache plan](repository-schema-cache.md); its first slice
   caches exact artifact bytes, not discovery grants or validation verdicts.
   The optional registry adapter and native runtime-owned resolution scopes now
   exist. Bounded host-owned sharing now isolates caller cancellation and retains
   abandoned load capacity through provider completion. Managed-host composition
   now accepts an explicit host-bound schema scope and lifecycle component. Keep provider
   timeout/allocation and shutdown ownership explicit at that boundary.
   `ManagedDocumentServices` owns a native runtime, but the managed publication
   accessor is package-private and no publication RPC is mounted. `ManagedSchemaAccess`
   supplies optional scope composition and shutdown ownership without a production
   dependency on the registry adapter. Scopes receive the actual caller and member
   and authorize every occurrence. Shutdown rejects new scopes, quiesces publication,
   and drains abandoned registry workers before shared resources close. A drain
   timeout retains those resources for another attempt. The host closes its borrowed
   registry store only after successful service shutdown; failed construction leaves
   schema cleanup with the caller. Configuration is checked before readers register.
   Real PostgreSQL/LocalStack/Git fixtures cover typed/opaque/replay, scoped caller
   forwarding and held-read shutdown. Public
   publication activation additionally needs the scoped authorization and durable
   session work below; schema wiring is not a document-creation grant.
   Establish tenant/security scope, exact schema identity, bounded ownership,
   eviction, concurrent lookup and cancellation before sharing cached entries.
   Test equal type URLs with different occurrence definitions, registry outage,
   revoked access and unresolved envelopes. Raw preservation must remain usable
   without descriptors; selected decoding still requires a definition. An outage
   must not silently become an authoritative missing-definition result.
3. **Provider durability and capacity.** Keep provider identity independent of
   implementation vocabulary. Qualify a non-S3 provider and its startup/dependency
   boundaries. Use RustFS for local performance and LocalStack for S3 correctness;
   record correctness parity separately from timings. Increase offered load in
   the existing multi-JVM benchmark, retaining latency distributions, pool sizes,
   provider failures and database contention. The four- and eight-client measurements
   showed no replica speedup; neither is a saturation or scale-out result. Eight
   clients exposed maximum-size slot-snapshot over-reservation, now fixed with
   exact-size reservation and a SQL length gate. The complete repeated workload
   passed without increasing its 128 MB payload budget. Retained manifest
   reconciliation now also reserves actual length after locked metadata checks,
   with a second size-gated SQL read. Qualification must include that extra round
   trip; no memory optimization alone establishes a latency improvement.
   Redis now has real SIGKILL/restart coverage for AOF with `appendfsync always`
   and `noeviction`: exact bytes/metadata and copies survive, and a second crash
   preserves reclamation of an object already recovered from the first crash.
   A persistence-disabled control loses non-expiring objects as expected. This
   establishes process-crash behavior only. Managed archival activation remains
   gated on immutable identity/key policy, shared lifecycle/recovery qualification,
   and explicit deployment durability requirements; it is not enabled by this test.

## Work gated by publication and retention guarantees

4. **Recovery.** Follow the [recovery design](repository-publication-recovery.md).
   Original-owner stage reconciliation now has real lost-acknowledgment and
   fresh-process forced-exit evidence without a capability handoff file. Claim
   expiry and transfer refuse the stale handle. The bounded manager now owns
   restoration lifetime and shutdown. Next, durably register ordinary runtime
   sessions. Claimed owner heartbeats now renew the owner and execution claim in
   one transaction; expired or transferred claims cannot be revived. This does
   not automatically register ordinary sessions or enable transfer. Also qualify
   the host ownership protocol; a shared token is not proof
   that its previous process is dead. Keep automatic claim transfer off
   until delayed provider writes and cleanup across claim loss are qualified.
   An internal opt-in session now registers claim, preparation and
   modes before admitting a claimed owner, with lost-acknowledgment tests at all
   four stages. Ordinary runtime creation is still unchanged. Before enabling it,
   qualify retained registration lifetime and cleanup. Scoped request authorization
   is now separate from the private journal capability. The
   scoped SQL fixture passes, while the production-JAR journaled provider variants
   still use process authority; add scoped end-to-end provider qualification before
   ordinary activation. The
   execution stage now uses the durable assessment-start marker for journaled
   sessions and commits it before CREATE; ambiguous marker acknowledgment leaves
   local staging sticky. Specify retention and
   cleanup for registered commands that later fail policy or ACL checks.
   Registered sessions now pass the real-provider acceptance/rejection matrix,
   including CREATE rollback and lost CREATE/decision acknowledgments. Separation of
   a host-owned journal capability from the actual scoped caller for V82/V83 is now
   implemented; bootstrap/readCommand remain process-only. Current destination/source
   access and revision preflight run before registration and before the sticky V83
   marker, using the frozen plan. Authoritative checks still repeat at mutation;
   separate preflight and journal transactions cannot exclude a concurrent ACL change.
   Before any possible journal commit a denial may discard the in-memory entry;
   afterward retain its exact identity for authorized retry or explicit cleanup.
   Scoped creation of a truly absent destination is currently rejected by replay
   authorization; resolve that policy explicitly, without widening journal access
   into a document-creation grant.
   Initial claim and preparation now commit atomically. Real SQL regression tests
   cover rollback, lost acknowledgment, cancellation before/after insertion and
   commit, exact retry identity and unchanged lease. The earlier claim-without-seeds
   window is closed for this path. Fixed modes and owner admission remain later
   transactions; qualify interrupted registration in a fresh process before ordinary
   activation. Never turn an explicit claim-only row into permission to invent
   replacement seeds.
5. **Restore, pruning and backup.** Test retained schema/content reachability,
   active read and pending-operation pins, current ACLs and failure recovery.
   Restore publishes through the same concurrency and validation boundaries;
   copying stored bytes alone is not a restored document. Demonstrate that
   pruning cannot remove referenced schema assets or provider versions, including
   while restore or recovery is in flight.
6. **Bounded hydration.** Reuse parts and rendition mechanics, with an immutable
   base and an explicit pending revision. Require a sealed component manifest,
   idempotent patch identities and explicit finalization. Test duplicate/missing
   chunks, stale bases, cancellation/expiry, invalid assembled content and atomic
   reader visibility. Normal readers and semantic review see only complete
   committed revisions. Arbitrary field merging remains outside this slice.

## Existing schema components to reuse carefully

`DocumentSchemaAdmission.Resolver` already selects a definition per occurrence;
its host captures authorization context. `SchemaRegistryStore` is an existing
registry abstraction. `ToolkitSchemaResolver` already handles registry, source
and descriptor-set inputs for actions. Reuse these capabilities without adding
action-layer dependencies to the repository admission leaf.

`ConfluentSchemaRegistryLoader` has a per-loader schema-ID cache and a separate
discovery cache. `ApicurioDescriptorLoader` has artifact-name and negative lookup
caches. These are existing registry loaders, not evidence that repository cache
isolation, immutable archival identity or byte budgets are implemented. In
particular, an artifact-name cache cannot stand in for an exact retained artifact
lookup. Apicurio's bulk inventory logs and skips individual load failures; an
admission adapter must preserve a distinction between incomplete discovery,
authoritative absence and failure rather than treating a partial inventory as
proof that a definition does not exist.

## Architectural requirement that applies throughout

Review new transaction and identity boundaries against the
[optional JCR assessment](repository-jcr-compatibility.md): reusable atomic
multi-object commits, stable identities, and reference retention belong in the
foundation. JCR sessions, workspaces, node types and version restoration belong
in the optional content extension. No base storage module gains JCR dependencies,
and no compliance claim precedes capability-specific conformance evidence.

## Current bounded verification

`DocumentRetainedPathMaterializationTest` now also verifies selected wire evidence
against changed revision, account and ordinal; altered Any bytes; corrupt
descriptor/metadata artifacts; and changed occurrence paths. Every cancellation
checkpoint must propagate cancellation and release all verification reservations.
The fixture uses actual descriptor and metadata encodings. This does not prove
remote-client behavior, authentication, or managed-host deployment.
