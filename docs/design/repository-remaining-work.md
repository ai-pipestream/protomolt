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
2. **Schema resolution and cache.** Reuse the existing resolver and registry
   abstractions. Separate authorized discovery from immutable artifact lookup.
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
   provider failures and database contention. The four-client measurement showed
   no replica speedup; it is not a saturation or scale-out result.

## Work gated by publication and retention guarantees

4. **Recovery.** Follow the [recovery design](repository-publication-recovery.md).
   Restore exact preparation, modes and sticky assessment coordinates under an
   explicitly supplied original live claim. A shared token is not proof that its
   previous process is dead. Prove same-owner reconciliation after a forced writer
   process exit without a local handoff file. Keep automatic claim transfer off
   until delayed provider writes and cleanup across claim loss are qualified.
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
