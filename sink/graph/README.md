# Trusted snapshot graph projection

## Implementation contract (recorded before edits)

Issue #752; branch `feat/752-jena-snapshot-projector`; clean starting commit
`bfcc7f816b2263cc6cd211ec40631e2a71ea32f7`, intentionally stacked on #747.
Owned scope: optional `sink/graph`, settings and BOM/catalog registration.

Behavior: an explicitly trusted caller supplies an authorized exact revision,
protobuf snapshot and genuine retained evidence. Replace one derived document
named graph and its revision guard in one abortable Jena transaction. Refuse
missing evidence, stale revisions and conflicting same-revision replays. Delete
using an authorized tombstone; keep the guard to prevent stale resurrection.

Reuse: `SourceRevision`, full `NodeAddress`, `ListLinksResponse`,
`PartSchemaBinding`, `ResolvedOccurrence`, `DocumentManifest`, `ConceptScheme`,
`SkosConverter`, `ProtoValidator`/`ProtomoltRuleSource`, `MessageFingerprints`,
`Rfc3986` and Jena Dataset transactions. There is no existing snapshot projector.
No generic mapper is needed for this small explicit RDF relation mapping.

Verified read path: `AccessionGrpcService.listLinks` ->
`DocumentLinkReads.listLinks` -> `DocumentReads.coreLinks/decodeCoreLinks` ->
authorized retained CORE/schema reads -> `ListLinksResponse`. This response has
neither source address nor schema evidence. The caller must supply those from
that same authorized revision; this library cannot derive them from the response.
ConceptScheme likewise has no repository identity. The projector does not decode
an Any, consult a live registry or claim to verify retained descriptor artifacts.

Projection path: supplied protobuf + evidence -> explicit authorization callback
-> validation and derived model -> Jena WRITE transaction -> revision guard,
graph replacement and commit. Read with the caller-owned Dataset's READ
transaction. Failure aborts the graph and guard together; nothing writes back to
the repository.

Acceptance: actual Jena transaction tests for replacement, rollback after writes,
replay, stale refusal, deletion, account/address separation, exact large revision,
static mapping, occurrence identity, selectors, ignored reader-relative flags,
required evidence and authorization refusal. Focused converter/projector tests,
zero-regex/test-naming and BOM/catalog checks, repository runtime dependency check.

## Integration contract and limits

`SnapshotProjector` takes a dedicated caller-owned abortable Jena Dataset, an
explicit mapping version, a static relation -> absolute predicate URI table and
an `Authorization` gate. The gate is mandatory on **every** call, including replay
and deletion. It must verify access and the source/snapshot/evidence association
using its trusted exact-revision read context. The gate receives the selected
scheme occurrence as a separate argument (null for links and deletion), so it can
verify that precise payload association even when CORE holds several schemes. Throw to refuse; unavailable reads
must throw with their cause. An empty gate is suitable only for synthetic tests.
The API is not safe to expose directly to untrusted callers.

`replaceLinks(SourceRevision, ListLinksResponse, Evidence)` requires the response
revision to equal the source revision and a genuine retained CORE binding. Supply
resolved Any occurrences from the same revision when present. It does not infer
which Any contains a link from a text path. Link occurrences retain the repository
`field_path` and ordinal separately. Unknown static relations fail; there is no
URI guessing, inverse-edge generation or selector evaluation. Dynamic selectors
remain base64 literals of the existing binary `DynamicLinkSelector` protobuf.
Reader-relative availability/tombstone flags are intentionally excluded from the
view and replay identity; their changes do not change the stored source revision.

`replaceScheme` additionally requires the scheme's retained resolved occurrence
and a configured `SkosConverter`. The gate must verify that the typed scheme came
from this occurrence under the retained schema, including original payload digest
and length. The projector cannot verify original wire bytes from a generated
message: it does not pretend deterministic reserialization is original wire
identity. It reuses strict SKOS export and records a separate deterministic typed
snapshot fingerprint. This does not add a generic Any or tag reader. Original
protobuf remains authoritative; RDF has the converter's documented profile limits.

`delete(DocumentManifest)` requires an authorized positive-revision tombstone
(and passes null evidence to the gate). It removes only that source document's
named graph. Incoming links remain references; no target state is asserted. The
control graph keeps its last revision, deletion flag and mapping version, so a
stale snapshot cannot resurrect it. This method does **not** accept revision-zero
purge events. Purge-event fencing belongs to a later live-feed implementation.

Document/graph URIs hash deterministic full `NodeAddress` protobuf (all four
components, including account). The graph URI is stable for replacement; subjects
and occurrence URIs include the exact revision. The graph records the full source
protobuf, exact int64 revision, mapping version, static relation table and retained
evidence as protobuf literals. An unpinned static target names the logical document;
a pinned target names its exact revision. Named graphs separate provenance, not ACLs.

One transaction checks the guard, replaces the graph and updates the guard.
Same-revision equivalent projection is a replay; different content, evidence or
mapping is a conflict. Older revisions fail; later snapshots may jump revisions.
Operators must rebuild into a fresh dedicated dataset for a mapping change;
there is no in-place migration API, and same-revision changes are refused. Do not modify projector graphs/control state externally.
The projector refuses an already active transaction rather than joining it.

This library buffers one snapshot and its derived RDF in memory. No document-size
ceiling, bounded-memory ingestion, durable disk store, crash recovery or native
image qualification is claimed. Jena is optional and is not wired into repo-service.
The tests use real in-memory Jena transactions; a commit failure is injected only
after graph and guard mutations, leaving actual abort/rollback to Jena.

Remaining integration work: authenticated retained reads and artifact verification,
account allowlists, a feed required at activation, durable event IDs/partition
checkpoints committed with graph mutations, revision-zero purge handling, consistent
backfill/high-water replay, disk-provider restart/crash tests, and query authorization
before joins/aggregates/inference with revocation handling despite projection lag.
The repository relay's delivery state is not a consumer checkpoint. No listener,
Kafka worker, end-user query API or ACL enforcement is delivered here.

## Verification

Verified on 2026-11-01 with Temurin JDK 25.0.4 and the catalog's Jena 6.2.0:

```bash
flock /work/.gradle-krick.lock ./gradlew :protomolt-graph:test :protomolt-graph:javadoc checkZeroRegex checkTestNaming :bom:checkBomCompleteness :bom:checkConsumerCatalog --max-workers=2
flock /work/.gradle-krick.lock ./gradlew :protomolt-skos:test --rerun --max-workers=2
flock /work/.gradle-krick.lock ./gradlew :protomolt-repo-service:dependencies --configuration runtimeClasspath --max-workers=2
```

11 projector tests and 18 converter tests: zero failures, errors or skips.
Zero-regex, test naming, Javadoc, BOM and consumer catalog checks passed.
The repository runtime report contains no `org.apache.jena`, `protomolt-graph`
or `protomolt-skos` dependency. Module/package naming follows `sink/graph` ->
`protomolt-graph` -> `ai.protomolt.proto.graph`. `git diff --check` passed.
Local logs: `/tmp/752-review-checks.log`, `/tmp/752-skos-tests.log`, and
`/tmp/752-repo-dependencies.log`. Initial API compilation errors were corrected;
these are the final passing runs. Gradle reports existing deprecation notices.
No repository integration tests, full build, disk durability, live feed or
end-user authorization qualification is claimed. GitHub companion CI is separate.
