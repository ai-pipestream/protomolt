# RESULTS — exact-version reclamation provider foundation

Assignment: `INSTRUCTIONS.md` (untracked handoff, not committed).
Worktree: `/work/worktrees/protomolt/kimi-exact-version-reclamation`
Branch: `agent/kimi-exact-version-reclamation`
Base: `bed8cf29de6bad75dea11372e1b7efedf35af943` (origin/main == github/main at preparation)
Head (qualified deliverable): `e240608a4f1c0e7d78acac56a37be10fa046e331` —
all gates in section 4 ran on exactly these sources. This RESULTS.md lands in
a following docs-only evidence commit; no code changed after the qualified
head.

## 1. What was delivered

A backend-neutral exact-version reclamation contract, an explicit S3
implementation, capability/discovery integration, real-provider acceptance
tests, and this evidence. No public pruning endpoint, no background deletion,
no SQL or protobuf change, no retention permission.

**API/capability choices** (full rationale in `docs/design/exact-version-reclamation.md`):

- `BlobCapability.EXACT_VERSION_RECLAMATION` — new enum value, distinct from
  `PHYSICAL_RECLAMATION` (whole-key semantics unchanged). Naming follows the
  prune design doc's "exact-version reclaim capability" and the enum's
  operation-noun convention.
- `ExactVersionReclaimer` (`reclaim(String bucket, String key, String versionId)`)
  — new functional port in `protomolt-repo-blob-spi`, exposed via
  `OpenedBlobStore.exactVersionReclaimer()` with the same
  capability-requires-port invariant as the whole-key port. Existing 5-arg
  constructors are retained; providers without immutable-version semantics
  (redis, cache, grpc) are untouched and route to a default port that throws
  `UnsupportedOperationException`.
- `ExactVersionOutcome` — exactly three outcomes: `DELETED` (present →
  acknowledged delete → verified absent), `ABSENT` (verified exact-version
  absence; permanent because version ids are never reused), `UNCERTAIN`
  (timeout/5xx/IO/interruption/inconclusive verification; retry of the same
  immutable target is safe and required). Explicit failures throw:
  `IllegalArgumentException` for invalid targets (null/blank bucket, null/empty
  key, null/blank/literal-`"null"` version) before any I/O;
  `BlobStoreException` with `PERMISSION_DENIED`/`UNAUTHENTICATED`/
  `NOT_FOUND`/`FAILED_PRECONDITION`/`UNIMPLEMENTED`/`DATA_LOSS` otherwise.
  No failure path reports absence.
- S3 mechanism (`S3ExactVersionReclaimer`): at most four requests per call —
  `GetBucketVersioning` (ENABLED required; suspended/never-enabled →
  `FAILED_PRECONDITION`; missing bucket → `NOT_FOUND`), versioned HEAD
  (existence; 404 → `ABSENT`, classified on HTTP status never the S3 error
  code, because RustFS answers `NoSuchKey` for deleted version ids), exact
  `DeleteObject(versionId)` (the only mutation), versioned HEAD (404 →
  `DELETED`, else `UNCERTAIN`). Never whole-key, never listing-driven, never
  key-only HEAD as absence evidence, never current-version substitution.
- Advertisement: static at open (existing provider convention). `s3` advertises
  `EXACT_VERSION_RECLAMATION` and wires the port; the advertisement is
  qualified by the LocalStack + pinned-RustFS evidence below. Capability
  advertisement is never inferred from whole-key availability.

**Caller guarantees/obligations** (documented on the port and in the design
doc): the port is a privileged mechanism, not permission. The future prune
worker must establish repository authorization and the permanent reclamation
fence before invocation, bind the version id from a verified observation, hold
no SQL locks during the call, retry only the same target on `UNCERTAIN`, and
treat thrown failures as explicit retained failure state. Late PUTs create new
version ids that survive; orphan cleanup is separate work.

## 2. Files changed

Modified: `repo/blob/spi/.../BlobCapability.java`,
`repo/blob/spi/.../OpenedBlobStore.java`,
`repo/blob/s3/.../S3BlobStoreProvider.java`,
`verification/repository-consumer/src/providerDiscovery/java/ProviderDiscoveryConsumer.java`
(capability-set assertion + explicit redis negative — narrowly required
provider capability test).

New: `repo/blob/spi/.../ExactVersionReclaimer.java`,
`repo/blob/spi/.../ExactVersionOutcome.java`,
`repo/blob/spi/.../OpenedBlobStoreExactVersionTest.java`,
`repo/blob/s3/.../S3ExactVersionReclaimer.java`,
`repo/blob/s3/src/test/.../S3ExactVersionReclaimerTest.java` (unit, stub
fault-injection), `S3ExactVersionReclaimerIT.java` (LocalStack correctness),
`S3ExactVersionRaceIT.java` (LocalStack races), `S3RequestRelay.java` (raw TCP
relay test helper), `RustFsExactVersionReclaimerIT.java` (RustFS
compat/timing), `docs/design/exact-version-reclamation.md`, `RESULTS.md`.

## 3. Acceptance checklist → tests

| Required scenario | Test(s) | Result |
|---|---|---|
| Two versions on one key: remove only approved older version, newer bytes survive | `S3ExactVersionReclaimerIT.twoVersionsOneKeyReclaimOnlyOld`; `RustFsExactVersionReclaimerIT.twoVersionsOneKeyReclaimOnlyOld` | pass (byte + version-id identity) |
| Same-prefix neighbor key and delete markers survive | `S3ExactVersionReclaimerIT.neighborKeyAndDeleteMarkersSurvive`; `RustFsExactVersionReclaimerIT.deleteMarkerSurvivesExactReclaim` | pass |
| Repeated deletion/reconciliation of same target safe with declared outcome | `S3ExactVersionReclaimerIT.repeatedReclaimIsIdempotent`; RustFS `repeatedReclaimIsIdempotent` (DELETED → ABSENT → ABSENT) | pass |
| Null/blank/literal-null version, invalid target, unversioned/suspended namespace, unsupported capability refuse without destructive effects | `S3ExactVersionReclaimerTest` (9-case table, zero stub requests); `S3ExactVersionReclaimerIT.refusalWithoutEffects`; `RustFsExactVersionReclaimerIT.rustfsSuspendedRefusal`; `OpenedBlobStoreExactVersionTest` (unsupported port); consumer probe (redis negative) | pass |
| Distinguish inaccessible backend/namespace and provider failure from absence | `S3ExactVersionReclaimerIT.failureClassificationNotAbsence` (closed port → UNCERTAIN; missing bucket → NOT_FOUND); `RustFsExactVersionReclaimerIT.wrongCredentialsRefuseWithClassification` (real 403 → PERMISSION_DENIED); unit delete-500/stall → UNCERTAIN | pass, with LocalStack deviation D1 |
| Held real PUT across exact deletion: new version survives | `S3ExactVersionRaceIT.heldPutSurvivesExactDeletion` (relay holds real PUT body upstream; reclaim DELETES old version mid-flight; released PUT's new version survives byte-exact) | pass |
| Overlapping calls + uncertain ack, retry converges on same target | `S3ExactVersionRaceIT.overlappingCallsConverge`, `uncertainAckThenRetryConverges`; `RustFsExactVersionReclaimerIT.rustfsOverlappingConverges` | pass, with LocalStack deviation D2 |
| Held delete: unrelated-key ops proceed; bounded timing on RustFS | `RustFsExactVersionReclaimerIT.rustfsHeldDeleteUnrelatedOpsProceedAndTiming` (30 mixed ops during held delete, all succeed) | pass; timings below |
| Whole-key reclamation, lifecycle/config, dependency boundaries stay green | `S3ObjectReclaimerIT` (7), `S3BlobStoreIT` (11), `S3LocalStackConditionalIT` (6), `S3NamespaceProvisionerIT` (3), `S3BoundedReadIT` (4), `RustFsConditionalBlobStoreIT` (3), `RustFsDamagedObjectReadIT` (1), `S3ProviderTest`/`S3ProviderLifecycleTest` etc.; `checkRuntimeBoundaries` on both modules; consumer boundary suite | pass |
| Discovery/capability integration from packaged JARs | `ProviderDiscoveryConsumer` via `verifyProviderDiscovery` (staged publication, both metadata modes); `S3ExactVersionReclaimerIT.discoveryAdvertisesCapability` | pass |

## 4. Commands, exit codes, counts

All gradle runs serialized with
`flock -w 1800 /tmp/protomolt-repository-qualification.lock` and
`--max-workers=2 --console=plain`, Java toolchain 25, Docker 29.9.0.

| Command | Exit | Counts |
|---|---|---|
| `./gradlew :protomolt-repo-blob-spi:check` | 0 | compile + unit tests (incl. `OpenedBlobStoreExactVersionTest` 4) + `checkRuntimeBoundaries` |
| `./gradlew :protomolt-repo-blob-s3:test` (final, unfiltered) | 0 | **136 tests, 0 failures, 0 errors, 0 skips** across 18 classes; `S3ExactVersionReclaimerTest` 31 executions, ITs: correctness 6, race 3, RustFS 7 |
| `./gradlew :protomolt-repo-blob-s3:check` | 0 | test + `checkRuntimeBoundaries` |
| `stageRepositoryConsumer` (init script, version `0.1.0-exact-version-proof`) | 0 | 143 tasks |
| consumer `check` (gradle-metadata mode) | 0 | 16 tasks, incl. `verifyProviderDiscovery` → `PROVIDER-DISCOVERY-CONSUMER OK` |
| consumer `check` (`-PpomOnly`) | 0 | 16 tasks |
| consumer `verifyProviderDiscovery --rerun-tasks` (evidence line) | 0 | `PROVIDER-DISCOVERY-CONSUMER OK: packaged discovery, identity, capabilities, selection` |
| `./gradlew :protomolt-repo-service:test --tests` (7 re-wrap-site classes: `ManagedRawDocumentIT`, `ManagedArchiveReadShutdownIT`, `DocumentPartialBenchmarkIT`, `BoundedArchiveTransportIT`, `BoundedArchiveHostIT`, `RepositoryReplicaProcessIT`, `RepositoryScaleBenchmarkIT`) | 0 | C1 re-qualification: all re-wrap sites construct and run against the new SPI (5m42s, 235 tasks) |
| `./gradlew :protomolt-repo-container:test --tests` (`DocumentUploadCoordinatorIT`, `DocumentStagingBenchmarkIT`, `DocumentPartStagerIT`, `DocumentPartParallelIT`) | 0 | C1 re-qualification |
| `./gradlew :protomolt-repo-container:admissionRuntimeTest` | 0 | runtime-inventory probes (incl. `ScopedPublicationProbe`, `HistoricalUploadFaultProbe`, `HistoricalPublicTakeoverProbe`, `NativeMixedTrafficProbe`): 3 tests, 0 skipped/failures/errors, results timestamped this run |

Red-before/green-after: the first post-review staging run failed at
`:protomolt-repo-blob-spi:javadoc` (unresolved `{@link #PHYSICAL_RECLAMATION}`
reference in the new constructor javadoc — publication builds javadoc jars,
which `check` alone does not). Fixed to `{@link BlobCapability#PHYSICAL_RECLAMATION}`.
Final re-run on the exact committed sources (`0.1.0-exact-version-proof-r2`,
fresh staging dir): `:protomolt-repo-blob-spi:check` exit 0,
`stageRepositoryConsumer` exit 0, consumer `check` exit 0 in both metadata
modes with `PROVIDER-DISCOVERY-CONSUMER OK` in each (logs:
`/tmp/final-spi.log`, `/tmp/final-stage.log`, `/tmp/final-consumer-gradle.log`,
`/tmp/final-consumer-pom.log` on the qualification host; not committed).

Environment/images: `localstack/localstack:3.8` (correctness),
`localstack/localstack:4.13` (pre-existing conditional suite),
`rustfs/rustfs:1.0.0-beta.11-preview.1` (pinned compat/perf), Testcontainers,
no compose files, no new dependencies anywhere.

RustFS timing evidence (`RUSTFS-TIMING` lines, image
`rustfs/rustfs:1.0.0-beta.11-preview.1`, env per
`RustFsConditionalBlobStoreIT` fixture):

- held delete duration: `heldDeleteMs=1774.49`, outcome `UNCERTAIN` (as
  designed — the relay severed the ack)
- 30 unrelated ops during held delete: `p50Ms=56.23 maxMs=64.52`
- baseline 30 unrelated ops: `p50Ms=50.52 maxMs=60.31`
- scenario total `totalMs=5973` vs `budgetMs=120000`

These are single-host observations on this machine only; they qualify provider
behavior under a held delete, not repository-level scaling.

## 5. Backend deviations (preserved loudly; nothing skipped, no assertion weakened)

- **D1 — LocalStack 3.8 cannot enforce credential denial.** Unknown access
  keys still reach bucket lookup; explicit Deny policies unenforced
  (`S3_SKIP_SIGNATURE_VALIDATION=0`, `ENFORCE_IAM=1` tried). Wrong-credential
  refusal is therefore qualified on RustFS, which answers real
  `403 InvalidAccessKeyId` → `PERMISSION_DENIED` (asserted, target survives).
  LocalStack behavior recorded in test comments/output.
- **D2 — LocalStack/moto duplicate concurrent versioned delete is non-S3.**
  Real S3 answers 204 to a versioned delete of an already-deleted version;
  LocalStack races answer `400 InvalidArgument` ("Invalid version id
  specified"). The strict zero-exception overlap form is **unqualified on
  LocalStack**: `S3ExactVersionRaceIT.overlappingCallsConverge` pins what does
  hold (only classified `INVALID_ARGUMENT` refusals or DELETED/ABSENT, no other
  exceptions, bounded retry converges to `ABSENT`, no side effects) and prints
  `LOCALSTACK-DEVIATION`. The strict form passes on RustFS
  (`rustfsOverlappingConverges`).
- **D3 — RustFS plain GET on suspended buckets returns `versionId=null`.**
  Recorded as evidence line; suspension itself works (`Suspended` reported,
  reclaim → `FAILED_PRECONDITION`, versions byte-identical via exact-versioned
  reads).

## 6. Independent review

Reviewer: isolated subagent reading the final diff cold, briefed to challenge
absence classification, version isolation, capability claims, overlap safety,
coverage mapping, test rigor, and scope hygiene. Verdict before fixes: not
mergeable as-is (one CRITICAL), mechanism otherwise sound — absence
classification, version isolation, idempotence, capability honesty and relay
rigor all survived challenge.

Findings and disposition:

- **C1 (CRITICAL) — capability-requires-port invariant broke ~17 existing
  re-wrap sites.** The 5-arg `OpenedBlobStore` constructor threw when handed an
  S3 handle's full capability set (now containing
  `EXACT_VERSION_RECLAMATION`) without the new port; those sites live in
  `repo/service` and `repo/container` tests/probes, outside this assignment's
  edit scope. **Fixed in the SPI instead**: re-wrapping constructors that
  cannot wire an exact-version port now *drop* the capability from the
  re-wrapped handle rather than throw — a decorator that cannot compose
  exact-version reclamation must not advertise it. Deliberately asymmetric
  with `PHYSICAL_RECLAMATION` (which predates re-wrapping and keeps its
  refusal). Documented in `OpenedBlobStore`, `BlobCapability` and the design
  doc; pinned by `OpenedBlobStoreExactVersionTest.capabilityIsDroppedRatherThanAdvertisedWithoutPort`.
  Re-qualification of every cited site: see section 4 rows for
  `:protomolt-repo-service:test`, `:protomolt-repo-container:test` and
  `:protomolt-repo-container:admissionRuntimeTest`.
- **MINOR 1 — untested implementation branches.** Fixed: 9 new stub tests
  (11 executions) pin qualify-5xx→UNCERTAIN, qualify-501→UNIMPLEMENTED,
  existence-HEAD 400/401/403 throws and 5xx→UNCERTAIN, post-delete
  verification-403→UNCERTAIN (suppression branch), delete-404 NoSuchVersion→
  ABSENT / NoSuchBucket→NOT_FOUND, interruption→UNCERTAIN with restored flag.
- **MINOR 2 — delete-404 misclassification risk.** Fixed: DELETE carries an
  error body, so 404 is classified by S3 error code — `NoSuchVersion` (racing
  same-target deleter) proceeds to verification and reports `ABSENT`, never
  `DELETED`; other 404 codes keep the `NOT_FOUND` throw. Pinned by stub tests.
- **MINOR 3 — SPI javadoc promised codes the impl never emitted.** Fixed:
  javadoc/design doc trimmed; 501 maps to `UNIMPLEMENTED`; everything
  unclassifiable is `UNCERTAIN`.
- **MINOR 4 — "four requests" false under SDK retries.** Fixed: docs now say
  "four API calls", attempts governed by the client owner's retry policy.
- **MINOR 5 — dead line in `restoreInterrupt`.** Removed.
- **MINOR 6 — exit-criteria artifacts.** This file, the design doc and the
  recorded gates; `INSTRUCTIONS.md` remains untracked and unreferenced.
- **MINOR 7 — relay 100-continue ordering (latent deadlock).** Fixed: the
  relay answers `100 Continue` before reading the held body when `Expect` is
  present; race ITs re-run green against the fixed relay.
- **NOTEs (challenged, accepted):** status-only absence classification on HEAD
  (protocol-forced, no body), static capability advertisement, LocalStack
  deviations D1–D3, relay determinism, scope hygiene (14→16 files, no
  SQL/protobuf/endpoint/dependency changes, no secrets in messages).

## 7. Exclusions and remaining work

Not in this change (by assignment): SQL migrations, retention predicates, the
V26 permanent reclamation fence, prune eligibility/ownership checks, durable
reclaim claims and workers, archive-origin pruning, schema-payload reclamation,
public APIs/protobufs, hydration, any pruning endpoint or background deletion.
The exact-version port carries no authorization; wiring it to the prune
transaction is the coordinator's next implementation. Other providers (redis,
cache, grpc) remain explicitly unsupported for exact-version reclamation.

Known unrelated baseline: a forwarding-branch CI run at this base failed in
`BoundedPublicationRpcCancellationProbe` (RESOURCE_EXHAUSTED) — a separate
repository-host issue, not repaired here; no broader gate reproduced it in this
work.

## 8. Pushes and PR

- Commits: `e240608a4f1c0e7d78acac56a37be10fa046e331` (deliverable, 14 files)
  + one docs-only evidence commit adding this RESULTS.md (its hash is the
  branch tip; no code changed after the qualified head).
- Remote hashes after push of the deliverable commit (matching):
  origin (Forgejo, canonical) `e240608a4f1c0e7d78acac56a37be10fa046e331`,
  github `e240608a4f1c0e7d78acac56a37be10fa046e331`. The evidence commit was
  then pushed to both in the same order (Forgejo first, GitHub second).
- Forgejo PR (coordinator review, not merged by the author):
  https://git.rokkon.com/ai-pipestream/protomolt/pulls/308 (#308, base main).
- Hosted CI: Forgejo runs no build CI by design (AGENTS.md). GitHub Actions
  `ci.yml` triggers only on main pushes/PRs/dispatch, so the build of record
  was dispatched manually on this branch:
  https://github.com/ai-pipestream/protomolt/actions/runs/38171512740 —
  status at evidence time: **in_progress** (jobs: build/conformance/console/
  integration). Not claimed green; final status is visible at that URL.
  Nothing was merged; both mains are untouched.
