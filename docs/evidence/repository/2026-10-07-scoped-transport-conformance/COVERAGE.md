# Scoped publication authorization — coverage matrix (task document)

Branch `agent/scoped-transport-conformance`, base `695371b241f2096e762ec65ed1af6107278e3325`.
Compiled from the sources listed in the handoff, the four existing IT classes
(`RepositoryCredentialAuthoritiesIT`, `RepositoryCreationGrantsIT`,
`DocumentScopedRegistrationIT`, `ScopedRepositorySuccessorIT`), the wider test
suite, and the existing evidence directories. "Library" means direct in-JVM
invocation of the publication boundary; "gRPC" means an authenticated gRPC
channel in front of the same boundary.

## Existing evidence

| Matrix scenario | Library (existing) | gRPC (existing) |
| --- | --- | --- |
| Valid scoped initial publication, exact grant, typed content, receipt + replay | `DocumentPublicationCommitIT.scopedJournaledPublicationUsesRealProviderAndKeepsReceiptAfterGrantRevocation` (typed+opaque, real PostgreSQL/LocalStack) — `2026-10-06-scoped-creation-integration`, `2026-10-06-scoped-commit-race` | None. Only drain replay of an already-committed receipt with a scoped credential (`ManagedJournaledDrainProbe`, `2026-10-06-publication-transport`) |
| Same principal/different key, wrong generation, absent binding, wrong account, changed command | `RepositoryCredentialAuthoritiesIT` (generation/rotation), `RepositoryCreationGrantsIT` (wrong key, changed command digest), `ScopedRepositorySuccessorIT` (wrong key), `DocumentScopedRegistrationIT` (wrong account/principal) | Only binding-mismatch refusal on the read path (`RepoServiceIT.authenticatedKeyIdentityReachesRepositoryBindingAndCannotBeChanged`) |
| Grant waives nothing (source READ, existing-target WRITE, deny rules, ownership, placement); mixed batch no partial commit | READ/WRITE/deny checks retained (`2026-10-06-scoped-creation-integration`), placement/backend gate (`RepositoryCreationGrantsIT`, `ScopedRepositorySuccessorIT`). **Mixed batch under a valid grant: no evidence anywhere** | None |
| Missing/wrong transport token; resolver failure fails closed | n/a (no transport) | Tokenless → UNAUTHENTICATED (`2026-10-06-publication-host`, `ManagedDocumentHostIT`). **Resolver failure / no-fallthrough on the publication service: none** |
| Grant revocation/expiry blocks unfinished creation; committed receipt survives; replay needs READ + original binding + live generation | `2026-10-06-scoped-commit-race` (all three final-check orderings, DB-clock expiry), `RepositoryCreationGrantsIT.completedObservationBindingSurvivesGrantRevocationButNotCredentialRevocation` | None |
| Credential revocation/rotation refuses key incl. committed replay; rotation does not transfer grants | `2026-10-06-scoped-commit-race`, `RepositoryCredentialAuthoritiesIT`, `RepositoryCreationGrantsIT.expiryAndRotationInvalidateLiveGrantWithoutRewritingIt`, `2026-10-06-terminal-mode-replay` | Only drain replay after revocation (`2026-10-06-publication-transport`) |
| Target READ revoked after commit blocks receipt/content delivery | `DocumentPublicationCommitIT.composedHistoricalReplaySuppressesResultWhenReadIsRevokedDuringSchemaLoad`, `2026-10-06-terminal-mode-replay`. **Explicit committed-receipt replay refusal after READ revocation: not recorded** | None |
| Revocation while provider upload held: settles, cannot commit, recovery-owned | `DocumentPublicationCommitIT.finalPublicationRechecksGrantAfterConcurrentRevokerCommits` (attempt rows retained), `2026-10-06-scoped-process-recovery` | None |
| Real PostgreSQL barriers: revocation-wins vs commit-wins; expiry on DB time after lock waits | `2026-10-06-scoped-commit-race` (all three, `pg_blocking_pids`-verified) | None |
| Two independent operations sharing one scoped key progress concurrently | Only shared-reader overlap on one key (`RepositoryCredentialAuthoritiesIT.concurrentReadersOverlapAndRevocationWaitsForTheirDecision`); **no two-operation overlap with real SQL/provider work** | None |
| Exact retries preserve identity, no duplicate receipts; pre-admission denials produce no provider effects | `DocumentPublicationCommitIT.callerFailureAfterCommitReplaysWithoutRepeatingPublication`, `2026-10-05-scoped-provider` (with the recorded caveat that the store handle was pre-opened) | None |

## Gaps identified before implementing this harness

Library path: (a) mixed-batch atomicity under a valid grant (no partial commit);
(b) explicit committed-receipt replay refusal after post-commit READ revocation;
(c) barrier-proven concurrent progress of two operations sharing one key;
(d) parity reruns of the already-qualified scenarios through the new shared
driver so both paths execute identical assertions.

gRPC path: every scenario above, executed over an actual in-process gRPC
server/channel with the production `ApiTokenServerInterceptor`, a test
`AuthenticatedCallerResolver` mapping synthetic tokens to provisioned
`CredentialBinding`s (two keys for one principal with distinct identities), the
production `DocumentPublicationGrpcService` adapter, and the shared production
publisher (`DocumentPublicationRuntime` → `DocumentPublicationFacade`).
Transport-only additions: missing/wrong token, resolver outage fail-closed with
no fallthrough to another authority or the operator principal, and
binding-dropping host mapper refusal.

## Result (2026-10-07)

The following records the external agent's original run. Coordinator integration
adds direct adapter-call observations and positive controls; current integration
results and remaining limits are recorded in [README.md](README.md). Recorded SQL
versions alone are not proof of zero provider writes.

The planned harness exists and is green on unmodified production code:

- `repo/container/src/test/resources/runtime-inventory/ScopedPublicationProbe.java`
  — 17 shared scenarios run through both invocation paths (library facade and
  authenticated in-process gRPC), the final-check barrier orderings
  (revoked / held-live / expired on the database clock) on both paths, and a
  transport-only fail-closed scenario. Real PostgreSQL 18 and versioned
  LocalStack S3; provisioning through the process-only ports; scoped calls
  verified to carry the provisioned key and never process authority.
- `repo/container/src/test/java/ai/protomolt/proto/repo/container/ledger/ScopedPublicationParityIT.java`
  — the driver: assembles the observed admission-transport runtime classpath,
  compiles the probe, runs it in a fresh standard JVM against Testcontainers
  PostgreSQL/LocalStack, and asserts all 42 scenario markers.
- `:protomolt-repo-container:scopedPublicationTest` — focused Gradle task
  (wired into `check`), mirroring the `admissionStorageTest` bundle wiring.
  First green run: 1 test, 0 failures, 0 skipped, 66.5 s.

No production defect was demonstrated by these cases; every change required
during bring-up was inside the new harness itself. Details and commands are in
[README.md](README.md) in this directory.

## Repair (2026-10-07)

| Matrix scenario | Library | gRPC (in-process) |
| --- | --- | --- |
| Exact provisioned identity (principal, issuer, credential ID, generation, no process authority) reaches the repository on every scoped invocation, including refusals | All scenarios via `Host.publish` | All scenarios via `Host.publish` |
| Same-principal substituted key accepted by the repository is still detected; wrong issuer/ID/generation/principal/authority fail the check | `IDENTITY_SUBSTITUTION_LIBRARY` | `IDENTITY_SUBSTITUTION_GRPC` (plus mapper substitution refused by the production adapter) |

Results: [repair/README.md](repair/README.md).
