# Historical: original external-agent run (superseded)

Preserved verbatim from PR #411's original evidence commit `4aa6c2c9e` for
provenance. It describes the agent's run against base `695371b24`, before the
coordinator integrated the harness as `5396e5002` and strengthened it in
`1b9cdd8a8`. Its conclusions about "provider effects" counted recorded SQL
versions, not adapter calls; the current integrated results are in
[README.md](README.md), and the 2026-10-07 repair results are in
[repair/README.md](repair/README.md).

The archives `scoped-publication-green.tar.gz` and `scoped-regressions-green.tar.gz`
belong to this historical run. Their tested source was `5d92bd4cc` plus the
log-path change later committed in `4aa6c2c9e` (the archived probe log exists only
because of that change). They are not results from any later head.

Its statement that scoped calls were "asserted at the repository boundary to carry
the provisioned key" overstated the check: `requireScopedCallersOnly` then tested
only that a binding was present. The repair replaces it with exact identity
equality.

---

# Scoped publication authorization parity: library and authenticated gRPC

Base: `695371b241f2096e762ec65ed1af6107278e3325` (`refactor/repository-composition`).
Head: `5d92bd4cc` (`agent/scoped-transport-conformance`).
Date: 2026-10-07. The coverage matrix this qualification executes is in
[COVERAGE.md](COVERAGE.md).

## What is qualified

The same scoped publication authorization scenarios run through two invocation
paths in front of one production publisher, in a fresh standard JVM whose
classpath is exactly the observed admission-transport runtime plus the compiled
probe (the `admissionStorageTest` mechanism; no ambient test classes):

- **Library**: `DocumentPublicationRuntime` journaled → `repository(selector)`
  facade, invoked directly with the provisioned `RepositoryCaller`.
- **gRPC**: an actual in-process gRPC server/channel with the production
  `ApiTokenServerInterceptor`, a fixture `AuthenticatedCallerResolver` mapping
  synthetic tokens to provisioned `CredentialBinding`s (two keys for one
  principal provision distinct identities), the production
  `DocumentPublicationGrpcService` adapter, and the same facade.

Provisioning uses the internal process-only ports
(`RepositoryCredentialAuthorities.register/revoke/rotate`,
`RepositoryCreationGrants.prepare/install/revoke`); no SQL is issued to
fabricate grants or receipts. Scoped calls are asserted at the repository
boundary to carry the provisioned key and never the operator token or process
authority. Storage is versioned LocalStack S3; SQL is PostgreSQL 18. Every
scenario asserts receipts, durable SQL state and provider effects — never
status alone — and distinguishes request/contract validity from authorization.

Scenario families (each run on both paths unless noted): exact-grant typed
publication with retained schema and cross-path receipt replay; opaque
publication; same-principal different key; rotation (old generation fenced,
grant not transferred); unbound principal; wrong account (a supplied account
name is not membership); changed command digest; mixed-batch atomicity under a
valid grant (source READ and existing-target WRITE denials; no partial commit,
no provider effects); transport fail-closed (missing/wrong token, resolver
outage without fallthrough to another authority or the operator principal,
binding-dropping host mapper; transport-only); grant-only revocation and
database-clock expiry blocking unfinished creation while the committed receipt
survives with the original binding and a live generation; credential rotation
refusing committed replay in both directions; post-commit READ revocation
blocking receipt replay and content delivery; revocation while a real provider
upload is held (the effect settles, never commits, stays recovery-owned); real
PostgreSQL barrier orderings (authorized commit wins and revocation waits, the
publisher waits in the final grant check for the revoker's outcome, expiry
evaluated on database time after the lock wait); two independent operations
sharing one scoped key progressing concurrently under a held pre-commit
transaction with real SQL/provider work; exact retry identity without duplicate
receipts or provider effects; contract violation (checksum) refused as
INVALID_ARGUMENT without consuming the grant.

## Commands and results

```sh
./gradlew :protomolt-repo-container:scopedPublicationTest --max-workers=2 --console=plain
```

Exit 0 (two consecutive runs, 1m 8s / 1m 9s). JUnit: 1 test, 0 failures, 0
errors, 0 skipped, 66.5 s probe time. The probe printed all 42 scenario markers
(17 scenario families × LIBRARY/GRPC, 3 final-check orderings × 2 paths,
transport fail-closed, parity sentinel) and `SCOPED_PUBLICATION_PARITY_OK`; the
driver asserted each marker. `scoped-publication-green.tar.gz` retains the XML
and binary results plus the persisted probe log.

Regressions for the surrounding scoped suites on the same checkout
(`:protomolt-repo-container:test`, one invocation, exit 0, no skips):

| Suite | Tests |
| --- | --- |
| `RepositoryCredentialAuthoritiesIT` | 7 |
| `RepositoryCreationGrantsIT` | 16 |
| `DocumentScopedRegistrationIT` | 7 |
| `ScopedRepositorySuccessorIT` | 5 |
| `DocumentPublicationCommitIT` | 32 |

`scoped-regressions-green.tar.gz` retains those XML results.

`:protomolt-repo-container:admissionStorageTest` (the packaged production-JAR
baseline, unchanged by this work): exit 0 in 7m 45s, 1 test, no failures or
skips; its XML is in the same archive. The default `test` task provably
excludes the bundle-dependent driver (`No tests found for given includes:
[... **/ScopedPublication*IT* ...] [*ScopedPublicationParityIT]`).

## Production defects

None demonstrated. The acceptance matrix passed on unmodified production
sources. All bring-up fixes were inside the new harness: probe-side seeding and
readback helpers, the host drain authority resolving per-operation callers,
scenario-appropriate SQL lock/statement timeouts for the long barrier holds,
and runtime/transport resource ordering in the barrier scenarios.

## Boundaries and remaining gaps

- Fixture tokens are synthetic; the resolver is test-side host code. Production
  authentication stores (`AccessPolicyCallers`, JDBC, OIDC) stay principal-only;
  no public credential/grant provisioning API was added or qualified.
- The gRPC leg is in-process. Loopback Netty parity of this adapter is already
  evidenced by `2026-10-06-publication-transport`; nothing here changes it.
- Grant retention/pruning, external identity-provider revocation, host
  provisioning and deployment remain out of scope per the assignment.
- The `expired` final-check cases wait on the database clock (20 s grant
  lifetimes) and use host SQL timeouts above those holds; on an overloaded host
  the pre-expiry wait assertions could time out. No clocks are mocked and no
  durable timestamps are rewritten.
- Concurrency evidence proves overlap functionally (the second operation
  commits while the first operation's pre-commit transaction is held, with real
  provider writes for both); it is not a throughput or latency benchmark, and
  no RustFS run was needed for this correctness slice.
- This is local qualification on one host, not hosted CI, merge, deployment or
  performance certification.
