# Parts API reconciliation

Scope: local cleanup on `reconcile/parts-based-repository`, based on
`974ea302ccebacd12ce299be505380792b5e929e`. Main and
`ai-slop-archive-by-astra` are not modified. This is not a new repository design
or completion of the wider protobuf-storage repair.

## Removed

- The `DocumentPublicationService` protobuf service declaration, server adapter,
  transport configuration, mounting and transport-specific drain code.
- The remote publication client and `protomolt-repo-publication-grpc` module,
  including BOM, consumer and build references.
- Tests that exist only to exercise that deleted transport. Mixed probes retain
  their library checks and remaining historical-read transport checks. See
  [the coverage change inventory](probe-coverage-changes.md).
- Active instructions recommending the competing public write API. The former
  guide is available in Git history, not copied into the active checkout.

## Preserved

- `DocumentService`, its selected-part save/read contract, and existing part
  split/assembly, chunk-set and copy-forward implementations.
- Internal admission, CEL/protobuf validation, receipt identity, revision
  retention, recovery, authorization and cancellation machinery.
- Historical read and materialization endpoints. They do not expose a second
  document-write operation.
- Internal execution messages and their annotations, now in
  `document_publication_execution.proto`, without a service declaration.
- Storage providers and database migrations. No content migration is introduced.

The parts engine, codec, admission and SPI behavior are unchanged. The retained
publication SPI Javadoc now identifies its internal role.
An independent source review checked transport removal. The later restart run
exposed a lifecycle ordering error that this initial review missed; see below.

## Explicit limits of this cleanup

Internal `DocumentPublicationRepository` and its execution runtime still exist
to preserve useful mechanisms and their tests. They are not exposed through the
removed gRPC service. This cleanup does not combine two execution engines or
claim that ordinary `SaveDocument` already invokes retained typed admission.

Existing buffering and internal payload limits are unchanged. No new document
size ceiling is introduced; large-document qualification and removal of existing
buffered-path restrictions remain separate work. Untouched-part re-upload and
the broader authoritative JSON audit are not fixed by this removal.

## Verification

- Production and test-source compilation passed for repo service and proto.
- Focused service options, internal protobuf contract, SPI input and codec tests:
  67 tests, zero failures or skips.
- Scoped library authorization/receipt qualification: one driver passed with
  22 required markers (18 scenarios, three barrier orderings and completion).
- Focused real host qualification: three tests passed, zero failures or skips.
- All runtime-inventory probe sources compiled against production dependencies.
- Backup-recovery resource sources compiled; standalone repository-recovery
  `compileJava` passed.
- `buf lint repo/proto/src/main/proto` passed.

JUnit summaries in `tests/` retain test outcomes and omit system output/error
logs. Full local logs are `/tmp/protomolt-canonical-service-tests.log`,
`/tmp/protomolt-parts-reconciliation-host-tests-final.log`,
`/tmp/protomolt-reconciled-probe-javac.log`, and
`/tmp/protomolt-reconciliation-recovery-compile.log`.

The first scoped driver run failed because its expected marker count still
included the removed RPC cases (44 instead of 22). The corrected driver passed;
no runtime assertion was relaxed. An earlier probe compile caught a mistakenly
removed library revocation method, which was restored before verification.

The first observed production-JAR recovery run reached restart cleanup with an
expired upload lease but no cleanup state. Removing the publication transport
had also removed `services()`'s lifecycle-start condition for that host. The
restart child was terminated after diagnosis rather than waiting out its
remaining timeout; that invocation is failed, not passing evidence.

`services()` now starts configured recovery whenever managed documents or
archives exist, independently of a removed RPC. This preserves its documented
startup promise. The existing restart/cleanup assertions remain unchanged.
The corrected observed-host run and three focused host reruns passed: four
tests, zero failures/errors/skips, in 19m 15s. This includes orphan reclamation,
forced crash recovery, receipt replay and the existing cold-restart checks.
Log: `/tmp/protomolt-parts-reconciliation-recovery-final.log`.

Across the recorded suites there are 72 passing JUnit cases, with no skips.
The one observed-host case contains multiple subprocess scenarios; its single
JUnit count is not presented as the total number of behavioral assertions.
The full repository suite and hosted CI were not run; nothing was pushed.
