# Historical schema and Any qualification

## Identity and scope of this evidence

| Item | Value |
|---|---|
| Assignment branch | `agent/historical-schema-qualification` |
| Base at creation | `8f79765942e03712599f9171968357ec53630428` (both mains) |
| PR310 status | Landed: Forgejo main `5412c68341c7c924c3fecde39e06ea447edc1147` ("Separate schema descriptor payloads from the identity catalog (V120)", #310) |
| Merge | `origin/main` merged normally into this branch before final qualification (fast-forward; the branch carried no commits of its own). No cherry-pick, no duplicated V120. Final testing below ran **after** that merge, against the V120 payload layout. |
| Head | The commit that adds this report; its parent is `5412c6834…` above. |
| Production changes | None. No behavioral test demonstrated a production defect, so per assignment scope no production correction was made. |
| New tests | `repo/container/src/test/java/ai/protomolt/proto/repo/container/ledger/DocumentHistoricalRetainedQualificationIT.java` (5 tests, real PostgreSQL via Testcontainers) |
| Excluded by assignment | `RepositoryBackupRehearsalIT`, `repo/container/src/test/resources/backup-recovery/` (Kimi-owned; untouched, not run), the full container suite, `admissionStorageTest`, backup rehearsals. No migration, provider-contract, or API changes. |

Evidence scope limits, stated plainly:

- Provider observations are the synthetic fixture family used by the neighboring
  suites (`DocumentSchemaRetentionFixture` / `DocumentNativePublicationFixture`):
  PostgreSQL state, admission proofs, descriptor bytes, and historical reads are
  real; object-store uploads/fetches are not exercised. No RustFS-backed fragment
  fetch was run; fragments are supplied to the historical reader exactly as the
  existing suites do.
- "Fresh host state" means the original `EntityManagerFactory` is closed and a new
  factory, `Tx`, `DocumentReadLedger`, and `DocumentHistoricalSchemas` are built on
  the same schema before any retained read. It is **not** a cross-process restart;
  see unresolved gaps.
- The counting resolver is the publish-time `DocumentSchemaAdmission.Resolver`
  boundary. Zero calls after publication is evidence the retained path performed
  no live lookup through that boundary; the historical reader classes take only a
  `Tx` and expose no resolver dependency at all (`DocumentHistoricalMaterializer`,
  `DocumentHistoricalSchemas`, `DocumentReadLedger#materializeFragment`).

## Commands and results

All container runs used the required lock and worker cap. Final qualification run
(actual execution via `cleanTest`, after the V120 merge):

```sh
flock -w 1800 /tmp/protomolt-repository-qualification.lock ./gradlew \
  :protomolt-repo-container:cleanTest :protomolt-repo-container:test \
  --tests '*DocumentHistoricalRetainedQualificationIT' \
  --tests '*DocumentHistoricalSchemasIT' \
  --tests '*DocumentHistoricalMaterializationIT' \
  --tests '*DocumentSchemaRetentionIT' \
  --tests '*DocumentHistoricalRestoreAssessmentIT' \
  --tests '*DocumentRevisionSchemaArtifactsIT' \
  --max-workers=2 --console=plain
```

Result: `BUILD SUCCESSFUL`, 78 tests, 0 failures, 0 errors, 0 skipped.

| Suite | Tests | Failures | Errors | Skipped |
|---|---:|---:|---:|---:|
| DocumentHistoricalRetainedQualificationIT (new) | 5 | 0 | 0 | 0 |
| DocumentHistoricalSchemasIT | 9 | 0 | 0 | 0 |
| DocumentHistoricalMaterializationIT | 7 | 0 | 0 | 0 |
| DocumentSchemaRetentionIT | 6 | 0 | 0 | 0 |
| DocumentHistoricalRestoreAssessmentIT | 42 | 0 | 0 | 0 |
| DocumentRevisionSchemaArtifactsIT | 9 | 0 | 0 | 0 |

JUnit XML for this exact run is archived under
`docs/evidence/repository/historical-schema-qualification/test-xml/`
(build-output copies at `repo/container/build/test-results/test/`).
The same six suites also passed before the merge (77 tests; the count difference
is one test PR310 added to `DocumentRevisionSchemaArtifactsIT`).

Development note, for the record: the first run of the new class failed two tests
on incorrect assumptions in the new tests, not on production behavior, and both
were corrected to the documented contracts before the green runs above. There is
therefore no red-before/green-after production-fix evidence in this assignment,
because there was no production fix. The two corrections:

1. A missing nested artifact also refuses root-only materialization, because
   `DocumentHistoricalSchemaRows.capture` requires the complete retained artifact
   set ("Historical schema artifact set is missing or exceeds limits"). The test
   now asserts `DATA_LOSS` for both — stricter than first assumed, and consistent
   with scenario 5's "fail explicitly when retained proof is required".
2. `DocumentSchemaRetentionFixture.prepare` always computes a typed proof and
   discards it for opaque members, so an opaque fixture still needs a resolver
   for that discarded computation. The opaque test asserts the published revision
   retained zero rows in `document_revision_schema_artifacts`,
   `document_revision_schema_assets`, and `document_revision_schema_evidence`,
   which is what makes the historical side genuinely unresolved.

## Coverage inventory by scenario

### 1. Retained read/materialization with live registry unavailable (fresh state, zero live lookup)

Existing: `DocumentHistoricalMaterializationIT#returnedContentOwnsAPinUseAndReservationsUntilClose`
(retained decode of a `StringValue` occurrence; pins/budget, no field-value
assertion, same factory). `DocumentHistoricalRestoreAssessmentIT` counting and
throwing resolvers prove zero live calls on the *assessment* path, not
materialization. `DocumentHistoricalSchemasIT#replaysRetainedProofWithFreshReaderAndKeepsItsHistoricalPolicy`
uses a fresh `DocumentHistoricalSchemas`/`Tx` but the same factory/JVM state and
replays proof equality, not decoded values.

New: `DocumentHistoricalRetainedQualificationIT#freshHostDecodesNestedRetainedClosureWithoutLiveResolution`
publishes a typed revision, closes the original `EntityManagerFactory`, rebuilds
factory/`Tx`/`DocumentReadLedger` on the same schema, materializes through
`captureHistorical` + `materializeFragment`, asserts actual decoded field values,
and asserts the publish-time counting resolver saw no further call. Wrong-account
capture on the fresh ledger returns `NOT_FOUND`.

### 2. Two revisions, same type URL, different definitions over time

Existing: `DocumentHistoricalSchemasIT#replaysSameUrlOccurrencesFromTheirDistinctRetainedDefinitions`
covers two definitions in **one** revision (CORE + PARSED), explicitly not this
scenario. `DocumentHistoricalMultiRevisionPublicationIT` publishes two revisions
but with the same `StringValue` definition and no per-revision decode.

New: `#twoRevisionsSameTypeUrlKeepTheirOwnPinnedDefinitions` publishes revision 1
with `archive.Record{first_label}` and revision 2 (same address, via
`DocumentPublicationCommit` with `expectedRevision=1`) with
`archive.Record{second_label}` under the identical type URL
`type.test/archive.Record`, then materializes each revision and asserts the
pinned descriptor-artifact SHA and the revision-specific field name and value.
Wrong-account capture of either revision returns `NOT_FOUND`.

### 3. Descriptor import closure retained and sufficient; custom nested Any absent from generated classes

Existing legs: `DocumentRetainedSchemaAssetsTest#freshReaderRebuildsCompleteImportClosure`
(multi-file closure, file-store stub), `DocumentRetainedPathMaterializationTest`
(nested walk, file-store), `DocumentHistoricalSchemasIT#replaysSameUrl…`
(PostgreSQL decode, one custom file, no imports, same factory).

New: the scenario-1 test uses only dynamically built descriptors with no
generated classes: `qual.Root` (imports `google/protobuf/any.proto`) holding
nested `qual.Child` Anys, where `Child`'s file imports `Leaf`'s file and `Child`
contains a `Leaf` message field (two custom files in the child closure plus
`any.proto`). After the fresh-factory rebuild it asserts decoded `Child.label`
and `Leaf.note` values — actual decoding, not row counts. Revision/account
separation is asserted in the same test.

### 4. Permitted opaque Any: exact raw round-trip; typed demand refuses

Existing: `DocumentAnyMaterializationTest#preserveReturnsExactValue` (in-memory
identity, not a round-trip), `DocumentHistoricalSchemasIT#opaqueHistoricalAdmissionIsExplicitlyUnsupported`
(typed `check` on an opaque `StringValue` revision → `FAILED_PRECONDITION`),
`DocumentSchemaPreparationTest#resolverAbsence…` (unit-level refusal).

New: `#explicitOpaqueUnknownAnyIsArchivedExactlyAndTypedHistoricalReadRefuses`
publishes an explicitly opaque revision whose CORE Any type (`unknown.Mystery`)
has no generated class and no retained definition (zero retained schema rows
asserted), then on a fresh factory: `requireOpaqueAdmission` succeeds (explicit,
never inferred), the retained read plan pins the CORE fragment's exact SHA-256
and size, reassembly via `DocumentRevisionAssembly` reproduces the exact type URL
and payload bytes, wrong-account capture returns `NOT_FOUND`, and typed
`DocumentHistoricalSchemas#check` returns `FAILED_PRECONDITION`.
`#typedAdmissionWithUnavailableRegistryFailsWithoutOpaqueFallback` proves the
admission side at container level: typed-required preparation with a rejecting
resolver propagates the failure and writes no document, revision commit, schema
admission, or artifact rows — no silent opaque downgrade.

Residual honesty note: a genuinely unresolvable-at-admission opaque publication
through the production candidate machinery (`DocumentPublicationCandidate` OPAQUE
mode) is covered by existing restore-assessment suites, not re-proven here; the
retention fixture family cannot prepare such a member because it always computes
(and discards) a typed proof first. See unresolved gaps.

### 5. Missing/corrupt retained assets vs legitimately unknown opaque; no live substitution

Existing: `DocumentHistoricalSchemasIT#missingRetainedArtifactIsReportedAsHistoricalDataLoss`
and `#corruptRetainedArtifactIsReportedAsHistoricalDataLoss` (whole-set damage,
asserted through `check()` only; updated by PR310 to the V120 payload relation).

New: `#missingNestedArtifactFailsMaterializationAndLiveDefinitionCannotSubstitute`
deletes exactly one nested child's payload row from
`repository_schema_artifact_payloads` (V120 layout; identity catalog row left in
place) and asserts `DATA_LOSS` from both `check()` and `materializeFragment`
(including the root occurrence, per complete-set capture). Before damaging, a
working live resolver resolves the same type URL to a *different* current
definition for a new admission (calls counted, new proof references the live
SHA), demonstrating behaviorally that a working live registry neither repairs
nor substitutes the pinned historical proof.

### 6. Any-bearing roots and nested locations

Production shape (source-verified during inventory): exactly two roots —
`Document.structured_data` (CORE) and `parser_results[key].document.shape`
(PARSED) — enumerated by `DocumentAnyRootInventory#inspect`;
`DocumentAnyRootInventoryTest#inventoriesEveryDeclared…` pins that set. Inside a
payload, `DocumentPayloadCheck#walk` (admission) and
`DocumentRetainedPathMaterialization#read` (historical) support singular fields,
repeated indexes, typed map keys with message values, and recursive Any
boundaries; extension ranges, non-message steps, unknown fields, and duplicate
map keys are refused. Unit coverage of those walks is thorough but file-store
based and CORE-only (`DocumentRetainedPathMaterializationTest`,
`DocumentSchemaMaterializationTest`, `DocumentRetainedRootMaterializationTest#parsedMapRoot…`).

New PostgreSQL coverage: the scenario-1 test materializes, through the real
historical path, a singular child (CORE), a repeated-index child (CORE,
index 1), and a map-key child nested under the PARSED root, asserting decoded
values and that each view's occurrence prefix equals the exact recorded
coordinate steps. This is not universal coverage; see unresolved gaps.

## Concrete unresolved gaps (follow-ups, not claimed here)

1. **Cross-process restart.** `HistoricalColdRestartDriver` exists but is wired
   only to marker-level runtime qualification. Follow-up: publish in one process,
   materialize nested occurrences in a second process against the same database,
   asserting field values. Until then, fresh-factory evidence (this report) is
   the strongest retained-decode proof.
2. **RustFS-fetched fragments.** No test in this assignment fetched fragments from
   a real object store; provider observations are synthetic per the fixture
   family. Follow-up: run scenario 1/6 materialization with fragments fetched
   through the RustFS-backed fixtures used by `HistoricalReadFailureIT`.
3. **Valid-digest unparseable descriptor and corrupt root evidence.** Missing and
   digest-mismatched payloads are covered; a payload whose bytes match a
   recomputed digest but do not parse as descriptors, and corruption of
   `document_revision_schema_evidence` bytes specifically, are not. Follow-up:
   one test each through `materializeFragment`.
4. **Unresolvable-at-admission opaque publication via production candidates.**
   The retention fixture cannot express it (see scenario 4 note). Follow-up:
   publish via `DocumentPublicationCandidate` OPAQUE mode with a type URL no
   resolver can resolve, then repeat the exact-bytes archival assertions.
5. **Deeper location matrix at PostgreSQL level.** Map keys of non-string types,
   three-plus Any boundaries, and nested-under-PARSED repeated indexes are proven
   in admission unit tests but not through PostgreSQL historical materialization.
   Follow-up: extend the new IT's walk fixture rather than new machinery.

## Delivery checklist against the assignment

- Source-backed inventory for all six scenarios: above, with class/method
  references and explicit scope limits.
- Missing behavioral tests implemented and passing (5 new); no production defect
  was demonstrated, so no production change and no red/green fix claim.
- Fresh-state retained decode, pinning across revisions, and unresolved-vs-damaged
  distinction: demonstrated by the new tests; remaining items are the named
  follow-ups above, not labeled complete.
- Final testing after PR310 landed: done — merged `origin/main` at `5412c6834…`
  and re-ran all six suites against the V120 layout (78 tests green, XML archived).
  The new damage SQL targets `repository_schema_artifact_payloads`; no old
  catalog payload columns are hardcoded.
- `RepositoryBackupRehearsalIT` and `backup-recovery/` untouched and not run;
  historical provider-error mapping unchanged. `INSTRUCTIONS.md` remains untracked.
