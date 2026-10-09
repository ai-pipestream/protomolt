# Inert historical preparation serialization

Baseline: `50e01d7e2aea365ec298df8bca9ce00488a160db`.
Local qualification only; no public restore activation or deployment.

The new regression initially failed with `UnsupportedOperationException` while
constructing a historical preparation record. Separating inert shape checks from
executable preparation allows the existing codec to retain exact historical
selectors, placement and private identity seeds without creating source pins,
operation rows or claims. Ordinary preparation keeps its full validation;
historical record.prepare(), execution claims and public execution remain gated.

The shared member/placement/attempt loop now has a void validation entry. The
historical record also uses the existing lease rule and seed validation for exact
uploading-member coverage and distinct owner/attempt/token UUIDs. No new wire
contract, private encoding version, SQL migration or provider behavior is added.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationPreparationCodecIT' --tests '*DocumentPublicationPreparationJournalIT' --tests '*DocumentHistoricalAuthorizationIT' --tests '*DocumentOperationUploadAdmissionIT' --tests '*DocumentHistoricalRestoreAssessmentIT' --max-workers=2 --console=plain
```

Final command exited 0 in 1m 11s. All 171 cases passed, zero skipped: codec 9,
journal 23, authorization 6, upload admission 91, historical assessment 42.
Tests run against real PostgreSQL. The new codec cases deliberately use a synthetic
revision UUID: they establish inert encoding, not source existence or publication.
They cover both mixed upload/history and zero-upload intent, rejected malformed
placement/lease/identity coverage, exact round-trip and continued execution refusal.
Provider effects are not qualified by this checkpoint. Attached XML/logs preserve
the existing suites' own fixture scope; trailing whitespace was normalized in XML.

Sol reviewed the source and design, found no blocker, and requested the included
zero-upload regression. The plan also now treats durable pending-source/schema
retention as a prerequisite for crash-safe claimed restore and preserves sticky
assessment identity per owner generation. Claimed registration, fresh authorized
source capture on resume, successor integration and public transport parity remain
required; this serialization checkpoint supplies none of those capabilities.
