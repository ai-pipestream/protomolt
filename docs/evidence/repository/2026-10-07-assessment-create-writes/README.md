# Shared assessment CREATE writes

Base: `dca42e4b306671f18e903b5a7f0d62c64455cee4`.

`DocumentAssessmentCreationWrites` contains the existing CREATE SQL write
sequence. The ordinary and unclaimed historical callers keep their transaction,
authorization, schema-policy and drive checks. Bounded preparation and the
manifest copy remain outside SQL. The helper borrows those values for the
synchronous transaction; it does not open another transaction or authorize a
caller.

Sol reviewed the extraction against the base and found no behavioral drift.
A normalized source comparison also confirmed that the statement sequence and
SQL helper bodies match the base, allowing only the historical-flag expression
and qualified return-type substitutions required by extraction.

Compilation passed. The focused run passed **82 tests**, and the production-JAR
storage regression passed its **one driver test**, with zero failures, errors or
skips in either task. Total Gradle time was **9m05s**. Compressed logs and XML
are archived here; `source-sha256.txt` identifies the tested sources. Runtime
inventory remained 38 artifacts and 15,992,702 bytes. These are local gates,
not hosted CI, merge or deployment evidence.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalRestoreAssessmentIT' \
  --tests '*DocumentAssessmentSlotsIT' \
  --tests '*DocumentAssessmentSlotSnapshotsIT' \
  --tests '*DocumentAssessmentArtifactsIT' \
  --tests '*DocumentAssessmentRootsIT' \
  --tests '*DocumentAssessmentStartJournalIT' \
  :protomolt-repo-container:admissionStorageTest \
  --max-workers=2 --console=plain
```

The historical-restore design now records a required difference for claimed
CREATE: bind the full fresh/historical origin set before retention locks, check
the captured pins, then write the assessment rows. Calling this writer from the
existing handle mutation callback would lock an incomplete origin set first.
This extraction does not enable claimed CREATE, provider execution or publication.
The proposed claimed ordering still requires implementation and contention tests.
