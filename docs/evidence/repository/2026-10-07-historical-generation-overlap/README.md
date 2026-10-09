# Provider publication across historical generations

Base: `6dcc7147c4f6f14f18a45cb050ebb096d22fad84`. Sol reviewed the fixture and cleanup.
No production Java, protobuf or SQL migration changed.

```
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

The final revision passed in 12m08s. JUnit records one aggregate case, zero failures,
errors or skips, 725.843 seconds, timestamp 2026-10-07T21:36:34.354Z. The initial
revision also passed; these archives describe the reviewed cleanup revision.
Source hashes were verified after completion.

The added host uses a separate database and the existing 90-second deadline.
PostgreSQL, LocalStack S3 and Redis are real. The scenario keeps an older historical
worker active while a successor reserves and installs across client calls. Fresh
source capture follows installation. Existing provider upload, byte readback,
assessment creation, publication and exact receipt assertions all run before the
old worker completes. Retirement cannot release that capture prematurely. Once
the worker closes, cleanup removes the old entry without losing the successor
retry identity; terminal retirement returns the complete byte budget.

Required markers were asserted by the driver:

- SCOPED_HISTORICAL_GENERATION_OVERLAP_PUBLICATION_OK
- SCOPED_INSTALLED_HISTORICAL_TERMINAL_RETIRED_OK
- HISTORICAL_GENERATION_OVERLAP_HOST_OK

Child logs are periodic snapshots. Terminal JUnit XML, Gradle exit status and
required-marker assertions establish completion. This is correctness evidence,
not a RustFS performance measurement. V52/V97 race qualification, managed public
historical routing, pruning and the broader repository goal remain incomplete.

The design note corrects an earlier race assumption: explicit SQL finalization
precedes JDBC commit. Tests must distinguish expiry before finalization from expiry
after successful checks while publication still owns the fencing locks.
