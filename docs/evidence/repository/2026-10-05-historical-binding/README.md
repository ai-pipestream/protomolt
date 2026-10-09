# Historical binding extraction

Parent commit: `d090333be16e86d12d55ae848f62d2c77f995075`.
Source hashes and original JUnit XML are retained alongside this file.

Command:

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalSchemasIT' --tests '*DocumentHistoricalSchemaBoundaryIT' --console=plain
```

Nine tests passed, zero failures/errors/skips. The tests use PostgreSQL containers;
provider observations in the publication fixtures are synthetic. They exercise
retained replay, historical policy binding, wrong/missing fragments, denied and
cross-account access, unsupported opaque/legacy admissions, cancellation, missing
retained assets, and revocation suppressing both results and validation details.
This is a focused regression run, not a full container-suite or provider test.

The extraction shares stored command/member/policy/container reconstruction.
Strict replay retains validation, full artifact/root comparisons and delivery
reauthorization. Sol reviewed the extraction and found no behavioral or
authorization drift. Optional materialization, crash recovery, public transport,
CI, merge and deployment are not established by this result.
