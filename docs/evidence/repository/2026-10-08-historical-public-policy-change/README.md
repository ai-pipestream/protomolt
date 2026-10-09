# Public historical admission-policy change

Parent: `b09ceee0a999cc4b8b0d8ffc7770201aa7fa4cbe`.

A policy replacement during historical upload exposed the internal StalePolicy
exception through the library API. DocumentPublicationFacade now maps that specific
exception to FAILED_PRECONDITION and preserves its cause. Other internal failures
retain their existing behavior.

The fixture lowers a valid fragment limit through the policy catalog while a real
LocalStack S3 upload reply is delayed. PostgreSQL commits new policy content and
revision before the reply is released. Library and authenticated in-process gRPC
must return the exact stale-policy error, with no assessment, revision commit or
operation success. Exact retry must report the same stale policy without another
provider write, schema resolution or host selection. It cannot silently change the
policy bound to the retained assessment.

Cleanup restores original policy content through CAS at a later revision. Later
independent examples read that revision and verify the original content. This does
not make old attempts current. Private StalePolicy contracts remain unchanged.

The first fixture proposal enabled a required root under an opaque-permitted policy
and was correctly rejected by validation. The corrected fixture reduces the fragment
limit. Its archived red run reproduces the internal exception at the public boundary.
An intermediate mapped run passed both policy cases but identified obsolete policy
selection in later examples; that fixture sequencing has been corrected.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  :protomolt-repo-container:test \
  --tests '*DocumentHistoricalPublicationIT' \
  --tests '*DocumentSchemaPolicyConcurrencyIT' \
  --tests '*DocumentSchemaPoliciesIT' \
  --max-workers=2 --console=plain
```

This tests policy replacement before upload completion, not publication-first SQL
races. Authentication uses an in-process test identity binding. Network failure,
external identity-provider integration and performance are outside this evidence.

Final validation passed in 1m43s: one public-runtime aggregate case (80.105 seconds)
and 18 private regression tests, zero failures, errors or skips. Sol reviewed the
facade mapping, exact error assertions and fixture sequencing without a blocker.
The final log and all four XML reports are archived alongside the red evidence.
