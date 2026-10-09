# Historical owned input composition

The retained historical attempt now stages uploads and prepares its assessment in
one owned callback. `DocumentPublicationInputs` supplies exact upload/current-reuse
ordinals under the borrowed upload view and pinned reader plan; historical reads
complete the fragment set. Assessment identity belongs to the original execution,
while the transfer child supplies upload authority. The registry receives the
assessment only after preparation, delivery checks and child cleanup succeed.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentUploadCoordinatorIT' :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

Passed in 1m39s: 47 upload-coordinator cases and one aggregate historical runtime
test, zero failures, errors or skips. The runtime suite took 60.136 seconds and
uses production JARs, PostgreSQL and LocalStack. Its initial-owner path now uses
the composed callback for actual assessment creation, publication and rejection
recovery. Required markers also cover CREATE reconciliation, revocation,
cancellation and shutdown. It does not prove public historical transport routing.

Two additional upload cases inject batch-close failure after real provider reads,
both during successful input use and after malformed returned batch shape. All
batch releases and the plan use are attempted, original capture failure survives,
and cleanup failures are suppressed. Common checks require zero byte reservations
and released SQL pins. These fault cases test input cleanup directly; they do not
inject transfer-child close failure into a composed historical assessment.

Sol review identified and verified repairs for two ownership gaps: input close
before callback return and transfer-child close before result delivery. Both now
close the undelivered assessment. The first runtime run also exposed an obsolete
pin-count assumption; the fixture now drains completed ordinary reads before
measuring retained historical/assessment pins.

No protobuf changed. Public accepted-call routing, host configuration and full
library/transport qualification remain open. The separate full storage run at
`eea9a8bb1` predates this composition and must not be cited as covering it.
