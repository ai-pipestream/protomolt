# Authenticated historical occurrence materialization

Parent commit: `7bbcc0a19ae99696c6de3c3ce1d794691c2e9d2c`.
The source hashes, focused JUnit XML and test-result archives accompany this file.

```sh
./gradlew :protomolt-repo-admission:test :protomolt-repo-container:test \
  --tests '*DocumentHistoricalMaterializationIT' \
  --tests '*DocumentHistoricalSchemasIT' \
  --tests '*DocumentHistoricalSchemaBoundaryIT' \
  --tests '*DocumentHistoricalValidationBudgetIT' --console=plain
```

All 224 admission tests and 20 focused container tests passed with no failures,
errors or skips. The final container run repeated the same four classes after
adding direct SQL pin-count and post-read reservation assertions; no production
Java changed between those runs.

Seven new PostgreSQL cases establish:

- The result owns an independent read-pin use. Closing the history handle cannot
  drain or release it while the result remains open. SQL pins exist and disappear
  after result close and explicit history release; byte reservations return to zero.
- Post-read reservations are below peak usage; full SQL snapshot and temporary
  scratch ownership do not remain with the result.
- Unknown root/path/ordinal selections return NOT_FOUND, not a corruption report.
- Wrong fragments, capacity refusal and cancellation release bytes and pin uses.
- Revocation after decoding suppresses the next content view without releasing
  its ownership before close.
- Revocation during snapshot/decode suppresses both success and corruption details.
- Cross-account callers cannot distinguish existing from random revisions through
  capture or the internal materializer.

Publication fixtures use real PostgreSQL and admission proofs, but provider
observations are synthetic. These are not end-to-end provider reads, a new public
RPC, crash recovery or deployment evidence. SQL capture currently loads the full
bounded retained artifact set; selected-only SQL loading is not claimed. The host
must still bound parsed heap and retain returned views' owning result through the
last consumer. Already-delivered content cannot be revoked retroactively.

Sol reviewed the source and final PostgreSQL tests, including the selection
absence distinction, and found no material blocker in this qualified scope.
