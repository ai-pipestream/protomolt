# Exact historical source selection

Local verification on 2026-10-05 with real PostgreSQL 18 Alpine:

```
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalSelectionIT' \
  --tests '*DocumentHistoricalReadCaptureIT' \
  --tests '*DocumentHistoricalDeliveryAuthorizationIT' --console=plain
```

Sol reviewed the implementation and final fixture changes with no blocking findings.

15 tests passed without skips or failures (8 new selection cases, 7 existing
capture/authorization regressions), Gradle 18 seconds. The source fixture uses
synthetic provider verification observations; this is SQL retention/authorization
evidence, not actual provider reads or a published historical restore.

The new batch selector checks one captured revision using its existing caller-owned
Use. Tests select r1 after r2 and r3 become current (the three revisions reuse the
same physical objects); check every physical coordinate;
refuse a foreign or closed Use; bound count and aggregate serialized bytes; reject
nested unknown fields; and refuse valid-shaped mismatches. Current READ revocation
wins over revision/ordinal mismatch details. Real database lock waits cover
cancellation, deadline expiry and use closure before delivery.

Sparse manifests retain full ordinals 1 and 3 (not compact indexes 0 and 1); batches
preserve selection order and refuse empty/deleted positions. A genuinely versionless
fixture succeeds only without an invented provider version. Earlier versioned
fixtures also refuse omission of their recorded version. Capture close retains pins
until the transferred Use closes, then SQL release removes them.

The first pass found an omitted normal admission step in the newer-publication
fixture; the database correctly refused its commit. The fixture now follows the
existing historical-pin compatibility test's admission sequence. No production SQL
guards were weakened. Existing fixture callers retain their original provider
version; only the new overload requests a versionless observation explicitly.

The batch performs one current READ authorization, with binary search over the
captured ordered ordinals. This is an implementation property, not a measured
latency or scalability result. The caller must keep the Use alive through provider
completion and any future new-reference transaction. The result is a retained
binding, not a typed verdict or write grant. The publication command still refuses
historical_reuse until complete admission/commit integration is tested.
