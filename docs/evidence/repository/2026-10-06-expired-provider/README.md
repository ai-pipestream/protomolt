# Expired manager and a delayed provider write

The LocalStack/PostgreSQL delayed-PUT test now runs graceful and expired-unquiesced
reservations. On 2026-10-06 the focused regression passed 30 tests, zero failures,
errors or skips, in 42 seconds:

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessor*IT' --tests '*RepositoryCoordinatorReservationIT' --tests '*DocumentSelectedTransferIT' --tests '*DelayedS3PutGatewayIT' --console=plain
```

The expired case keeps the old manager open after its actual SDK timeout. Provider
activity is zero before closing the SDK. Both V90 and V91 are independently absent
at epoch one. After natural claim/owner/attempt expiry, a second manager reserves
through V97; an old-manager retry throws the exact stale-claim failure before another
gateway request. The replacement reloads retained preparation under its live claim,
installs, activates and publishes against the real provider.

SQL cleanup observes absence before the buffered original signed PUT reaches
LocalStack. A second cleanup removes that late version; the old object remains
unverified and unreferenced. The successor's exact provider bytes and receipt remain
readable. Test teardown releases local drained read handles without coordinator
drain or reader-QUIESCED attestation.

Sol reviewed the final test without a blocking finding. `fixture-red.xml.gz`
preserves the initial two failures: missing local read-handle release in teardown,
and an attempted graceful pre-V94 preparation reload refused by the existing V91
fence. The former was corrected in teardown. The graceful case again uses its
pre-drain retained preparation; only the uncertain case freshly loads through its
successor claim. Fresh-process graceful bootstrap remains a recorded design gap.

Scope: two managers in one JVM, admin authority, opaque CORE data, real HTTP proxy
and LocalStack effects. This does not establish process death, an SDK request still
active at transfer, scoped typed uncertain recovery, old reader/schema-worker
quiescence, automatic failover, or performance. No production code changed in this
checkpoint. Results are local validation, not hosted CI, merge or deployment.
