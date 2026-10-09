# Successor rejection delivery authorization

Base: 2a2b860d5. The PostgreSQL/LocalStack successor rejection fixture now ends
the first registry borrow and runtime call, checks the runtime is idle, and opens
a separate accepted call. That call obtains the original rejection with the
provider reader closed, then completes normal terminal retirement.

After retirement, credential revocation makes client receipt replay fail with
UNAUTHENTICATED. Same-principal process authority can still obtain the original
receipt, and SQL contains one rejection. Existing assertions cover assessment
identity, zero published revisions, session cleanup and memory cleanup.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

The final version passed in 1m3s, Gradle exit 0: one aggregate, zero failures,
errors or skips. HISTORICAL_SUCCESSOR_REJECTION_REVOKED_OK is required. Sol
reviewed the final runtime-call boundaries and found no blocker. Production
code is unchanged. Revocation during a pending decision and concurrent decision
ordering remain unqualified. No public historical routing is enabled.
