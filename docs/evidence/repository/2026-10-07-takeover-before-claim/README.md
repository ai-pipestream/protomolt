# Takeover before claim acquisition

Base: 039ce5dad1326582710dc47c3d2a5baab5c21ea7. Sol reviewed the final fixture
and mandatory aggregate mapping. Production implementation is unchanged.

```
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.takeoverBeforeClaim' --max-workers=2 --console=plain
```

Final: 1 test, 0 failures/errors/skips; Gradle 1m07s, JUnit 63.73s,
timestamp 2026-10-07T22:51:07.048Z. Initial run passed in 1m23s.
Real PostgreSQL and LocalStack back the packaged production-JAR host.

A connection gate precedes publication transaction admission. After lease expiry,
V97 commits and confirms the replacement claim. The paused publisher backend is
idle without a transaction. Assessment and verified-upload deadlines remain live.
On release, the old publisher throws the exact Fenced exception and leaves no
result or revision. The successor then installs, captures fresh sources, publishes
the same command and verifies provider bytes, receipt identity and cleanup.

Both the focused task and mandatory aggregate require these markers. The new
aggregate has not finished. Public historical routing remains separate work;
these tests do not establish completion of the repository goal.
