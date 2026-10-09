# Local successor terminal retirement

Base: 055afc0b61469e4789580394285f999c5fb7f32e. Test changes reviewed by Sol.
Source hashes identify the qualified fixture.

```
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.commitWinner*' --max-workers=2 --console=plain
```

Final run: 2 tests, 0 failures/errors/skips; Gradle 2m07s, JUnit 124.428s,
timestamp 2026-10-07T22:27:44.116Z. PostgreSQL and LocalStack back the
production-JAR hosts. Earlier runs passed in 1m14s (new-first) and 2m07s (both).

After database lease expiry, the local owner selects a pending successor while
publication pauses after SQL finalization. A separate direct reservation waits
on the publisher PID, then rejects the terminal operation. Local advancement
runs after commit and also rejects it. The local authorization path is not the
observed V97 waiter.

Both disposal orders preserve generation identity and routing. Active Work blocks
old-generation retirement and capture-drain attestation. Closing Work permits one
V107 record; the uninstalled proposal adds none. The old generation rejects new
execution. Provider bytes and receipt identity remain correct, both IDs disappear,
and retained byte reservations return to baseline.

The full storage gate passed at 5316e95ee6222394978109090f930f70e4c4cafa:
13m58s, 1 aggregate test, 0 failures/errors/skips, JUnit 835.534s,
timestamp 2026-10-07T22:30:54.336Z. Command:

```
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

This result covers both retirement orders and the existing storage hosts.
Later claim-expiry additions are excluded from this checkpoint. Hosted CI and
public historical routing remain separate qualifications.
