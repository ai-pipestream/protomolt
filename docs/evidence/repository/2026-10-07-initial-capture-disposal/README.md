# Initial capture disposal prerequisites

Base: `60c649b0b4c898ebea3e39ecccb5a4090c25cf19`. Production sources unchanged.
Sol reviewed the fault fixture; its suggested exact receipt assertion is included.

```
./gradlew :protomolt-repo-container:test --tests '*DocumentPreparationCaptureDrainIT' --max-workers=2 --console=plain
```

Final source: 26 tests, 0 failures/errors/skips; Gradle 35s, JUnit 30.413s,
timestamp 2026-10-07T23:09:28.607Z. Earlier run: 26 passing tests in 42s.

The extended parameterized case injects faults before registration commit and
immediately after real PostgreSQL commit. Registration has returned before the
fixture inspects the tentative capture. Accepted Work prevents local release in
both cases. Closing that Work allows cleanup without fencing an unrelated capture.

Rollback leaves no claim, binding, preparation, history set, capture batch, owner
or source-pin rows. Local cleanup is retryable and cannot create a V107 receipt.
A lost acknowledgement after commit produces one exact LOCAL capture-drain receipt,
confirmed by capture identity. Original pins release; the unrelated capture remains
usable. Publication observations used to seed historical revisions are fixture
supplied, so this test makes no object-provider durability or throughput claim.

These tests qualify existing disposal primitives. They do not implement or qualify
an initial-entry owner, uncertain-registration classifier, or public historical
routing. Those remain required managed-host work.
