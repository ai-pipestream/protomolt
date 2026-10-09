# Legacy initial capture migration

Base `8b767bc1289a4de43a8286cfcfb2cb2780a12f4c`, branch
`refactor/repository-composition`, plus the source fingerprint retained here.

The new test creates a historical preparation against V103, before capture
evidence tables exist, then applies the remaining migrations. Canonical retained
roots remain exact, but capture coverage is unknown. A later reader can register
a valid batch; after actual pin release, reader quiescence and a V107 recovery
receipt, coverage still refuses the missing initial batch. The retained roots
remain exact after that refusal. No SQL guard is bypassed or drain row fabricated.

The fixture uses real PostgreSQL 18. Its source publication uses synthetic provider
observations; this test does not qualify object-store effects or public claimed
historical execution. Multiple owner epochs and atomic root release remain open.

Command: exit 0, 11 tests, no failures, errors or skips.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentCaptureAdmissionClosureIT' --max-workers=2 --console=plain
```

The first compilation exposed an AssertJ generic inference ambiguity, corrected
with local variables. The first executed test incorrectly expected later capture
registration itself to fail. Sol identified that later captures are legitimate;
the final test instead verifies they cannot establish initial capture coverage.
Sol reviewed the corrected test with no blocker. No production or wire changes.
Retained JUnit XML trailing whitespace is normalized.
