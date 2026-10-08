# Writer crash and orphan recovery

Base: `fd33b539c1f5c2b902548cddd1124a9e5d49c7b1`.
Commands for the accompanying harness and archive test:

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.coldProcessRestart' --max-workers=2 --console=plain
./gradlew :protomolt-repo-container:test --tests '*ArchiveExternalQuiescenceIT' --tests '*RepositoryHistoricalRecoveryBoundIT' --max-workers=2 --console=plain
```

The crash matrix passed all 3 cases without failures or skips in 1m37s. It uses
production JARs, actual writer and recovery JVMs, PostgreSQL and LocalStack.
Writer termination is verified from the retained Process handle and disappearance
of uniquely named SQL sessions before recording a durable receipt.

After publication, the old reader still protects the original capture. Root release
fails until exact reader quiescence, bounded pin recovery and capture cleanup.
The original roots are then released with replayable receipts. The request handoff
contains only command and upload bytes. This passed at initial START, reservation
and unactivated installation.

The first installed-phase run used the immediate predecessor instead of the
original preparation. Sol identified the incorrect generation. The corrected fixture
loads the original sealed capture record from SQL using bounded memory and existing
canonical digest, command, nonce and generation checks. No production behavior
was changed to pass the test. Sol reviewed the correction without a blocker.

The archive and recovery-limit suite also passed without failures or skips in
1m55s; exact counts are in the archived XML. Archive fixtures are synthetic SQL
objects, not provider-read evidence. The test preserves another active reader.

Production host wiring and verification, other crash phases, full storage regression
and public routing remain unfinished. These runs do not establish load performance.
