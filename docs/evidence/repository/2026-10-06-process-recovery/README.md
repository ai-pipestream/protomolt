# Writer process death and fresh JVM recovery

Command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationProcessRecoveryIT' \
  --tests '*DocumentSuccessor*IT' \
  --tests '*RepositoryCoordinatorReservationIT' --console=plain
```

Final run: 25 tests, zero failures/errors/skips; BUILD SUCCESSFUL in 41 seconds.
The new process test took 28.818 seconds. The XML files retain the results.
Production source did not change; the production-JAR task was not rerun.

The test starts two separate JVMs against real PostgreSQL 18 and LocalStack 3.8.
The writer completes a versioned PUT and is held before returning its result to
repository verification. The parent independently checks the bytes and SQL state,
forcibly kills and reaps the writer (exit 137), then starts the replacement. Only
the original public intent and raw payload files cross the process boundary.
Private predecessor identities and preparation are recovered from SQL, with
natural claim/owner expiry before V97 reservation and V93/V94 publication.

Assertions establish fresh successor attempt/key/version and exact bytes,
selection of only the successor in its revision, unchanged unverified predecessor
bytes, receipt replay without BlobStore calls, absent predecessor V90/V91 markers,
and unchanged ACTIVE predecessor reader incarnations. The provider decorator
forwards actual operations; it does not manufacture successful responses.

Scope: admin/opaque CORE request resubmission after a completed provider PUT.
The discovery SQL is test-only. This does not establish automatic host recovery,
late remote effects, pin reclamation, scoped typed recovery, replacement death
before activation, or performance. Late effects and tombstone cleanup have their
own test. RustFS scaling remains outstanding.
