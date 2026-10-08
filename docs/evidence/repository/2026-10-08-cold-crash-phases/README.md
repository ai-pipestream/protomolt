# Restart after initial, reserved and installed states

Base: 46c549a60. The focused production-JAR regression passed in 2m8s: four
aggregate cases, zero failures or skips.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.coldProcessRestart' --tests '*HistoricalRuntimeQualificationTest.coldOwner' --max-workers=2 --console=plain
```

Three independent PostgreSQL/LocalStack fixtures terminate the writer after
initial START, after recovery reservation, or after successor installation before
activation. The driver confirms exit 23 and SQL-session termination before
starting a new JVM. The fourth case is the existing same-process cold owner.

Each new process asserts the expected persisted phase and writer-side SQL counts,
waits for actual claim/owner expiry, and recovers from command and upload bytes.
Cold discovery accepts expired unactivated reservations/installations; warm
discovery still requires an expired bound predecessor. Invalid payload checks
compare against the existing SQL counts. Recovery asserts exactly one new install,
one supersession for an unactivated predecessor, one activation, publication,
receipt replay and fresh-process resource cleanup.

Sol reviewed the test changes without blocking findings. The mandatory storage
driver runs all three crash phases in separate databases. This evidence does not
qualify crashed-incarnation reclamation, all later crash points, public routing,
authenticated transport parity or performance.
