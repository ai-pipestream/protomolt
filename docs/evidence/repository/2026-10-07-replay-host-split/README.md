# Bounded assessment replay host

Base `1ddcbba2b`, plus the three test sources in `sources.sha256`. No production,
protocol, provider configuration or operation timeout changes. Sol reviewed the
split and its rejected-receipt database dependency without a blocking finding.

The integration candidate's hosted run 37661101717 at `cc8a811` exhausted the
210-second aggregate host limit after reaching claim-loss recovery. The retained
log is consistent with cumulative work reaching the intentional claim-expiry wait;
it does not establish a deadlock. Local logs from different source checkpoints
helped locate phase boundaries, but are not identical-head performance evidence.

All eight operation-replay scenarios now run unchanged in a required production-JAR
host on a separate test database. It seeds the same pre-policy rejection targets,
uses the original policy definition, and has a 90-second process limit. The original
host still has its 210-second limit. All original marker assertions remain required
across the two mandatory hosts, with additional explicit replay-host completion checks.
The crash-writer scenario and mixed-reuse replay remain in the original host.

Replay runs before the original host creates lease-sensitive restart fixtures. It
explicitly waits for its rejected writer's 60-second lease to expire using PostgreSQL's
clock rather than relying on time consumed by unrelated tests. The rejected receipt
reader runs at its previous point with the replay database environment and the same
persisted command file. Its 30-second process limit is unchanged. The replay phase
restores its policy definition; the original host no longer needs its revision changes.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

Exit 0, BUILD SUCCESSFUL in 10m19s. One aggregate JUnit test, 616.984 seconds,
zero failures/errors/skips. Logs capture both hosts, the rejected restart, and the
later historical reconciliation host. Terminal JUnit XML proves the entire sequence
finished. This local result is not proof that hosted CI is green; that remains to be
rerun on the integrated candidate. The split does not complete the repository goal.
