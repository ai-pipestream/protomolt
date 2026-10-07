# Recovery-limit decision versus expired-owner supersession

Base: `a5f1229bc8ea7eabcd7659f48297277a6449658c`. This checkpoint adds a real
PostgreSQL concurrency case; no production code, migration or protobuf changes.
Exact source hashes accompany the test evidence.

At 16 actual retained capture batches, the test installs a successor with a
five-second lease and holds its decision transaction immediately before JDBC
commit. Both decision and rejection rows are tentative at that point. A separate
connection waits for the database's actual claim and owner expiry without changing
their timestamps. Recovery discovery reports INSTALLED_NOT_ACTIVATED and supplies
the exact identity for V98 supersession. An explicit database-time assertion
confirms both leases expired before discovery.

The test starts that supersession, then observes its INSERT blocked by the
decision backend through `pg_blocking_pids`. Releasing the decision causes its
normal deferred live-identity check to fail. Both terminal rows roll back. The
waiting supersession then commits and its exact reservation confirms.
Before installation, the test checks the new claim epoch/token and unchanged
predecessor owner generation/nonce separately; reservation does not install an owner.

The old plan cannot produce a new decision. A genuine V93 installation under the
new reservation permits the fresh owner to decide; exact retry returns that
receipt. Final state has one terminal pair, one supersession, two installations,
the original 16 batches, and no execution, historical activation or capture drain.
Decision/replay do not renew the fresh owner's leases, and the payload budget is
fully released. No expired owner is impersonated or renewed.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalLimitSupersessionIT' \
  --max-workers=2 --console=plain
```

The test passed with zero failures/errors/skips. `results.tar.gz` contains
JUnit XML; `gradle.log` contains the build output. Initial provider observations
are synthetic. SQL expiry, blocking, rollback, reservation and installation are
real. This is not provider performance or deployed process-recovery evidence.
Sol reviewed the case with no blocker; the explicit expiry and intermediate
identity assertions incorporate the review suggestions.

This case uses normally deferred constraints. It does not revise the documented
guard-time semantics when a transaction explicitly forces constraints early.
A decision committed while live, followed by expired-lease replay and attempted
supersession, remains a separate case. Corrupt retained evidence and source-root
release also remain open; public historical execution is still unmounted.
