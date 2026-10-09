# Standalone bounded archive process

`RepoBoundedArchiveMain` is a separate production entry point. It validates one
environment snapshot, token, account, drive, limits and managed Redis qualification
before opening resources. It bootstraps the drive through the local port before
starting its authenticated archive-only listener. Existing stored location remains
authoritative. The default repository entry point is unchanged.

Eight tests passed with no failures, errors or skips. The combined test and
distribution build completed in 35 seconds:

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*RepoBoundedArchiveMainTest' \
  --tests '*BoundedArchiveProcessIT' :protomolt-repo-service:installDist --console=plain
```

Log: `/tmp/protomolt-archive-launcher-reviewed.log`.
The process tests launch the actual main class in a separate JVM with real
PostgreSQL and Redis. They cover authenticated write/read/retry, missing credentials,
an unmounted DriveService RPC, restart with changed defaults and unchanged physical
location, suspended/provider-incompatible drive refusal, and bad configuration
before connection to an unavailable database.

The SIGTERM case observes an accepted write blocked by an actual PostgreSQL row
lock, retains it beyond the first ten-second shutdown deadline, then releases the
lock. The call succeeds, the child exits with Linux SIGTERM status 143 without an
uncaught shutdown exception, and a new process reads the committed value. This
process suite is Linux-specific qualification: exit 143 assumes Linux SIGTERM
semantics and is not a portable Windows test. Redis
has already completed its write at this boundary. Delayed provider I/O, exhaustive
unmounted RPCs, concurrent bootstrap winners and crash durability remain open.

Sol identified an exceptional-cleanup path that could suppress a drain timeout
after listener startup and exit prematurely. It now uses the same typed-timeout
retry, clearing a preexisting interrupt during cleanup and restoring it afterward.
Unit coverage distinguishes direct timeout retry from other cleanup failures.
A newly arriving interruption during cleanup remains a failure, not an unlimited
retry guarantee. Sol reviewed the correction and process-test boundaries.

Initial test corrections: the Redis fixture now explicitly disables TTL; retry
assertions expect `deduplicated=true` while comparing the entire remaining result.
Neither correction relaxed stored identity or retention expectations.

The distribution built successfully. Launching the installed `lib/*` classpath
with the token absent exited with the expected token-required error before resource
acquisition (`/tmp/protomolt-archive-installed-smoke.log`). Successful process cases
above use the test runtime classpath; the installed-distribution smoke is narrower.

Hosted CI, merge, deployment and minimal distribution dependencies are separate
from this local qualification.
