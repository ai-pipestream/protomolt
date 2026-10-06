# Managed retry qualification

These are correctness tests with PostgreSQL, LocalStack S3 and a real Git schema
registry. Elapsed times are test durations, not performance measurements.

- `availability-and-owner.log`: three PostgreSQL routing regressions plus the
  packaged storage integration passed. The packaged test took 208.239 seconds.
  Coverage includes real publication, terminal eviction and open recovery-owner
  disposal under full initial byte-budget pressure.
- `managed-shutdown.log`: packaged integration passed in 207.873 seconds. Fresh
  publication and terminal replay succeed with an unusable recovery authority
  callback. An accepted typed publication completes after outer close, selects
  verified provider versions, and final shutdown releases cache and SQL pool.
- `expired-recovery.log`: packaged integration passed after adding an independently
  owned 10-second predecessor. Descriptor selection fails after verified uploads
  but before assessment creation. Natural database-clock expiry precedes managed
  resubmission, with exactly one reservation, installation, activation and success,
  then exact receipt replay and predecessor fenced shutdown.

The final rerun passed after full-key authority checks and cleanup failure
reporting were added. `final-green.log` records the successful build;
`focused-green.tar.gz` contains 32 recovery-owner and 27 journaled-session cases,
all passing with no skips. `packaged-green.tar.gz` contains the passing aggregate
production-JAR integration report, including all three managed-service scenarios.

Remaining work includes managed recovery of unactivated successors, cancellation
following terminal observation, later credential/read revocation, and immutable
payload preflight before succession. Known completed-retry entries bypassing
recovery authority remain owned until shutdown disposal. The managed publication
entry point is in-process; publication RPC parity and transport shutdown remain
open. This checkpoint does not complete the repository composition goal.
