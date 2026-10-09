# Terminal mode binding and current credentials

68 PostgreSQL tests passed with no failures, errors or skips: terminal modes 12,
legacy replay 12, mode journals 22, creation grants 16 and recovery authority 6.
The exact Gradle invocation selected those five classes under
`:protomolt-repo-container:test`; validation.log and results.tar.gz retain the run.

The new internal replay overload binds requested modes to the terminal generation,
owner token, preparation nonce and canonical command under current read authority.
It works after owner expiry without renewal. Missing or corrupt bindings fail
closed. Valid different modes return FAILED_PRECONDITION. Tests cover success and
rejection, ACL revocation, corrupt journal entries, missing journals, cancellation,
and unchanged owner state. Pending and unobserved states remain observations.

Four added credential revoke/rotate cases initially failed because replay returned
receipts for existing-document operations without consulting credential authority.
The red log and XML retain that failure. The shared no-grant authorization branch
now validates supplied credentials; the final cases also check missing credential
records. Both legacy replay and the new overload reject revoked/rotated keys.
Keyless legacy trusted callers and process callers retain their current behavior.
The credential cases register a scoped key after process-authority publication;
they prove current delivery authorization, not original scoped creation binding.

Sol reviewed the mode comparison and subsequent authorization fix, including lock
ordering. Tests use real PostgreSQL journals and receipts with synthetic native
revision observations. No object-store or gRPC execution is claimed here. The new
publication facade must still call the mode-aware overload after complete input
validation; the staged service is not mounted. Canonical command SQL parameters
can allocate up to 1 MiB per call; concurrent admission is a host responsibility.
