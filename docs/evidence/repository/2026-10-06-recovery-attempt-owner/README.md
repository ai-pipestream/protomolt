# Retain recovery identities across uncertain commits

Base: `829e15fc1acd2ace4ba41854eaac587cdf08edf0`.

The private recovery-attempt owner retains one exact command, authenticated caller
binding, reservation proposal and installation plan before their mutations. It
bounds entry count, active handles and encoded data reservations. SQL runs outside
the shared map monitor. Closing admission permits accepted handles to finish;
unresolved entries keep their identities and budget leases.

The initial bounded-transaction run failed all five new cases because reservation
confirmation used a nontransactional read. `bounded-read-red.tar.gz` preserves
that result. Confirmation now uses a short transaction so transaction-local SQL
timeouts apply. The older supersession fault hooks now inspect the exact committed
reservation before injecting faults, rather than firing on a confirmation read.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT' --tests '*DocumentSuccessorManagerIT' --tests '*RepositorySuccessorInstallIT' --tests '*RepositoryCoordinatorExpirationIT' --tests '*RepositoryCoordinatorSupersessionIT' :protomolt-repo-container:admissionStorageTest --console=plain
```

The 60 targeted cases passed without skips: 6 recovery-owner, 8 manager, 11 install,
17 expiration and 18 supersession cases. The owner cases use real PostgreSQL
commits with cancelled replies at reservation, installation and activation. Retry
preserves the proposal and committed token/owner, with one record per transition.
They also cover budget exhaustion before reservation, changed-command refusal,
exclusive handles, and closure before each phase followed by successful drain.
These owner tests do not perform provider I/O; provider recovery is qualified in
separate process-recovery evidence.

Sol reviewed the owner, transactional confirmation and fault-hook changes with no
blocker for this private checkpoint. Activation and attachment use the session
manager's separate transaction view; managed host construction must supply a
bounded view there as well. The owner constructor alone does not enforce it.

This is not managed recovery integration. Expired proposals, explicit supersession,
reconciled discard, graceful discovery and resource-complete shutdown remain
unfinished. Lease expiry is not permission to discard identity or provider pins.
No public endpoint, automatic recovery scheduler, merge or deployment is claimed.

`green.tar.gz` preserves all 60 targeted cases and the passing production-JAR
storage-runtime case, with no skips. These are local results, not hosted CI.
