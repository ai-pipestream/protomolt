# Retire journaled session cache after permanent claim fencing

Base: `90b4709415ebfafd2e06c190b2be457464e19d42`.

`RepositoryClaimRetirement` shares the existing exact claim proof between recovery
attempts and journaled sessions. The manager's new process-authorized method derives
identity from its own registration, borrows the entry exclusively, runs SQL outside
the monitor and releases only local session/command capacity after permanent fencing.
The old owner-based retirement method is unchanged.

The foreign-winner cases now explicitly retire both owners. A transferred claim
with unchanged operation owner permits claim-based retirement, while owner-based
retirement stays false. Process authority is required; the winning SQL claim and
owner remain unchanged. The initial 27 cases (13 recovery owner, 14 local recovery)
passed without skips (`initial-green.tar.gz`):

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT' --tests '*DocumentPublicationRecoveryIT' --console=plain
```

The later test additions put the session manager behind the controlled real-JDBC
commit callback as well. Cancellation after its proof transaction preserves its
cache and command bytes; exact retry frees them. Existing accepted-activation gate
cases also call the new method while the session has an active user and require
CONFLICT rather than retirement.

Sol reviewed the shared proof and cache ownership without a blocker. Managed host
construction supplies bounded SQL timeouts; private constructors must supply that
view explicitly. These tests perform no provider I/O and make no pin-reclamation
or worker-quiescence claim. This is pre-close maintenance. Fenced identities in an
already captured shutdown snapshot still need separate reconciliation.

Final verification passed 24 cases (13 recovery owner, 10 successor manager,
1 packaged storage runtime), with no failures, errors or skips. Results are in
`final-green.tar.gz`; the build completed in 3m 56s.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT' --tests '*DocumentSuccessorManagerIT' :protomolt-repo-container:admissionStorageTest --console=plain
```
