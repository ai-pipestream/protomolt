# Retained recovery retry inputs

Base: `e96358bbe0ef4394384bddb7ab22a68a5759c9de`.

The private recovery owner now takes the retry's mode map and exposes `resume`
without discovery. This prepares managed-host integration; no new public endpoint
or automatic recovery is enabled. The existing immutable journal continues to
supply execution modes. The added check ensures a prospective host retry cannot
silently disagree with those modes.

Two regression cases first failed when the new mode argument was not enforced
(`red.tar.gz`). With the comparison implemented, all 15 recovery-owner database
cases passed (`initial-green.tar.gz`). They cover mismatches before V93 and V94,
corrected retries preserving the same proposal, repeated activated-handle checks,
uncertain commit acknowledgments, pending supersession, caller/canonical identity,
exclusive use, admission closure and malformed modes before SQL. The mismatch
status follows the existing mode journal: FAILED_PRECONDITION.

Sol reviewed synchronization and mode lifetime. The final adjustment clears the
activated handle's fixed-mode map on close; a closed handle cannot advance.
These tests do not qualify managed-host wiring or provider execution through that
future entry point.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT' --console=plain
```

After the handle-close adjustment, the 5 affected mode/closure cases passed with
no failures, errors or skips (`final-green.tar.gz`):

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT.changedRetryModesCannotInstallOrActivate' --tests '*RepositoryRecoveryAttemptsIT.acceptedCallRetainsItsIdentityAcrossAdmissionClosure' --console=plain
```
