# Per-operation recovery targets within one session manager

Base: `e5cce2c440ba66176f2ace9ae3b9a33db8162f04`.

A private manager-minted target binds its owning manager, exact operation key,
command SHA and a fresh incarnation. The recovery owner retains it before reserving
SQL ownership. There is no extra target registry and no global manager-identity
rotation. The target is not an authorization for a proposal: retained fingerprints
and the existing SQL protocol still enforce its complete identity and current
caller rights.

The initial run passed all 18 cases (2 target, 6 recovery-attempt and 10 manager),
without skips; `initial-green.tar.gz` records it. A later Javadoc clarification
states the distinction between target identity and proposal authorization.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessorTargetIT' --tests '*RepositoryRecoveryAttemptsIT' --tests '*DocumentSuccessorManagerIT' --console=plain
```

The target cases use real PostgreSQL and bounded session transactions. They cover
foreign-manager, wrong-operation, digest and incarnation refusal before activation;
three coexisting sessions with two distinct target incarnations and the original
fixed incarnation; exact activation retry; and shutdown V90 records carrying each
session's own incarnation. The fixed-incarnation restoration API explicitly refuses
a target-bound owner. A second case manually supersedes the same manager's expired
unactivated session using V98, confirms that claim transfer alone cannot retire it,
installs the next owner, safely retires the old session and activates the new target.
The original manager incarnation remains unchanged throughout.

Sol reviewed capability ownership, retention, drain identities and restoration
scope without a blocker. The tests perform no provider I/O. They do not establish
automatic orchestration of expired recovery entries, full shutdown of both owners,
provider quiescence, or reclamation. Those remain separate lifecycle work.

The final affected and production-JAR run also passed, without skips: 25 journaled
session cases, the 2 target cases, 6 recovery-attempt cases and one packaged storage
runtime case. Results are in `final-green.tar.gz`.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessorTargetIT' --tests '*RepositoryRecoveryAttemptsIT' --tests '*DocumentJournaledSessionsIT' :protomolt-repo-container:admissionStorageTest --console=plain
```

These local results do not establish hosted CI, merge or deployment.
