# Atomic successor registration

The first broader run passed 106 focused cases across preparation codecs/journals,
modes, sessions, drain, handoff and installation, followed by the production-JAR
storage and restart regression. Those focused results are stored by suite name;
`runtime-before-owner-expiry.xml.gz` records the initial full runtime result.

Sol then identified that the new owner lease could expire while its transaction
waited before commit. `owner-expiry-red.xml.gz` reproduces that defect with a live
successor claim and a held transaction after the owner update. The deferred check
now requires both leases live.

The install tests cover all three records committing together, exact retry without
renewal, rollback after the owner update, actual commit reply loss, incomplete
transaction rollback, claim expiry at commit, owner expiry at commit, competing
plans, and attempts to substitute modes or owner identity. Later transactions cannot
reuse the install proof. PostgreSQL executes all constraints and faults are applied
around real transactions. This is registration evidence, not provider I/O or a
successful recovered publication.

The initial claim stamp proposal was unnecessary: the narrow registration checks
use a locked live claim and the current transaction identity. General V91 execution
closure stays unchanged. Current source READ is checked; WRITE, schema, policy and
placement checks remain prerequisites for the later execution path. No public RPC,
provider call, cleanup release or pre-owner recovery path is enabled here.

Final command:
`./gradlew :protomolt-repo-container:test --tests '*RepositorySuccessorInstallIT' :protomolt-repo-container:admissionStorageTest --console=plain`.
All 8 install cases and the production-JAR storage/restart regression passed after
the lease fix. See `install-final.xml.gz` and `runtime-final.xml.gz`. Sol reviewed
the final SQL permissions and transaction tests. Mismatch coverage checks attempts
to write different modes or owner identities; it is not exhaustive fault coverage.
