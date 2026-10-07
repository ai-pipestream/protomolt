# Accepted historical activation work

Base: 9189b5bd0cd7b0c986c67ca5e34c964ac610d438.

The package-private activation overload forks caller-owned accepted source Work
instead of reopening source admission. It verifies the exact source owner and
execution caller before sharing the existing V94/V109 activation transaction.
Existing capture-only activation retains its existing lifecycle. This is a
prerequisite for successor execution attachment, not an executable handle or a
public recovery API. Cold activation evidence grants no Work permit.

Real PostgreSQL 18 tests cover source admission closure while accepted work remains
live, refusal of a different capture's Work for the same command, and lost JDBC
commit acknowledgement followed by exact retry without another capture batch.
Local capture drain remains pending until the caller closes its permit. Existing
scoped activation tests run alongside these cases. Published source fixtures use
synthetic provider observations; this is SQL/lifecycle proof, not provider durability.

Command:

    ./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalSuccessorActivationIT' --tests '*ScopedHistoricalSuccessorActivationIT' --max-workers=2 --console=plain

Final result: 17 tests, 0 failures, 0 errors, 0 skips.
Compressed XML and Gradle log plus source hashes are retained here. Sol reviewed and approved this
narrow prerequisite. The future owned activation attempt must explicitly retain/close its
caller permit and reauthorize before granting executable attachment; the returned
capture continues to be durable activation/drain evidence only.
