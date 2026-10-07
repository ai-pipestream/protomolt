# Private claimed historical CREATE checkpoint

Base: `23f1fdba7c657bad34e92294bf9e3c1b70393b14`. This qualifies the private,
initial-handle CREATE boundary described below. It does not complete public
historical restore, publication, or restart recovery.

The draft adds private CREATE through the registered historical execution handle.
An assessment carries a private handle identity in addition to its retained source
work, command and fixed modes. The CREATE transaction binds the complete physical
origin set before retention, checks capture pins, then writes the shared assessment
rows. A sticky local attempt marker prevents blind repeat CREATE after a possible
commit; durable reconciliation and restart qualification remain required.

Sol reviewed the lock sequence and identified an authorization window before
schema staging. The corrected draft holds current policy and caller authorization
in the same transaction as schema artifact claims. Sol found no remaining blocker
in that correction. Authorized staged claims can remain after a later CREATE
rollback; schema staging and assessment CREATE are separate transactions.

The first runtime driver failed compiling the new probe because the fixture used
`DocumentPublicationScopeCalls` in try-with-resources, although it is not
AutoCloseable. The initial log and XML are retained here. The fixture now closes
that scope explicitly in finally. The corrected run passed 27 focused tests and
the production-JAR storage driver, with zero failures, errors or skips, in 8m20s.
Its log, XML and tested source hashes are preserved in `reuse-only/`.

That first successful real-provider probe exercises reuse-only claimed CREATE,
committed start identity, exact discovery/retained reconciliation, repeat refusal
and absence of publication. That run alone does not qualify mixed uploads, rollback/lost-ACK,
foreign-handle rejection or policy/revocation contention. Add and pass those cases
before landing or advertising claimed CREATE. Public claimed publication and
session gates remain closed.

The commit-fault run adds a JDBC interceptor
that targets only the transaction inserting the exact assessment ID, identified
by its actual creation transaction. One case throws before commit; the other
throws after the real commit returns. The probe requires complete rollback of
assessment rows or exact discovery and retained reconciliation, respectively.
Both cases passed in the production-JAR storage driver in 7m48s, with zero
failures, errors or skips. The log, XML and probe hashes are in `commit-faults/`.
Mixed uploads and the additional assertions below were not part of that run.

Qualification plan at the start of this draft (later results below):

- Add retained-object rows and unchanged owner/claim leases to commit-fault checks.
- Prepare on one of two valid handles from the same registration and submit via
  the other. Require rejection before claims, then permit the rightful handle.
- For a credential revocation that wins after initial preflight, hold the current
  schema-policy row, prove staging waits with `pg_blocking_pids`, revoke through
  the real credential authority and release the policy row. Require no claims for
  the new operation and no assessment rows. Existing historical schema artifacts
  are shared catalog entries, so their presence is not a failed assertion.
- Gate staging and final CREATE commits separately and prove that a losing
  revoker waits on each transaction's authorization locks. Authorized staging may
  remain if CREATE later loses authorization; a committed CREATE must reconcile
  even when postcommit authorization prevents returning success to the caller.
- Add real fresh uploads mixed with historical references, including complete-set
  origin contention. Provider transfer observations must come from actual writes.

Run `/tmp/historical-claimed-create-mixed.log` added actual
fresh PARSED uploads alongside retained historical content, foreign-handle
rejection before schema claims, retained-object rollback checks and exact
owner/claim lease comparisons. These added cases are not yet qualified. The
fresh transfers use the actual provider adapter while the handle remains open;
they do not qualify a public claimed upload coordinator or asynchronous lifetime.

That run failed because the new mixed fixture omitted the required container
definition for fresh typed content. The error and XML are in `mixed-initial/`;
the fixture now supplies the Document descriptor explicitly. It also compares
discovered selections and retained slot declarations against independently
supplied expectations and reads the exact fresh provider version back.

The scoped authorization fixture was then added. Its first run failed Java
compilation due to a duplicate local variable name (`authority-initial/`). Sol
also found that its invocation was missing from the scenario branch. Both issues
are corrected; the driver requires the distinct revocation marker. The current
run `/tmp/historical-claimed-create-authority-fixed.log` failed on the independent
mixed-slot assertion: the fixture expected `UPLOAD`, whereas the SQL constraint
and slot binder use `NEW_CONTENT`. Its log and XML are preserved in
`authority-slot-assertion/`. The corrected assertion includes ordinal, expected
declaration and actual declaration in its failure message. The rerun
`/tmp/historical-claimed-create-authority-slots-fixed.log` passed in 7m44s:
one aggregate driver, zero failures, errors or skips. `mixed-and-stage-revocation/`
contains its XML, driver log, completed initial host probe log and verified source
hashes. It exercises mixed historical/fresh uploads with actual provider reads,
foreign-handle refusal, retained-object rollback, owner/claim lease preservation,
lost-acknowledgement reconciliation and credential revocation after preflight but
before staging authority. The full driver also completed its restart checks.
This does not establish the winner-race or complete-origin-contention cases below.

Sol reviewed the next authorization race cases against the current sources:

- Gate the exact stage-claim transaction before commit, start real credential
  revocation, and prove PostgreSQL reports the revoker blocked by that writer.
  Delegate the real commit, hold its return until revocation completes, then
  release it. Authorized schema claims may remain, but final CREATE must report
  `UNAUTHENTICATED` and leave no assessment rows.
- Gate the exact assessment-owner insertion transaction in the same way. The
  assessment must commit durably while final caller authorization prevents
  delivery. The revoked caller must also be denied discovery and reconciliation.
  An explicitly trusted process caller with the same principal must discover and
  reconcile the original identity. This does not grant the revoked client access.
- Both cases must preserve the owner/claim lease tuple, avoid a second assessment,
  and release both gates in cleanup even if an assertion fails. These cases are
  planned, not yet executed.

The JDBC gate for those winner cases was first drafted outside the build sources.
Sol reviewed its targeting and identified checked interruption handling in the
JDBC proxy; it now restores interrupt status and reports `SQLException`. The
integrated stage fixture proves zero claims before arming and uses a single writer;
the CREATE gate matches the exact assessment and current creation transaction.
Both gates release before worker shutdown, and the factory closes after workers
settle.

The gate is now integrated as a runtime probe with both winner cases. The initial
run (`commit-winners-red/`) passed the stage-winner assertions but failed the
CREATE-winner assertion: the revoked scoped caller received success after the
real assessment commit. `DocumentAdmissionAuthorization.authorizeHistory` checked
account membership and READ policy but omitted a supplied credential's live
generation. Claimed CREATE's postcommit `work.authorize` reached that shared
historical-read path, so a public READ grant incorrectly permitted delivery.

The fix checks a supplied scoped credential after document/access locks, matching
existing observation and admission checks. It preserves the separate principal-only
host model. Sol reviewed the call sites and lock ordering. Direct PostgreSQL
capture/delivery tests additionally cover revocation and rotation with READ policy
unchanged, refusal of new capture without extra pins, and explicit process and
principal-only controls. The first direct-test compilation failed on an ambiguous
transaction lambda (`credential-test-compile/`); its statement body is corrected.
Validation `/tmp/historical-claimed-create-credential-fixed.log` passed: all 14
focused historical tests and the full storage driver, with zero failures, errors
or skips. The driver requires both winner markers and completed its restart and
cleanup cases. XML, driver/host logs and verified source hashes are archived in
`credential-fixed/`. This qualifies the supplied-key fix and these exact two
commit orderings, not public claimed upload coordination or general recovery.

The reviewed mixed-origin contention probe is now integrated as
`HistoricalMixedOriginContentionProbe`. It holds a fresh attempt's row,
observes CREATE blocked at the complete origin-lock query, and independently
locks every candidate retention row with `NOWAIT`. That must succeed while CREATE
still waits; the test then releases the origin and requires successful CREATE.
The full driver passed in 7m50s with zero failures, errors or skips
(`/tmp/historical-claimed-create-mixed-contention.log`). Its required markers
include mixed-origin contention and all prior CREATE cases; restart and cleanup
checks also completed. Final XML, driver and initial host logs, and verified
source hashes are in `mixed-origin-contention/`. Logs and XML are compressed
without changing their contents. These are local checks, not hosted CI or a merge.

Sol reviewed the complete checkpoint and found no concrete blocker in its private
CREATE boundary. Public claimed publication, asynchronous upload coordination and
restart/successor recovery remain unqualified. The sticky attempt marker belongs
to one in-process handle; a newly minted handle does not yet carry that attempted
state. Fixed V83 identity and SQL uniqueness prevent a second committed assessment,
but do not establish host-level reconciliation before a new handle retries. That
remains required recovery work before exposing this path publicly.
