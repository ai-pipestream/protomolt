# Scoped repository creation authority

Status: internal grant implementation and execution integration under qualification;
not an available provisioning API. Original design baseline:
`6bc32d582a65e1c17ea79340e662d8ab96c734f1`. This extends the ownership work in
[repository composition](repository-composition.md). It does not replace the
remaining recovery, archival, provider or progressive-hydration work.

## What is missing

`RepositoryCaller` carries a principal, process authority, account memberships
and ACL identities. It deliberately grants no right to create an absent target.
The original `DocumentAdmissionAuthorization.lockAndAuthorize` overload refuses
scoped creation. Its explicit grant-aware path now accepts only an exact live grant
after validating selected placement under locks; see integration evidence below.
Existing-target updates still require WRITE and preserve policy, datasource and
placement. Proposed ownership in a request cannot establish caller authority.

The original credential resolution lost key identity. The optional authenticated
resolver/context path now preserves a provisioned issuer, key UUID and generation.
`AccessPolicyCallers` still maps rotation credentials to a principal without
provisioned key IDs and remains explicitly unbound.
A principal-bound grant would consequently be usable by every key for that
principal. Calling such a grant key-specific would be incorrect.

Network listeners now require a credential. That authenticates the operator or a
policy principal; it does not supply the following creation grant.

## Implemented identity boundary

`AuthenticatedCallerResolver` resolves a caller and optional `CredentialBinding`
in one lookup. `CallerResolver.chain` preserves the first complete match and never
continues after a store failure or upgrades an unbound match. gRPC carries that
result alongside the existing scope caller; mismatched contexts fail closed.

`DocumentGrpcService.withAuthenticatedBindings` supplies the result to the host
account/ACL mapper and requires its repository caller to preserve the exact key
identity, principal and authority. The default adapter preserves key identity but
grants no account membership. The older principal-only mapper refuses identified
credentials if it drops their binding. `RepositoryCredentialBinding` is a value in
the repository SPI, with no dependency on authentication or storage modules.

This is identity propagation only. No credential ID is synthesized by the shipped
policy/OIDC resolver, no grant is provisioned, and no live revocation check is
implemented by these values. Historical/archive adapters and future publication
transport still need an explicit binding-aware path before key-bound grants can be
used there. [Qualification evidence](../evidence/repository/2026-10-06-authenticated-key-binding/README.md).

## Authenticated binding

The trusted host must resolve an opaque credential binding consisting of issuer,
credential ID and generation, alongside principal/account/ACL information. No raw
API token belongs in a repository caller, command, receipt, ledger or log. The
issuer namespace prevents two credential stores from colliding. Binding IDs are
provisioned by the credential authority; request fields cannot supply them.

Add this as an explicit identity result at the resolver/context-to-repository
boundary. Keep scope checks on the existing `Caller`. Resolvers that only return
a principal remain usable for existing capabilities, but cannot issue a key-bound
creation context. Do not synthesize a credential identity from the principal or
silently upgrade such a resolver. Rotation to another key does not inherit an
outstanding creation grant. The host can explicitly authorize a new operation.

The repository must have authoritative durable binding state for the creation
path: enabled/revoked status and a monotonically fenced generation. A cached
transport identity is evidence of authentication at ingress, not proof the key is
still permitted to commit. External identity providers need a declared mechanism
to update this durable state; until that mechanism is qualified, do not advertise
immediate external revocation. No external network lookup runs under repository
revision locks.

## Durable credential primitive

V99 adds repository-local credential authority state keyed by issuer and opaque
credential UUID, with an immutable principal and current generation. The internal
RepositoryCredentialAuthorities port is process-only for provisioning, revocation
and compare-and-set rotation. Exact registration retries cannot change identity or
revive a revoked generation. Revoke is idempotent for the current generation;
revoking an absent or replaced generation returns NOT_FOUND and makes no promise
about a future registration. Rotation requires the expected current generation and
advances exactly one; stale/repeated rotation conflicts. It does not transfer grants.

The live check verifies exact principal/key/generation under a shared SQL lock in
the caller's existing transaction. A dedicated SQL function checks READ COMMITTED
even when the key is absent, without an extra client round trip. Mutation triggers
also enforce that isolation. Database constraints reject identity changes,
generation rollback/skips, same-generation revival and tombstone deletion.

Publication admission and replay now call this primitive for granted operations. It provides
no account membership and no creation grant. Its successful revocation applies to
future uses of this live-check primitive, not to all authenticated requests on the
platform. Key state is retained indefinitely at this stage; pruning requires the
future operation/grant retention proof. Existing operations gain no authority from
migration. [Evidence](../evidence/repository/2026-10-06-credential-authority/README.md).

## Grant and operation identity

A host with process authority provisions one immutable authorization record for an
exact account, principal, credential binding, operation UUID, command codec/version
and canonical command digest. Persist the operation-to-binding association so
another key for the same principal cannot resume it by guessing an operation UUID.
The grant also approves the bounded set of creation destinations and their current
placement commitments: stable drive UUID, drive snapshot, backend generation and
profile identity. The command already covers the proposed ownership/security,
datasource and source-deletion policy; use its canonical identity rather than
introducing a competing digest for those fields.

Grant installation must validate the full command and placement set, refuse an
existing operation with different bindings, and be idempotent only for the exact
same authorization. A grant is authority for absent destinations only. It does not
supply source READ, existing-target WRITE, ACL mutation, provider administration or
an evaluation verdict. Mixed batches still authorize every member and source.

Keep grant state separate from command and receipt protobufs. A private SQL-backed
implementation can make authorization atomic with the current ledger. The public
SPI must describe identity and authorization without importing SQL or provider
SDKs. No new publication RPC is required to qualify this foundation.

## Transaction and revocation rules

Use the existing owner/claim fencing and globally ordered revision-key locks.
Within the shared authorization path, acquire shared locks on the exact credential
binding and grant before accepting their state. Revocation touches only authority
rows and must not acquire owner or document locks afterward. When both binding and
grant are touched, acquire binding first. Review every caller of lockAndAuthorize,
not just registration, before integrating the grant check.

For first grant installation, reuse the unique immutable execution-scope insertion
from V79. In one transaction, attempt to insert a claimed scope for the exact
account/principal/operation. Only its insertion winner may install a new grant;
validate placement and live credential state and commit the scope and grant
atomically. A competing first claim or standalone operation insertion waits on that
same unique key. If admission wins first, a later installer cannot attach a grant.
If installation loses, it may only confirm an already installed identical live
grant. An existing bare scope is not permission to install one. Refuse it.

Do not add FOR UPDATE to establish_repository_execution_scope. Operation admission
can already hold a claim before inserting an operation, while a claim INSERT retry
reaches the scope trigger before waiting on the claim. Locking existing scopes in
that shared function creates a claim-to-scope versus scope-to-claim cycle. The
unique insertion protocol orders first admission without adding that lock.

Test both installation/admission winners, exact installation retry after claim
admission, orphan scope refusal, rollback after scope insertion, and concurrent
claim/operation retry. A revoke against an absent grant must return NOT_FOUND
without promising to prevent future installation, or use a separately specified
durable deny tombstone. An UPDATE affecting zero rows is not successful revocation.

Shared authority locks permit concurrent independent operations using one key.
There must be no global authorization mutex, account-wide exclusive lock, database
connection held across upload, or provider/registry I/O under SQL locks. An indexed
lookup bounded by the command's member limit must be part of the existing admission
transaction; do not add one transaction per member or per stage just to check a key.

Revocation takes an exclusive lock on the corresponding authority row. It either
wins before a commit's authorization check, causing refusal, or waits for an
already-authorized transaction to finish. A successful revocation response means
subsequent authorization transactions cannot use that binding/grant. Expiry is
checked against database time at the transaction's authorization decision. It does
not promise that physical commit finishes before the wall-clock deadline. No
lease or authorization lock spans a provider upload.

Recheck at registration, upload admission, assessment start, final publication and
successor activation. Checks before expensive work reduce waste; the final locked
check establishes publication authority. An upload that already reached a remote
provider when authority is revoked remains recovery-owned and cannot be published.
Recovery may clean retained effects with private process authority; it must not
turn that authority into permission to publish for the revoked client.

## Observation, retries and retention

Pending and rejected operation observations check the current complete read set.
An absent `ifAbsent` destination additionally needs the live creation grant.
Source READ is never waived. A readable stale revision can still be observed as
pending; observation does not reapply write preconditions or grant takeover.
Unknown operations retain the existing NOT_OBSERVED behavior. The existing
principal/account-scoped command-conflict signal is checked before document READ:
a caller naming its own operation with a different command can receive a conflict
without current document access. Do not describe this as complete existence hiding.

Successful replay checks current target READ and the original durable operation
binding. Revoking creation authority alone does not erase a committed receipt or
require a new create grant. Grant expiry or revocation refuses unfinished creation,
including pending/rejected observations that need authority for an absent target.
It does not revoke access to a committed receipt. Successful replay still requires
the original credential binding and a live credential generation: credential
revocation or rotation therefore refuses that key even after success. This does
not by itself revoke authentication on unrelated services.
Broader key revocation also requires the resolver to stop authenticating the key,
with separately qualified cache invalidation. A new credential needs an explicitly defined access/recovery path rather than an
implicit transfer of execution authority.

Revocation is terminal for a grant identity. Do not delete and recreate it to
resurrect an old operation. Retain a bounded tombstone through operation/retry
retention. Active operations and recovery obligations pin required authorization
records; cleanup cannot orphan recoverable provider effects. Define grant limits,
expiry and account retention before exposing provisioning to clients.

## Implementation and acceptance order

1. Add trusted credential-binding propagation with tests for two keys sharing one
   principal, rotation, absent identity, resolver outage and spoofed request fields.
   Default bindings fail closed for scoped creation.
2. Add durable binding/grant records and process-only provisioning/revocation.
   Test exact installation retry, conflicting operation/command, wrong key generation,
   expiry and terminal revocation. Apply migrations to existing repository rows;
   legacy operations gain no creation grant automatically.
3. Extend the shared admission gate. Prove positive scoped creation and refusal for
   wrong account, target, ownership, command or placement; keep source READ and
   existing-target policy constraints. Use real PostgreSQL barriers to race revoke
   against admission/commit and another creator against an absent target.
4. Integrate retained sessions and fresh-process recovery. Cover pending/rejected/
   successful replay, key rotation, revoke during upload, uncertain commit response,
   cancellation and successor activation. Prove denied work produces no successful
   receipt and no newly authorized provider effect.
5. Run equivalent library/transport cases and a controlled RustFS concurrency
   measurement. Record SQL transaction/connection counts and verify shared-key reads
   overlap. Only then document an available scoped creation entry point.

This supplies authorization primitives for the repository foundation. It does not
claim JCR sessions, workspaces, node types, references or version-restore semantics;
those remain governed by the [JCR capability assessment](repository-jcr-compatibility.md).

## Durable creation grant primitive (V100)

The internal installer now stores exact command and placement hashes with key
identity/generation and expiry. Only the winner of a new execution-scope insertion
can install a grant, in the same transaction. A transaction stamp and SQL guard
prevent adoption of previously admitted or bare scopes. Existing-scope retries only
confirm an identical live grant and do not revalidate current placement; that is
an installation fact, not permission to publish.

Grant readers lock credential authority before the grant and evaluate expiry after
any row-lock wait. Admission must first validate its selected drive snapshots, then
pass a digest derived from those actual placements. A digest copied from the grant
or supplied by a request cannot establish placement validity. Placement hashing
streams through the existing aggregate and per-field bounds without persisting a
second snapshot. Scope arbitration adds no lock on existing scopes.

Publication integration is implemented and under qualification. The primitive does not grant account
membership, source READ, existing-target WRITE or policy changes. Revocation retains
a tombstone; retention limits and recovery behavior remain governed by the design
above. [Qualification](../evidence/repository/2026-10-06-creation-grants/README.md).

## Admission integration trace (2026-10-06)

Source baseline: `6e2616f692993b9fcc218cc802f897b4f2e98af8`. The following
execution paths must move together before scoped creation is enabled:

| Existing path | Current locks/checks | Required change |
| --- | --- | --- |
| `DocumentPublicationRegistration` preflight and `admitInitial` | Initial path acquires claim/coordinator binding before document authorization; preflight has no owner | Complete selected-placement and grant authorization in both paths, preserving two transactions total |
| `DocumentOperationUploadAdmission` capture and staging | Owner fence, document authorization; staging then checks drives/profiles | Shared completed authorization for capture and staging, before any retained read or attempt insertion |
| `DocumentAssessmentCreation` journal-load authorization and CREATE | Owner fence, document authorization; CREATE then checks drives/profiles | Recheck grant in both transactions; retain mode, source and artifact fences |
| `DocumentPublicationCommit` | Owner, schema policy, document authorization, modes, drives/profiles | Complete authorization before writes; grant locks remain held through SQL commit |
| `RepositorySuccessorExecution` activation and attach | Claim/owner then document authorization | Use execution caller's original key, never recovery process authority to approve publication |
| Pending/rejected observation, assessment delivery and reserved preparation | Command/read-set authorization, no selected placement argument | Check immutable operation binding and live grant for uncreated targets without treating a stored digest as current placement proof |
| Successful replay | Owner observation and current READ policy | Check original operation key binding and live credential; do not require unexpired/unrevoked creation grant |

Do not add a grant lookup to the current `lockAndAuthorize` and leave later drive
locks intact. The installer takes drive locks before authority locks, while these
execution paths currently take drives after document authorization. Grant checks
there would introduce a drive/authority inversion. Keep the old entry point
fail-closed until execution paths use a completed shared authorization step.

Prepare the immutable placement set and bounded digest outside SQL locks. In the
transaction, retain claim/owner/schema-policy ordering, lock and authorize the
complete revision/read set, validate sorted drive snapshots and immutable backend
profiles, then lock credential followed by grant. The digest is valid only after
those actual selected snapshots match. Preserve each composition's backend gate;
a generic SQL snapshot check must not silently bypass `DriveLedger.validateBackend`.
No provider or registry I/O may occur under these locks.

The shared step must distinguish absent `ifAbsent` destinations from missing
expected-existing destinations, and preserve READ for every source and WRITE for
every existing target. An exact grant covers proposed ownership only for new
objects; it cannot waive policy/datasource changes on existing objects. Check the
whole set before reporting stale revisions. Do not accidentally dereference a
missing document when comparing existing policy fields.

Acceptance starts with a real journaled-session scoped creation test. Then test
wrong key/account/command/placement, mixed denied sources or existing targets,
revocation winning before each phase, revocation waiting for final commit, expiry
while blocked, grant-only revocation after successful commit, and credential
rotation after commit. Preserve exact retry tokens, the current transaction budget,
and recovery ownership of already-started provider effects. This trace is a plan;
none of these integration behaviors is claimed by the V100 primitive evidence.
