# Scoped repository creation authority

Status: design for implementation, not an available API. Baseline:
`6bc32d582a65e1c17ea79340e662d8ab96c734f1`. This extends the ownership work in
[repository composition](repository-composition.md). It does not replace the
remaining recovery, archival, provider or progressive-hydration work.

## What is missing

`RepositoryCaller` carries a principal, process authority, account memberships
and ACL identities. It deliberately grants no right to create an absent target.
`DocumentAdmissionAuthorization.lockAndAuthorize` therefore refuses scoped creation.
Existing-target updates still require WRITE and preserve policy, datasource and
placement. Proposed ownership in a request cannot establish caller authority.

Credential resolution currently loses key identity. `CallerResolver` returns a
`Caller`; `AccessPolicyCallers` maps all rotation credentials of one principal to
that same caller. `ApiTokenServerInterceptor` passes the caller into gRPC context.
A principal-bound grant would consequently be usable by every key for that
principal. Calling such a grant key-specific would be incorrect.

Network listeners now require a credential. That authenticates the operator or a
policy principal; it does not supply the following creation grant.

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
binding and grant before accepting their state. Grant provisioning must inspect/lock any existing operation first, then take the
required revision locks before binding and grant locks, and acquire no owner or
document locks afterward. Reuse the operation admission serialization for absent
owner rows; an unlocked absence query is not a fence. Revocation takes only the
authority locks and must not acquire owner/document locks afterward. When both
binding and grant are touched, acquire binding first. Test concurrent install and
admission explicitly. Review every caller of
`lockAndAuthorize`, not just registration, to verify a single lock order before
landing a migration.

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
require a new create grant. The durable creation binding check refuses this grant's execution/replay after
revocation; it does not by itself revoke authentication on unrelated services.
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
