# Authorization scopes

Status: design of record for separating authentication from
authorization. Anything implemented here changes this chapter first.
The planned-work entry ([security and operations](planned-work.md))
states the gap this closes; this chapter states the decisions.

## The gap

Authentication answers who is calling; authorization answers what that
caller may do. Every guarded surface today stops at the first
question: one shared secret (`PROTOMOLT_API_TOKEN`), compared in
constant time, unlocks the entire process — all forty-plus catalog
verbs, the registry's writes as well as its reads, every workflow and
delegation verb. The one authorization decision the server can express
is all-or-nothing: supplying the token disables the browser console
and its proxies entirely, because a page that could hold the process
credential would hold total authority.

The consequences reach beyond the surfaces themselves. The metric
mapping's row-level security and compile-time query rewrite are
explicitly sequenced behind this layer; a masking policy that derives
classes from the caller instead of trusting the caller to ask needs a
caller identity to derive from; scoped console sessions need something
narrower than the process credential to put in a cookie.

## What a scope is

A **scope** is a named unit of authority over a family of operations.
A **principal** is a named caller holding a set of scopes. A request
is refused unless the principal holds the scope the operation
declares. Scopes only gate; they never transform a request, and a
caller can never widen what a scope grants by phrasing the request
differently.

This generalizes the intake service's model, which is the in-tree
precedent: a credential resolves to an authority object, requests may
only narrow within it, and anything outside it is `PERMISSION_DENIED`
by name. The account service's `IdentityResolver` seam was declared
for exactly this arrival; this layer is its first consumer surface.

## The scope vocabulary

Scopes are kebab-case slugs. The vocabulary is closed: a policy
document naming a scope outside it is refused at load, so a typo in a
policy is a loud failure, not a silently dead grant.

| Scope | Grants |
|---|---|
| `schema-read` | Reading schemas and every pure computation over caller-supplied or registered schemas: compilation, validation, diffing, compatibility, rendering, inference, metadata extraction, masking transforms, CEL evaluation, mapping, joins, merges, shape synthesis |
| `schema-write` | Registry mutation: publishing subjects and configuration, federation sync that pushes |
| `service-invoke` | Calling other services through the platform: reflection-driven invocation, chain execution and submission, job inspection and completion, model inference |
| `workflow-run` | Workflow and pipeline execution and their evidence verbs: recording, replay, promotion, work-record export and evaluation |
| `artifact-access` | Reading and writing the artifact repository outside a workflow run's own recording |
| `worker-coordinate` | The delegation and mesh coordination surfaces: offering tasks, accepting checkpoints, steering workers, node registration and capacity |
| `search-query` | Querying a search service |
| `search-index` | The search service's workflow-driven indexing, deletion, and replay verbs |
| `metrics-query` | Querying a metric service: describing mappings and running aggregate queries |
| `metrics-rebuild` | Rebuilding a metric service's rollup tables |

Every scope in the vocabulary guards at least one live operation, and
every guarded operation names exactly one required scope. Operations
that only describe the server to its caller — health, OpenAPI, the
tool manifest's shape — stay behind authentication alone, as today.

## Declaring requirements

The action catalog is the single dispatch point for gRPC, REST, MCP,
the CLI, and the ACP agent, and the RPC-to-action-to-tool mapping is a
pure name transform. The required scope is therefore declared once, on
the action:

```java
public interface ProtoAction {
    /** The scope this action requires, from the closed vocabulary. */
    default String requiredScope() { return ""; }
    // name(), description(), inputSchema(), execute(...)
}
```

Every built-in action declares its scope. An action that declares none
is served normally when no access policy is mounted and refused by
name when one is: a plugin author who has not thought about
authorization gets a working action on an open server and a loud,
attributable refusal — never a silent grant — on a scoped one.

Surfaces that are not catalog verbs carry their own declarations in
the same vocabulary: the registry server's routes split into
`schema-read` and `schema-write` at the route table, with its action
endpoint dispatching through the scoped catalog so each verb's own
declaration applies; the search and metric services declare their
scopes per gRPC service, with method overrides where one verb differs.

## The access policy document

Authorization is configuration, and it follows the platform's
configuration rules: a protobuf document, validated before it takes
effect, versioned by its commit when it travels the config lane, and
carrying **no secrets** — credentials appear only as SHA-256 digests.

```protobuf
package ai.pipestream.proto.authz.v1;

message AccessPolicy {
  repeated Principal principals = 1;  // named, non-empty, unique names
}

message Principal {
  string name = 1;                        // slug, unique in the policy
  repeated string credential_sha256 = 2;  // lowercase hex; several during rotation
  repeated string scopes = 3;             // from the closed vocabulary, unique
}
```

A presented credential is hashed and looked up; the match yields the
principal and its scopes. Several digests on one principal make
rotation-with-grace a property of the model rather than a feature: the
current and the outgoing credential resolve to the same authority
until the old digest is removed.

A policy is refused at load — naming the defect — when a principal
name is duplicated or not a slug, a digest is duplicated anywhere in
the document or is not 64 lowercase hex characters, a scope is outside
the vocabulary or duplicated on a principal, or a principal holds no
scopes. On the config lane the refusal keeps the previous policy live,
exactly as the trust-snapshot and postal mounts behave.

The document reaches the server two ways: a file named by
`PROTOMOLT_ACCESS_POLICY` (JSON or binary `AccessPolicy`), read at
startup, for standalone serving; and the config-lane subject
`access-policy` for platform deployments, verify-then-swap, so a
running fleet re-scopes without a restart.

## The operator credential

`PROTOMOLT_API_TOKEN` keeps its meaning: the process credential,
holding every scope, always. Mounting a policy adds narrower
principals; it never narrows the operator, because whoever can set the
process environment already owns the process, and because a policy
must not be able to revoke the only credential that could correct it.
A deployment that wants no unrestricted credential in circulation
treats the operator token as it treats a root password: set at
provisioning, stored in the secret manager, never distributed.

Callers therefore fall into exactly three cases: the operator token
(every scope), a credential whose digest a mounted policy names (that
principal's scopes), and everything else (`UNAUTHENTICATED`, as
today). Local process-boundary surfaces — the CLI and the stdio MCP
server — run with the operating-system user's authority and hold every
scope; the boundary there is process ownership, not a header.

## Enforcement

Resolution happens once per request (or once per MCP session, at
initialization), at the transport edge where the credential is
presented. The resolved caller travels explicitly — a `Caller` value
passed to the catalog, a gRPC `Context` key inside the serving roles —
never a thread-local guessed at later.

| Surface | Where |
|---|---|
| gRPC service | The token interceptor resolves the caller onto the call context; the per-method handler checks the action's scope before dispatch |
| REST gateway | The per-route token check resolves the caller; a missing scope is `403` with the same named refusal (`401` stays what it means: not authenticated) |
| MCP over HTTP | The caller is pinned to the session at `initialize`; `tools/list` serves only tools whose scope the caller holds; `tools/call` refuses the rest by name |
| Registry server | Route-table split before dispatch: reads, writes, and action execution each name their scope |
| Search and metric services | An identity interceptor beside the validating one, mounted only when the service is started with a resolver; a service without one stays an open, trusted-network surface, as today |
| Console sessions | A console login issues a session bound to a principal's scopes, never to the operator token — which un-disables the console in token mode, closing the console-sessions item |

The refusal is uniform everywhere: error code `permission-denied`,
gRPC `PERMISSION_DENIED`, HTTP `403`, message naming the principal,
the missing scope, and the operation —
`caller 'ci-reader' does not hold 'schema-write', which publish-config
requires`. The credential itself never appears in a refusal, a log
line, or an error detail; principals are named, credentials are not.

## The platform's roles

The document platform wires this layer through role selection, so a
node's security posture is one environment decision, not per-service
assembly:

- `PROTOMOLT_API_TOKEN` set makes the node guarded: the registry's
  HTTP surface and the search and metric gRPC servers all demand a
  credential, and the node's own outbound calls — remote-role
  channels, the boot publish of the platform contracts, the config
  lane's registry pulls — present the same token, so a split-role
  fleet stays whole when every node is guarded. Unset, the node is the
  open, trusted-network surface it always was.
- `PROTOMOLT_ACCESS_POLICY` names a policy file read at boot; the
  config-lane subject `access-policy` re-scopes a running node without
  a restart, behind the same verify-then-swap gate as every other
  mount, so a malformed document keeps the previous policy live. A
  guarded registry node publishes the `AccessPolicy` contract at boot
  beside the document model, which is what lets the config gate
  validate policy documents. A policy without the operator token
  refuses at boot, naming both variables.
- The intake role keeps its own account-bound resolver: tenancy and
  operator scoping stay separate layers, as below. The in-process
  channels between co-mounted roles stay inside the process trust
  boundary; the guard sits on the network edges.
- A guarded node refuses to mount the search console, which has no
  principal sessions yet: serving it would put an unauthenticated
  browser surface in front of guarded services. Unmount the role or
  run the node open.

## What this layer does not do

- **It is not row-level security.** A scope gates operations, not
  rows. The metric mapping's compile-time rewrite (drop members,
  inject filters from the caller) is sequenced after this layer and
  consumes the caller identity it introduces; it is not part of it.
- **It does not issue credentials.** There is no token mint, no
  expiry, no JWT parsing. A credential is an opaque string whose
  digest a policy names; issuance and distribution belong to the
  operator or an external IdP. The resolver is a seam — the intake
  service already resolves keys against OIDC introspection and JDBC
  stores, and the same seam here admits the same stores.
- **It does not authenticate workers.** A delegation worker's id is an
  application-level claim inside an already-authorized coordinator
  stream; `worker-coordinate` gates who may open that stream, not
  which worker id it may assert.
- **MCP resources stay behind authentication.** `tools/list` and
  `tools/call` are scope-checked; the resource reads (registry
  documents, service profiles, delegation transcripts) remain
  authenticated-only in this version, a recorded edge rather than a
  silent one.
- **It is not tenancy.** The intake service's account-bound scope, with
  its per-account axes, stays exactly as it is; this vocabulary gates
  the operator-facing surfaces. Mapping external principals onto
  accounts remains the account service's `IdentityResolver` seam.
- **Masking still masks what it is asked to mask.** This layer lets a
  future policy derive the classes from the caller; the derivation
  rule itself is deliberately not designed here.

## The testing contract

The planned-work sentence is the contract: test every route and every
action against its required scope.

- An inventory test walks every registered action in every shipped
  catalog and asserts its declared scope is in the vocabulary — an
  action without one cannot land.
- A matrix test executes every catalog action as a principal holding
  everything except the required scope and asserts the named
  refusal arrives before the action runs; and as a principal holding
  only that scope, asserting the refusal does not.
- Route-level tests on each surface (the secured-serve pattern) pin
  the observable behavior: status codes, refusal text, no credential
  echo, `tools/list` filtering, and the unchanged open surfaces.
- Policy validation tests refuse each malformed-document case by
  name, and the config-lane test proves a refused policy keeps its
  predecessor live.

## Composable later, deliberately not in v1

- Signed scope assertions (a receipt-layer trust snapshot vouching for
  an external issuer's scope claims) — the verification machinery
  exists; binding it to call credentials is its own decision.
- OIDC introspection and JDBC resolvers at the serve layer, mirroring
  the intake service's stores.
- Per-scope rate and payload budgets, following the intake scope's
  per-key caps.
- The metric mapping's row-level rewrite, which starts from the
  `Caller` this layer establishes.
