# Signed work records

Status: design of record for the receipt layer — portable, signed,
offline-verifiable records of work the platform performed. Anything
implemented here changes this chapter first. The planned-work entry
([evidence and receipts](planned-work.md)) states the gap this closes;
this chapter states the decisions.

## What a record is

The platform keeps bounded, fingerprinted, gap-honest evidence of
everything it does: workflow run evidence with per-step results and
fingerprints, delegation transcripts with every protocol frame in
recorded order, masking results that name their unresolved paths,
screening findings that never quote text, physical plans, config
versions. What none of that evidence can do is leave. Nothing signs
it, and nothing outside the platform can check it.

A **work record** is one projection of that evidence into a canonical,
signed, self-contained message that survives the platform: a relying
party holding only the record, the issuer's public key, and a trust
snapshot can verify it with zero network calls, years later, against a
platform that no longer exists. The record is a projection, not a
second evidence system — every fact in it is a fact the platform
already emits, and the projector adds nothing the work did not
produce.

## The hard line

A verified record proves **integrity and attribution**: these bytes
are what the named issuer signed, and they have not changed. It does
not prove the issuer told the truth, that the work was correct, that
the clock was honest, or that the record describes everything that
happened. Verification therefore reports its **non-claims** as stable
machine-readable identifiers alongside its checks —
`issuer-honesty`, `trusted-time`, `world-completeness`,
`execution-correctness`, and `artifact-custody` whenever artifact
bytes are not supplied — and nothing in this layer ever presents a
signature as an endorsement. This is the same honesty discipline the
evidence objects already follow: a masking result that cannot resolve
a path says so, promoted to the layer boundary.

## Canonical bytes: deterministic protobuf

The manifest is a protobuf message with two mandated properties: the
message contains no map fields, and it is serialized with the
deterministic serializer. Together those make the bytes a function of
the content — the discipline the platform's fingerprints already rest
on (tag-ordered, map-free serialization), here made an explicit rule
of the format rather than a property of the message that happens to
hold. The record's identity is `sha256(manifest_bytes)`, the
**manifest digest**. There is no second canonicalization (no
canonical JSON): one platform, one byte discipline, one place to be
wrong.

Two consequences are accepted deliberately:

- **The digest binds bytes, not semantics.** Two serializations of the
  same logical manifest are two different records. The deterministic
  serializer exists so independently produced projections of the same
  evidence compare equal; verification itself never re-canonicalizes —
  it hashes and checks the bytes as given.
- **A strict parse is part of verification.** The verifier bounds what
  it accepts: a record of at most 4 MiB, no unknown fields in the
  container, a manifest version it recognizes, reserialization
  equality (parsing the manifest and deterministically reserializing
  it reproduces the input bytes exactly). A record that fails any
  bound is refused by name, never partially accepted.

## The container

A signed record is one message:

- the manifest bytes, exactly as signed;
- one or more detached signatures, each naming its key id and
  algorithm; and
- nothing else.

Artifact bytes never ride in the container. The manifest references
them through `RecordArtifact` — digest, media type, size, redaction
flag — and custody of the bytes is a separate concern with a separate
check (below). This keeps records small, bounded, and safe to store
anywhere.

The container is strict: unknown fields refuse, absent signatures
refuse, a duplicate signature by one key refuses, an algorithm the
profile does not name refuses. The v1 profile admits exactly one
algorithm, **Ed25519** (RFC 8032), because the JDK provides it
natively and a zero-dependency verifier stays possible in any language
with an Ed25519 primitive.

## The manifest

The manifest carries, all as facts the platform already emits:

- **Identity**: manifest version, record id, issuer id, key id,
  issuance timestamp *as claimed by the issuer* (a non-claim covers
  its honesty).
- **Subject**: what work this records — a subject kind plus the
  identity fields that kind demands (below).
- **Evidence**: per-step results with their outcomes, timing where the
  evidence has it, service targets, artifact references, model
  identity, and token counters.
- **Completeness**: `COMPLETE`, `PARTIAL`, or `HISTORICAL`, with
  ordered missing-evidence reasons, evaluated against a committed
  evidence policy named by id, version, and digest. The honest
  counters that exist elsewhere — rows unenriched, unresolved payload
  paths — become signed facts here instead of log lines.
- **Revision link**: the prior record's manifest digest when this
  record re-issues one, the versioned-workflow fingerprint pattern
  applied to evidence. A revision chain is walkable offline.
- **Disclosure**: when the record is a masked projection (below), the
  original's manifest digest and the sensitivity policy that produced
  the projection.

## Subject kinds

The subject answers "what work is this?", and it answers differently
for different families of work. `RecordSubject` therefore carries a
`kind` slug and the union of the identity fields the kinds need, with
three declared message rules keeping the two honest:

- `subject.known_kind` — the kind is one this manifest version
  defines. The vocabulary is closed per manifest version, and v1
  defines exactly `workflow-run` and `delegation-task`.
- `subject.workflow_fields` — a `workflow-run` subject names the
  workflow (`workflow_name`), its definition fingerprint
  (`workflow_fingerprint`), and the run (`run_id`), with
  `workflow_version` set when the run executed a promoted version and
  empty for a draft probe.
- `subject.delegation_fields` — a `delegation-task` subject names the
  task (`task_id`, field 6, a UUID), the worker it was delegated to
  (`worker_id`, field 7, a slug), and the fingerprint of the offered
  spec (`spec_sha256`, field 8).

The identity fields are optional at the field level and formatted
whatever the kind — a set `task_id` must be a UUID, a set
`workflow_fingerprint` must be a lowercase hex SHA-256 — and the
per-kind rules then demand the fields that kind cannot do without.
Adding a kind is a new rule and a new set of fields, never a
reinterpretation of the ones already frozen.

`spec_sha256` is the delegation subject's load-bearing fact: the
SHA-256 of the offered `TaskSpec`'s deterministic bytes, computed by
the same fingerprint primitive the rest of the platform uses. It is
the contract the work was judged against, playing exactly the role
`workflow_fingerprint` plays for a run. A relying party holding the
spec can prove which contract the record attests; one holding only the
record still knows there was exactly one.

## Trust is an explicit input

Verification takes two inputs: the record and a **trust snapshot**.
The snapshot names issuers, their keys, each key's state (active,
retired, revoked), each key's validity window, and — per issuer — the
subject kinds that issuer may sign. Authorization is explicit and
positive: an issuer trusted for `workflow-run` and not for
`delegation-task` is refused on a delegation record even though every
signature verifies. The verifier makes zero network calls; whoever
runs it chooses which snapshot to trust, and that choice is theirs,
not the record's.

The snapshot is itself a typed config document. Inside the platform it
distributes on the [config lane](config-distribution.md) exactly like
taxonomies, screening mounts, and postal packs: registry-stored,
verify-then-swap, commit as version, subject `trust-snapshot`. Outside
the platform it is a file the relying party pins. Same document, two
custody models, and the lane is never required for verification.

The workbench's verifying verbs hold both. `PROTOMOLT_TRUST_SNAPSHOT`
pins a file the server defaults to; a server following the lane
(`PROTOMOLT_CONFIG_URL`, with `PROTOMOLT_CONFIG_REFRESH_SECONDS` to keep
pulling) prefers the lane's snapshot once a document has applied, and
falls back to the pinned file otherwise — so a lane outage costs
freshness, never custody. The verbs read that source per request rather
than at registration, which is what lets a published snapshot re-scope a
running node with no restart. A request's own snapshot still wins over
both, because custody is the default and never an override.

## Verification: named checks, named refusals

The verifier runs a fixed ordered pipeline, each check with a stable
identifier, each failure refusing by name:

1. `container-bounds`: the size cap, a strict parse with unknown
   fields refused, at least one signature, no duplicate signing key,
   every algorithm inside the profile;
2. `manifest-parse`: a strict parse with unknown fields refused, a
   known manifest version, and every rule the manifest declares
   holding — the subject's per-kind rules, and the revision and
   disclosure links, which are digest-formed by declared rule, so link
   well-formedness is part of this check;
3. `reserialization-equality`: deterministically reserializing the
   parsed manifest reproduces the signed bytes exactly;
4. `key-trusted`: the issuer is in the snapshot, the manifest's own
   key carries a signature in the container, every signing key
   resolves under that issuer, none is revoked, and the claimed
   issuance time falls inside each key's validity window — keys
   resolve before signatures can be checked, which is why this
   precedes the signature check;
5. `signature-valid`: every signature verifies over the manifest
   bytes, the manifest's own key among them;
6. `issuer-authorized`: the snapshot authorizes this issuer for the
   record's subject kind;
7. `completeness-consistent`: missing reasons present exactly when the
   record is not complete;
8. `artifact-rehash`, when bytes are supplied: all-or-nothing — every
   referenced artifact, at run scope and on every step, matches digest
   and size, or the check fails as a unit; skipped, and named as
   skipped, when none are supplied.

Walking a revision chain — following prior digests across records — is
the relying party's traversal over verified records, not a
single-record check.

The result carries every check's outcome plus the non-claims. A
relying party who wants a decision, not a checklist, uses the
evaluation sidecar (below); the verifier itself never collapses the
checklist into a verdict beyond "all checks passed".

## The projector seam

A projector is a pure function: **evidence in, manifest out**. It
reads one terminal evidence object, maps every field it emits to a
field, fingerprint, or counter that evidence already holds, names the
committed policy its completeness claim is measured against, and
returns. It signs nothing, reads no clock of its own, resolves no
keys, and touches no trust snapshot — issuance identity arrives as an
`Issuance` argument, and signing happens after the projection is
built. That narrowness is the whole design: new families of evidence
plug in without touching signing, verification, or the format.

Two rules hold across every projector, and both are refusals of the
same kind:

- **Only terminal evidence projects.** A record claims what work
  produced, and work still running is still producing. Live evidence
  is refused, by name, with the reason.
- **Incompleteness is recorded, not refused.** Work that ended without
  succeeding projects as `PARTIAL` with ordered reasons. Recording
  honest incompleteness is the point; a receipt layer that could only
  describe successes would be a marketing layer.

### Workflow runs

The workflow projector reads terminal `RunEvidence` and emits a
`workflow-run` manifest: the workflow name, version, fingerprint, and
run id as the subject; one step per recorded step with its outcome,
timing, method, and content-addressed request and response artifacts;
model identity and token usage for model-driven steps; and the run's
input and output artifacts by digest. Completeness is measured against
policy `workflow-run-evidence`, version 1, whose digest is the SHA-256
of the policy text signed into the record. A succeeded run projects
`COMPLETE`; a failed one projects `PARTIAL` whose reason is the run's
own failure summary, bounded, or a plain statement that it failed
before completing when the run left no summary; a cancelled one
projects `PARTIAL` saying so.

### Delegated tasks

The delegation projector is the seam's peer, not its appendix. It
reads a terminal task's transcript — the recorded frames of the
delegation protocol, in recorded order — and emits a
`delegation-task` manifest measured against policy
`delegation-task-evidence`, version 1, digested the same way.

The subject is the task's identity: the task id, the worker the offer
went to, and the offered spec's fingerprint. A transcript with no
recorded offer is refused outright — without an offer there is no
contract to attest, and a record whose subject cannot name what the
work was judged against would be an assertion, not evidence.

The steps are the task's **lifecycle milestones**, each carrying the
words the participants actually recorded:

| Milestone | Outcome | Summary |
| --- | --- | --- |
| `offer` | succeeded | the spec's objective |
| `accept-attempt-<n>` | succeeded | — |
| `candidate-r<n>` | succeeded | the worker's summary of the candidate |
| `revision-r<n>` | failed | the reviewer's feedback |
| `accepted` | succeeded | the reviewer's verdict |
| `cancelled` | cancelled | the cancelling side's reason or note |
| `expired` | failed | — |
| `failed` | failed | the worker's error |

Those summaries are the chapter's quiet claim: a delegation record
does not merely say a task was accepted, it carries the sentence the
reviewer wrote when accepting it and the sentence they wrote when
sending a revision back, bounded and signed. A judgement without a
reason is not one a transcript can defend later, so the record carries
the reason.

Everything else on the wire — admissions, lease renewals, heartbeats,
progress, checkpoints, free messages — is recorded context rather than
a milestone, and none of it is dropped. The transcript's own
deterministic bytes ride as a content-addressed artifact
(`application/x-protobuf`, no parameters, because the manifest's
media-type rule admits none; which message those bytes decode as is
stated by the policy text instead). A relying party holding the
transcript can check it against the record; one holding only the
record still knows exactly which transcript it attests. Alongside it
sit the accepted candidate's own artifacts by digest — the candidate
open at the moment of acceptance, since a revision request closes the
one before it.

Terminality decides completeness. Acceptance projects `COMPLETE`. Each
other terminal ending projects `PARTIAL` with the reason that fits it:
the task was cancelled before acceptance, the lease expired before
acceptance, the worker reported failure before acceptance, the worker
withdrew before acceptance. A task still in flight projects nothing at
all.

### What the second projector proves

Two projectors over unrelated evidence families, sharing one manifest,
one signer, one verifier, and one corpus, are the seam's proof. The
delegation family looks nothing like the workflow family: its identity
is a task and a worker rather than a workflow and a run, its steps are
protocol milestones rather than service invocations, its "step
outcomes" are review verdicts, and its highest-fidelity evidence is a
single blob of frames rather than per-step request and response pairs.
None of that reached the format. It cost one subject kind, one CEL
rule, three identity fields, and one policy document — and no change
whatsoever to canonical bytes, the container, signing, key custody, or
the verification pipeline.

> **The same server issues both.** The platform runs as containers:
> `./gradlew :protomolt-serve:installDist :protomolt-acp-agent:installDist
> && docker compose build && docker compose up` brings up HTTP on 8080
> (Console at `/console`, MCP at `/mcp`, Swagger at `/docs`), gRPC on
> 9090, and the registry on 8081 (see [docker](../apps/docker.md)).
> Workflow records come from the workbench verbs; a terminal
> delegation hands over its receipt from the task console. An agent
> reaches the same surface over MCP with
> `claude mcp add --transport http protomolt http://localhost:8080/mcp`.

## Signing and key custody

The signer is a small primitive over the JDK's Ed25519: canonical
bytes in, signature out, key id attached. The private key is resolved
from an operator-supplied reference and is never stored in config
documents, the registry, logs, or errors — the credential discipline
service profiles already follow. Rotation is a new key id plus a trust
snapshot update; the old key retires with its validity window intact,
so records it signed keep verifying. Revocation is a snapshot state
change, and what a revoked key's past signatures mean is the relying
party's policy, not ours — the verifier reports the state and the
window, nothing more.

> **Signing identity is all-or-nothing.** `PROTOMOLT_RECEIPT_KEY_FILE`
> (the file holding the raw 32-byte Ed25519 seed),
> `PROTOMOLT_RECEIPT_KEY_ID` (the id the trust snapshot knows the key
> by), and `PROTOMOLT_RECEIPT_ISSUER` (the issuer name records carry)
> configure a signing server. None of the three set means a server
> that issues no records; some but not all set is a startup refusal
> naming the missing variables, because a half-configured signing
> identity is a way to sign the wrong thing.

Multiple signatures on one manifest are legal in the container from
the start. The issuer signs in v1; countersignatures (a reviewer, a
second coordinator, a mesh peer) are a later composition that changes
no byte of the manifest.

## Registry transparency

Publishing a record's manifest digest to the registry — which already
treats a Git commit as a version — yields an append-only, replicated
log of issued records for free. The log is one typed document
(`WorkRecordLog`, subject `work-records`) appended through the
existing config gate: each publish is a registry commit, the commit
history is the append-only view, and the document is the current one.
A relying party who can see the registry can detect equivocation (two
entries claiming one record id with different digests) and rollback
(an entry the history shows and the document no longer carries). This
is a projection of the registry's existing behavior, not a new
transparency system; SCITT-style receipts remain a later composition.

## Disclosure projections

A record that must cross an audience boundary is not redacted in
place. The masker and screener run over the evidence first — the run
recorder already masks `pii` and `secret` classes before an artifact
is saved, so this extends a discipline the evidence is born under —
the projector emits a new manifest from the masked evidence, and the
issuer signs the projection as its own record carrying the original's
digest and the policy that produced it. Every audience gets a
whole, verifiable record; nobody gets a record with holes punched in
someone else's signature. Unresolved mask paths remain what they are
everywhere else in the platform: named facts, here signed ones.

## Replay-backed evaluation

Replay re-executes a recorded run. The **evaluation sidecar** wraps it
in a reproducible decision procedure for relying parties: frozen
inputs, per-check results, a predeclared policy, and a versioned
evaluation record written *beside* the original — the signed record is
never modified. This is the platform's structural advantage over a
format-only receipt: a record here is not just checkable, it is
re-runnable.

## Conformance corpus

The corpus is part of the format, not part of the tests: a set of
valid records and a larger set of invalid ones, each invalid fixture
failing exactly one named check. The fixtures are byte-stable — fixed
RFC 8032 test seeds, constant timestamps, deterministic Ed25519 — and
a freeze test pins their digests. The platform verifier must pass the
corpus, and any external verifier claims conformance by passing the
same corpus. Freezing v1 of the manifest is a compatibility promise of
the same class as the validate.v1 dialect; the corpus is what makes
the promise testable.

One fixture is worth reading on its own. `unauthorized-kind` is a
schema-valid `delegation-task` subject — a real UUID task id, a slug
worker id, a well-formed spec digest — signed by an issuer the
snapshot authorizes only for `workflow-run`. Every manifest rule
holds, every signature verifies, and the record is still refused, at
`issuer-authorized` and nowhere else. That fixture is how the corpus
pins the boundary between *well-formed* and *authorized*: the format
says what a record may say, and the trust snapshot says who may say
it.

> **Verify anywhere.** The first external verifier is
> [`protomolt-record-verifier`](../apps/record-verifier.md): pure JDK,
> zero dependencies, sharing nothing with this runtime but the wire
> contract. It restates the format by hand — a strict canonical wire
> reader stands in for reserialization equality — and its cross-check
> suite holds it to the runtime's verdict on every corpus fixture and
> to acceptance agreement on randomly mutated records.

## Deliberately out of v1

- Trusted timestamps (RFC 3161) — composable later; issuance time
  stays a claimed fact with a non-claim.
- Transparency receipts (SCITT) — the registry log stands in.
- Environment attestation (RATS/EAT).
- Hardware key custody — the key reference seam admits it without a
  format change.
