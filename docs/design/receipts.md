# Signed work records

Status: design of record for the receipt layer — portable, signed,
offline-verifiable records of work the platform performed. Anything
implemented here changes this chapter first. The planned-work entry
([evidence and receipts](planned-work.md)) states the gap this closes;
this chapter states the decisions.

## What a record is

The platform already keeps bounded, fingerprinted, gap-honest evidence
of everything it does: workflow run evidence with per-step results and
fingerprints, masking results that name their unresolved paths,
screening findings that never quote text, physical plans, config
versions. What none of that evidence can do is leave. Nothing signs
it, and nothing outside the platform can check it.

A **work record** is one projection of that evidence into a canonical,
signed, self-contained message that survives the platform: a relying
party holding only the record, the issuer's public key, and a trust
snapshot can verify it with zero network calls, years later, against a
platform that no longer exists. The record is a projection, not a
second evidence system — every fact in it is a fact the platform
already emits, and the projector adds nothing the run did not produce.

## The hard line

A verified record proves **integrity and attribution**: these bytes
are what the named issuer signed, and they have not changed. It does
not prove the issuer told the truth, that the work was correct, that
the clock was honest, or that the record describes everything that
happened. Verification therefore reports its **non-claims** as stable
machine-readable identifiers alongside its checks, and nothing in this
layer ever presents a signature as an endorsement. This is the same
honesty discipline the evidence objects already follow — a masking
result that cannot resolve a path says so — promoted to the layer
boundary.

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
  it accepts: size limits, no unknown fields in the container, a
  manifest version it recognizes, reserialization equality (parsing
  the manifest and deterministically reserializing it reproduces the
  input bytes exactly). A record that fails any bound is refused by
  name, never partially accepted.

## The container

A signed record is one message:

- the manifest bytes, exactly as signed;
- one or more detached signatures, each naming its key id and
  algorithm; and
- nothing else.

Artifact bytes never ride in the container. The manifest references
them through `ArtifactReference` — digest, media type, size, redaction
flag — and custody of the bytes is a separate concern with a separate
check (below). This keeps records small, bounded, and safe to store
anywhere.

The container is strict: unknown fields refuse, absent signatures
refuse, an algorithm the profile does not name refuses. The v1 profile
admits exactly one algorithm, **Ed25519** (RFC 8032), because the JDK
provides it natively and a zero-dependency verifier stays possible in
any language with an Ed25519 primitive.

## The manifest

The manifest carries, all as facts the platform already emits:

- **Identity**: manifest version, record id, issuer id, key id,
  issuance timestamp *as claimed by the issuer* (a non-claim covers
  its honesty).
- **Subject**: what work this records — the workflow name and
  fingerprint, the run id, the versioned-workflow version when one
  exists.
- **Evidence**: per-step results with their fingerprints, service
  targets, timing, and outcome; artifact references; usage counters.
- **Completeness**: `COMPLETE`, `PARTIAL`, or `HISTORICAL`, with
  ordered missing-evidence reasons, evaluated against a committed
  evidence policy named by id, version, and digest. The honest
  counters that exist today — rows unenriched, unresolved payload
  paths — become signed facts here instead of log lines.
- **Revision link**: the prior record's manifest digest when this
  record re-issues one, the versioned-workflow fingerprint pattern
  applied to evidence. A revision chain is walkable offline.
- **Disclosure**: when the record is a masked projection (below), the
  original's manifest digest and the sensitivity policy that produced
  the projection.

## Trust is an explicit input

Verification takes two inputs: the record and a **trust snapshot**.
The snapshot names issuers, their keys, each key's state (active,
retired, revoked), each key's validity window, and what each issuer is
authorized to sign. The verifier makes zero network calls; whoever
runs it chooses which snapshot to trust, and that choice is theirs,
not the record's.

The snapshot is itself a typed config document. Inside the platform it
distributes on the [config lane](config-distribution.md) exactly like
taxonomies, screening mounts, and postal packs: registry-stored,
verify-then-swap, commit as version, subject `trust-snapshot`. Outside
the platform it is a file the relying party pins. Same document, two
custody models, and the lane is never required for verification. The
workbench's verifying verbs take the file custody themselves:
`PROTOMOLT_TRUST_SNAPSHOT` pins a snapshot the server defaults to when
a request carries none, and a request's own snapshot always wins.

## Verification: named checks, named refusals

The verifier runs a fixed ordered pipeline, each check with a stable
identifier, each failure refusing by name:

1. container bounds: the size cap, a strict parse with unknown fields
   refused, at least one signature, every algorithm inside the
   profile;
2. manifest parse: a strict parse with unknown fields refused, a known
   manifest version, and every rule the manifest declares holding —
   the revision and disclosure links are digest-formed by declared
   rule, so link well-formedness is part of this check;
3. reserialization equality: deterministically reserializing the
   parsed manifest reproduces the signed bytes exactly;
4. key trust: the issuer is in the snapshot, every signing key
   resolves under that issuer, none is revoked, and the claimed
   issuance time falls inside each key's validity window — keys
   resolve before signatures can be checked, which is why this
   precedes the signature check;
5. signature validity: every signature verifies over the manifest
   bytes, the manifest's own key among them;
6. issuer authorization for the record's subject kind;
7. completeness consistency: missing reasons present exactly when the
   record is not complete;
8. artifact rehash, when bytes are supplied: all-or-nothing — every
   referenced artifact matches digest and size, or the check fails as
   a unit; skipped, and named as skipped, when none are supplied.

Walking a revision chain — following prior digests across records — is
the relying party's traversal over verified records, not a
single-record check.

The result carries every check's outcome plus the non-claims. A
relying party who wants a decision, not a checklist, uses the
evaluation sidecar (below); the verifier itself never collapses the
checklist into a verdict beyond "all checks passed".

## The projectors

The first projector reads a terminal workflow **run evidence** object
and emits a manifest. It adds no information: every manifest field
maps to an evidence field, a fingerprint, or a counter that already
exists. A run that never completed projects as `PARTIAL` with reasons,
not as a refusal — recording honest incompleteness is the point.

The delegation transcript is the second projector, deferred until the
first has a conformance corpus. The projector seam is deliberately
narrow — evidence in, manifest out, pure — so new evidence families
plug in without touching signing or verification.

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

Multiple signatures on one manifest are legal in the container from
the start. The issuer signs in v1; countersignatures (a reviewer, a
second coordinator, a mesh peer) are a later composition that changes
no byte of the manifest.

## Registry transparency

Publishing a record's manifest digest to the registry — which already
treats a Git commit as a version — yields an append-only, replicated
log of issued records for free. The log is one typed document
(`WorkRecordLog`, subject `work-records`) appended through the
existing config door: each publish is a registry commit, the commit
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

Replay already re-executes a recorded run. The **evaluation sidecar**
wraps it in a reproducible decision procedure for relying parties:
frozen inputs, per-check results, a predeclared policy, and a
versioned evaluation record written *beside* the original — the signed
record is never modified. This is the platform's structural advantage
over a format-only receipt: a record here is not just checkable, it is
re-runnable.

## Conformance corpus

The corpus is part of the format, not part of the tests: a set of
valid records and a larger set of invalid ones, each invalid fixture
failing exactly one named check. The platform verifier must pass the
corpus, and any external verifier claims conformance by passing the
same corpus. Freezing v1 of the manifest is a compatibility promise of
the same class as the validate.v1 dialect; the corpus is what makes
the promise testable.

The first external verifier is
[`protomolt-record-verifier`](../apps/record-verifier.md): pure JDK,
zero dependencies, sharing nothing with this runtime but the wire
contract. It restates the format by hand — a strict canonical wire
reader stands in for reserialization equality — and its cross-check
suite holds it to the runtime's verdict on every corpus fixture and to
acceptance agreement on randomly mutated records.

## Deliberately out of v1

- Trusted timestamps (RFC 3161) — composable later; issuance time
  stays a claimed fact with a non-claim.
- Transparency receipts (SCITT) — the registry log stands in.
- Environment attestation (RATS/EAT).
- Hardware key custody — the key reference seam admits it without a
  format change.
