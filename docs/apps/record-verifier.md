# Record verifier

`protomolt-record-verifier` verifies [signed work
records](../design/receipts.md) with nothing but the JDK: no protobuf
runtime, no platform modules, no dependencies at all. Its whole value
is what it does not share — a relying party can hold the record, the
issuer's public key in a pinned trust snapshot, and this one small
codebase, and check the record offline, years later, against a
platform that no longer exists.

## What it shares with the platform, and what it does not

The verifier shares the wire contract and nothing else. The receipt
messages are restated by hand as typed models over a hand-rolled
protobuf wire reader; Ed25519 and SHA-256 come from the JDK; the
declared formats the contract uses — lowercase slug, lowercase hex
SHA-256, dashed UUID, and the RFC 6838 media-type grammar — are
restated with the platform parsers' semantics, counting Unicode code
points where the platform counts them. A source-level test pins the
main source set to JDK-only imports, so the independence is a build
failure rather than a promise.

One check is deliberately implemented differently. The platform
verifier proves canonical form by reserializing the parsed manifest
and comparing bytes. This verifier has no serializer; its wire reader
is strict instead, recording every tolerated deviation — field order,
duplicate fields, non-minimal varints, non-minimal tags and length
prefixes, explicitly encoded defaults — and canonical form is exactly
"no deviations noted". A reader that tolerated nothing would have
produced the bytes itself.

## The relying party's walk

Verification consumes exactly two things — the record bytes and the
serialized trust snapshot — and, optionally, a third: the referenced
artifact bytes. There is no network call, no clock the caller did not
supply, and no ProtoMolt process anywhere in the picture. The checks
run in the platform's fixed order, stop at the first failure, and
refuse by name:

| Check | What it establishes |
| --- | --- |
| `container-bounds` | 1..4 MiB, parses as a signed record, no unknown fields, at least one signature, no duplicate signing key, every algorithm inside the v1 profile |
| `manifest-parse` | parses as a manifest, no unknown fields, manifest version 1, every declared rule holds |
| `reserialization-equality` | the manifest bytes are in canonical form |
| `key-trusted` | the issuer is in the snapshot, the manifest's key carries a signature, every signing key resolves under that issuer, none is revoked, each key decodes as Ed25519, and the claimed issuance time falls inside each key's window |
| `signature-valid` | every signature verifies over the manifest bytes |
| `issuer-authorized` | the snapshot authorizes this issuer for the record's subject kind |
| `completeness-consistent` | missing reasons present exactly when the record is not complete |
| `artifact-rehash` | every referenced artifact — run scope and per step, request and response — matches digest and size, all-or-nothing; skipped and named as skipped when no bytes are supplied |

The result reports what verification does not establish alongside what
it does: `issuer-honesty`, `trusted-time`, `world-completeness`, and
`execution-correctness` always, plus `artifact-custody` whenever the
rehash check was skipped. A signature is never an endorsement here
either.

An unusable trust snapshot is the caller's error, not the record's.
The snapshot is parsed and rule-checked before any record byte is
examined — issuer and key counts, slug and key formats, 32-byte
Ed25519 keys, defined key states, unique and well-formed subject
kinds, no duplicate issuer or key id — and a violation throws rather
than refusing the record.

## Subject kinds

The subject is where a record says what work it describes, and the
verifier mirrors the manifest's per-kind rules exactly, in two moves.

First, every identity field that is *set* is format-checked whatever
the kind: `workflow_name`, `workflow_version`, `run_id`, and
`worker_id` as slugs of at most 128 characters, `workflow_fingerprint`
and `spec_sha256` as lowercase hex SHA-256, `task_id` as a dashed
UUID. A malformed field is a refusal even on a kind that does not
require it, because a field nobody demanded is still a field somebody
might read.

Then the kind demands its own:

- **`workflow-run`** names the workflow, its fingerprint, and the run.
  Any of the three empty is a refusal.
- **`delegation-task`** names the task, its worker, and the spec
  fingerprint — `task_id`, `worker_id`, `spec_sha256`. Any of the
  three empty is a refusal.

Anything else is refused as a kind this manifest version does not
define. The default arm is not a fallback: an unknown kind never
reaches the trust check, because a verifier that shrugged at a subject
it could not interpret would be certifying bytes it did not
understand.

Passing the subject rules is not authorization. A perfectly formed
`delegation-task` subject signed by an issuer the snapshot trusts only
for `workflow-run` verifies its signature and is then refused at
`issuer-authorized` — the conformance corpus carries exactly that
fixture, so the two-stage boundary is pinned rather than assumed.

## Conformance

The cross-check suite is the conformance claim: on every fixture of
the [conformance corpus](../design/receipts.md#conformance-corpus) the
external verifier produces the runtime verifier's verdict — same
acceptance, same refusing check, same manifest digest — and on
hundreds of seeded single-bit mutations of a valid record the two
verifiers agree on acceptance. Attribution may differ on arbitrary
corruption; acceptance may not. Hand-built non-canonical encodings —
a non-minimal varint, a non-minimal tag, a non-minimal length prefix,
reordered fields — are held to a sharper standard still: both
verifiers must refuse them at `reserialization-equality`, which is the
evidence that a strict reader and a reserializer really do decide the
same question.

## Command line

```shell
java -cp protomolt-record-verifier.jar ai.protomolt.receipt.verify.Main \
    record.binpb trust.binpb [artifact-dir]
```

The checks print in pipeline order with their details, followed by the
non-claims, the manifest digest, and the verdict. Exit 0 is verified,
1 is refused, 2 is a usage or input error — an invalid trust snapshot
is the caller's error, never the record's. The optional artifact
directory supplies referenced artifact bytes for the rehash check, one
file per artifact named by its SHA-256 hex digest; without it the
rehash check is skipped and `artifact-custody` is reported as a
non-claim.

> **Where the two files come from.** A signing platform issues the
> record: it runs as containers
> (`./gradlew :protomolt-serve:installDist :protomolt-acp-agent:installDist
> && docker compose build && docker compose up`, HTTP on 8080 with the
> Console at `/console` and MCP at `/mcp`, gRPC on 9090, registry on
> 8081 — see [docker](docker.md)), signs under
> `PROTOMOLT_RECEIPT_KEY_FILE`, `PROTOMOLT_RECEIPT_KEY_ID`, and
> `PROTOMOLT_RECEIPT_ISSUER`, and exports workflow-run records from the
> workbench verbs and delegation-task records from the [task
> console](task-console.md). The snapshot is the same document that
> server pins as `PROTOMOLT_TRUST_SNAPSHOT`. Neither file binds the
> relying party to that server afterwards: verification is the two
> files, this jar, and arithmetic.

## The library

```java
ExternalVerifier.Result result = ExternalVerifier.verify(recordBytes, trustBytes);
result.verified();        // no check failed
result.refusal();         // the refusing check, when there is one
result.manifestDigest();  // sha-256 of the manifest bytes as signed
result.nonClaims();       // what verification does not establish
```

The three-argument overload takes referenced artifact bytes keyed by
SHA-256 hex and runs the rehash check instead of skipping it. Every
check outcome is available in order from `result.checks()`, each with
its stable identifier, its status, and a detail string — the same
material the command line prints.
