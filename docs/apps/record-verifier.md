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
declared formats the contract uses (slug, hex digest, media type) are
restated with the platform parsers' semantics. A source-level test
pins the main source set to JDK-only imports.

One check is deliberately implemented differently. The platform
verifier proves canonical form by reserializing the parsed manifest
and comparing bytes. This verifier has no serializer; its wire reader
is strict instead, recording every tolerated deviation — field order,
duplicate fields, non-minimal varints, explicitly encoded defaults —
and canonical form is exactly "no deviations noted". A reader that
tolerated nothing would have produced the bytes itself.

## Conformance

The cross-check suite is the conformance claim: on every fixture of
the [conformance corpus](../design/receipts.md#conformance-corpus) the
external verifier produces the runtime verifier's verdict — same
acceptance, same refusing check, same manifest digest — and on
hundreds of seeded single-byte mutations of a valid record the two
verifiers agree on acceptance. Attribution may differ on arbitrary
corruption; acceptance may not.

## Command line

```shell
java -cp protomolt-record-verifier.jar ai.pipestream.receipt.verify.Main \
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

## The library

```java
ExternalVerifier.Result result = ExternalVerifier.verify(recordBytes, trustBytes);
result.verified();        // no check failed
result.refusal();         // the refusing check, when there is one
result.manifestDigest();  // sha-256 of the manifest bytes as signed
result.nonClaims();       // what verification does not establish
```
