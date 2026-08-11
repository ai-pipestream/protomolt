# Security and policy

## Objective

Define authenticated mesh sessions, non-secret entity posture, policy
derivation, credential resolution, cross-domain integrity, and privacy controls
that apply uniformly to every processor profile.

## Dependencies

Requires the [core contracts](01-core-contracts-and-annotations.md). It can be
implemented in parallel with routing and service advertisement, then wired
into both.

## Trust layers

Transport context comes from TLS 1.3, mTLS or bearer authentication, and
server-owned metadata. It establishes session, peer, node, tenant, and trust
domain identity. Credential references resolve immediately before connection
and never enter protobuf payloads, logs, errors, evidence, or registry data.

An entity carries a typed, non-secret `SecurityPosture` or its immutable
reference. It describes classification, clearance, allowed environments and
processor profiles, delegation depth, retention, disclosure, PII policy, and
canonical digest. A node derives effective posture from authenticated identity,
local policy, and requested posture. Forwarding may preserve or reduce
authority, never increase it.

For cross-administrative meshes, add a signed capsule over canonical header
bytes, payload digest, scope, issuer, audience, issued time, expiry, and nonce.
Use a versioned signature profile and public-key reference. Keep private keys
in the host credential resolver.

## Enforcement points

- session admission and node advertisement;
- outbound endpoint and reflection policy;
- route candidate filtering;
- transform and disclosure boundary;
- processor invocation and delegation;
- artifact access and retention;
- result validation and rehydration; and
- MCP action authorization.

Unknown policy versions, missing required posture, signature failure, expired
capsules, audience mismatch, or attempted authority escalation fail closed.

## Tests

Use in-process TLS identities, a fake credential resolver, fake clock, and
test signing keys. Cover mTLS and bearer identity binding, downscoping,
escalation rejection, capsule tampering, replay nonce, expiry, wrong audience,
credential no-echo, forbidden endpoint policy, artifact authorization, and
privacy-safe evidence.

## Acceptance criteria

- No contract or serialized evidence contains credential material.
- Every remote processor call has an explicit effective posture decision.
- A forwarded entity is cryptographically bound to its payload digest and
  scope when cross-domain signing is required.
- Policy failures occur before reflection, dispatch, or artifact disclosure.

## Exclusions

Do not define Keycloak deployment here, place ACLs in free-form labels, allow
payloads to assert identity, or treat TLS alone as end-to-end authorization.
