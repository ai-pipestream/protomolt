# Conformance and mesh acceptance

## Objective

Prove interoperability, lifecycle integrity, privacy, recovery, and
contract-driven software integration across all mesh packages.

## Dependencies

Consumes the other work packages. Keep the conformance fixtures independent of
production application schemas and external services.

## Conformance kit

Publish test protobufs, a reflected service, scripted processors, fake clock,
fake identity and credential providers, capturing transports, repository
conformance fixtures, transcript vectors, and expected canonical digests.
Another implementation should be able to run the vectors without ProtoMolt
Java internals.

Test vectors cover:

- envelope and descriptor canonicalization;
- valid and invalid lifecycle transitions;
- advertisement refresh, expiry, and descriptor substitution;
- route selection and transform fingerprints;
- every stream cardinality and recursive completion policy;
- security posture downscoping and signed-capsule verification;
- PII detection and privacy-safe evidence;
- claim-check integrity and artifact corruption;
- idempotent replay and conflicting duplicates; and
- restart, lease fencing, checkpoint resume, and cancellation races.

## Required in-process scenario

1. Start Node A and Node B in-process with authenticated test identities.
2. A advertises a custom reflected gRPC service.
3. B validates the target, reflects it, fingerprints it, and creates a
   TTL-bound service profile.
4. B generates and compiles a client from the reflected schema.
5. B receives a custom `Any` and verifies its exact schema fingerprint.
6. A registered CEL route maps, projects, and validates the service request.
7. OpenNLP alpha4 finds and masks PII embedded in an allowed free-text field.
8. The reflected service executes through dynamic invocation.
9. Its validated result grounds a scripted LLM software-generation worker.
10. The worker scatters deterministic build and test entities.
11. A checkpoint barrier waits for the declared completion policy.
12. The scope rehydrates a validated result with artifact and check evidence.
13. B returns the typed result through gRPC and MCP.
14. A fresh runtime replays the transcript and evidence offline.

The capturing transport proves that the PII sentinel is absent from every
remote request, prompt, artifact, transcript, log, exception, and evidence
record. A non-sensitive sentinel must remain present so the proof is not
vacuous.

## Optional live suite

Use explicit environment gates for TLS DNS routes, Keycloak authentication,
PostgreSQL recovery, RustFS artifacts, a real reflected service, and a live LLM
provider. The normal build requires no container, GPU, public endpoint, or
secret. Optional failures must report which external capability was missing.

## Acceptance criteria

- `buf` format, lint, build, and breaking checks pass.
- All Java modules build, test, and produce clean Javadoc.
- Two nodes exchange a custom protobuf contract without pre-generated clients.
- Every remote boundary validates input, projection, security, and PII policy.
- Recursive results survive restart and replay to the same manifest digest.
- The LLM candidate cannot complete without deterministic acceptance evidence.
- Published conformance vectors are stable and usable by a non-Java node.

## Exclusions

Do not make live infrastructure part of the default build, weaken assertions
when a provider is nondeterministic, or accept implementation-specific state as
the interoperability contract.
