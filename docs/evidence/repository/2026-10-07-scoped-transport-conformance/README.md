# Scoped publication parity integration

External contribution: `5d92bd4cc054c707c4ec1416e5879521c269e6c2`, based on
`695371b241f2096e762ec65ed1af6107278e3325`. Integrated as `5396e5002` on top of
`e57e61fc800f5b66e82a95bc43c259aa27f53aa6`, with coordinator review fixes in this
checkpoint. Sol reviewed the original harness; no authorization bypass was found.

The probe uses PostgreSQL 18, versioned LocalStack S3, production repository code
and an authenticated in-process gRPC server. Its resolver, tokens, placements and
schema selection are explicit fixture inputs. A fresh JVM loads the observed
production JAR runtime plus the compiled probe. Provisioning uses existing internal
process-only ports. This does not qualify public provisioning or an external
identity provider.

```sh
./gradlew :protomolt-repo-container:scopedPublicationTest --max-workers=2 --console=plain
```

The final local run passed: exit 0 in 1 minute 8 seconds, one JUnit harness test,
zero failures, errors or skips. Its XML retains all 42 verified probe markers;
these are scenario markers, not 42 independent JUnit tests. `gradle.log` records
the command result. XML trailing whitespace was normalized. The driver requires:
17 shared scenarios through each invocation path, three final-check orderings
through each path, transport refusal and the final parity marker. See COVERAGE.md
and the probe assertions for the scenario details; markers alone do not define
what a scenario proves.

## Review changes and evidence limits

The original `providerEffects` helper counted recorded SQL versions. It is now
named `recordedVersions`. A delegating real-store observer separately counts PUT,
streaming PUT, conditional PUT and COPY calls and their normal returns. Refusal
cases and exact-retry cases compare snapshots, including cases with prior writes
for fixture seeding. The observer does not fabricate responses or intercept SDK
exceptions. Its counts are adapter invocations, not HTTP retry counts; normal
return does not independently prove durability. Existing version/receipt and byte
readback assertions supply separate evidence for successful publications.
The positive control requires both initial call and normal-return counts to equal
the number of uploaded parts, separately through library and gRPC. This prevents
negative checks from passing merely because the observer was bypassed. Sol
reviewed this addition with no blocker.

The held-upload wrapper pauses before delegating to the SDK. That case proves
an accepted invocation may settle after revocation without publishing its bytes.
It does not demonstrate a remotely in-flight request. Independent operations
sharing a key are checked for overlap using real SQL commit barriers; this is not
a throughput benchmark. RustFS performance qualification remains separate.

The runtime is closed by each host after its scenarios. The observer borrows the
real store; environment teardown owns it. No runtime API or protobuf contract is
changed by these test additions. Hosted CI, main merge and deployment are not
claimed by this local checkpoint.

## 2026-10-07 repair (PR #411)

The conclusions above describe the coordinator integration at `1b9cdd8a8` and
remain its record. A later repair (code `5b1dd08be`, current tested source `ef9fdbecb` with
target `2d60befbd` and a BOM repair; earlier tested at `c01ca4e11`) replaces
the presence-only `requireScopedCallersOnly` binding check with an exact
per-invocation identity assertion (principal, issuer, credential ID, generation,
no process authority), adds `IDENTITY_SUBSTITUTION` on both paths (44 markers in
total), and persists the probe log under the task's results directory. Its
commands, results, fingerprints and limits are in [repair/README.md](repair/README.md).
