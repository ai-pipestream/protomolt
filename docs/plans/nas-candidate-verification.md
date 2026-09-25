# Goal 2 NAS candidate verification

Verified 2026-09-25. Portainer owns stack **21**,
`protomolt-goal2-candidate`, on NAS endpoint **3**. Stack 20, the previous
coordinator, was not changed or deleted. Candidate volumes, names, secrets and
ports are separate. The deployed application source is commit
`11abaa6750bd6530e767dc4e9b7a57a1cc6e2c0b`, following the reviewed Goal 2
implementation at `d8b674c68f6ceea637c5183c8b8c662305ca0796`.

## What ran

The fixed `CorrectionService.RunCorrection` RPC ran `correct-contact` on the NAS.
The NAS called an authenticated `InferenceService` bridge on krick; that bridge
used the existing Kimi CLI login through `AcpClient`. Provider identity was
`kimi-cli`, model alias `kimi-code/k3`. No native response-format capability was
claimed and no provider credentials were copied to the NAS.

Run `nas-contact-20260925-1` returned `ASSESSMENT_DISPOSITION_ACCEPTED`, reason
`source-supported`. The projected input was `Ada Lovelace; ada [at] example.org`;
the validated candidate was `Ada Lovelace`, `ada@example.org`, record `contact-1`.
Private source notes were absent from the evaluation evidence. Generation and
evaluation were separate calls to the same configured provider, not independent
model-quality evidence.

- The coordinator returned HTTP `UP`; the correction gRPC health service returned
  `SERVING`. All five health-checked long-running candidate containers were healthy.
- An invalid uppercase run ID returned `INVALID_ARGUMENT`.
- Reusing the completed run ID returned `ALREADY_EXISTS`.
- `GetCorrection` returned the same accepted outcome from verified stored evidence.
- Reflection without a token returned `UNAUTHENTICATED`.
- Only public trust and evidence (`artifacts`, `runs`, `outcomes`, `trust.pb`) were
  copied out. The signing key was not copied.
- A separate JVM ran `CorrectionSample --verify` against those copied files with
  explicit trust. It returned `receipt-verified=true` and
  `verified correction policy and evidence`, without contacting a model.

The candidate signing identity persists on its own volume. Trust was obtained
through the operator's authenticated NAS access, not accepted from an arbitrary
RPC caller. A verified receipt authenticates the recorded evidence and policy;
it does not establish that the model's semantic judgment is objectively true.

## Source and checks

The full Gradle build at `d8b674c6` passed with **6,941 tests, zero failures or
errors, and five skipped**. Buf lint, descriptor compilation, compatibility
against `main` and deployment static checks passed. Sol reviewed and implemented
bounded modules; Luna authored contract, runtime, cancellation and receipt tests.
The coordinating agent reviewed the changes and performed deployment and live checks.

The first candidate startup exposed a pre-existing annotation mismatch:
`ModelEntry.credential_ref` documented an empty value as allowed, while its
pattern rejected that value. Commit `11abaa67` adds `ignore_if_zero`; four runtime
validator regressions cover absence, valid references, malformed references and
required identity fields. Affected inference, correction and sample tests and all
three application distributions passed after the fix. Buf lint and compatibility
also passed. Portainer then updated the candidate to those images.

Images were built locally, loaded on the NAS and selected by commit-specific
tags, with `pull_policy: never`. This was a candidate deployment, not a public
image release or a merge to main. NAS image IDs:

- `protomolt-serve:goal2-11abaa6750bd`:
  `sha256:6b380a801770d73270ead2113b4af69e607b53aac1f68ef13e41584f6db29186`
- `protomolt-repo-service:goal2-11abaa6750bd`:
  `sha256:9f8dc66a8c0ffd8fe317dfee5a427433f01e2c8b229fb053b2cd0a459649e41b`
- `protomolt-correction:goal2-11abaa6750bd`:
  `sha256:cad2dc45b3b3a90b3ca2f5a9628c4171c9cd2e8530cb71cd3bc95a404b74592f`

The Compose SHA-256 is
`a764499007b1c36bcb2c9ee7759204cc6355260bc18cc12b569f5296bb689d35`.

Local operator evidence is in
`~/.local/state/protomolt-goal2-candidate/` and
`apps/correction/build/nas-verification/`. The former includes private runtime
configuration; do not copy it into Git or task messages. Verification output is
`/tmp/protomolt-goal2-nas-offline-verification.log`. The full build log is
`/tmp/protomolt-goal2-full-build-verified.log`; the annotation-fix check is
`/tmp/protomolt-goal2-credential-fix.log`.

## Operational boundary and next work

The candidate's NAS listeners are loopback-only: 29902 HTTP, 29903 coordinator
gRPC, 29904 registry and 29905 correction gRPC. On krick, the enabled user units
`protomolt-goal2-kimi-bridge` and `protomolt-goal2-nas-tunnel` keep the authenticated
bridge and SSH tunnel running. The candidate-owned socat service connects the
container network to the loopback reverse tunnel. This setup requires krick and
its Kimi login; it is not yet the standalone Compose onboarding experience.

The two new RPCs are fixed-workflow run and lookup, not a new arbitrary-schema
generation API. Existing workflow, projection, validation, evaluation-envelope,
artifact and receipt models are reused. Generic `EvaluationService.Evaluate`
mounting remains separate work. Registered service profiles currently refuse
unresolved credential references, so coordinator `ServiceInvoke` and MCP/ACP
parity for this authenticated correction service are **not** claimed by this
direct gRPC test. Resolving that credential boundary belongs in Goal 3's protocol
integration, along with guided console setup, OpenAPI translation and distributable
images. No generator parity claim was added.

The separate delegation revision/attempt review-binding patch remains unmerged;
this candidate does not enable automatic external delegation review. In-progress
correction runs remain single-use and non-resumable, as the contract states.
