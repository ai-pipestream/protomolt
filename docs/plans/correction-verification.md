# Goal 2 correction verification

Local implementation on `feat/starter-contracts`, based on
`dc49800cd66950356a883d99141e00427df6ad65`, 2026-09-25 UTC.
See the [runnable walkthrough](../tutorials/recorded-correction.md).

The coordinating agent authored the assessment contract, correction coordinator,
CLI and offline policy checks. Sol implemented/reviewed response correlation and
assessment receipts. Luna authored the end-to-end correction tests. Sol also
added regressions against the unchanged court schema and reviewed the final
sample. Review findings fixed here include rejected-input evidence missing its
workflow dependencies, signature authentication before referenced artifact reads,
request-only assessment validation, and verifying local policy independently
from the generic signed-record checks.

## Verified local behavior

- The fixed named workflow resolves once, compiles and records its fingerprint.
  Source notes are excluded from model grounding and masked in stored input.
- Real structured generation machinery handles successful first output, repair,
  three failed attempts, and provider failure. Invalid input/grounding does not
  call the provider; invalid candidates cannot call the evaluator.
- Evaluation checks request/response identity and rubric/answer agreement before
  policy application. Invented facts and explicit uncertainty do not accept.
  Wrong source identity routes to review without evaluating the candidate.
- Evaluation timeout and caller interruption yield failed assessments, cancel
  the waiting evaluator, and cannot apply a late result.
- The sample signs its normal execution record and a separate assessment record
  using the existing v1 format. Offline checks verify signatures, artifact bytes,
  exact record projections, reconstructed grounding, schema identity, and policy.
  Freshly signed but inconsistent decisions and altered grounding are rejected.
- A second process reopens the deterministic run and verifies its policy and
  evidence without calling a model or requiring a private key. Reusing a run ID
  is refused; this sample does not promise crash recovery or idempotent retries.
- Six original-court-schema fixtures distinguish valid incomplete declarations
  from invalid missing reasons and structurally valid invented facts. The
  generic external `CompleteStepValidationTest` passes separately; a court job
  harness port remains COURT-1 work.

## Commands

```sh
buf lint
buf build --as-file-descriptor-set -o /tmp/protomolt-goal2-descriptors.binpb
buf breaking --against /work/worktrees/protomolt/main-land
./gradlew :protomolt-inference-proto:test :samples:test \
  :protomolt-grpc-service-contract:test :protomolt-workflow:test \
  :protomolt-inference-service:test :protomolt-inference-structured:test \
  :protomolt-receipt:test :protomolt-delegation:test \
  :protomolt-jobs-service:test --tests '*CompleteStepValidationTest' --console=plain
./gradlew :samples:runCorrection
git diff --check
```

Buf lint, descriptor compilation, and FILE compatibility passed. The Gradle
verification passed with 453 tests and zero failures/errors/skips: inference
contracts 9, samples 34, service contracts 19, workflows 146, inference service
24, structured generation 18, receipts 53, delegation 148, and the two selected
job completion tests. The jobs test filter applies only to that final task.
The samples include 16 correction and 6 court regression tests.

The deterministic CLI run `contact-e8357a22-18fe-4386-b8c0-7937352a26d1`
recorded ACCEPTED, successful replay, and verified receipts. Reopening it with
`-PverifyCorrectionRun` and its explicit demonstration trust file returned
`verified correction policy and evidence` in a fresh process. Logs are
`/tmp/protomolt-goal2-verification.log`, `/tmp/protomolt-goal2-demo-final.log`,
and `/tmp/protomolt-goal2-offline.log` on this host. Demonstration trust is not
an independently trusted production issuer.

## Live-provider acceptance is still open

Two configured existing model servers were exercised through temporary loopback
SSH forwards to `krick-1`; the forwards were removed afterward. No model,
deployment, image, GPU configuration, or runtime service was changed by this work.

- Glimmer, `muse-glimmer-30b`, run
  `contact-2a65a5b9-7baf-420e-ac82-b294adadea9f`: the sample recorded FAILED with
  a verified partial receipt. The initial server request logged an unsupported
  HTTP upgrade and HTTP 400. A separate minimal HTTP/1.1 diagnostic then reached
  the engine, which failed with `UR_RESULT_ERROR_DEVICE_LOST` and HTTP 500.
  HTTP/1.1 therefore did not establish a working provider. The transport was
  not changed speculatively.
- Qwen, `ggml-org/Qwen2.5-VL-7B-Instruct-GGUF:Q4_K_M`, run
  `contact-44f5462f-bc96-43bc-b47e-2d9e9d0ded5b`: health and model discovery
  returned HTTP 200, but generation failed with `UR_RESULT_ERROR_OUT_OF_RESOURCES`
  in the server's SYCL backend. The sample retained FAILED evidence, a verified
  partial receipt, and an unsuccessful process exit. No semantic acceptance
  occurred. Its model daemon restarted through its existing runtime policy.
  Reopening this failed run in another process returned
  `verified failed correction record` without a provider connection; log:
  `/tmp/protomolt-goal2-live-failure-offline.log`.

These checks prove recorded provider-failure behavior, not successful live
correction or evaluation. Goal 2 is not marked complete pending a healthy
configured provider and a passing end-to-end live check. Logs:
`/tmp/protomolt-goal2-live.log` and `/tmp/protomolt-goal2-live-qwen.log`.
After the first failed run, the CLI was tightened to exit unsuccessfully for
FAILED assessments even when the failure receipt verifies; the Qwen run confirms
that behavior. The full samples suite and deterministic CLI were rerun and passed
after that change (`/tmp/protomolt-goal2-sample-final.log`).

## Scope and remaining work

### Kimi CLI follow-up, 2026-09-25

The existing local Kimi Code CLI 2.1.1 login works through ProtoMolt's
`AcpClient` launching `kimi acp`. No Kimi Web server, API key extraction, or
direct hosted API adapter was used. The explicitly selected configured model
alias was `kimi-code/k3`.

A separately labeled prompt-guided probe projected the same synthetic contact
through `MessageProjection`, requested `CorrectedContact`, parsed strict protobuf
JSON, and ran `ProtoValidator` plus source-ID equality before invoking a fresh
judgment session. Its first run rejected Markdown-fenced JSON; a bounded repair
produced a valid candidate, but the fenced judgment was also rejected. No fences
were silently stripped. A fresh run with explicit no-Markdown instructions
produced a valid first-attempt candidate and a valid `supported` judgment, with
all three probability labels checked for uniqueness, bounds and a unit sum.

The result was `contact-1`, `Ada Lovelace`, `ada@example.org`, with no review flag.
Only projected source fields went into the prompts. Each ACP call used a fresh
session, rejected permission requests and had a 90-second timeout; no existing
coding-worker session was resumed. A custom CLI agent profile requested no tools,
and the ACP client supplied no MCP servers or client filesystem/terminal support;
this probe is not a general sandbox-security qualification.

Evidence is retained locally under `samples/build/kimi-cli-check/`, including the
Java probe, exact prompts/replies and session IDs for both runs. The final log is
`passing-run` alongside `protomolt-kimi-validation-v2.log`. This is a diagnostic
live probe, not an installed product command or a model-quality benchmark.

This does **not** close the recorded workflow's live acceptance gate. The existing
`StructuredGenerator` contract requires a native JSON-Schema response-format
capability; the current ACP client carries text prompts and has no schema
constraint parameter. Kimi was not registered with a false capability flag,
and no signed workflow receipt was claimed for these standalone calls. To route
this CLI through the recorded structured workflow, explicitly distinguish
prompt-guided output from native constraints in its reviewed contract and keep
the same strict parsing, validation, repair bound and evidence gates. Do not
silently fall back from native constraints to prompt guidance.

### Remaining implementation

CORE-1 is the local correction path. The generic `EvaluationService.Evaluate`
handler and mounting remain EVAL-1; its generated contract is not an available
endpoint. The new reusable response gate is a post-parse boundary and does not
independently resolve arbitrary `Any` payloads or authoritative task contracts.
The assessment verifier establishes recorded claims; the sample's own verifier
additionally checks its fixed schema, evidence and policy.

Automatic external delegation review remains gated on the separate review-binding
patch, rechecked with 23 uncommitted files in its original worktree. No delegation
runtime changes or automatic task-review integration are included here.
OpenAPI translation, Compose startup and protocol mounting remain Goal 3 work.
No platform commit, push, hosted CI, merge, or platform deployment is claimed.

The website reference inventory was separately published as Cloudflare production
deployment `9f6da607-0e05-4062-a86b-ed43da54a4c5`. Its exact 150 project IDs and
directories match the published baseline; the Pages production URL independently
returned all 150 entries. The custom domain's automated check receives a
Cloudflare challenge. This publication contains the existing-code inventory,
not the uncommitted starter implementation.
