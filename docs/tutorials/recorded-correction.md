# Recorded correction with a verifiable assessment

The correction app and executable sample resolve the stored `correct-contact` workflow, project
permitted source fields, generate a typed result with up to three attempts,
validate it, evaluate source support, and sign the recorded outcome. It uses
the existing workflow runner, structured generator, Git schema/workflow store,
artifact repository, replay engine, and v1 signed work records.

The candidate app mounts a fixed `CorrectionService` gRPC surface. The generic
`EvaluationService.Evaluate` handler remains separate work. The sample CLI is
still useful for deterministic, offline verification without a running service.

## Run without a model provider

From the repository root, using the project's configured Java toolchain:

```sh
./gradlew :samples:runCorrection
```

The default provider deliberately returns an invalid email followed by a valid
correction. The real structured generator parses, validates, and repairs the
first answer. A deterministic fixture evaluator then checks the selected name
and email against the supplied contact text. This exercises the implementation
without an account, model download, container, or network inference call. It is
not a model-quality benchmark or a general semantic evaluator.

Expected terminal output includes `mode=deterministic-fixture`,
`disposition=ASSESSMENT_DISPOSITION_ACCEPTED`, `offline-replay=true`, and
`receipt-verified=true`, plus a generated run ID and output directory.

By default, state is under `samples/build/correction-example`. To keep it outside
the build directory, supply `-PcorrectionWorkspace=/absolute/path/to/workspace`.
The workspace contains Git-backed workflow definitions in `registry`,
content-addressed bytes in `artifacts`, run evidence in `runs`, and per-run
assessment and receipt files in `outcomes`.

## Verify in another process

Use the run ID printed above and explicitly supply the public trust snapshot:

```sh
./gradlew :samples:runCorrection \
  -PverifyCorrectionRun=contact-REPLACE-WITH-PRINTED-ID \
  -PcorrectionTrust=/absolute/path/to/workspace/outcomes/contact-REPLACE-WITH-PRINTED-ID/trust.pb
```

Include the same `-PcorrectionWorkspace` if you changed it. This path opens stored
artifacts, checks both signatures and their exact manifests, replays the workflow
offline, reconstructs projected evidence, and recomputes the local policy
decision. It performs no generation or evaluation calls and needs no private key.

The sample creates a fresh demonstration signing key for each invocation; the
private key is held only in memory. Its `trust.pb` demonstrates verification but
does not establish independent issuer trust. A real verifier must obtain its
trust configuration independently from an authorized source. A valid signature
and matching policy do not prove that an evaluator's factual judgment is true.

## Run the candidate gRPC service

Build the distribution with `./gradlew :protomolt-correction:installDist` or
build `apps/correction/Dockerfile` from its module directory after that task.
The service requires `PROTOMOLT_CORRECTION_API_TOKEN` in its environment. Its
reflection, health, `RunCorrection`, and `GetCorrection` methods all require
that token as `api_token` metadata or a bearer credential.

The isolated NAS candidate uses:

```sh
--host 0.0.0.0 --port 9090 --workspace /data/correction \
  --mode remote-prompt --inference-target host.docker.internal:29930
```

It also requires
`PROTOMOLT_INFERENCE_API_TOKEN`, used only on the outbound inference gRPC call
to the workstation bridge. The bridge owns Kimi ACP credentials; the NAS app
does not receive them. `--mode fixture` runs without an inference bridge for
service checks. `--mode remote-native` requires a remote model that actually
reports native structured output. Prompt guidance is explicitly selected and
never advertised as that capability.

Call `CorrectionService.RunCorrection` with `workflow_name: "correct-contact"`,
a new `run_id`, and a `RawContact` source. It returns an assessment and both
signed receipts in `outcome`. `GetCorrection` reads the same verified outcome
without a model call after the run completes. Reusing a run ID returns
`ALREADY_EXISTS`; unfinished or interrupted runs are not returned by `Get`.
The service keeps one active correction at a time and returns
`RESOURCE_EXHAUSTED` when busy. Its signing identity persists under the
workspace, with the public trust snapshot at `/data/correction/trust.pb`.
An external verifier must establish that trust file's provenance independently.

## Use a configured model separately

Set `PROTOMOLT_CORRECTION_ENDPOINT` and `PROTOMOLT_CORRECTION_MODEL` for an
OpenAI-compatible endpoint, then run:

```sh
./gradlew :samples:runCorrection -PliveCorrection
```

If authentication is required, `PROTOMOLT_CORRECTION_CREDENTIAL_REF` supplies
the existing provider's credential reference, such as `env:MODEL_API_KEY`.
Keep the key itself in the referenced environment variable. Configuration is
recorded as evidence, so endpoints and credential references must not embed
secret values. This sample uses the selected provider for both generation and
judgment; it does not claim evaluator independence. A live run is reported as
`mode=live-provider` and must be assessed separately from the deterministic run.
Failed correction exits unsuccessfully after retaining and verifying its failure
record. A valid receipt alone does not turn a failed run into a passing check.

The provider adapter uses a 25-second HTTP timeout per call. Generation permits
at most three attempts. Evaluation makes one call with a 30-second waiting
limit. Timeout or caller interruption cancels that local wait and produces a
failed assessment. Cancellation is best-effort at a remote provider; a late
answer has no path to change the recorded decision. The workflow's deadline is
not advertised as an end-to-end deadline covering every retry and assessment.

## Contract and decision boundaries

The [correction schema](../../apps/correction/src/main/proto/ai/protomolt/proto/correction/v1/correction.proto)
separates permissive `RawContact`, projected `ContactGrounding`, strict
`CorrectedContact`, and the evidence pair `ContactEvidence`. ProtoMolt's runtime
validator checks the input and projected grounding before generation. The
structured generator validates candidates before they can reach evaluation.
The host also checks that the result preserves the source record ID.

The [native definition](../../apps/correction/src/main/resources/starter/correct-contact.workflow.json)
and [prompt-guided definition](../../apps/correction/src/main/resources/starter/correct-contact.prompt-guided.workflow.json)
are the only reviewed variants. The selected definition is resolved once and
compared with the appropriate canonical workflow. The compiled
definition and complete descriptor closure are captured with the outcome.
Changing a registry alias cannot change an already constructed coordinator.
This deliberately fixed example is not a general-purpose arbitrary-schema API.

`internal_notes` is excluded from model grounding and marked secret for recorded
input masking. Evaluation is reconstructed from recorded, permitted source
bytes, not the original private input. If that evidence is insufficient, the
result cannot be accepted. Source text and candidate data are still sensitive;
the local sample's workspace is not an encrypted multi-user storage service.

The [evaluation response gate](../../inference/service/src/main/java/ai/protomolt/proto/inference/service/EvaluationResponses.java)
checks annotated request and response messages, exact request/binding identity,
question IDs and answer kinds, option labels, probability distributions, and
score consistency. Its 1 MiB check is a post-parse bound. It does not resolve an
arbitrary `Any` payload or serve an RPC. This sample derives, parses, and validates
its known evidence payload before calling its configured adapter.

The local policy accepts only a valid, same-ID result with `needs_review=false`
and a contract-valid `supported` judgment. Incomplete, unsupported, and unresolved
results route to review. Invalid input, exhausted repair, provider failure,
malformed judgment, timeout, and cancellation never produce acceptance. A
contract-valid invented email remains a semantic failure in the fixture tests.

## Two records, one run

The execution receipt describes what the workflow executed. A separate
[`WorkflowAssessment`](../../surface/grpc/workflow/src/main/proto/ai/protomolt/proto/grpc/workflow/v1/workflow_assessment.proto)
binds the run, original signed execution record, permitted input, candidate,
schema/projection closure, evidence, rubric, evaluator configuration, response,
and local policy. The [assessment projector](../../transform/workflow/src/main/java/ai/protomolt/proto/workflow/WorkflowAssessmentRecords.java)
uses the existing v1 signature/manifest format with a distinct completeness
policy. No synthetic evaluator step is inserted into execution evidence.

The generic projector/verifier establishes stored-byte identity and recorded
claims. The sample's own verifier additionally ties the schema, projected
evidence, and decision to its reviewed workflow. Receipt completeness describes
evidence coverage: a reviewed-but-unaccepted result can have complete evidence;
a failed or unevaluated run is partial.

The existing `EvaluateWorkRecord` execution-policy action does not evaluate this
new assessment policy. The correction service invokes the fixed local assessment
and verifier; no general evaluation handler is mounted. No automatic delegation review is
performed; its attempt/revision binding patch remains a separate prerequisite.

Run IDs are single-use, including failures and interrupted processes. The
exclusive per-run directory prevents a second invocation from duplicating
provider calls under the same ID. A crash can leave an incomplete directory:
this sample does not promise resumable execution or idempotent retries. Inspect
that evidence and deliberately start a new run ID. The evaluation request and
configuration snapshot are persisted before calling the evaluator; an unfinished
directory without a final signed assessment is not a completed result.

## Implementation and regression coverage

- [Coordinator and offline policy verification](../../apps/correction/src/main/java/ai/protomolt/proto/correction/ContactCorrection.java)
- [Executable provider adapters and CLI](../../apps/correction/src/main/java/ai/protomolt/proto/correction/CorrectionSample.java)
- [Correction, repair, failure, cancellation and tamper tests](../../samples/src/test/java/ai/protomolt/proto/samples/ContactCorrectionTest.java)
- [Original court-schema regressions](../../samples/src/test/java/ai/protomolt/proto/samples/CourtOpinionContractRegressionTest.java)

The court tests compile the actual in-tree `OpinionMetadata` schema unchanged.
They retain the distinction between a valid incomplete declaration, an invalid
missing reason, and a plausible but unsupported value. They are deterministic
regressions, not a rerun of the historical corpus or a live model comparison.
