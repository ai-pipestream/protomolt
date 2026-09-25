# Starter contract inventory

Contract review snapshot: 2026-09-24, `dc49800cd66950356a883d99141e00427df6ad65` (local HEAD, `origin/main`, and `github/main` at inventory time). Goal 2 subsequently added a local [recorded correction sample](../tutorials/recorded-correction.md); the new Evaluation RPC remains unmounted. The separate `review-binding` worktree still has an uncommitted patch; its attempt/revision checks are a prerequisite for automatic external task review. The wider product sequence is recorded in the [entry plan](/work/website/protomolt/.research/contract-first-entry-plan.md) outside this checkout.

## Existing surface and starter use

The RPC inventory is complemented by the complete
[capability-family review and module roster](inventory-coverage.md). That review
includes non-RPC applications, browser routes, examples, storage and deployments;
the service below is not intended to enumerate every ecosystem service.

The [descriptor-native `ProtoMoltService`](../../surface/grpc/service-contract/src/main/resources/ai/protomolt/proto/grpc/service/v1/protomolt_service.proto) declares **44 RPCs**. Its methods become catalog actions only when the host mounts their handlers. The grouped inventory below covers every method; `E` means existing contract and action, while `X` means an existing method whose starter documentation, fixture, or later binding needs extension. `X` does not propose a wire-signature change.

| Group | RPCs and classification | Starter role |
| --- | --- | --- |
| Schema and validation | `Compile` X, `ListTypes` X, `ValidateMessage` X, `DiffSchemas` E, `CheckCompat` E, `RenderJsonSchema` X, `RenderPrompt` E, `RenderIndexMappings` E, `EvalCel` E, `MapMessage` X, `ExtractMetadata` E, `SynthesizeShape` E, `JoinMessages` E, `MergeSchemas` E, `CheckRules` X, `InferSchema` E, `MaskMessage` E | Discover the exact target type, compile the full source/import closure, check rules and validate samples before execution. Mapping and JSON Schema help explain the correction edge. Compatibility checking is separate from accepting a generated result. |
| Workflow and evidence | `RunWorkflow` X, `CheckWorkflow` X, `CompileWorkflow` X, `SuggestMappings` E, `RecordWorkflowRun` X, `ReplayWorkflow` X, `PromoteWorkflow` X, `ExportWorkRecord` X, `VerifyWorkRecord` X, `EvaluateWorkRecord` X | Compile and execute a named or inline workflow; record, replay, promote, sign, verify and evaluate its evidence. A starter should call a named workflow for correction so clients do not build its internal steps. |
| Remote gRPC and profiles | `Reflect` E, `GrpcInvoke` E, `GenerateStubs` E, `GatherGit` E, `ServiceRegister` X, `ServiceList` E, `ServiceInspect` X, `ServiceRefresh` E, `ServiceInvoke` X | Discover and test an external contract, then pin its profile/descriptor in an authored workflow. A reflected call is a probe, not an accepted workflow deliverable. |
| Knowledge output | `EmitOkf` E | Optional schema reference material. |
| Durable jobs | `SubmitWorkflow` X, `GetJob` X, `ListJobs` E, `CompleteStep` X | Submit a compiled/stored workflow, inspect checkpoints, and complete a parked external step when configured jobs storage exists. |
| Model catalog | `InferenceGenerate` E, `InferenceListModels` E, `InferenceDescribeModel` E | General model calls/catalog discovery. These do not by themselves provide the structured result gate. |

`SchemaSource` already takes **exactly one** registered `type`, inline `sources`, or `descriptor_set_base64`; its CEL rule enforces the choice, and `root` only selects an inline entry file. The starter must document the complete import/descriptor dependency closure, chosen fully qualified message name, immutable descriptor fingerprint, missing-dependency behavior, and configured request/descriptor size limits. Neither a mutable registry alias nor a supplied fingerprint should silently replace the resolved descriptor. Unsupported validation rules must produce an explicit refusal or unsupported finding, never an implicit pass. See the [`SchemaSource` and validation requests](../../surface/grpc/service-contract/src/main/resources/ai/protomolt/proto/grpc/service/v1/protomolt_service.proto) and the [source convention test](../../surface/actions/src/test/java/ai/protomolt/proto/actions/SchemaSourceConventionTest.java).

The [workflow contract](../../surface/grpc/workflow/src/main/proto/ai/protomolt/proto/grpc/workflow/v1/grpc_workflow.proto) and [workflow guide](../transform/workflows.md) already support typed edges, projected grounding, gRPC steps, structured-generation steps, run evidence, and offline replay. The [typed-edge test](../../transform/workflow/src/test/java/ai/protomolt/proto/workflow/TypedEdgeTest.java) proves that excluded internal fields stay out of model grounding and invalid projected grounding is refused before provider invocation. [`structured.proto`](../../inference/proto/src/main/proto/ai/protomolt/proto/inference/v1/structured.proto) defines a target type, catalog model, bounded attempts (at most three), validated `Any` result, and attempt history; the [generator](../../inference/structured/src/main/java/ai/protomolt/proto/inference/structured/StructuredGenerator.java) implements this as a library. There is **no** `GenerateStructured` method on `ProtoMoltService` today. First prove the named correction workflow; add a thin direct RPC only if that experience demonstrably needs one, reusing these types and explicit descriptor/model resolution.

[`RunWorkflow`](../../surface/grpc/service-contract/src/main/resources/ai/protomolt/proto/grpc/service/v1/protomolt_service.proto) accepts exactly one inline definition or stored workflow name and reports `ok`, output, steps, and failure detail. It does not promise deduplication of repeated synchronous calls. `SubmitWorkflow` accepts the same choice plus an optional client UUID `job_id`; the documented current behavior returns the existing job for that ID. A later implementation must compare the canonical submitted workflow/input identity before treating a reused ID as the same operation and refuse the same ID with different payload. Jobs snapshot definitions, persist completed step responses, and park at external steps; uncheckpointed calls may repeat after a crash, so external side effects need their own idempotency keys. `CompleteStep` is documented as idempotent by `job_id` and step name; response mismatch must be a conflict, not silently accepted. `GetJob` exposes `QUEUED`, `RUNNING`, `WAITING`, `COMPLETED`, `FAILED`, and `DEAD`. Deadline or client cancellation of a synchronous call must not be described as canceling already submitted work; the starter must state the actual job cancellation contract before offering a cancel control.

The [delegation messages](../../transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation.proto) already provide `TaskSpec.contract` (`DeliverableContract` with descriptor closure and type name), `CompletionCandidate.result` (`Any`), candidate revision, attempt, acceptance checks, and artifact references. The [12 catalog action envelopes](../../transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation_actions.proto) cover `OfferTask`, `AcceptTask`, `CancelTask`, `SubmitCandidate`, `RecordCheckpoint`, `SendTaskMessage`, `ReportProgress`, `ReviewCandidate`, `ReadTranscript`, `WatchEvents`, `ListWorkers`, and `RegisterWorker`. [`AgentDelegationService.Delegate`](../../transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation.proto) is a separate bidirectional stream. The workflow-authoring sample declares `WorkflowDeliverable` in `samples`, referencing the compiled workflow artifact, descriptor fingerprint, fixture identities, and reported test evidence. Authoritative acceptance criteria remain in `TaskSpec.required_checks`; the deliverable does not redefine them. The candidate must pack that message under the offered `TaskSpec.contract`; workflow checking alone cannot satisfy an agent task gate. Distinguish worker-submitted test reports from reviewer-run checks. The current [reviewer interface](../../transform/delegation/src/main/java/ai/protomolt/proto/delegation/CandidateReviewer.java) receives task, spec, and candidate context; automatic external decisions must bind to the live attempt and candidate revision after the separate review-binding patch lands.

[`ExportWorkRecord`, `VerifyWorkRecord`, and `EvaluateWorkRecord`](../../surface/grpc/service-contract/src/main/resources/ai/protomolt/proto/grpc/service/v1/protomolt_service.proto) already use the [signed receipt contract](../../transform/receipt/src/main/proto/ai/protomolt/proto/receipt/v1/receipt.proto). A signed record references content-addressed artifacts, and verification has explicit checks and non-claims. A bare workflow call is not a recorded, signed result. Evaluation evidence should first be stored by digest and linked from existing candidate/run evidence; changing the frozen `WorkRecord` manifest or signature format requires a separately reviewed version/migration. `EvaluateWorkRecord` evaluates a signed record against stored evidence and replay policy; it is distinct from the proposed semantic `EvaluationService.Evaluate` below.

## Existing court experiment and frontend

Follow-up audit, 2026-09-24 America/New_York: the original inventory above was
too focused on RPCs. The following are existing assets to reuse in the starter,
not work to recreate. This audit inspected source and saved experiments; it did
not rerun live inference or deploy the console.

### Court-opinion metadata filling

The [court-decoration example](../../protobuf/llm/examples/README.md) remains in
the repository. Its [OpinionMetadata schema](../../protobuf/llm/examples/court-decoration/proto/court/v1/opinion_metadata.proto)
declares court, caption, docket, year, posture, panel, topics, authorities,
headnote, and incomplete/reason fields. `llm.v1` instructions and safeguards live
beside `validate.v1` field and message rules. Three saved personas fill the same
contract: appellate researcher, citator, and headnote editor.

Four in-tree rounds retain 315 per-document/persona result records: 45, 90, 90,
and 90 rows. Their findings describe Qwen3-14B-int4 runs with three personas;
these files do not establish a multi-model benchmark. The millions of opinions
mentioned in the README describe the source corpus, not millions of validated
decoration runs. Preserve that distinction in website claims.

The saved findings capture concrete inputs to the contract design:

- [Round 1](../../protobuf/llm/examples/court-decoration/round-1/FINDINGS-round-1.md):
  structurally valid fills could still invent dockets or copy prompt examples.
  Content fidelity must be checked independently of shape.
- [Round 2](../../protobuf/llm/examples/court-decoration/round-2/FINDINGS-round-2.md):
  `no-prose-values` CEL caught sentences in docket fields, but all ten declared
  incomplete results still failed ordinary required-field rules.
- [Round 3](../../protobuf/llm/examples/court-decoration/round-3/FINDINGS-round-3.md):
  `skip_when: "incomplete"` suspended field validation for declared incomplete
  results while message CEL continued requiring a reason. All eight declared
  incomplete results were recorded as valid. This means a valid declaration of
  insufficiency that should route to review, not a trustworthy complete fill.
- [Round 4](../../protobuf/llm/examples/court-decoration/round-4/FINDINGS-round-4.md):
  persona wording reduced prose-valued fields but suppressed incomplete
  declarations. The form was unchanged; prompt wording altered model behavior.

These observations survive in runtime code:
[`ProtoValidator.skipFieldRules`](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/ProtoValidator.java)
skips the field walk, including nested validation, while still evaluating
message rules. [SkipWhenTest](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/SkipWhenTest.java)
covers declaration, missing reason, normal validation, nested fields, and invalid
skip-field definitions. Its six existing tests passed in this follow-up audit.

The full external harness also still exists at
[/work/main/sea-of-slop/qa/court-decoration](/work/main/sea-of-slop/qa/court-decoration/README.md),
with sampled texts, rendered prompts, raw model replies, analysis, and a later
jobs experiment. Its
[CourtDecorationService.Fill contract](/work/main/sea-of-slop/qa/court-decoration/jobs/proto/court/jobs/v1/decoration.proto)
describes an external step: a job parks, a filler supplies OpinionMetadata through
CompleteStep, and the declared response rules are checked. Its saved 90-job
comparison records a final-status timing issue and two changed outcomes under
model nondeterminism. It is historical evidence, not a current integration test.

The harness uses old `SubmitChain` naming and `ai.pipestream.proto` imports;
port it to current `SubmitWorkflow`/workflow definitions and `ai.protomolt.proto`
contracts before running it against current main. Reuse the actual court schema,
raw examples and failure taxonomy for the representative Goal 2 walkthrough.
Keep the small contact sample as a fast contract fixture. Do not infer a complete
appeals/re-adjudication service solely from the example README's design narrative.

### Existing browser workbench

The [console router](../../apps/console/src/router.ts) already connects these
views. Goal 3 should guide users through them and package their dependencies.

- **Workflow editing, checking, saving, running:**
  [WorkflowsView](../../apps/console/src/views/WorkflowsView.vue) has a JSON
  definition editor, Check/Save controls, findings, stored-workflow list, input
  editor, and typed/JSON results. It uses existing CheckWorkflow, CompileWorkflow,
  RunWorkflow and registry endpoints through
  [workflows.ts](../../apps/console/src/services/workflows.ts).
  [NamedWorkflowTest](../../apps/serve/src/test/java/ai/protomolt/proto/serve/NamedWorkflowTest.java)
  passed in this audit: the actual server refuses an invalid save, stores/reads
  a valid definition, and executes it by name. Frontend request/gate tests also
  exist in `services/workflows.test.ts` and `services/workflows.gate.test.ts`;
  they were inspected, not rerun here. This is already a workflow editor, not a
  proposed new UI or a graphical drag-and-drop builder.
- **Schema/contract exploration and editing:**
  [SubjectDetailView](../../apps/console/src/views/SubjectDetailView.vue) provides
  source, version diff, type exploration, Try It, and compatibility panels.
  [TryItPanel](../../apps/console/src/components/TryItPanel.vue) builds a form
  from descriptors and round-trips protobuf JSON. That local type check does
  not execute the full Java annotation/CEL validator. Compatibility checking
  and write-gate configuration likewise need their documented server behavior;
  do not infer stronger enforcement from a panel label.
- **Service discovery and calls:**
  [ConnectServiceView](../../apps/console/src/views/ConnectServiceView.vue) acquires
  descriptors from reflection, Git, or pasted sources;
  [ServicesView](../../apps/console/src/views/ServicesView.vue) and
  [MethodInvokePanel](../../apps/console/src/components/MethodInvokePanel.vue)
  expose discovered methods and request forms. Preserve these entry points.
- **Agent task contracts and conversation:**
  [TaskConsoleView](../../apps/console/src/views/TaskConsoleView.vue) already offers
  tasks with objectives and required checks, shows worker/transcript/check status,
  sends guidance, and supports acceptance/revision. Its current offer form exposes
  acceptance checks; it is not a complete editor for arbitrary descriptor-set
  `TaskSpec.contract` payloads. Reuse useful editors and viewers, but evaluate the
  task interaction model against the user's reported stalls and direction drift.
  The separate review-binding patch remains
  the prerequisite already recorded above.
- **Receipt export, verification, evaluation:**
  [ReceiptsView](../../apps/console/src/views/ReceiptsView.vue) exports a run record,
  verifies it, and evaluates it against a selected stored workflow after compiling
  that definition. [ReceiptsView tests](../../apps/console/src/views/ReceiptsView.test.ts)
  cover those UI interactions with mocked responses; this is not a live browser
  deployment claim.

Planning consequence: use court enrichment as a regression scenario on the
current protocol, including incomplete/review and externally completed jobs.
Do not make the historical harness a dependency of the minimal Goal 2 starter.
Goal 3 should provide guided startup and connections, reusing console components
where they help the selected interaction model.
The new evaluation contract supplies a missing provider-neutral interface; it
does not replace the older prompt, validation, workflow or frontend work.

### Additional reuse inputs: discovery, storage, and mesh

- [WorkspaceResources](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/WorkspaceResources.java)
  already exposes `protomolt://workspace` with instructions, the live tool
  inventory, and a catalog fingerprint. Reuse it for MCP orientation; do not
  invent a parallel discovery service. This does not by itself solve task stalls.
- [DelegationRuntime](../../apps/serve/src/main/java/ai/protomolt/proto/serve/DelegationRuntime.java)
  restores transcripts through the configured repository service and fails
  startup on repository/key errors rather than silently falling back to memory.
  [S3BlobStore](../../repo/container/src/main/java/ai/protomolt/proto/repo/container/blob/S3BlobStore.java)
  implements the repository's S3-compatible blob adapter. The checked-in
  [NAS topology](../../deploy/portainer/README.md) connects repository service,
  PostgreSQL, and RustFS. This is source/configuration evidence, not a fresh
  deployment or recovery test.
- [MeshClusterRuntime](../../apps/serve/src/main/java/ai/protomolt/proto/serve/MeshClusterRuntime.java)
  mounts a persistent cluster directory with encrypted repository-backed events
  when configured; without that repository it is memory-only.
  [DelegationBridge](../../mesh/cluster/src/main/java/ai/protomolt/proto/mesh/cluster/DelegationBridge.java)
  validates and maps worker identities/capabilities into leased discovery
  advertisements. That mapping does not replace delegation lifecycle semantics
  or demonstrate automatic cross-machine task recovery.
- Starter packaging must distinguish Git-backed schema/workflow definitions,
  workflow artifact/run evidence storage, jobs storage, and delegation/mesh
  transcript storage. The existing `--demo` provides a seeded walkthrough but
  does not alone establish persistent distributed execution. Provide explicit
  minimal and distributed configurations without requiring the latter for first
  use. Their packaging and recovery checks remain implementation work.

The [two-design comparison](starter-contracts.md#design-independence-and-two-api-designs)
governs reuse: preserve useful contracts, invariants, and implementations; change
interaction patterns that fail the common acceptance scenarios.

### Coordination follow-up from the Luna audit

The user's description of tasks appearing paused must not be interpreted as a
specific protocol state without the original transcript. Existing recovery uses
a new attempt with an optional prior checkpoint in
[InProcessDelegationCoordinator.offer](../../transform/delegation/src/main/java/ai/protomolt/proto/delegation/InProcessDelegationCoordinator.java).
It is not a dedicated pause/resume operation on the same attempt.

[AgentHost.reportUnfinishedWork](../../apps/agent-host/src/main/java/ai/protomolt/proto/agenthost/AgentHost.java)
explicitly warns about a worker turn accepting a task without submitting a
candidate: unless another coordinator frame arrives, that path will not progress
and its lease will expire. This is an existing behavior to cover in the refined
coordination design, not proof of the cause of a historical stall. Acceptance
cases should exercise accept-without-candidate, missing follow-up, lease expiry,
checkpoint re-offer, and a visible next action in the app.

Role/tool restrictions and typed task envelopes remain valuable. They do not
prove that a syntactically valid assignment matches the user's intent. Add a
wrong-direction scenario to the design review: show the assigned objective,
scope, checks, and worker; make a requested correction explicit; prevent stale
results or reviews from satisfying the changed assignment. These are proposed
acceptance cases, not completed fixes. The audit was read-only and did not run
live agents or recovery tests.

## Proposed contract-only addition

**N — `ai.protomolt.proto.inference.v1.EvaluationService.Evaluate`**, in [`inference/proto/src/main/proto/ai/protomolt/proto/inference/v1/evaluation.proto`](../../inference/proto/src/main/proto/ai/protomolt/proto/inference/v1/evaluation.proto). This is a provider-neutral semantic judgment contract. `EvaluateRequest` binds either a task ID, attempt, and candidate revision or a recorded workflow run, plus result/descriptor/evidence/projection/rubric/policy SHA-256 identities, a typed evidence `Any`, bounded rubric questions, and a configured evaluator profile without credentials. `EvaluateResponse` echoes the request ID and immutable binding and carries one typed choice, ordinal score, or probability judgment per rubric question, plus actual provider/model identity. The host must match answer IDs, kinds, option sets, normalized distributions, weighted scores, and stored digests; annotations alone cannot prove those relationships. The local decision policy, not provider prose, decides acceptance, revision, or human review. Missing, malformed, or unavailable judgments mean no affirmative acceptance and must be treated as unknown/escalated by that policy; `UNKNOWN` is a policy outcome, not an `EvaluateResponse` enum.

This new service is **not mounted, implemented, or advertised as callable** by adding its descriptor. A later host action must share catalog validation and side effects across gRPC/MCP/ACP before protocol parity can be claimed. The optional evaluator never bypasses mechanical protobuf validation, evidence completeness, reviewer authorization, or receipt verification. No provider endpoint, credential, paid account, or GPU is required for the no-provider starter fixtures. The new sample data-correction and workflow-deliverable messages belong in `samples` and illustrate valid, invalid, repairable, and refused cases; they are fixtures, not claims of a running evaluator.

## Contract obligations before handlers

- **Validation and failures:** annotate required fields, bounds, legal enum values, exclusive choices, finite/probability ranges, and cross-field relationships with ProtoMolt validation and CEL. Define success/failure response invariants. Descriptor resolution, stored-state checks, authorization, digest equality, replay protection, and candidate freshness are handler obligations with negative tests. Invalid input should identify field paths; unavailable provider, unsupported rule, stale candidate, digest mismatch, and policy refusal need distinct machine-readable failure categories. Avoid a successful response carrying contradictory `ok`/status/error fields.
- **Retries and cancellation:** distinguish bounded structured repair from provider transport retries and durable job retries. No semantic evaluator retry may change the bound subject, rubric, or policy; repeats with the same evaluation ID and same canonical payload return the same result, while the same ID with different payload conflicts. A canceled/deadline-exceeded evaluation must not post a review decision later. A durable job may continue after the submitting call's deadline; document an explicit cancellation operation before promising job cancellation.
- **Immutable review binding:** external evaluation and review decisions must name task ID, current attempt, candidate revision, result digest, rubric/policy digest, and the evidence projection evaluated. A stale or changed candidate cannot inherit an earlier favorable answer. Until the local review-binding patch is landed and tested, semantic answers remain advisory for human review.
- **Schema publication:** [JSON Schema generation](../../surface/http/jsonschema/src/main/java/ai/protomolt/proto/http/jsonschema/ProtoJsonSchemaGenerator.java) reads the shared validation rule model and exposes CEL as `x-protomolt-cel`; byte-length and temporal bounds are documented gaps. [OpenAPI generation](../../surface/http/openapi/src/main/java/ai/protomolt/proto/http/openapi/ProtoOpenApiGenerator.java) separately derives route/type schemas and does not yet establish full parity with runtime validation. No generator change is included here. Before publication, compare required values and numeric/string/collection limits against runtime fixtures, mark CEL and stateful checks as runtime-only, and avoid claiming that OpenAPI clients enforce them.
- **Evolution:** keep released field numbers and enum values stable; add optional fields with new numbers, reserve removed numbers/names, and do not reinterpret zero/default meanings. Compile the full import closure, lint and run compatibility checks against the baseline descriptor, then run valid/invalid fixtures through ProtoMolt's validator. A new response validation boundary must be explicit; assess a shared catalog response gate separately before changing all existing actions.

## Small implementation backlog after contract review

Goal 1's implemented descriptor and fixture gates are recorded in
[the verification record](starter-contract-verification.md). The following work
is deliberately unimplemented. IDs name acceptance cases for later changes;
they are not claims that those tests already pass.

1. **CORE-1, recorded correction (Goal 2).** Contract: the checked
   `correct-contact` definition, `GenerateStructuredRequest/Response`, and existing
   run/receipt models. Acceptance: `correction_repairs_then_records` and
   `correction_exhausts_budget_without_acceptance`, with invalid grounding never
   calling the provider, preserved record ID, no internal notes in grounding,
   offline replay, and signed-record verification. Include a deterministic
   provider fixture and separately labeled live-provider check.
   Add `changed_source_invalidates_evaluation`,
   `redacted_evidence_cannot_prove_missing_source`, and
   `named_workflow_resolves_pinned_version` for the source/recording obligations.
   The contract-valid invented-email fixture is not a passing semantic answer.
   **Local implementation:** `ContactCorrection` now provides this fixed named
   sample with configured evaluation, original execution and separate assessment
   receipts, and offline policy verification. See the
   [walkthrough](../tutorials/recorded-correction.md) and
   [Goal 2 verification](correction-verification.md). Live-provider mode is
   separate from the deterministic proof; no generic Evaluation RPC is mounted.
2. **EVAL-1, evaluation handler (after CORE-1).** Contract: `EvaluateRequest` and
   `EvaluateResponse`, subject to the [specified boundaries](starter-contracts.md).
   Acceptance: `unresolved_any_never_invokes_provider`, `oversize_envelope_refused`,
   `unknown_rule_refused`, `altered_digest_refused`, `changed_task_contract_refused`,
   `policy_covers_offered_checks`, `answer_ids_and_kinds_match`,
   `distribution_matches_rubric`, `invalid_response_never_applies_policy`, and
   `provider_failure_never_accepts`. Store request, resolved profile, response, and
   policy evidence. Implement the provider-neutral seam before the Jev adapter.
3. **REVIEW-1, bind and deduplicate decisions.** Contract: `EvaluationBinding`,
   `request_id`, and the separate `ReviewCandidateRequest` attempt/revision patch.
   Review and land that prerequisite independently before automatic review.
   Acceptance: `late_review_cannot_accept_new_revision`,
   `identical_retry_returns_recorded_response`, `changed_payload_conflicts`,
   `profile_change_does_not_change_retry`, and `cancel_race_has_one_committed_outcome`.
4. **ENTRY-1, protocol/schema consistency (Goal 3).** Contract: shared catalog
   request/response descriptors and the new evaluator service once implemented.
   Acceptance: `same_contract_same_outcome_across_transports`,
   `nested_grpc_payload_rules_enforced`, and `openapi_representable_rules_match_runtime`.
   Mount gRPC/MCP/ACP actions only after handlers exist. Enforce successful-response
   validation for the new service explicitly; review existing actions separately.
5. **AUTHOR-1, agent-authored workflow acceptance (Goal 5).** Contract:
   `TaskSpec.contract`, `WorkflowDeliverable`, `SubmitWorkflow`, and `CompleteStep`.
   Acceptance: `artifact_and_inline_workflow_match`,
   `required_checks_independently_verified`, `receipt_names_same_run`,
   `job_payload_reuse_conflicts`, `external_completion_reuse_conflicts`, and
   `restart_preserves_completed_steps`. Validate all content references, execute
   fixtures, promote, submit, and inspect checkpoints. In-flight side effects
   need explicit idempotency.
6. **ENTRY-2, starter packaging and browser setup (Goal 3).** Contracts: existing
   named workflow, schema/service profiles, run evidence, jobs, and browser
   action routes. Reuse `serve`, console components, MCP workspace discovery and
   optional role composition; do not require the whole document platform.
   Acceptance: `clean_install_to_recorded_result_without_host_jdk`,
   `restart_preserves_configured_state`, `bootstrap_catalog_matches_mounted_tools`,
   and `authenticated_console_can_run_starter_without_public_proxy_bypass`.
   Compare the small serve and role-node configurations, explicitly assigning
   each store and connection to its owning process. Existing profile credential
   references are refused without a resolver; either implement the resolver with
   `authenticated_profile_reflection_and_invoke` or explicitly keep that remote
   integration outside the first starter. Never claim the configured reference
   alone enables authenticated calls.
7. **COORD-1, actionable coordination and scope changes (Goal 4).** Contracts:
   `TaskSpec`, task/attempt/revision frames, checkpoints, guidance, and bound
   review. Acceptance: `accepted_without_candidate_has_next_action`,
   `missing_followup_expires_without_acceptance`,
   `checkpoint_reoffer_names_new_attempt`, `pending_batch_restart_preserves_cursor`,
   and `scope_change_cannot_reuse_old_acceptance`. UI state must name the reason
   for waiting, responsible actor, and available recovery action. Reuse durable
   transcripts and provider-session state; assess any actual lifecycle extension
   in a separate contract review rather than advertise pause/resume today.
8. **COURT-1, representative source-support regression (Goal 2 extension).**
   Contract: existing `court.v1.OpinionMetadata`, current workflow/job envelopes,
   and the provider-neutral evaluation contract where configured. Port the old
   harness rather than publish old SubmitChain commands. Acceptance:
   `fabricated_docket_not_accepted`, `copied_exemplar_not_accepted`,
   `incomplete_with_reason_routes_to_review`, `unsupported_citation_not_accepted`,
   and `external_completion_records_same_job`. If document retrieval/screening
   is configured, add `citation_matches_pinned_source_span` and
   `configured_screening_refusal_never_calls_provider`; these are not mandatory
   infrastructure for the small inline starter. Use deterministic regression
   fixtures; keep any live-provider results separately labeled.
   **Partial local coverage:** six tests compile the unchanged in-tree court
   schema and cover incomplete, invalid, invented, copied-exemplar and unsupported
   citation cases with a bounded fixture policy. The existing generic
   `CompleteStepValidationTest` also passes, but a court-schema external-completion
   walkthrough and historical harness port remain open. Do not infer full
   COURT-1 completion from the contact sample.
