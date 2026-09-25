# Goal 1 verification record

Verified 2026-09-24 America/New_York (2026-09-25 UTC) in
`/work/worktrees/protomolt/starter-contracts`, branch `feat/starter-contracts`,
based on `dc49800cd66950356a883d99141e00427df6ad65`.

The coordinating agent authored the contracts and sample checks, reviewed Sol's
inventory/design/gate tests and Luna's evaluation fixtures, and ran the final
checks below. Sol's final independent review reported no blocking issue after
reviewing the contract/specification. Review findings addressed before completion:

- An inactive oneof alternative must not carry an unconditional required rule.
  A valid task-candidate fixture caught this in `EvaluationBinding.run_id`; the
  rule now checks nonempty content only when that alternative is selected.
- Valid evidence fixtures use a packed message; a separate case records that
  a required Any envelope does not validate its payload. Host obligations are
  explicit rather than claiming this is handled by annotation validation.
- The correction edge carries internal notes before projection, making the
  projection's exclusion meaningful. The separate projection test proves removal.
- The deliverable test links its complete descriptor set through the real
  `DeliverableContracts` loader, checks a packed result, and refuses an incomplete
  closure. Merely testing envelope presence would not prove linking.
- Evaluation request/response metadata marks internal data, including evidence,
  rubric, and answers. Configured storage/logging still must honor that policy.

## Commands and outcomes

Run from the checkout root:

```sh
buf lint
buf build --as-file-descriptor-set -o /tmp/protomolt-goal1-descriptors.binpb
buf breaking --against /work/worktrees/protomolt/main-land
./gradlew :protomolt-inference-proto:test \
  :samples:test --tests '*StarterContractTest' --tests '*StarterSchemaCoverageTest' \
  :protomolt-grpc-service-contract:test :protomolt-workflow:test \
  :protomolt-inference-structured:test :protomolt-delegation:test --console=plain
git diff --check
```

Buf lint, full descriptor compilation, and FILE compatibility all passed against
the unchanged main checkout. Gradle compiles generated Java and gRPC contracts
with their full imports; the final test invocation passed. Existing runtime code
and existing wire signatures were unchanged.

The final XML reports contain **344 tests, zero failures/errors/skips**:

- Inference contracts: 9 evaluation fixture tests.
- Samples: 7 starter contract/schema coverage tests.
- Service contract: 19 existing tests.
- Workflow: 143 tests, including existing grounding/repair/replay coverage.
- Structured generation: 18 tests.
- Delegation: 148 tests, including 5 new reviewer-boundary tests.

These are local tests. No live paid evaluator, container deployment, hosted CI,
remote push, merge, or image publication was performed. New source is local to
this branch; the review-binding patch remains in its original separate checkout.

## Completion criteria and evidence

1. **Reviewed inventory and reuse.** [Inventory](starter-contract-inventory.md)
   classifies all 44 existing service RPCs and the new evaluation service, with
   delegation, jobs, projections, and receipts traced to existing types. It
   separates documentation/binding extensions from wire changes.
   The subsequent [capability reconciliation](inventory-coverage.md) accounts
   for all 150 declared Gradle projects plus browser, examples, deployment and
   tutorial/script surfaces, with family dispositions and source/test references.
   This is inventory coverage, not a claim of 150 freshly tested libraries.
2. **First workflow and agent envelope.** The checked JSON definition and
   `RawContact`, `ContactGrounding`, `CorrectedContact`, and `WorkflowDeliverable`
   schemas live in samples. `StarterContractTest` checks and compiles the workflow,
   exercises SchemaSource exclusivity and the existing named-call envelope,
   projects fields, links the deliverable closure and validates packed results.
   No new GenerateStructured RPC was needed.
3. **Executable annotations.** `EvaluationContractTest` exercises both subject
   alternatives and all three typed answers; missing required/oneof values,
   absent versus explicit zero, finite probability bounds, duplicate IDs/labels,
   collection/text bounds, choice membership and score scale. It exercises every
   new message descriptor through the validator, including lazy CEL compilation.
4. **Validation before judgment.** `DelegationJudgeGateTest` drives the existing
   bridge/coordinator. Missing, wrong-type, field-invalid, and CEL-invalid results
   preserve the prior transcript and lease state, with zero reviewer calls.
   A valid result invokes the reviewer and can still receive a revision decision.
5. **Identity and stateful obligations.** [Operational specification](starter-contracts.md)
   defines candidate/run/contract/evidence/projection/rubric/policy binding,
   profile snapshots, response matching, and atomic review rechecks. These are
   future handler obligations, not implemented enforcement claims. The baseline
   and uncommitted review-binding patch were inspected; remote main hashes were
   verified to match the baseline at the start of this work.
6. **Failure, retries, unsupported rules.** The specification defines gRPC
   statuses/diagnostic codes, bounded transport versus structured repair retries,
   cancellation races, duplicate-ID behavior, schema capability admission, and
   semantic-versus-structural validity. New handler acceptance cases have IDs in
   the backlog; they are not represented as passing runtime tests.
7. **Generated schema coverage.** The coverage below records actual generated
   output; generator work is deferred to Goal 3.
8. **Bounded implementation backlog.** CORE-1, EVAL-1, REVIEW-1, ENTRY-1,
   AUTHOR-1, ENTRY-2, COORD-1, and COURT-1 identify the relevant contracts and
   named acceptance cases. New
   endpoints remain explicitly unimplemented and unmounted.

## Inventory reconciliation verification

The follow-up audit retained the wire definitions and tested fixtures. Sol
reviewed the application/execution and data foundations families; Luna reviewed
the contract/proof obligations independently. The coordinating agent reviewed
their findings, asset/build families, the complete roster, and actual source
boundaries. The reconciliation added no runtime implementation.

- Re-ran `buf lint`, full `buf build`, and `buf breaking` against main-land:
  passed. Both remote main refs were freshly read with `git ls-remote` and
  remained at the baseline above.
- Re-ran each of the six test tasks above with Gradle's per-task `--rerun` flag:
  344 tests, zero failures/errors/skips. Six test tasks executed; dependency
  compilation tasks remained up-to-date. Log:
  `/tmp/protomolt-goal1-reconciliation-tests.log`.
- Compared the module roster to every `include` and explicit project directory
  in `settings.gradle`: 150 entries, no missing/duplicate/extra projects.
  Checked every inventory source link and all 44 RPC classifications.
- Re-inspected the separate review-binding patch: 23 modified files, still
  uncommitted; the coordinator checks attempt/revision under its lock. This
  review did not land it or treat it as available on main.
- Added explicit authority checks against the immutable task offer's descriptor,
  result type and required checks, rather than accepting a caller's alternative
  schema. Existing binding fields suffice; the corresponding handler acceptance
  cases remain future EVAL-1 work.
- Distinguished permitted source/grounding artifacts from the recorder's redacted
  snapshots and bounded attempt metadata. Insufficient evidence cannot prove
  source support. Court/document citations get explicit later acceptance cases;
  the minimal inline fixture does not acquire a search/screening prerequisite.
- Mapped the discovered setup, authenticated console, profile connection,
  coordination recovery and court regression gaps to the implementation backlog.
  The two app designs remain candidates, not two promised replacement backends.

The family audits inspected existing integration tests and deployment manifests;
they did not execute those suites or start live hosts. Fresh test claims remain
limited to the six named tasks. Goal 2 handlers, Goal 3 packaging/generator work,
and Goal 4 recovery UX are explicitly outstanding work outside Goal 1.

## Generated schema coverage

`StarterSchemaCoverageTest` produces these local review artifacts in
`samples/build/starter-contracts/`:

- `contact.schema.json`
- `evaluate-request.schema.json`
- `evaluate-response.schema.json`
- `evaluation.openapi.json`

The generation test registers only an in-memory descriptor for documentation;
its invoker throws if called. No HTTP or gRPC server is started.

Actual JSON Schema output includes required requestId/binding/evidence/rubric/
evaluatorProfile, required contact recordId/displayName, displayName length 2-120,
and response answers minItems 1/maxItems 32. The contact cross-field rule appears
under `x-protomolt-cel`; ordinary JSON Schema clients cannot execute that extension.
Runtime tests validate the corresponding rules independently.

Actual OpenAPI 3.0.3 output has a single EvaluationService/Evaluate documentation
route and descriptor-derived request/response/component shapes. At this revision,
the request's annotation-derived required list is absent, probability has type
number/double without its annotation-derived minimum/maximum, and answers is an
array without its annotation-derived minItems/maxItems. These concrete omissions
are the Goal 3 parity work, not assertions that it is already complete.

Digest matching against authoritative storage, candidate freshness, authorization,
profile pinning, request/response correlation, signature/trust verification, and
semantic truth remain runtime/review obligations. Field annotations and generated
API descriptions alone cannot establish them.
