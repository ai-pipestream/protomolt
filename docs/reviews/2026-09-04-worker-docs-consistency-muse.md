# Worker-docs consistency review — 2026-09-04 (amended)

Reviewed in full, at `origin/main` (`f745fae6`):

- `docs/design/work-tags.md`
- `.claude/skills/protomolt-worker/SKILL.md`
- `docs/transform/delegation.md` (Task lifecycle, Deliverable contract, and the
  surrounding sections on how a task is judged)
- cross-checked against `transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation.proto`
  and `delegation_actions.proto` as the source of truth

Correction to the first version of this report: the lifecycle sentences quoted
below faithfully describe the gRPC stream contract (`TaskReject`,
`Heartbeat`, `LeaseRenewal`, `TaskSpec.context` all exist in
`delegation.proto`). The inconsistency is one layer up: the twelve-verb
MCP/actions surface (`delegation_actions.proto`: `OfferTask`, `AcceptTask`,
`CancelTask`, `SubmitCandidate`, `RecordCheckpoint`, `SendTaskMessage`,
`ReportProgress`, `ReviewCandidate`, `ReadTranscript`, `WatchEvents`,
`ListWorkers`, `RegisterWorker`) and the worker skill give a worker no way to
send four of those stream members. A session working only through the skill
and the MCP verbs cannot reject an offer, heartbeat a lease, renew a lease,
or read task context — while the lifecycle says attempts do these things.

What still agrees: contract field semantics, `@type` spelling, revision
numbering from 1, evidence shape (`checkName`/`verdict`/`ranAt`/`detail`),
commit shape (`repository`/`commit` 40-hex/`subject`),
reducer-before-reviewer ordering, refusing a `result` exactly when no
contract is declared and vice versa, and the end-of-task `tokens-spent` NOTE.

## Findings

### 1. No way to reject an offer over MCP

Quote (`transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation.proto`):

> description: "Offer rejection with a bounded reason; terminal for the attempt."

`TaskReject` is field 12 of the worker-to-coordinator frame, so "accepts or
rejects" is true on the stream. But `delegation_actions.proto` has
`AcceptTask` and no reject RPC, and the skill's flow is accept-or-ask
(`.claude/skills/protomolt-worker/SKILL.md`: "| `offer` for your worker id |
read the spec, then accept or ask (below) |"). An MCP-only worker that must
decline can only stall in QUESTION.

Proposed: add a reject RPC to the actions surface and the skill's
accept-or-ask step, or document the decline path (QUESTION stating inability,
coordinator cancels).

### 2. No heartbeat verb, though heartbeats keep the lease

Quote (`transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation.proto`):

> // Leases are explicit: an offer grants one attempt's lease, heartbeats keep

`Heartbeat` is field 13 ("Liveness signal on the active lease"), and the
lease "runs without renewal" per its own comment — yet the actions surface
has no heartbeat RPC and the skill never mentions heartbeats. An MCP-only
worker cannot send the liveness signal the lease model relies on; only
accept/progress/checkpoint/candidate frames mark it alive.

Proposed: add a heartbeat RPC (or state that lifecycle frames double as
liveness for MCP workers) and mention it in the skill's Work/report section.

### 3. No lease-renewal verb, though renewals move the expiry

Quote (`transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation.proto`):

> description: "Lease renewal: the new coordinator-declared expiry, strictly advancing."

`LeaseRenewal` is field 12 of the coordinator-to-worker frame, but the
actions surface has no renew RPC and the skill's Offer section only sets
`leaseSeconds`. The skill documents re-offer with `resumeFrom` after expiry,
which is expiry-plus-replacement, not renewal of the live lease.

Proposed: add a renewal RPC and its skill step, or change the lifecycle
wording to expiry-and-re-offer for the MCP path.

### 4. Skill's offer shape omits `context`

Quote (`transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation.proto`):

> repeated ai.protomolt.proto.grpc.workflow.v1.ArtifactReference context = 5 [

`TaskSpec.context` ("Content-addressed context the worker starts from") is
in the proto and in the lifecycle list ("context artifact references"), but
the skill's Coordinator/Offer member list (`workerId`, `leaseSeconds`,
`spec.objective`, `spec.allowedScope`, `spec.constraints`,
`spec.requiredChecks`, `spec.contract`, `resumeFrom`) has no `context`
member. A coordinator following only the skill never sends starting context.

Proposed: add a `spec.context` bullet to the skill's Offer section.

## Verification

- All three documents read in full, plus the two proto files (this review
  quotes the worktree copies).
- Each quote above verified with `grep -F -- '<quote>' <document>` (exit 0).
- Report committed on branch `agent/muse-1`; `git status` clean.

```json
{
  "headline": "Stream contract has reject, heartbeat, renew and context members the MCP verbs and skill cannot send",
  "documentsReviewed": [
    "docs/design/work-tags.md",
    ".claude/skills/protomolt-worker/SKILL.md",
    "docs/transform/delegation.md"
  ],
  "findings": [
    {
      "document": "transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation.proto",
      "quote": "description: \"Offer rejection with a bounded reason; terminal for the attempt.\"",
      "problem": "TaskReject exists on the stream, but the actions surface has AcceptTask and no reject RPC, and the skill flow is accept-or-ask; an MCP-only worker cannot decline an offer.",
      "proposed": "Add a reject RPC and skill step, or document the decline path (QUESTION, coordinator cancels)."
    },
    {
      "document": "transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation.proto",
      "quote": "// Leases are explicit: an offer grants one attempt's lease, heartbeats keep",
      "problem": "Heartbeats keep the lease on the stream, but no heartbeat RPC exists and the skill never mentions heartbeats; only lifecycle frames mark an MCP worker alive.",
      "proposed": "Add a heartbeat RPC, or state that lifecycle frames double as liveness for MCP workers."
    },
    {
      "document": "transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation.proto",
      "quote": "description: \"Lease renewal: the new coordinator-declared expiry, strictly advancing.\"",
      "problem": "LeaseRenewal exists on the stream, but no renew RPC exists; the skill only re-offers with resumeFrom after expiry, which is replacement, not renewal.",
      "proposed": "Add a renewal RPC and skill step, or reword the lifecycle to expiry-and-re-offer for the MCP path."
    },
    {
      "document": "transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/delegation.proto",
      "quote": "repeated ai.protomolt.proto.grpc.workflow.v1.ArtifactReference context = 5 [",
      "problem": "TaskSpec.context is in the proto and the lifecycle list, but the skill's Offer member list has no spec.context entry, so a skill-following coordinator never sends starting context.",
      "proposed": "Add a spec.context bullet to the skill's Offer section."
    }
  ],
  "findingCount": 4
}
```
