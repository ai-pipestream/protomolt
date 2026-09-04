# Worker-docs consistency review — 2026-09-04

Reviewed in full, at `origin/main` (`f745fae6`):

- `docs/design/work-tags.md`
- `.claude/skills/protomolt-worker/SKILL.md`
- `docs/transform/delegation.md` (Task lifecycle, Deliverable contract, and the
  surrounding sections on how a task is judged)

Four statements in `docs/transform/delegation.md` contradict the worker skill
and the twelve-verb catalog: the lifecycle promises frames a worker or
coordinator can send (reject, heartbeat, renew, context) that no documented
verb produces. Everything else checked agrees: contract field semantics,
`@type` spelling, revision numbering from 1, evidence shape
(`checkName`/`verdict`/`ranAt`/`detail`), commit shape
(`repository`/`commit` 40-hex/`subject`), reducer-before-reviewer ordering,
refusing a `result` exactly when no contract is declared and vice versa, and
the end-of-task `tokens-spent` NOTE.

## Findings

### 1. No way to reject an offer

Quote (`docs/transform/delegation.md`):

> The worker accepts or rejects the offer. An accepted attempt may send

Conflicts with `.claude/skills/protomolt-worker/SKILL.md`:

> | `offer` for your worker id | read the spec, then accept or ask (below) |

and with the twelve catalog verbs in `docs/transform/delegation.md`
(`delegation-worker-register`, `delegation-worker-list`, `delegation-offer`,
`delegation-accept`, `delegation-progress`, `delegation-checkpoint`,
`delegation-candidate`, `delegation-review`, `delegation-cancel`,
`delegation-message`, `delegation-watch`, `delegation-transcript`), which
contain `delegation-accept` but no reject verb. A worker that must decline can
only stall in QUESTION.

Proposed: change the lifecycle sentence to "The worker accepts the offer, or
asks before accepting", and either add a `delegation-reject` verb or document
the decline path (e.g. QUESTION stating inability, coordinator cancels).

### 2. No heartbeat verb

Quote (`docs/transform/delegation.md`):

> heartbeats, monotonic progress, and resumable checkpoints. The coordinator may

Conflicts with the twelve catalog verbs, which have no heartbeat verb, and
with the skill, which never mentions heartbeats (progress and checkpoints
only). An accepted attempt cannot send what the lifecycle says it may send.

Proposed: drop "heartbeats, " from the sentence, or add the heartbeat verb to
the catalog and the skill's Work/report/checkpoint section.

### 3. No lease-renewal verb

Quote (`docs/transform/delegation.md`):

> renew or expire the lease. A later attempt can resume from a recorded

Conflicts with the twelve catalog verbs (no renew verb) and with the skill's
Offer section (`leaseSeconds` with no renewal step). The skill documents
re-offer with `resumeFrom` after expiry, which is expiry-plus-replacement, not
renewal of the live lease.

Proposed: change to "may expire the lease and re-offer with `resumeFrom`",
or add a renewal verb and its skill step.

### 4. Skill's offer shape omits `context`

Quote (`docs/transform/delegation.md`):

> - context artifact references;

Conflicts with the skill's Coordinator/Offer member list (`workerId`,
`leaseSeconds`, `spec.objective`, `spec.allowedScope`, `spec.constraints`,
`spec.requiredChecks`, `spec.contract`, `resumeFrom`), which has no `context`
member. A coordinator following only the skill never sends the context
artifact references the lifecycle says a `TaskSpec` carries.

Proposed: add a `spec.context` bullet to the skill's Offer section (artifact
references the worker may read), mirroring the lifecycle list.

## Verification

- All three documents read in full (this review quotes the worktree copies).
- Each quote above verified with `grep -F -- '<quote>' <document>` (exit 0).
- Report committed on branch `agent/muse-1`; `git status` clean.

```json
{
  "headline": "Lifecycle promises reject, heartbeat, renew and context frames that no documented verb can send",
  "documentsReviewed": [
    "docs/design/work-tags.md",
    ".claude/skills/protomolt-worker/SKILL.md",
    "docs/transform/delegation.md"
  ],
  "findings": [
    {
      "document": "docs/transform/delegation.md",
      "quote": "The worker accepts or rejects the offer. An accepted attempt may send",
      "problem": "Lifecycle promises a rejection path, but the twelve catalog verbs contain delegation-accept and no reject verb, and the skill's flow is accept-or-ask; a worker that must decline can only stall in QUESTION.",
      "proposed": "Change to 'The worker accepts the offer, or asks before accepting', and either add a delegation-reject verb or document the decline path."
    },
    {
      "document": "docs/transform/delegation.md",
      "quote": "heartbeats, monotonic progress, and resumable checkpoints. The coordinator may",
      "problem": "Lifecycle says an accepted attempt may send heartbeats, but no heartbeat verb exists in the catalog and the skill never mentions heartbeats.",
      "proposed": "Drop 'heartbeats, ' from the sentence, or add the heartbeat verb to the catalog and the skill."
    },
    {
      "document": "docs/transform/delegation.md",
      "quote": "renew or expire the lease. A later attempt can resume from a recorded",
      "problem": "Lifecycle says the coordinator may renew the lease, but no renew verb exists; the skill only documents re-offer with resumeFrom after expiry, which is replacement, not renewal.",
      "proposed": "Change to 'may expire the lease and re-offer with resumeFrom', or add a renewal verb and its skill step."
    },
    {
      "document": "docs/transform/delegation.md",
      "quote": "- context artifact references;",
      "problem": "Lifecycle lists context artifact references as TaskSpec members, but the skill's Coordinator/Offer member list has no spec.context entry, so a skill-following coordinator never sends them.",
      "proposed": "Add a spec.context bullet to the skill's Offer section."
    }
  ],
  "findingCount": 4
}
```
