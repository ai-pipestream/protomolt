# Worker documentation consistency review — 2026-09-04

## Headline

The three documents disagree twice: once on how a task is offered (the offer
verb is never loaded), once on when the lease clock starts.

## Documents reviewed

- `docs/design/work-tags.md`
- `.claude/skills/protomolt-worker/SKILL.md`
- `docs/transform/delegation.md` (the completion-candidate contract lives in
  "Task lifecycle"; no section literally named "Deliverable contract" exists)

## Finding 1 — the offer verb is never loaded

- Document: `.claude/skills/protomolt-worker/SKILL.md`
- Quote: `mcp__protomolt__delegation-review,mcp__protomolt__delegation-cancel,mcp__protomolt__delegation-transcript`
- Verified: `grep -F 'mcp__protomolt__delegation-review,mcp__protomolt__delegation-cancel,mcp__protomolt__delegation-transcript' .claude/skills/protomolt-worker/SKILL.md` exits 0.
- Problem: step 1 ("Load the tools") loads a fixed select list that ends at
  `delegation-transcript` and never includes `delegation-offer`. The same step
  is shared by the coordinator role ("load the tools (step 1 above)"), yet the
  Coordinator/Offer section then instructs the coordinator to describe a task
  with `spec.objective`, `spec.allowedScope`, `spec.constraints` and
  `spec.requiredChecks` and send it through `delegation-offer`
  ("then `delegation-offer`:"). The verb that describes and sends the task is
  not in the loaded set, so a coordinator following the skill literally cannot
  offer work. This contradicts the coordinator flow the same document defines.
- Proposed wording: extend the select list with
  `mcp__protomolt__delegation-offer` (and, for symmetry with the transcript
  guidance, `mcp__protomolt__delegation-transcript` is already present).

## Finding 2 — when the lease clock starts

- Document: `.claude/skills/protomolt-worker/SKILL.md`
- Quote: `The lease starts at acceptance`
- Verified: `grep -F 'The lease starts at acceptance' .claude/skills/protomolt-worker/SKILL.md` exits 0.
- Problem: this states the attempt budget begins when the worker accepts.
  `docs/transform/delegation.md` ("Task lifecycle") says the offered TaskSpec
  carries "a lease duration and expiry" — an absolute expiry delivered with the
  offer. Observed offers compute `expiresAt` from the offer's `sentAt`, so the
  deadline is already elapsing before acceptance; a worker that asks a QUESTION
  first (as the same section requires) spends budget it has not accepted. The
  two documents disagree on when the clock starts.
- Proposed wording: either change `docs/transform/delegation.md` to "a lease
  duration, with the expiry computed and sent at acceptance", or change
  SKILL.md to "the offer states `expiresAt`; acceptance must land before it,
  and the budget runs from the offer's send time".

## Notes

`docs/design/work-tags.md` is consistent with both on description (tier/tags
on the first constraint line), proof (commit or artifact plus named checks),
and judgement (reviewer verifies evidence before deciding).
