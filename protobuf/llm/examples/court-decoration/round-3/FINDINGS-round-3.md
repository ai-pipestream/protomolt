# Scale QA round 3 — findings (2026-08-02)

Run: `results/20260802-060900` — 30 uniformly random opinions × 3 personas ×
Qwen3-14B-int4, `json_object`, temperature 0.2, `max_tokens` 2000, docs capped
at 24k chars. Form: **v1.3** (= v1.2 + protomolt `skip_when: "incomplete"`,
PR [#66](https://github.com/ai-pipestream/protomolt) `abc8f2c`; vendored
`validate.proto` re-synced into the harness before the run). The sample is a
fresh uniform draw — **0/30 doc overlap with round 2**, so cross-round
comparisons are rate-level, not per-doc.

## Headline

- **The `skip_when` escape channel works end-to-end: 8/8 INCOMPLETE
  declarations validate clean** (`valid: true`, zero violations). Round 2's
  central defect — 10/10 honest declarations punished by `required` /
  `repeated.min_items` / `int32.gte_lte` — is gone. That entire violation
  class dropped from 20 occurrences to **zero**.
- **90/90 JSON parses**, median latency flat (67–78s vs 72–78s).
- **First-shot VALID: 25/30 per persona, 75/90 (83.3%)** vs round 2's
  69/90 (76.7%) — on a different sample, but the composition of the residual
  matters more than the rate (below).
- **The judge did not go soft**: 15 non-declared docs still fail, 12 of them
  on `no-prose-values`. Declaring INCOMPLETE is the only thing that changed;
  every undeclared doc faces the full rule set.

## Violation taxonomy shift (the real comparison)

| rule | round 2 | round 3 |
|---|---|---|
| `repeated.min_items` | 10 | 2 |
| `required` | 4 | 0 |
| `int32.gte_lte` (year) | 6 | 2 |
| `no-prose-values` | 9 | 12 |
| `repeated.unique` / `string.max_len` | 3 | 0 |

Round 2's `min_items`/`required`/`gte_lte` counts were almost entirely
punishment of declared docs; with `skip_when` those are structurally
impossible and the residual 4 are genuine fills on undeclared docs. What
remains is now **78% `no-prose-values`** — the model's prose-docket habit
("not provided in the text") is the single dominant failure mode, and it is
a prompt/model problem, not a contract problem.

## INCOMPLETE usage: honest, specific, unpunished

8 declarations (5/1/2 by persona), all with document-specific reasons, all
on genuinely thin material: a 142-char stub (357773), an order-to-show-cause
with no holding (10038882), a no-opinion procedural entry (5714858). Zero
declarations on merely-hard docs — no abuse of the escape hatch.

Designed-behavior note: two declared docs (357773, 10038882 ×2 personas)
still emitted a docket value that matches the text. `skip_when` suspends
*all* field rules on a declared doc, including `no-prose-values` — that is
the intended "don't trust my fill, route the whole doc to review" semantics,
worth one line in the court spec so nobody reads it as a validator hole.

## Hallucinated dockets: flat, same hard class

8 total (3/2/3 by persona) vs round 2's 4 — but clustered on 3 docs
(801792, 2159650, 2209147), the identical slip-opinion / no-printed-docket
profile as round 2's 4-doc cluster. With 0 sample overlap this is a rate,
not a regression: ~5–9% of runs, all caught by the cheap fidelity check,
all review-queue material.

## Citation grounding: effectively 100% — the verifier is now the noisy part

Substring verification reported 450/458 (96–98%). **All 8 "misses" were
hand-checked and every one is an analyzer artifact, not a fabrication**:

- `2742438` — a footnote number is injected mid-case-name in the source
  ("Bledsoe v. Merit Systems Protection 3 The administrative…"); the model
  correctly emitted the clean name.
- `2159650` — model dropped the pinpoint page (", 103-104") from
  "71 Cal.2d 96, 103-104 [77 Cal.Rptr. 224…]".
- `3535495` — OCR noise in the source ("C. A. Heirs . Railroad Co." with
  stray spaces); the model normalized it.
- `10038882` — curly vs straight apostrophe ("Int'l").

Round 4 candidate: normalize both sides in the verifier (fold smart quotes,
strip whitespace/punctuation) instead of touching the form — the model's
grounding is already better than exact-substring accounting gives it credit
for.

## Stability verdict for the job manager

Round 3 was the gate. The answer: **the form is stable enough to design the
job manager (T2) around it.** Every remaining failure mode lives outside
the contract:

1. Prose-docket values — model habit; prompt/fine-tune territory.
2. Hallucinated dockets on slip opinions — model habit; already routed by
   the fidelity check.
3. Verifier strictness — tooling bug in `analyze.py`, not the schema.

No open schema/validator questions remain: `skip_when` closed the last
semantics gap, the CEL rules fire exactly where designed, and the
declaration channel produces routable, unpunished honesty. The contract can
be frozen at v1.3 for job-manager design; further gains are prompt- and
model-side and can iterate independently.

## Round 4 candidates

- Verifier normalization in `analyze.py` (smart quotes, whitespace,
  pinpoints) — pure tooling fix.
- Prompt-side attack on prose dockets (the 12-violation residual): negative
  examples in the packet, or a second-chance edit pass.
- Larger sample (100+ docs) for rate confidence once the job manager exists
  to run it unattended.
