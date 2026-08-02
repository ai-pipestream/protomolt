# Scale QA round 2 — findings (2026-08-01)

Run: `results/20260801-223615` — 30 uniformly random opinions × 3 personas ×
Qwen3-14B-int4, `json_object`, temperature 0.2, `max_tokens` 2000, docs capped
at 24k chars. Form: **v1.2** (docket example-leak removed, `no-prose-values`
CEL rule, `incomplete` + `incomplete_reason` + `incomplete-needs-reason`
message CEL). Corpus: rebuilt `opinions.ndjson` (see appendix).

## Headline

- **90/90 JSON parses** — the `max_tokens` 900→2000 fix eliminated round-2's
  only structural failure mode.
- **Citations: 449/449 verified in the source text (100%)**, all three
  personas. Round 1's fabricated-authority failure mode is gone.
- **Hallucinated docket numbers: 4 total** (2/1/1 by persona), down from 15
  in round 1 — and every one lands on the known hard class (slip opinions and
  pre-1900 cases with no printed docket). All four are caught by the cheap
  fidelity check: review-queue material, not silent corruption.
- **Omission became the dominant honest behavior**: 28 docket omissions vs
  round 1's 1. The model now leaves fields out instead of inventing values.

## INCOMPLETE: it works, and it exposed a real design gap

10 declarations (5/3/2 by persona — appellate-researcher most willing,
headnote-editor least), all with coherent, document-specific reasons
("only includes procedural orders", "lacks the holding"). Zero abuses on
merely-hard docs.

**But every honest declaration was punished by the judge.** All 10
`repeated.min_items` violations, all 4 `required` violations, and all 6
`int32.gte_lte` (year) violations land on INCOMPLETE-declared docs — the
model omits `topics`/court/caption/year it cannot truthfully fill, and the
hard field rules fire anyway. First-shot VALID (70–86%) is therefore *not*
comparable to round 1's 100%: the judge now surfaces what used to be silent.

This is the round's most important finding, and it is a design input for the
court, not a model problem:

> **Validation needs an INCOMPLETE escape channel.** Options: (a) v1.3 form
> with message-level CEL — `this.incomplete || size(this.topics) > 0` etc.;
> (b) a protomolt validator feature — a `skip_when` message option naming a
> boolean field that suspends field rules; (c) the court treats
> `incomplete=true` + rule violations as a third verdict class ("declared",
> route to review) rather than pass/fail. (b) is the generic one — every
> form with an honesty flag needs it.

## The deterministic judge earns its keep

- `no-prose-values` CEL caught **9 prose-valued dockets** that round 1 would
  have recorded as silently VALID ("not provided in the text", "example…").
  Prompt safeguards alone did not stop the behavior; the CEL rule converts
  it into a routable verdict. Round 2's VALID-rate drop is precisely this
  accounting change.
- `repeated.unique` fired 2× (duplicate panel entries) — real, cheap catches.

## Persona behavior

Validity pressure differs by willingness to declare INCOMPLETE and by prose
habits: appellate-researcher 21/30 VALID (5 incomplete, 3 prose dockets),
citator 22/30 (3, 5), headnote-editor 26/30 (2, 1). Citation fidelity is
100% for all three. The v1 set stands; default remains appellate-researcher.

## Round 3 candidates

1. Adopt one INCOMPLETE/validation reconciliation (recommend validator
   `skip_when` in protomolt, measured here first via v1.3 message CEL).
2. Year rule vs pre-1600 corpus content: widen range or fold into the
   incomplete escape.
3. Feed the 4 residual hallucination docs + the 10 INCOMPLETE docs to the
   review-queue prototype as its first realistic docket.

## Appendix: corpus rebuild

Round 2 ran on `opinions.ndjson` **rebuilt from `chunks-full.ndjson`**
(rebuild_opinions.py in /work/court-corpus): 86,633,399 chunks → 9,740,254
opinions, 0 non-contiguous rejections, cross-verified 92,765/92,765
content-identical against the canary. The rebuild is deterministic and the
verification is reproducible; the original file's deletion during the v7
build cost nothing.
