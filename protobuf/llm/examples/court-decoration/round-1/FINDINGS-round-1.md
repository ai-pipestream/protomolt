# Scale QA round 1 — findings (2026-08-01)

Run: `results/20260801-185245` — 15 uniformly random opinions (line numbers in
`line-numbers.txt`) × 3 persona candidates × Qwen3-14B-int4, `json_object`,
temperature 0.2, docs capped at 24k chars (2 of 15 truncated).

## Headline

- **45/45 first-shot schema VALID** (100%), 45/45 JSON parses. The
  decoder+validator story holds at scale: nothing the model produced broke a
  single declared rule on the first attempt, across ~3.3k-token prompts and
  ~60s median latency.
- **When a docket number is printed in the opinion: 15/15 correct** (5 docs ×
  3 personas), including correct normalization ("Case: 23-7035" →
  "No. 23-7035").
- **Citation fidelity ~96% real** (the raw 92% was a checker artifact — the
  checker's ` v. ` regex missed "In re X" names; all such "misses" verified
  present in the text by hand).

## The one real failure mode: absent docket numbers

10 of 15 sampled docs print no docket number in the caption (mostly pre-1900
cases). Across those 30 persona-runs:

| outcome | count | verdict |
|---|---|---|
| found a legitimate identifier deeper in the text (reporter cite, file no.) | 9 | defensible |
| **copied the prompt's own example** (`No. 08-2575` — from the field instruction's `e.g.`) | 5 | self-inflicted prompt bug |
| **invented a plausible number** (`No. 91-00123`, `No. 1897-12345`) | 8 | hallucination |
| wrote prose into the field (`Not specified in the text`, `(example…)` ) | 7 | schema-semantics miss |
| correctly omitted the field | 1 | the only fully correct runs |

Same story on citations, one doc: 7341286 (an 1897 case, sparse citation
language) made **all three personas** fabricate authorities — including
placeholder-shaped ones (`Equity v. Creditor, 123 N.Y. 456`, three invented
`123 N.Y. 456` filler cites in one fill). When source material is thin, the
model fills the form's *shape* anyway. This is exactly the class of error the
human-review queue exists for — validation can't see it (the strings are
well-formed), but fidelity-checking against the source text can, and it is
cheap (substring checks, as here).

## v1.1 form changes (applied to `proto/court/v1/opinion_metadata.proto`)

1. **Removed the concrete example** from `docket_number`'s instruction —
  it leaked into 5 fills. (General lesson: never put a realistic-looking
  literal in an llm.v1 instruction; the model treats it as a value, not an
  illustration.)
2. **Added an omission safeguard**: leave the field out when no docket is
  printed; never write "not specified" or an example.
3. **Added a CEL validation rule** rejecting prose-shaped docket values —
  turns outcome #4 above from silently-VALID into judge-caught INVALID.

Open schema question (not changed): mixed dispositions. Doc 4471323 was
affirmed-in-part, reversed-in-part; all personas correctly perceived the
outcome in their headnotes but the enum forced a single `REVERSED`. Consider
`AFFIRMED_IN_PART` / composite posture in v2 of the form.

## Persona verdict

Validity, parse rate, and latency are statistically identical (100%, 100%,
61–69s median). As designed, personas steer content, not validity:

- **appellate-researcher**: most authorities per doc (75 total), balanced
  headnotes. → **v1 default.**
- **citator**: fewest topics, terse headnotes — right voice for the
  citation-index use case, but hallucinated 3 case cites on the thin 1897 doc.
- **headnote-editor**: most precise topics (`plea to the jurisdiction`,
  `immunity of officials`), adds procedural posture to headnotes — right
  voice for a digest product.

Keep all three as the v1 set; default to appellate-researcher. The choice is
per-template, per-use-case — not a competition.

## Round 2

Re-run with the v1.1 form, ~30 docs, and the fixed fidelity checker
(prefix-stripped docket compare, name-based citation compare — in
`run_qa.sh`). Success bar: docket hallucinations → 0 hard failures, with
prose values now surfacing as judge-caught violations instead of silent VALID.
