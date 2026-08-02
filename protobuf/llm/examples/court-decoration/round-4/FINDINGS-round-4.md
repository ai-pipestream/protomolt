# Scale QA round 4 — findings (2026-08-02)

Run: `results/20260802-082857` — 30 uniformly random opinions × 3 personas ×
Qwen3-14B-int4, `json_object`, temperature 0.2, `max_tokens` 2000, docs capped
at 24k chars. Form: **v1.3** (unchanged, frozen). Personas: **1.1.0** — one
added safeguard: *"If a field's value does not appear in the text, omit the
field entirely — never write 'not provided', 'not specified', 'unknown', or
any explanation into a field value."* Analyzer: loose ordered-token citation
matching (round-4 tooling fix, below). Fresh sample, **0/30 overlap** with
rounds 2 and 3.

## Headline

- **The safeguard hit its target: prose-valued fields collapsed 12 → 2**
  (round 3 → 4), with headnote-editor at zero. The `no-prose-values` CEL now
  has almost nothing left to catch.
- **Side effect: INCOMPLETE declarations went 8 → 0.** The model read "omit
  the field entirely" literally and applied it to stub documents it cannot
  truthfully fill — silently omitting instead of declaring. The hard rules
  fired on the omissions: 13 of 22 invalid runs sit on docs of 102–1,936
  chars, exactly the declaration class from round 3.
- **First-shot VALID 68/90 (75.6%)**, down from round 3's 75/90 (83.3%) —
  entirely explained by the missing declarations: violation mix shifted back
  to `repeated.min_items` (11) and `int32.gte_lte` (7) on stub docs.
- **90/90 parses**, latency improved (medians 52–62s vs 67–78s).

## Violation taxonomy

| rule | round 2 | round 3 | round 4 |
|---|---|---|---|
| `repeated.min_items` | 10 | 2 | 11 |
| `required` | 4 | 0 | 0 |
| `int32.gte_lte` | 6 | 2 | 7 |
| `no-prose-values` | 9 | 12 | 2 |
| `string.max_len` | 1 | 0 | 3 |
| `repeated.unique` | 2 | 0 | 0 |

The `string.max_len` triple is one doc (10133969, a 24k-char truncated
monster) whose caption exceeds the 500-char cap from all three personas —
a real catch, and a directive tension worth noting ("use the full caption"
vs a 500-char field). Minor tooling note: the harness records that
violation with `field: null`; field attribution for string rules should be
fixed in the runner.

## Citations: 407/409 grounded — and the 2 flags are the good kind

With the loose matcher (ordered tokens, gap-tolerant, prefix fuzz for OCR
truncations, 80% cover for footnote-swallowed words), all three historical
rounds re-verify at 100% and round 4 lands at 99.5%. The two flags are on
doc 2450863: the text mentions "Alam and Hurd" **by surname only**, and the
model expanded them into full "Hurd v. State" / "Alam v. State" citations.
The parties' "v. State" half is not in the text — the verifier is right to
flag these, and they pin down a real (if mild) fabrication class:
*reconstructing full citations from surname mentions*. New prompt-safeguard
material.

## Hallucinated dockets: 9, same hard class

Four docs (183763, 1219178, 1491083, 2450863), slip/no-printed-docket
profile, all caught by the fidelity check. Notably 183763 produced
near-identical inventions from all three personas ('08-1822', '08-1825',
'08-1812') — the model pattern-completes docket shapes when the caption is
silent. Rate is flat across rounds (~5–10% of runs); this is a model habit
the contract already routes.

## The round's real finding: prompts and channels interact

Round 4 is a clean controlled experiment — one sentence added to the
personas, nothing else changed — and it moved two metrics in opposite
directions. The safeguard fixed prose values but suppressed the declaration
channel, because "omit the field entirely" says nothing about *when omission
is the wrong tool*. The contract (v1.3) is not the problem; the instruction
is incomplete.

**Round 5 candidate (persona 1.1.1, still no contract change)**: couple the
two behaviors explicitly — *"Omit a single missing field. If the text lacks
most of what the form requires, declare the fill incomplete with a reason
instead of omitting field after field."* Prediction: stub docs return to
the declaration channel, `min_items`/`gte_lte` collapse again, and VALID
climbs past round 3 while prose values stay near zero.

## Tooling: the matcher fix (shipped this round)

`analyze.py` now verifies citations by ordered-token containment (gaps
allowed, window-bounded, prefix fuzz for OCR truncation, 80% token cover,
1-char alpha tokens dropped). Re-run on history: rounds 1–3 all verify
100%, and every docket previously flagged as hallucinated is still flagged
— the check became more accurate in both directions. Remaining weakness,
noted for the record: very short case names ("In re X") verify weakly
because anchor tokens are common words.
