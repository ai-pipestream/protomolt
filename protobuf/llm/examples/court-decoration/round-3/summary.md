# Scale QA summary

Runs: 90 (30 docs x 3 personas)

## appellate-researcher

- first-shot VALID: 25/30 (83%)
- JSON parse ok: 30/30
- declared INCOMPLETE: 5/30
- avg fields filled (of 9): 7.6
- median latency: 72.37s, avg prompt tokens: 3328
- docket found in text: 8/16 (prose-valued: 5)
- citations found in text: 159/162 (98%)
- violations:
  - None / no-prose-values: 5x

## citator

- first-shot VALID: 25/30 (83%)
- JSON parse ok: 30/30
- declared INCOMPLETE: 1/30
- avg fields filled (of 9): 8.0
- median latency: 66.99s, avg prompt tokens: 3318
- docket found in text: 9/15 (prose-valued: 4)
- citations found in text: 150/155 (96%)
- violations:
  - None / no-prose-values: 4x
  - None / repeated.min_items: 1x

## headnote-editor

- first-shot VALID: 25/30 (83%)
- JSON parse ok: 30/30
- declared INCOMPLETE: 2/30
- avg fields filled (of 9): 7.9
- median latency: 77.84s, avg prompt tokens: 3323
- docket found in text: 9/15 (prose-valued: 3)
- citations found in text: 141/144 (97%)
- violations:
  - None / no-prose-values: 3x
  - None / int32.gte_lte: 2x
  - None / repeated.min_items: 1x
