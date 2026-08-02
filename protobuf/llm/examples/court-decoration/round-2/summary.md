# Scale QA summary

Runs: 90 (30 docs x 3 personas)

## appellate-researcher

- first-shot VALID: 21/30 (70%)
- JSON parse ok: 30/30
- declared INCOMPLETE: 5/30
- avg fields filled (of 9): 7.7
- median latency: 77.53s, avg prompt tokens: 3508
- docket found in text: 15/20 (prose-valued: 3)
- citations found in text: 147/147 (100%)
- violations:
  - None / repeated.min_items: 5x
  - None / no-prose-values: 3x
  - None / int32.gte_lte: 2x
  - None / required: 2x
  - None / repeated.unique: 1x

## citator

- first-shot VALID: 22/30 (73%)
- JSON parse ok: 30/30
- declared INCOMPLETE: 3/30
- avg fields filled (of 9): 8.2
- median latency: 72.11s, avg prompt tokens: 3498
- docket found in text: 14/20 (prose-valued: 5)
- citations found in text: 154/154 (100%)
- violations:
  - None / no-prose-values: 5x
  - None / repeated.min_items: 3x
  - None / int32.gte_lte: 2x
  - None / repeated.unique: 1x

## headnote-editor

- first-shot VALID: 26/30 (86%)
- JSON parse ok: 30/30
- declared INCOMPLETE: 2/30
- avg fields filled (of 9): 7.8
- median latency: 76.42s, avg prompt tokens: 3503
- docket found in text: 15/17 (prose-valued: 1)
- citations found in text: 148/148 (100%)
- violations:
  - None / int32.gte_lte: 2x
  - None / repeated.min_items: 2x
  - None / required: 2x
  - None / no-prose-values: 1x
  - None / string.max_len: 1x
