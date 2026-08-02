# Scale QA summary

Runs: 90 (30 docs x 3 personas)

## appellate-researcher

- first-shot VALID: 23/30 (76%)
- JSON parse ok: 30/30
- declared INCOMPLETE: 0/30
- avg fields filled (of 9): 7.8
- median latency: 62.24s, avg prompt tokens: 3125
- docket found in text: 10/14 (prose-valued: 1)
- citations found in text: 138/145 (95%)
- violations:
  - None / repeated.min_items: 4x
  - None / int32.gte_lte: 2x
  - None / no-prose-values: 1x
  - None / string.max_len: 1x

## citator

- first-shot VALID: 21/30 (70%)
- JSON parse ok: 30/30
- declared INCOMPLETE: 0/30
- avg fields filled (of 9): 7.6
- median latency: 52.91s, avg prompt tokens: 3115
- docket found in text: 8/12 (prose-valued: 1)
- citations found in text: 134/141 (95%)
- violations:
  - None / repeated.min_items: 7x
  - None / int32.gte_lte: 2x
  - None / no-prose-values: 1x
  - None / string.max_len: 1x

## headnote-editor

- first-shot VALID: 24/30 (80%)
- JSON parse ok: 30/30
- declared INCOMPLETE: 0/30
- avg fields filled (of 9): 7.6
- median latency: 52.15s, avg prompt tokens: 3120
- docket found in text: 8/11 (prose-valued: 0)
- citations found in text: 118/123 (95%)
- violations:
  - None / int32.gte_lte: 3x
  - None / repeated.min_items: 3x
  - None / string.max_len: 1x
