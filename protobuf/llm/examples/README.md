# Examples: LLM metadata decoration with grounded truth

> Note: AI-generated. Human review needed.

This directory holds a worked example of what the `llm.v1` annotations and
the protomolt validation framework are for: **filling out metadata with
grounded truth**. The example is real — it is the metadata pipeline we built
for a case-law search engine over the CourtListener corpus — and every round
of output is saved here, unedited, so you can see what worked and what did
not.

## The problem: dirty data

The corpus started as 9.7 million court opinions scraped as HTML. Scraped
legal data is dirty in specific, structural ways: markup embedded in the
text, OCR artifacts, missing caption fields, and documents that are not
really opinions at all. Before an LLM can decorate metadata, the input has
to be worth reading.

**Cleaning round 1 — HTML.** We built
[`grpc-lol-html`](https://git.rokkon.com/ai-pipestream/grpc-services), a gRPC
service wrapping Cloudflare's `lol-html` streaming HTML rewriter, and ran
the corpus through it. That produced the clean plain text we chunked and
indexed (9,740,254 opinions → 86.6 million chunks).

**Cleaning round 2 — PDFs.** With clean text in hand we could finally see
the next defect class: roughly 1,500 documents are garbage because their
source was a PDF, not HTML — extraction artifacts, not text. Those are
being re-downloaded as PDFs and cleaned properly. Dirty data is not one
problem; it is a queue of problems you discover one layer at a time.

## From clean text to decorated metadata

A search engine over opinions needs more than the text: court, caption,
docket number, year, posture, panel, topics, leading authorities, headnotes.
That metadata exists inside the text, but extracting it at corpus scale
means delegating to a model — and a model's output is only useful if it can
be checked.

That is the piece protomolt provides, and the reason this example lives in
this repository:

- The metadata shape is a **protobuf message** (`proto/court/v1/opinion_metadata.proto`).
  Field directives, safeguard text, and validation rules are declared on the
  schema itself via `llm.v1` and `validate.v1` annotations — the schema is
  both the prompt and the contract.
- `render-prompt` compiles that schema into the instruction packet the model
  sees. `validate-message` checks the model's output against the same
  schema. The validator is deterministic: it does not grade on a curve.
- Three **personas** (`personas/`) — an appellate researcher, a citator, and
  a headnote editor — fill the same form from different professional
  perspectives, so we can measure whether the contract holds regardless of
  who is answering.

## Why grounded truth needs appeals, not snapshots

The deeper problem surfaced once decoration worked: **metadata cannot be
kept up to date.** Each decorated document is committed atomically — a row
written at a moment in time, by a particular model version, against a
particular index generation. Knowledge drifts; models improve; the corpus
itself gets re-cleaned (see the PDFs above). Re-deriving all metadata on
every change is too compute-expensive to ever finish.

So the system treats model output not as fact but as a **claim**: generated
with citations into the source text and the index generation it was
produced against, stored so it can be **challenged, corrected, and updated**
later. An appeal re-runs decoration for a document against newer knowledge;
the presiding metadata is simply the latest unchallenged claim. Grounded
truth is a process, not a snapshot.

## The QA rounds

Each round is 15–30 uniformly random opinions × 3 personas × a local
Qwen3-14B-int4 model, first-shot output validated against the form. The
full harness (runner, analyzer, raw model responses) lives in
`sea-of-slop/qa/court-decoration`; the outputs that matter are saved here.

- **round-1/** — the contract holds structurally (45/45 schema-valid), but
  the failure taxonomy appears: hallucinated docket numbers, the model
  copying the prompt's own example, prose written into typed fields.
- **round-2/** — prompt fixes plus the `no-prose-values` CEL rule. Citations
  100% verified. The model can now declare `INCOMPLETE` with a reason — and
  the judge punished every declaration, which exposed the need for an
  escape channel.
- **round-3/** — the `skip_when` validator feature (built in this repo for
  exactly this) lets declarations validate clean: 8/8 clean, residual
  failures are model habits, not contract gaps. The form was frozen at v1.3
  as the basis for the job-manager design.
- **round-4/** — a one-sentence persona safeguard against prose-in-field
  values works (12 → 2) but suppresses the declaration channel (8 → 0): a
  controlled experiment showing how prompt text and escape channels
  interact. The contract stayed frozen; only persona wording moved.

Read the `FINDINGS-*.md` in each round directory first; `results.jsonl` is
the raw per-run record (doc, persona, parsed fields, violations, latency).
