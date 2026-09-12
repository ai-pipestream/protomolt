# Asset formats and characterization

The archive stores opaque bytes on purpose. This design adds the layer that
says what those bytes *are* — typed **formats** with strict, contract-level
validation; a **classification state machine** that makes "we don't know
yet" and "the evidence disagrees" first-class stored facts instead of
silent defaults; **characterization**, the process that identifies formats
and content from the bytes themselves; and **bridging**, the derived
renditions that carry an asset from the format it arrived in to the shapes
the rest of the platform consumes.

The vocabulary is the industry's: *format* and *format identification*
(digital preservation), *characterization* (the same world's word for
extracting a file's technical properties), *classification* (data
catalogs), *media type* (IANA), *fixity* (already present as the archive's
SHA-256 manifests). No invented terms.

Everything here is protobuf-first: the format facts, classifications, and
content profiles below are `v1` messages, and internal code passes the
generated objects — there is no parallel Java model.

## Where it sits

A new leaf family, `asset/`:

- **`asset/proto`** (`ai.protomolt.proto.asset.v1`) — the contract: format
  facts, classification, content profiles. A leaf dependency the way
  `repo/proto` is, consumed by the archive (which stores classifications),
  the parse family (which produces them), and the search mapping subjects
  (which facet on them).
- **`asset/characterize`** — the engine: pure-JDK format identification
  (magic bytes, extension grammar, per-format probes) behind one seam, so
  the parse coordinator's existing content sniffing and the archive's doors
  consult the same identifier and can never disagree about what a `tar`
  is.
- **`asset/bridge`** — the transformations: the routing rule that says
  which bridges a characterized format applies, the well-known derived
  rendition names and the shape each output pins, and the pure-JDK bridges
  a host can run without reaching any other service.

The archive's spine already carries the metamodel's structural concepts,
so none of them need new machinery: a *collection* is an archive; a *file
group* is an entry with sub-keyed renditions; a *directory* is a `path`
metadata convention; a *file* is a rendition object; an object-store
*origin* is entry metadata. What the spine lacks — and this design adds —
is the typed answer to "what kind of asset is this entry?"

## Formats as messages

Each format the platform recognizes is one proto message whose fields
carry that format's invariants as `validate.v1` rules — the contract *is*
the definition, the way the platform's other contracts work. The initial
registry, grouped by kind:

| Kind | Formats (v1) |
| --- | --- |
| Container | `TarArchive`, `ZipArchive`, `GzipFile` |
| Dataset | `ParquetDataset`, `DelimitedTable` (CSV/TSV), `NdjsonDataset`, `AvroDataset` |
| Semi-structured | `JsonDocument`, `XmlDocument`, `YamlDocument` |
| Document | `PdfDocument`, `WordDocument`, `SpreadsheetDocument`, `PresentationDocument`, `MarkdownDocument`, `HtmlDocument`, `PlainText` |
| Media | `RasterImage` |

Audio and video wait for a consumer: a format joins the registry when
something in the tree validates or bridges it, not before. The
object-store origin is deliberately not a format — it is where an asset
came from, carried on the classification as `ObjectStoreOrigin` (bucket
and key required, region, storage class, and the provider's attributes),
strict by the no-assumed-defaults rule.

Each message declares what must be true for the claim to be valid, and the
rules are strict by design. Illustrative, not exhaustive:

- `TarArchive.filename` matches the tar name grammar and nothing else:
  `.tar`, `.tar.gz`, `.tgz` — a `.zip` name can never validate as a tar
  claim. Filename presence splits by role: a producer's DECLARATION must
  carry the filename (the claim is about a named file — a door rule,
  refused by name), while characterization's IDENTIFIED fact may omit it,
  because bytes can prove a format without endorsing a name that
  contradicts it.
- `DelimitedTable` requires a declared delimiter and header presence — a
  CSV whose parsing rules are unstated is not a classified CSV.
- `ObjectStoreOrigin` requires its origin attributes (bucket, key) —
  an asset claiming an object-store origin without them is refused naming
  the missing field, per the platform's no-assumed-defaults rule.
- `RasterImage` requires media type consistency (`image/*`).

The union rides one message:

```proto
message FormatFact {
  oneof format {
    TarArchive tar = 1;
    ZipArchive zip = 2;
    ParquetDataset parquet = 3;
    DelimitedTable delimited = 4;
    NdjsonDataset ndjson = 5;
    JsonDocument json = 6;
    PdfDocument pdf = 7;
    // ... the registry above
  }
}
```

A closed `oneof` rather than an open string is deliberate: format claims
gate behavior (validation, bridging), and behavior-gating vocabulary must
be exhaustive at compile time. Growing the registry is an additive proto
change with a new message and its rules — the same discipline as every
other contract here. The rendition vocabulary itself stays open; it is
only the *typed claims about* renditions that come from a closed set.

## Classification is a state, not an option

An optional type claim reproduces the proto3 absent-versus-default trap at
the model level: "no claim" and "nobody looked" become indistinguishable,
and strictness turns into a policy argument at every door. Instead, every
entry carries a classification whose **state** is a stored, queryable
fact:

```proto
enum ClassificationState {
  CLASSIFICATION_STATE_UNSPECIFIED = 0;
  // Stored, no format known. A first-class work item — listable,
  // countable, filterable — never a silent default.
  CLASSIFICATION_STATE_UNCLASSIFIED = 1;
  // The producer declared the format and the declaration validated
  // against the format's rules at the door. A stored DECLARED
  // classification is always rule-consistent, because an invalid
  // declaration never gets in.
  CLASSIFICATION_STATE_DECLARED = 2;
  // Characterization identified the format from the bytes, with the
  // evidence recorded. Nothing was declared; nothing was invented.
  CLASSIFICATION_STATE_IDENTIFIED = 3;
  // Declared AND independently identified, and the two agree — the
  // strongest state, and the precondition trust-sensitive consumers
  // can demand.
  CLASSIFICATION_STATE_VERIFIED = 4;
  // Declared and identified, and they disagree. Stored loudly, never
  // resolved silently: the conflict is the fact. Bridging refuses
  // conflicted entries; the console shows them as work.
  CLASSIFICATION_STATE_CONFLICTED = 5;
}

message Classification {
  ClassificationState state = 1;
  // The format of record: the declaration when DECLARED/VERIFIED, the
  // identification when IDENTIFIED. Both are carried when they differ.
  FormatFact declared = 2;
  FormatFact identified = 3;
  // What the identifier saw: magic bytes matched, probe outcomes,
  // extension grammar hits. Evidence, not narrative.
  repeated CharacterizationEvidence evidence = 4;
  // Who classified, stamped at write time, never invented.
  // (repo.archive.v1.WriteAttribution)
  WriteAttribution classified_by = 5;
}
```

This resolves record-versus-refuse without a policy knob:

- A declaration that **fails its format's rules** refuses at the door —
  the grammar is the contract, and a `.zip` claimed as tar never lands.
- A declaration that **passes its rules but contradicts the bytes**
  lands as `CONFLICTED` — recorded, flagged, and excluded from bridging.
  The mess is representable and named, which is what an archive owes it.
  Contradiction is judged through a compatibility relation, not naive
  inequality: identification often concludes a *generalization* of the
  truth (delimited tables read as plain text, OOXML documents read as
  ZIP, a compressed tar reads as gzip), and a claim standing inside its
  generalization is not in conflict — the claim stands as `DECLARED`
  with the evidence kept. Only a conclusion that rules the claim out
  conflicts.
- **No declaration** lands as `UNCLASSIFIED` until characterization runs,
  and as `IDENTIFIED` after. Nothing is ever assumed.

The archive stores the classification on the entry (sourced from its
primary rendition) and the stats gain per-state counts, so "how much of
this archive is unclassified or conflicted" is one exact ledger read.

## Content classes

Format says what the bytes are physically; **content class** says what
the content is *about* structurally — the dimension bridging actually
routes on. A rendition (typically a derived one) carries a content
profile:

```proto
enum ContentClass {
  CONTENT_CLASS_UNSPECIFIED = 0;
  // Prose written to inform: articles, reports, documentation.
  CONTENT_CLASS_INFORMATIONAL_TEXT = 1;
  // Turn-taking text: chat exports, transcripts, threads.
  CONTENT_CLASS_CONVERSATIONAL_TEXT = 2;
  // Rows-and-measures data that loads into an analysis tool.
  CONTENT_CLASS_TABULAR_DATA = 3;
  // Text recovered from images or scans. Presumed lossy; carries a
  // measured quality score, never an assumed one.
  CONTENT_CLASS_OCR_TEXT = 4;
  // Source code and configuration.
  CONTENT_CLASS_CODE = 5;
}

message ContentProfile {
  ContentClass content_class = 1;
  // Class-specific measured properties: OCR quality dimensions,
  // conversational turn counts, tabular row/column counts.
  // Scored through the platform's quality-scoring seam
  // (CEL dimensions, weighted scores) — measured, not asserted.
  QualityScore quality = 2;
}
```

OCR text is the motivating case: it *suggests* mess and missing fields,
so its profile must carry a measured quality score (character confidence,
dictionary hit rate, layout coherence — dimensions defined in the
quality-scoring family's existing CEL vocabulary). A consumer can then
gate on quality the way it gates on classification state.

## Bridging: derived renditions under standard names

A bridge is a characterization-gated transformation that adds derived
renditions to the same asset — provenance-stamped, never replacing the
original, refused when the asset is `CONFLICTED` or `UNCLASSIFIED`. Most
format pairs deliberately do not bridge; a bridge exists only where a real
tool in the platform produces it. The v1 bridge set:

| From (format) | Derived rendition | Content |
| --- | --- | --- |
| `TarArchive` / `ZipArchive` | `members` | the container's member listing (paths, sizes, hashes) as a typed dataset |
| `ParquetDataset` / `AvroDataset` | `schema` | the dataset's schema and row/column profile |
| `DelimitedTable` / `NdjsonDataset` | `schema` + `dataset` | inferred schema; the normalized tabular form (`CONTENT_CLASS_TABULAR_DATA`) |
| `SpreadsheetDocument` | `dataset` | sheet data normalized for analysis tools |
| `PdfDocument` / `WordDocument` / `HtmlDocument` | `text` | extracted prose (`INFORMATIONAL_TEXT`) |
| `RasterImage` | `ocr-text` | recovered text with its measured `OCR_TEXT` quality profile |
| chat/transcript formats | `conversation` | turn-segmented text (`CONVERSATIONAL_TEXT`) |

The derived rendition names (`members`, `schema`, `dataset`, `text`,
`ocr-text`, `conversation`) are well-known names in the archive's open
rendition vocabulary — conventions, not a closed enum — and each derived
rendition's descriptor pins its shape via `schema_subject` (taken from the
generated message's own descriptor, so the pin cannot drift), so a
bridge's output is itself schema-validated data, not loose bytes.

The bridge KINDS, by contrast, are a closed `BridgeKind` enum, for the
same reason format claims are: bridge selection gates behavior. A scanned
PDF is the one row above that is not a routing decision — "scanned" is a
finding, not a property of the format, so a PDF routes to `text` and
escalates to `ocr-text` only once the text bridge reports it found no
prose.

### Applicable, executable, and what a bridge reports

Two questions are separated on purpose. **Applicability** is the routing
rule above: a pure function of the format of record, which the state
machine supplies (the declaration under `DECLARED`/`VERIFIED`, the
identification under `IDENTIFIED`; the other two states name no single
format and are refused). **Executability** is a property of the running
host, and of the format: the container and schema bridges are pure JDK,
but the same schema bridge that reads a CSV, an NDJSON file, and an Avro
container does not read a Parquet footer, and text extraction runs
through a parser service a given host may not reach.

So every bridge a run considers reports an outcome, and none is silently
skipped:

- `PRODUCED` — a derived rendition landed.
- `UNCHANGED` — the bridge reproduced exactly the bytes already stored.
  Derived renditions are content-addressed like every other rendition, so
  re-running a bridge moves no root checksum and lands no version:
  idempotence falls out of the archive's addressing rather than needing a
  bridge-ran-already flag.
- `DEFERRED` — applicable here, executed elsewhere, with the reason named
  (a Parquet reader, the Parquet emitter, a parser service). The caller is
  told what the work needs, so it can route it instead of assuming it
  done.
- `FAILED` — the bridge ran and could not finish, with the reason
  verbatim. Nothing lands.

A run lands every rendition it produced in ONE new version, so bridging an
asset costs one version however many bridges applied.

### The derived-primary rule

An entry classifies from its primary rendition, and bridging adds
renditions to that entry — so a derived rendition must never become the
primary, or bridging would silently re-point an asset's classification at
its own output. The rule: the primary is `original` when the entry has
one, otherwise the first rendition that is **not** a well-known derived
name. The list of derived names comes from `BridgeKind` itself, so it
cannot fall out of step with what the bridges actually write.

### Declared schemas and inferred ones

The `schema` rendition carries both kinds of answer and marks which it
is. Avro **declares** its schema in the container header, so the bridge
reads it, sets `declared_by_format`, and infers nothing. Delimited tables
and NDJSON declare nothing, so the bridge **infers** from a bounded
prefix of the rows and reports `rows_inspected` alongside a `row_count`
of -1 when it stopped early. A consumer can then tell a schema the format
stated from one a bridge concluded, and weigh them differently.

Inference widens only — integers among decimals read as double, mixed
values read as string — so the answer does not depend on row order. A
delimited table's delimiter and header presence come from the producer's
declaration, never from a guess, which is the same fact that keeps
characterization from identifying one.

### Execution beyond this host

Bridges that need a real extraction tool ride existing machinery end to
end: the parse coordinator's plugin contract runs the extraction, a
durable `bridge-entry` workflow (jobs family) makes a bridge a resumable
run with evidence, the quality family scores what needs scoring, and the
archive's `UploadRendition` lands the outputs with attribution. Bridging
adds no new runtime — only the routing rule *characterized format →
applicable bridges*, and the outcome vocabulary that says who ran what.

## Surface changes

**Archive** (`repo/proto` archive/v1, additive):

- `EntryInfo.classification` — the entry's `Classification`.
- `PutEntryRequest` / `UploadRenditionHeader` gain an optional
  `FormatFact declared` — validated at the door against the format's own
  rules; refusal names the failing rule.
- `RenditionManifestEntry.content_profile` — the rendition's
  `ContentProfile`, when one was measured.
- `ClassifyEntry` RPC — declare (or re-declare) after the fact, and
  request characterization; returns the resulting `Classification`.
- `ListEntries` gains a classification-state filter;
  `ArchiveStats` gains per-state counts.
- `BridgeEntry` RPC — run the bridges the entry's classification makes
  applicable (or a named subset, refused by name when one does not
  apply); returns the landed version and one `BridgeOutcome` per bridge
  considered.

**Characterization** (`asset/characterize`): one seam —
`Characterizer.identify(bytes prefix, filename) → (FormatFact, evidence)`
— implemented pure-JDK: the shared media-type sniffer (the parse
coordinator routes on the same table, so routing and characterization
can never disagree), format grammars compiled from the very expressions
the contract annotates (a descriptor-parity test refuses drift), and
cheap probes. A format whose claim needs producer-stated parameters — a
delimited table's delimiter and header presence — is never identified,
only declared: concluding nothing is the honest verdict, and the
evidence still records what was seen.

**Search and metrics**: `AssetCatalogRow` is one flat row per stored
asset, carrying classification state, format kind, content class, size,
rendition counts, and measured quality. Its fields declare their own
index hints and metric members, so the catalog view (how many Parquet
datasets, how much is unclassified, what the OCR quality distribution
looks like) is an ordinary group-by query over an ordinary subject — no
endpoint in the tree knows those questions, and no parallel metadata
system holds the answers.

The row is a projection, never a second store: every value it carries is
already stored, and `AssetCatalogRows.of(EntryInfo, VersionManifest)`
computes nothing, reads no object, and calls no service. `ListEntries`
grew an `include_manifests` flag so one paged listing carries everything
the projection needs.

Two conventions keep a facet honest. Enums project as their bare names
(`VERIFIED`, `OCR_TEXT`) rather than numbers, so a facet reads as itself.
And an absent value is a real answer that must not vanish: an asset whose
classification names no single format has a blank `format_kind`, which
would be skipped by an index that drops unset fields, so the fields
carrying "no answer" declare a `null_value` and refuse to be skipped. An
unmeasured quality is -1 rather than 0 for the same reason, with the mean
filtered to what was actually scored, because a distribution over "nobody
looked" and one over "scored zero" are different questions.

## Rules

1. **The format's rules are the claim's contract.** A declaration
   validates against its message's `validate.v1` rules at the door or it
   does not land. No lenient mode.
2. **States are stored facts, never defaults.** Unclassified and
   conflicted are queryable conditions with counts, not absences.
3. **Identification carries evidence.** A stored `IDENTIFIED` or
   `CONFLICTED` classification lists what the identifier saw. The
   platform never invents an attribution or a verdict.
4. **Bridges derive; they never convert in place.** The original
   rendition is immutable; every bridge output is a new rendition with
   provenance and, where applicable, a measured quality profile.
5. **One detector.** Every consumer of "what is this file" — archive
   doors, parse routing, bridge gating — calls the same characterization
   seam.
6. **Proto-first.** The generated `v1` messages are the internal model.

## Sequencing

1. `asset/proto` — the format registry, classification, content
   profiles, with the full validation rules.
2. `asset/characterize` — the identifier seam + the magic-byte/grammar
   engine; migrate parse sniffing onto it.
3. Archive integration — classification storage, door validation, the
   `ClassifyEntry` RPC, state stats.
4. Bridges, one at a time, each landing with its tests: the routing
   rule, the outcome vocabulary, and container `members` (cheapest,
   purely structural, pure JDK); then `schema`/`dataset`; then
   `text`/`ocr-text` with quality scoring, which is where the parser
   plugin contract and the durable `bridge-entry` workflow enter.
5. The catalog subject and its facets over the new fields.

## Decisions of record

- **Registry breadth**: v1 ships the formats above and no more; audio
  and video join when something in the tree consumes them.
- **Entry vs rendition classification**: the entry classifies from its
  **primary rendition** — `original` when the entry has one, its first
  rendition otherwise — and renditions profile individually. A save
  characterizes the primary only when its bytes are in hand (the save
  carries them, or the streaming door captured the prefix in flight);
  `ClassifyEntry` re-reads the stored bytes on demand. A declaration
  once made is never silently withdrawn: later saves without a fresh
  declaration re-resolve against the standing claim.
- **Conflict resolution ergonomics**: the console surface for
  `CONFLICTED` entries (re-declare, accept the identification, leave
  flagged) is a follow-on, alongside the bridge trains.
- **Which readers the asset family carries**: the family holds itself to
  the JDK, so it reads tar, zip, delimited text, NDJSON, and the Avro
  container header, and it does not read a Parquet footer — that needs
  the Parquet library, which belongs with the emitters rather than with a
  leaf contract. Parquet's `schema` and every `dataset` rendition are
  therefore `DEFERRED` with the reason named, not silently missing.
