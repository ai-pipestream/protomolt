# Well-known types

Status: design of record for the validated typed commons. Changes land
in this chapter first.

## The principle: wire-plain, server-strict

A well-known type stays a plain proto scalar on the wire. Any protobuf
client in any language produces and consumes it untouched; a protomolt
gate enforces the semantics and hands a foreign producer a structured
`INVALID_ARGUMENT` naming the violation — never silent corruption,
never a client-side library requirement. On protomolt-to-protomolt
paths the same declaration also fires client-side (the serde's
validate-on-write), so bad data dies at the edge. Foreign systems
degrade gracefully; house systems get the full contract. One
declaration enforces at every gate the platform already mounts: the
gRPC interceptor, the serde's write and read paths, the registry's
config gate, and the config consumer's apply.

## Tier 1: named formats, not wrappers

For values with a format but no structure — GUIDs, dates, media types,
digests — the field stays a string and the schema names the format:
`string: {date: true}` instead of a regex. Enforcement is a first-class
parser per format in `core/formats`, **never a regex**: the calendar
refuses `2026-02-30` where a pattern would accept the shape, and the
violation says what the value must be, not what expression it failed.
`core/formats` stays zero-dependency; where Apache OpenNLP's regex-free
string utilities cover a format, the approach is ported (Apache to
Apache, NOTICE attribution when code moves) rather than the jar added —
OpenNLP itself arrives only in the screening layer below, which
depends on it anyway.

The format set: `date`, `date_time`, `mime_type`, `language_tag`,
`currency_code`, `phone_number`, `sha256_hex`, `sha1_hex`, `hex`
(composes with `len` for sized ids like W3C trace ids), `base64`,
`email`, `uuid`, `hostname`, `uri`, `ip`, `ipv4`, `ipv6`, `slug`,
`region_code`, `protobuf_fqn`, `path_safe_name`, `gtin`, `decimal`,
`endpoint_address`, `identifier`, and `link_relation`. Mechanics:

- "Absent or valid" is a declaration, not a regex alternation:
  `ignore_if_zero: true` on the field rules skips every rule at the
  zero value.
- The JDK-backed vocabulary formats bundle **no data**: the JDK's own strict BCP 47
  parser answers `language_tag`, its ISO 4217 table answers
  `currency_code`, and its ISO 3166 table answers `region_code`
  (Unicode CLDR underneath), so nothing needs attribution and nothing
  needs maintaining.

Name identities split into **two namespaces**. Author-chosen *local
names* (workflow, step, run, and delegation identities) are `slug`:
lowercase `a-z0-9` with interior single `.`, `_` or `-` separators,
starting and ending alphanumeric, two separators never touching;
length composes with the len rules. Machine-derived *reference names*
— dependency aliases carrying service fully-qualified names by the
workflow compiler's own convention, sanitized endpoint placeholders,
the hyphenated structured-step sentinel — are `path_safe_name`: an
ASCII letter or digit, then letters, digits, `.`, `_` or `-`; the
leading alphanumeric keeps a value safe as a single path segment.
The hand validators delegate to the `core/formats` linear-scan
parsers throughout (names to slug, references to path-safe,
fingerprints, hosts, media types, UUIDs and SHA-1 to their parsers;
the secret-reference and branch-id composites are scans): no regex on
the Java side.

The workflow contract follows the `ClusterValidation` convention: the
name families, fingerprints, and media types are `validate.v1`
annotations on `grpc_workflow.proto`, and `WorkflowValidation` runs
`ProtoValidator` first (it recurses into nested messages), keeping
hand code only for what annotations cannot express — duplicates,
declared-dependency references, fingerprint agreement, step-shape
exclusivity. Blank-allowed fields use `ignore_if_zero`, because
string formats reject the empty value.

Domain formats: `gtin` verifies the GS1 mod-10 check digit, so a
barcode with a wrong check digit refuses; `decimal` is the schema.org
money-in-a-string shape, unsigned and exponent-free; both are linear
scans. `endpoint_address` is a *precedence* grammar, not an
alternation: a value shaped like `host:port` must carry a dialable
port and never falls back to the URI reading (`localhost:99999`
parses as a URI with scheme `localhost` otherwise); anything else
must be an absolute URI with a non-empty rest. `identifier` is a bare
`[A-Za-z_][A-Za-z0-9_]*` protobuf segment name. `link_relation` is the
relation a document link declares: a bundled Dublin Core relation term or IANA
registered name, matched exactly, or else an absolute URI (an RFC 8288 extension
relation); its tables are the one vocabulary format that bundles data, attributed
in `NOTICE`. There is no generic
`any_of` combinator in the dialect: an alternation with precedence is
a domain grammar, and domain grammars become named formats.

What stays a pattern, and why: the `scheme:ref` key and credential
references (single grammars that differ per site; a shared format
would loosen one of them), single bare-identifier and
at-least-one-dot name sites (the `protobuf_fqn` parser accepts
dotless), and SKUs (no universal SKU grammar exists, so the demo
shop's shape is honestly local). No `semver` format: nothing in the
repo promises semver.

## Tier 2: structural types with platform behavior

Values with structure and invariants become messages in
`protomolt/types/v1`, each carrying cross-field rules and shipping only
with at least one platform behavior attached — no shape zoo:

- **DateRange / LongRange / DoubleRange** — `{begin, end,
  include_head, include_tail}` with begin-not-after-end,
  at-least-one-bound, and flag-without-bound rules as message-level
  CEL. Bounds are `optional` (an unset bound is an open end) and an
  absent inclusivity flag means included — the gte/lte convention —
  while an explicit `false` excludes the bound. The audit found the
  need twice over: the indexing SPI declares five `*_RANGE` field
  types with no canonical bounds message, and the repo holds two
  *conflicting* span conventions (`PageSpan` closed, `IntSpan`
  half-open) — the inclusivity flags exist to end exactly that
  ambiguity. The platform behavior shipped with the types: the index
  SPI resolves them by name (a field of a canonical range type needs
  no hint at all), and all three engine mappers honor open ends,
  per-end inclusivity, and `DateRange`'s day-grain semantics (an
  excluded bound day is dropped whole). The metric surface's
  `MetricRange` has converged: `MetricFilter.range` IS the canonical
  `DateRange` (a deliberate pre-GA wire break under a temporary buf
  waiver), the compiler honors open ends and the exclusivity flags,
  and the compiled bounds stay inclusive epoch millis so the
  executors never see the flags.
- **TreePath** — a taxonomy path as repeated segments, root
  first, wire-plain; the "/" delimiter exists only in rendered forms,
  so a segment must not contain it (that rule is what keeps `["a/b"]`
  and `["a", "b"]` distinguishable). At least one segment; segments
  may repeat. The platform behavior shipped with the type: the index
  SPI resolves it by name (a field of the canonical type infers
  TREE_PATH, facetable, with no hint at all) and every engine mapper
  emits the ancestor chain ("a", "a/b", "a/b/c") as keyword terms —
  hierarchical drill-down facets count at any depth and a path-prefix
  filter is an exact term match. The metric surface speaks the type
  too: a `TreePath` field is a `TREE_PATH` dimension (the
  walk keeps it a leaf like Timestamp) that groups by the whole leaf
  path — never per ancestor, which would count a row once per depth
  and break additivity — and `MetricFilter.prefix` (a wire-plain
  `TreePath`) is the descendant-or-self filter: Lucene answers it as
  one exact term match against the indexed ancestor chain; the lake
  needs no chain column at all, because the TreePath struct the lake
  already writes *is* the column and DuckDB derives the rendered path
  (`array_to_string(col['segments'], '/')`) for both grouping and
  `starts_with` prefixing, so the two engines label buckets
  identically. Equality on a TREE_PATH dimension is refused toward
  prefix (a term match would silently mean descendant-or-self), and
  rollups hold the rendered leaf path as a plain keyword: equality
  works over a rollup subject, prefix honestly refuses there and runs
  on the base subject.
- **Adopt-and-validate `google.type`** — Money, Date,
  Interval, LatLng are not reinvented; the built-in
  `GoogleTypeRuleSource` (on the default validator chain) carries
  their documented invariants keyed by type full name, so any
  descriptor named `google.type.*` gets them with no dependency on
  the generated classes and no annotation on the schema. Money:
  bounded nanos, sign agreement, ISO 4217 currency required as soon
  as an amount is set (a zero Money stays legal). Interval: ordered
  (CEL compares Timestamps natively). Date: documented field bounds
  plus the day-without-month contradiction; whether a day exists in
  its month is calendar work the structural bounds leave alone.
  LatLng: coordinate bounds. The audit found the reinventions to
  converge: schema.org `Offer.price` (a decimal string, because
  doubles lose money) beside `MonetaryAmount.value` (a double, losing
  money) is the case study. PostalAddress and PhoneNumber are
  data-free: PhoneNumber requires exactly one kind, its
  e164 form goes through the Tier-1 E.164 parser, a short code is
  complete or refused, and the extension keeps its documented
  40-character bound; PostalAddress requires its `region_code`
  (checked against the JDK's ISO 3166 table — zero bundling), takes
  BCP 47 language codes, and pins `revision` to 0. Per-country
  postal-code grammar is operator-loaded data, never bundled: one
  config document (`PostalCodePack`, subject `postal-codes`) whose
  regions carry UPU-convention masks — `N` a digit, `A` an uppercase
  letter, anything else literal. A mask is a fixed template matched
  by one linear scan, so operator data can never smuggle in a regular
  expression. A validator built over the mounted catalog enforces the
  masks on `google.type.PostalAddress`: a mounted region's non-empty
  `postal_code` must match one mask (`postal.code_grammar`, pack
  version as evidence); an unmounted region is unchecked, because no
  schema declaration binds a postal code to the pack — it is a
  per-region opt-in, not a fail-closed gate. On the document platform,
  `DOCUMENT_PLATFORM_POSTAL_CODES=true` follows the pack off the
  config lane into the search service's document gate; until the pack
  applies, every region is unchecked, the same opt-in stance.

## Taxonomies and ontologies

First-class vocabulary support without bundling anyone's data, in
order of preference:

1. **JDK-shipped data** (languages, countries, currencies, timezones):
   zero bundling, zero attribution, zero list maintenance.
2. **Structural validation without data** (media types per RFC 6838,
   URIs per RFC 3986): the grammar is the standard.
3. **Operator-loaded taxonomy documents** for everything
   else: a `Taxonomy` is a config document on the config lane, and
   the validator checks a bound `TreePath` field's membership against
   the *mounted* taxonomy. The mechanism ships; the data is the
   operator's (or a separately-licensed pack).

The taxonomy mechanics. A `Taxonomy` (types/v1) is just
`repeated TreePath entries`: an entry names a node by its full path
from the root, and every ancestor along the way is a node too, so
listing the leaves is enough. The document carries no name and no
version — its config subject (`taxonomy:<name>`) is the identity and
the config source's version (a git commit, a topic offset) is the
version, the config lane's own version-as-evidence stance; a copy of
either inside the document could only agree or lie. The `Taxonomy`
*type* is registered and compat-gated like any schema; the documents
are per-mesh operator data behind the registry's config gate (which
re-gates on read — config documents deliberately do not federate).

A field binds to a taxonomy in the schema: `taxonomy: "products"` on
the field rules of a `TreePath` field — singular or repeated;
anywhere else fails rule compilation. The binding is schema truth
(same schema, same requirement, forever) while the taxonomy's content
is mount configuration: `TaxonomyMounts` follows `taxonomy:<name>`
subjects through `DistributedConfig` — verify-then-swap, so a
document failing `Taxonomy`'s own declared rules never mounts — and a
gate constructs its validator over that catalog. Enforcement is
fail-closed with the version as evidence: a declared taxonomy that is
not mounted refuses (`taxonomy.unmounted`, never a silent pass), and
a path that is not a node of the mounted taxonomy refuses naming the
taxonomy and the mounted version (`taxonomy.member`). An empty path
is left to the type's own structural rules. Given a mounted version
the verdict is deterministic, which is what keeps this on the
validation side of the screening line below.

The first platform consumer is the search service's **document gate**,
opt-in by environment (`DOCUMENT_PLATFORM_TAXONOMIES`, a
comma-separated list of names; it requires the config lane): with
taxonomies named, the service validates each fetched document over the
live mounts before anything indexes — membership enforced as of index
time, refusals naming the violations, fail-closed while a declared
taxonomy has not yet mounted. Unset, no declared-rules validation
runs at index time. Packed `google.protobuf.Any` payloads
stay with the seams that own a descriptor registry (the indexing
facade and the engines' payload gate); this service's registry-free gate
deliberately covers the document's own typed fields.

Licensing rule, in force because this project intends to be
Apache-governed: bundle in-tree only JDK-derived data or ASF
category-A sources (Unicode/CLDR, IANA and tzdata, Dublin Core CC-BY
4.0 with NOTICE attribution, SPDX). **schema.org is CC-BY-SA and is
never bundled** — the SEO module models its shapes without copying its
text, and that line holds. Nothing NC or ND, ever. Prefer validated
strings over proto enums for vocabularies: an enum freezes the list
into every federated schema, breaks wire-plain, and turns a new
language into a schema migration.

### Concept schemes (SKOS)

Status: specification for #537, ahead of any proto change. The repository side
(link fields, the catalog edge and tag tables, dangling links) is in
`docs/design/parts-based-repository-requirements.md` §17.

A `Taxonomy` names a node by its path, which is right for a drill-down facet and
wrong for a subject vocabulary: a renamed node is a different node, a node has one
parent, there are no synonyms, and two taxonomies cannot say that their nodes mean
the same thing. Concept schemes add those, modelled on W3C SKOS and expressed in
protobuf. `Taxonomy` and `TreePath` stay as they are for path facets; nothing
migrates.

**Concept scheme.** One scheme is one facet: subject, department, region,
sensitivity, product line. It is a config document on the config lane, subject
`concept-scheme:<scheme_id>` (`scheme_id` a `slug`). As with `Taxonomy`, the
subject is the identity and the config source's version is the version; neither is
copied inside the document.

```proto
// ai/protomolt/proto/types/v1/concepts.proto
message ConceptScheme {
  // The scheme's title, at most one per language.
  repeated LangLabel pref_label = 1;
  // At least one concept; ids unique.
  repeated Concept concepts = 2 [(ai.protomolt.proto.validate.v1.field).repeated.min_items = 1];
}

message Concept {
  // Stable opaque id, unique in the scheme, independent of every label.
  // A retired concept keeps its id; an id is not reused.
  string id = 1 [(ai.protomolt.proto.validate.v1.field) = {
    required: true
    string: {path_safe_name: true, max_len: 200}
  }];
  // Preferred label: at most one per language.
  repeated LangLabel pref_label = 2 [(ai.protomolt.proto.validate.v1.field).repeated.min_items = 1];
  // Alternate labels (synonyms, abbreviations).
  repeated LangLabel alt_label = 3;
  // Hidden labels: matched by search, not displayed (misspellings).
  repeated LangLabel hidden_label = 4;
  // Parents in this scheme. Several are allowed; the graph is acyclic
  // (a ProtoMolt profile rule, below).
  repeated string broader = 5 [(ai.protomolt.proto.validate.v1.field).repeated.unique = true];
  // Associated concepts in this scheme; symmetric, not hierarchical.
  repeated string related = 6 [(ai.protomolt.proto.validate.v1.field).repeated.unique = true];
  // Cross-scheme mappings. Each target names another scheme.
  repeated ConceptRef exact_match = 7;
  repeated ConceptRef close_match = 8;
  repeated ConceptRef broad_match = 9;
  repeated ConceptRef narrow_match = 10;
  repeated ConceptRef related_match = 11;
  // Notes, at most one definition per language.
  repeated LangLabel definition = 12;
  repeated LangLabel scope_note = 13;
  // Retired: still a member, still matched, not offered for new tags.
  bool deprecated = 14;
  // For a deprecated concept, its successors in this scheme.
  repeated string replaced_by = 15;
  // The concept's IRI, kept for SKOS round trips. Not an identity.
  optional string iri = 16 [(ai.protomolt.proto.validate.v1.field).string.uri = true];
  // The concept's skos:notation values, as written in the source, in order.
  repeated ConceptNotation notation = 17 [(ai.protomolt.proto.validate.v1.field).repeated.unique = true];
}

// One skos:notation: a literal and its datatype.
message ConceptNotation {
  // The literal, unchanged.
  string value = 1 [(ai.protomolt.proto.validate.v1.field) = {
    required: true
    string: {min_len: 1, max_len: 1000}
  }];
  // The literal's datatype IRI. Empty for a plain literal.
  string datatype_iri = 2 [(ai.protomolt.proto.validate.v1.field) = {
    ignore_if_zero: true
    string: {uri: true}
  }];
}

// Text in one language.
message LangLabel {
  string language = 1 [(ai.protomolt.proto.validate.v1.field) = {
    required: true
    string: {language_tag: true}
  }];
  string text = 2 [(ai.protomolt.proto.validate.v1.field) = {
    required: true
    string: {min_len: 1, max_len: 1000}
  }];
}

// A document tag, and a mapping target: scheme and concept id.
message ConceptRef {
  string scheme_id = 1 [(ai.protomolt.proto.validate.v1.field) = {
    required: true
    string: {slug: true, max_len: 200}
  }];
  string concept_id = 2 [(ai.protomolt.proto.validate.v1.field) = {
    required: true
    string: {path_safe_name: true, max_len: 200}
  }];
}
```

Design rules:

- **One direction stored.** Only `broader` is stored; `narrower` is its inverse,
  computed by the mount and written on export. Only one side of a symmetric
  relation (`related`, `exact_match`, `close_match`, `related_match`) needs to be
  written; the mount closes it. `broad_match` and `narrow_match` are each other's
  inverse across schemes. Storing both sides would let them disagree.
- **Labels are not identity.** Renaming a concept edits `pref_label`; its id, its
  tags and its mappings are unchanged. Retiring one sets `deprecated` and usually
  `replaced_by`; a concept is not removed while documents may tag it.
- **Label integrity** (SKOS S13, S14): at most one `pref_label` per language, and
  within one language a text is in at most one of `pref_label`, `alt_label`,
  `hidden_label`.
- **Mappings cross schemes.** A mapping whose `scheme_id` is the concept's own
  scheme is invalid; in-scheme relations use `broader` and `related`.

**Mount checks.** A scheme mounts through `DistributedConfig` verify-then-swap,
like `Taxonomy`, so a scheme failing any of these does not mount and the previous
version stays live:

1. Every `broader`, `related` and `replaced_by` id names a concept in the scheme.
2. The `broader` graph is acyclic. SKOS itself permits cycles in
   `skos:broaderTransitive`; rejecting them is a ProtoMolt profile rule, so that
   descendants are finite and drill-down counts are well defined. The check is a
   depth-first walk over the whole scheme, and a violation names one cycle as its
   concept ids in order.
3. `related` does not connect a concept to one of its ancestors or descendants
   (SKOS S27).
4. `replaced_by` is set only on deprecated concepts. The `replaced_by` graph is
   acyclic (a cycle is rejected and named, as for `broader`), and every chain ends
   at a concept that is not deprecated.

A cross-scheme mapping to a scheme that is not mounted, or to an id the mounted
scheme lacks, does not block the mount: two schemes evolve independently. It is
recorded as unresolved, skipped by expansion, and reported in the expansion
evidence.

**Tagging documents.** A field binds `ConceptRef` values to schemes in the schema:
`concept_schemes: ["subject", "region"]` on the field rules (new `FieldRules`
field 20), legal only on a singular or repeated `ConceptRef` field. The value's
`scheme_id` must be one of the bound schemes and its `concept_id` a concept of the
mounted scheme. Enforcement is fail-closed with the version as evidence, as for
taxonomies: an unmounted bound scheme rejects (`concept.unmounted`), an unknown
concept rejects (`concept.member`) naming the scheme and mounted version. A
deprecated concept stays a member, so a document whose CORE is carried forward
unchanged keeps validating after a retirement.

**Query expansion.** A search or catalog filter on a `ConceptRef` expands it
against the mounted schemes at query time; the index and the catalog store only
the tagged ids, so a scheme change needs no reindex.

```proto
message ConceptExpansion {
  // Add in-scheme descendants (narrower, transitive).
  bool narrower = 1;
  // Add close_match targets, one hop from the queried concept.
  bool close_match = 2;
}
```

Expansion reads one snapshot: at the start of a query it captures the mounted
version of every scheme it may touch and uses only those versions until it ends,
so a remount during a query does not mix two versions of one scheme. Every
traversal (the `exact_match` closure, `replaced_by` chains, descendants) keeps a
visited set and adds each concept once, so cycles in the symmetric relations, and
any cycle a cross-scheme mapping forms, end the walk instead of looping.

Given a queried concept C:

1. **Always:** C, its `exact_match` closure across mounted schemes (symmetric and
   transitive), and deprecated concepts whose `replaced_by` chain reaches C. That
   is the default and needs no flag.
2. **`narrower`:** add every in-scheme descendant of each concept from step 1.
   Polyhierarchy can reach one descendant by several paths; it is added once.
3. **`close_match`:** add the `close_match` targets of C. `close_match` is not
   transitive, so the hop is one, from C only.
4. **Not expanded:** `broader`, `related`, `broad_match`, `narrow_match`,
   `related_match`. They are navigation, offered to a reader, not applied to a
   query.

The expanded set is bounded (`max_expanded_concepts` on the mount, default 10,000);
a query over the bound is rejected with `INVALID_ARGUMENT` naming the concept and
the bound, not truncated. The response evidence lists each scheme's mounted
version and the unresolved mappings skipped. Given the mounted versions the
expansion is deterministic, which keeps it on the validation side of the
screening line.

**SKOS files at the edge.** SKOS Turtle and RDF/XML are import and export formats,
as JSON is for messages: a converter verb reads a SKOS file into a
`ConceptScheme` and writes one back out. The repository, the config lane and the
mount store only binary protobuf. Import rules:

- A concept's id is the UUIDv5 of its IRI under a fixed protomolt namespace,
  for every imported concept, and the IRI goes in `iri`. Re-importing the same
  file gives the same ids. The id is not taken from a notation, so an id never
  needs provenance to say where it came from.
- Every `skos:notation` is kept in `notation`, in source order, with its value
  unchanged and its datatype IRI (empty for a plain literal). Several notations
  and typed notations survive the round trip (SKOS reference §6.5). ProtoMolt
  does not require notations to be unique in a scheme; SKOS states that as a
  convention, not an integrity condition.
- `skos:narrower` statements are read as the inverse `broader`; a file whose
  `broader` and `narrower` statements disagree is rejected by name.
- Constructs without a field (`skos:Collection`, SKOS-XL labels, notes other than
  definition and scope note, mappings to non-SKOS resources) are rejected by name,
  not dropped. Supporting one is a schema change first.

Export writes `iri`, every `notation`, the labels, notes, `broader` with its
computed `narrower`, `related` and the mappings. A concept without an `iri` is
rejected by name on export; the converter does not mint IRIs.

The converter lives in its own edge module; `types/v1` and the mount depend on no
RDF library. Whichever parser the converter uses must meet the licensing rule
above (Apache Jena is ASF).

**Link relations.** The `link_relation` Tier 1 format (§17 of the requirements)
checks a relation name against two bundled vocabularies, both allowed by the
licensing rule: Dublin Core terms (CC-BY 4.0, NOTICE attribution) and the IANA
link relations registry. Anything else must parse as an absolute URI, the RFC 8288
form of an extension relation. Relation names are validated strings, not an enum,
for the reason given above.

## Screening is not validation

Deterministic checks — everything above — are schema truth: same
input, same verdict, forever, safe to federate and compat-gate.
Model-driven detection (Apache OpenNLP NER/PII) is probabilistic and
versioned by its model: the same input can change verdicts on a model
update with no schema change, so it must never masquerade as schema
validity. The design: a field declares *that* it is screened (riding
the `meta.v1` sensitivity classes), while the model, threshold, and
policy are mount configuration — distributable through the config
lane — and the policy is usually mask-or-tag rather than refuse
(the platform already owns `mask-message` and part-masked documents).
The model version is evidence in every mask or refusal.
