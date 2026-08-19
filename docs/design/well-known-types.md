# Well-known types

Status: design of record for the validated typed commons, decided
2026-08-18 with the project owner. Tier 1 is landing; later tiers change
this chapter first.

## The principle: wire-plain, server-strict

A well-known type stays a plain proto scalar on the wire. Any protobuf
client in any language produces and consumes it untouched; a protomolt
door enforces the semantics and hands a foreign producer a structured
`INVALID_ARGUMENT` naming the violation — never silent corruption,
never a client-side library requirement. On protomolt-to-protomolt
paths the same declaration also fires client-side (the serde's
validate-on-write), so bad data dies at the edge. Foreign systems
degrade gracefully; house systems get the full contract. One
declaration enforces at every gate the platform already mounts: the
gRPC interceptor, the serde's write and read paths, the registry's
config door, and the config consumer's apply.

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

The repo audit (2026-08-18, ~100 regex sites across 101 protos, 52%
dialect adoption) fixed the initial format set: `date`, `date_time`,
`mime_type`, `language_tag`, `currency_code`, `phone_number`,
`sha256_hex`, `sha1_hex`, `hex` (composes with `len` for sized ids like
W3C trace ids), and `base64`, joining the seven formats the dialect
already had (`email`, `uuid`, `hostname`, `uri`, `ip`, `ipv4`, `ipv6`).
Two audit findings shaped the mechanics:

- The `^$|...` alternation idiom (20+ sites) is now a declaration:
  `ignore_if_zero: true` on the field rules skips every rule at the
  zero value — "absent or valid" without a regex.
- Vocabulary formats bundle **no data**: the JDK's own strict BCP 47
  parser answers `language_tag` and the JDK's ISO 4217 table answers
  `currency_code` (Unicode CLDR underneath), so nothing needs
  attribution and nothing needs maintaining.

Landed later (2026-08-18, charset agreed with the project owner): a
`slug` format — lowercase `a-z0-9` with interior single `.`, `_` or
`-` separators, starting and ending alphanumeric, two separators never
touching; length composes with the len rules, exactly as `hex` does —
and a `region_code` format (ISO 3166-1 alpha-2 through the JDK's own
table, the zero-bundling route again). The audited sites converted
in their sweep (2026-08-18, 40 conversions): 26 slug-family sites to
`slug` + `max_len: 128` (a deliberate tightening — the old pattern
allowed uppercase and doubled separators, and the mesh contract test
pins that they now refuse), 7 digest sites to `sha256_hex`/`sha1_hex`,
5 UUID sites to `uuid` (case-identical), and 2 dotted-name sites to a
new `protobuf_fqn` dialect field over the existing parser. A follow-up
sweep (2026-08-19) converted the eight remaining `^$|` sites of the
same families to `ignore_if_zero` plus the named format. Patterns
that remain are the held alternations, single bare identifiers,
at-least-one-dot names (the `protobuf_fqn` parser accepts dotless),
and domain shapes like SKUs and GTINs — though GTIN is a conversion
candidate, not a permanent hold: a `gtin` parser would verify the
mod-10 check digit the regex cannot, and a `decimal` parser would
name the schema.org price-in-a-string shape. Still held: formats for
alternations (`host:port` or URI) until multi-format semantics are
designed; `semver` (nothing in the repo promises semver). Known
divergence to close: `WorkflowValidation`'s hand-rolled `NAME` regex
still admits uppercase where the annotations now declare `slug`; the
fix is delegating the Java validators to the `core/formats` parsers.

## Tier 2: structural types with platform behavior

Values with structure and invariants become messages in
`protomolt/types/v1`, each carrying cross-field rules and shipping only
with at least one platform behavior attached — no shape zoo:

- **DateRange / LongRange / DoubleRange** (landed) — `{begin, end,
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
- **TreePath** (landed) — a taxonomy path as repeated segments, root
  first, wire-plain; the "/" delimiter exists only in rendered forms,
  so a segment must not contain it (that rule is what keeps `["a/b"]`
  and `["a", "b"]` distinguishable). At least one segment; segments
  may repeat. The platform behavior shipped with the type: the index
  SPI resolves it by name (a field of the canonical type infers
  TREE_PATH, facetable, with no hint at all) and every engine mapper
  emits the ancestor chain ("a", "a/b", "a/b/c") as keyword terms —
  hierarchical drill-down facets count at any depth and a path-prefix
  filter is an exact term match. The metric surface speaks the type
  too (landed): a `TreePath` field is a `TREE_PATH` dimension (the
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
- **Adopt-and-validate `google.type`** (landed) — Money, Date,
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
  money) is the case study. PostalAddress and PhoneNumber landed
  data-free (2026-08-18): PhoneNumber requires exactly one kind, its
  e164 form goes through the Tier-1 E.164 parser, a short code is
  complete or refused, and the extension keeps its documented
  40-character bound; PostalAddress requires its `region_code`
  (checked against the JDK's ISO 3166 table — zero bundling), takes
  BCP 47 language codes, and pins `revision` to 0. Per-country
  postal-code grammar is deliberately data the platform does not
  bundle: it waits on an operator-loaded pack, the same stance the
  taxonomy section takes.

## Taxonomies and ontologies

First-class vocabulary support without bundling anyone's data, in
order of preference:

1. **JDK-shipped data** (languages, countries, currencies, timezones):
   zero bundling, zero attribution, zero list maintenance.
2. **Structural validation without data** (media types per RFC 6838,
   URIs per RFC 3986): the grammar is the standard.
3. **Operator-loaded taxonomy documents** (landed) for everything
   else: a `Taxonomy` is a config document on the config lane, and
   the validator checks a bound `TreePath` field's membership against
   the *mounted* taxonomy. The mechanism ships; the data is the
   operator's (or a separately-licensed pack).

The taxonomy mechanics, as landed. A `Taxonomy` (types/v1) is just
`repeated TreePath entries`: an entry names a node by its full path
from the root, and every ancestor along the way is a node too, so
listing the leaves is enough. The document carries no name and no
version — its config subject (`taxonomy:<name>`) is the identity and
the config source's version (a git commit, a topic offset) is the
version, the config lane's own version-as-evidence stance; a copy of
either inside the document could only agree or lie. The `Taxonomy`
*type* is registered and compat-gated like any schema; the documents
are per-mesh operator data behind the registry's config door (which
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

The first platform consumer is the search door's **document gate**,
opt-in by environment (`DOCUMENT_PLATFORM_TAXONOMIES`, a
comma-separated list of names; it requires the config lane): the door
historically indexed fetched documents without validating their
declared rules, and with taxonomies named it validates each fetched
document over the live mounts before anything indexes — membership
enforced as of index time, refusals naming the violations, and
fail-closed while a declared taxonomy has not yet mounted. Unset, the
door behaves exactly as before. Packed `google.protobuf.Any` payloads
stay with the seams that own a descriptor registry (the indexing
facade and the engines' payload gate); this door's registry-free gate
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
