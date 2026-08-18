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

Held deliberately: a `slug` format (the `[A-Za-z0-9._-]` family, ~30
sites) until its exact charset and length variants are agreed; formats
for alternations (`host:port` or URI) until multi-format semantics are
designed; `semver` (nothing in the repo promises semver).

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
  `MetricRange` (inclusive both sides) is the first convergence
  target.
- **TreePath** — a delimited taxonomy path with segment rules. The
  platform behavior is hierarchical facets in the search door
  (drill-down), path-prefix filters in metrics, and struct columns in
  the lake. The audit's dotted-field-path family (identical regexes in
  three modules) and the flat `facetable` keyword facets are the
  consumers.
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
  money) is the case study. PostalAddress and PhoneNumber wait on
  format work (postal vocabularies; E.164 already has the
  phone_number Tier-1 format).

## Taxonomies and ontologies

First-class vocabulary support without bundling anyone's data, in
order of preference:

1. **JDK-shipped data** (languages, countries, currencies, timezones):
   zero bundling, zero attribution, zero list maintenance.
2. **Structural validation without data** (media types per RFC 6838,
   URIs per RFC 3986): the grammar is the standard.
3. **Operator-loaded taxonomy documents** for everything else: a
   `Taxonomy` is a config document — registry-stored, compat-gated,
   federated, versioned by commit — and the `TreePath` validator
   checks membership against the *mounted* taxonomy. The mechanism
   ships; the data is the operator's (or a separately-licensed pack).

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
