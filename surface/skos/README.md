# SKOS edge conversion

Implementation slice recorded before production edits: branch
`feat/skos-jena-edge`, clean base `7aa5f25266fa216fff1f8552d732ab4d67e654f1`.

Required behavior: convert an explicitly selected SKOS scheme from Turtle or
RDF/XML to the existing binary `ConceptScheme`, and export it again. Preserve
concept IRIs, multilingual labels, typed notations, hierarchy, related links and
explicitly resolved cross-scheme mappings. Refuse unrepresentable RDF and invalid
protobuf instead of dropping it. Import IDs are UUIDv5 of the resolved RDF
concept IRI under the fixed ProtoMolt namespace specified by the converter.

Reuse: Jena RIOT parses and writes RDF; existing `ConceptScheme`, `Concept`,
`ConceptRef`, `LangLabel` and `ConceptNotation` are the internal model;
`ProtoValidator` with `ProtomoltRuleSource` checks field annotations, and
`ConceptSchemeRules` checks graph and label integrity. No new schema or storage
format is introduced. There is no existing SKOS edge converter to reuse.

Call path: caller-supplied stream + explicit scheme identity + external concept
bindings -> Jena statement stream -> typed protobuf builders -> both existing
validation gates -> returned `ConceptScheme`. The caller may publish that binary
document through its existing config lane. Export validates the protobuf and
context before Jena writes the selected external RDF syntax. This module has no
repository write, config publication, network fetch or runtime-host dependency.

Acceptance assertions: real Turtle and RDF/XML import/export retain supported
values and relations; UUID identity is stable; notation lexical values and
source order survive import; invalid labels and graph cycles fail both import
and export; unknown RDF predicates/types, blank nodes, unresolved mappings,
unknown protobuf fields and missing export IRIs fail explicitly; malformed RDF,
input/output failures and XML external-entity input cannot return a valid result.
The minimal repository runtime must not depend on Jena.

## Using the converter

Build only this optional distribution:

```bash
flock /work/.gradle-krick.lock ./gradlew :protomolt-skos:installDist --max-workers=2
surface/skos/build/install/protomolt-skos/bin/protomolt-skos \
  import subject https://example.org/subjects turtle surface/skos/examples/subjects.ttl subjects.pb
surface/skos/build/install/protomolt-skos/bin/protomolt-skos \
  export subject https://example.org/subjects rdfxml subjects.pb subjects.rdf
```

The output is published only after conversion succeeds and an existing output
is never overwritten. `subjects.pb` is the existing `ConceptScheme` wire format,
not a new container. Nothing is mounted or stored remotely by these verbs.

The Java entry point allows explicit external concept bindings from an operator's
known schemes. A mapping target absent from this map is refused; the converter
does not fetch IRIs or infer scheme membership from URL prefixes.

```java
var converter = new SkosConverter("subject", "https://example.org/subjects",
    Map.of("https://other.example/physics",
        ConceptRef.newBuilder().setSchemeId("other").setConceptId("physics").build()));
ConceptScheme scheme = converter.read(source, SkosConverter.Format.TURTLE);
converter.write(scheme, destination, SkosConverter.Format.RDF_XML);
```

The scheme slug and external scheme IRI belong to the caller's context because
`ConceptScheme` deliberately stores neither identity nor version. Export needs
that same context and external bindings. Imported concept IDs use namespace
`6583fc39-429f-583c-9abf-cd7ddab29f54`, the UUIDv5 of
`https://protomolt.ai/ns/skos/concept` under the standard URL namespace. Changing
this constant would change imported identities. Export accepts existing opaque
concept IDs, but reimport assigns the defined IRI-derived ID; RDF carries IRIs,
not ProtoMolt's independent hand-authored IDs. RDF parsing resolves IRI references,
including dot segments in absolute references, before identity is computed;
IDs do not preserve the source syntax's IRI lexemes. Percent-encoded and literal
spellings remain distinct when RDF resolution leaves them distinct. Context,
external bindings and exported concept/datatype IRIs must already be resolved:
the converter refuses an IRI whose RDF resolution would change its identity.

## Supported profile

Input declares one selected `skos:ConceptScheme`, and every local concept declares
`rdf:type skos:Concept` and `skos:inScheme` for that scheme. Supported properties:

- Scheme: `skos:prefLabel`.
- Concept: `skos:prefLabel`, `altLabel`, `hiddenLabel`, `definition`, `scopeNote`,
  `notation`, `broader`, `narrower`, `related`, and all five mapping relations.
- Retirement: `owl:deprecated` and `dcterms:isReplacedBy`.

Labels require language tags because `LangLabel` does; Jena canonicalizes their
case to lowercase on import. Notation lexical values
and their datatype IRIs remain intact; distinct statements retain source order
on import. RDF 1.1 equates a plain literal with an `xsd:string` literal, so both
import with an empty `datatype_iri`. Export requires that canonical form and
refuses explicit `xsd:string` rather than promising an impossible distinction.
RDF is a set of statements: duplicate source triples are one fact, and exporting
duplicate protobuf values that would collapse to the same fact is refused.
External RDF writers can reorder statements; protobuf ordering and original RDF
syntax are not round-trip guarantees. `owl:deprecated false` normalizes to the
protobuf default, which exports as absence of the retirement flag.

If both `broader` and `narrower` are present, this first strict profile requires
them to describe the same edge set. Export emits both directions. Cyclic broader
graphs and label conflicts are refused by the existing ProtoMolt profile rules.
Unknown predicates/types, collections, SKOS-XL, top-concept assertions,
directional language literals, blank nodes and undeclared concepts are refused
by name; no unsupported statements disappear into a successful import.
Unknown protobuf fields are refused on export, including nested messages.
Malformed RDF retains its parse cause; unavailable input remains an I/O failure.
Turtle input requires well-formed UTF-8; malformed byte sequences are refused
instead of replaced. RDF/XML honors its declared encoding. Protobuf strings
with unpaired UTF-16 surrogates are refused before export can replace them.

This library materializes one scheme. It does not claim constant-memory graph
ingestion, arbitrary RDF round trips, registry discovery, or native-image
qualification. Jena 6.2.0 is confined to this optional edge module; the core
types, scheme mount and repository service do not acquire Jena dependencies.

## Next graph slice

The separate Jena fork already implements protobuf stream adapters, protobuf
RDFPatch and an optional Jetty-free gRPC transport under
[apache/jena#4364](https://github.com/apache/jena/issues/4364). That transport is
under review, not an upstream released prerequisite for this converter.

A later graph role can consume the repository's existing protobuf accession,
tombstone and purge events. Its graph must be rebuildable from exact retained
revisions and schemas, with explicit mappings from declared links and concept
tags to RDF, and recorded mapping versions and occurrence provenance. Registry
reflection decodes an `Any`; it does not define its ontology automatically.
The feed needs idempotent replay, per-document revision ordering, durable
checkpoints and deletion handling. Current repository eventing is disabled
when Kafka is absent, so graph activation must explicitly require a feed.

Query authorization must restrict the graph before joins, aggregates and
inference, and current ACL revocations must take effect despite projection lag.
Named graphs alone are not ACLs. Keep that graph runtime and its dependencies
outside the minimal repo distribution; a separate JVM graph role avoids making
embedded Jena native-image compatibility an unverified requirement.

## Verification

Verified on JDK 25, 2026-11-01:

```bash
flock /work/.gradle-krick.lock ./gradlew checkZeroRegex checkTestNaming \
  :protomolt-skos:test :protomolt-skos:javadoc :protomolt-skos:installDist --max-workers=2
flock /work/.gradle-krick.lock ./gradlew :protomolt-repo-service:dependencies \
  --configuration runtimeClasspath --max-workers=2
```

Sixteen tests passed, zero failures/errors/skips. The installed launcher also
converted the included example through Turtle -> protobuf -> RDF/XML -> protobuf.
The repo-service runtime report contains neither Jena nor this module; the SKOS
distribution contains no Jetty or Servlet JARs. No repository integration test,
live mount, native-image build or graph runtime is claimed by these checks.
