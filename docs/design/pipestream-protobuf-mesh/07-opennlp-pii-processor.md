# OpenNLP alpha4 PII processor

## Objective

Add content-aware PII detection and rewriting to protobuf transform boundaries.
This complements schema annotations by finding sensitive values embedded in
free text before remote services, LLM providers, artifacts, and evidence.

## Dependency and isolation

Create an optional processor module, such as `transform/pii-opennlp`, using the
`ai.pipestream:opennlp-api` and `ai.pipestream:opennlp-runtime`
`0.1.0-alpha4-SNAPSHOT` family. Pin both artifacts through the version catalog
and BOM. Do not leak alpha4 implementation types into core mesh contracts.

Expose a small ProtoMolt SPI with operations for detect, enforce, rewrite, and
verify. The adapter maps that SPI to the OpenNLP alpha4 API. This permits
alpha-compatible upgrades without changing entity, route, or evidence wire
formats.

## Alpha4 capabilities to integrate

The adapter uses:

- `PiiExtractor` and `PiiMention` as the detection boundary;
- `CompositePiiExtractor` to combine deterministic extractors;
- `PiiPacks` for payment, contact, network, secrets, crypto, US identity, EU
  identity, Canadian identity, device, and all-structured presets;
- `PiiAnnotator` to add PII information to document layers;
- `MaskPolicy` and `MaskPolicies` for type-aware masking;
- `HmacTokenizer` for deterministic, non-reversible correlation tokens;
- `Pseudonymizer` for consistent scoped replacements;
- `PiiRewrite` to retain old-to-new offsets and remap annotations after text
  length changes; and
- `PiiAuditReport` for aggregate, privacy-safe reporting.

The supported structured mentions include email, telephone, IBAN, payment
card, IPv4, IPv6, MAC, AWS access key, GitHub token, JWT, Bitcoin and Ethereum
addresses, ABA routing, URL credentials, US SSN and ITIN, UK NHS number, German
Steuer ID, IMEI, and Canadian SIN. Tests must derive the active list from the
alpha API instead of duplicating it as an independent ProtoMolt registry.

## Policy model

A versioned `PiiPolicy` selects fields through typed message options and
descriptor-checked selectors. It names extractor packs, allowlists, action,
minimum confidence when available, correlation scope, key reference, audit
level, and pre-processing and post-processing requirements.

Actions are:

- detect and report counts;
- reject disclosure;
- remove;
- mask with a selected type-aware policy;
- tokenize through a trust-domain-scoped HMAC key; and
- pseudonymize within an entity, scope, tenant, or declared correlation scope.

HMAC keys and pseudonymization secrets arrive through `CredentialResolver`.
The policy carries an opaque reference only. A missing key fails before remote
dispatch and must not echo the reference or secret.

## Protobuf integration

Walk only annotated or policy-selected string and bytes fields. Support nested
messages, repeated fields, maps with explicitly selected values, and text
documents stored in a typed claim check. Preserve unknown fields and fields
outside the projection.

When rewriting free text, use `PiiRewrite` offset mapping to update compatible
spans and document annotations. Reject a rewrite when required annotations
cannot be mapped safely. Return a new immutable protobuf message rather than
mutating the input builder shared by another route.

Evidence stores policy digest, extractor-pack ids, counts by PII type, action,
protected correlation token when permitted, rewrite digest, and verification
outcome. It never stores raw matched text, neighboring text, secret material,
or reversible replacement maps.

## Processing locations

- after mapping and before the final consumer projection validation;
- before any LLM prompt or remote processor request;
- before an artifact becomes accessible outside its trust domain;
- after processor output to detect prohibited reintroduction; and
- before evidence or transcript serialization.

Schema sensitivity still removes fields known to be prohibited. PII content
inspection handles values that appear inside otherwise allowed free text.

## Tests

All default tests run in-process and use deterministic alpha4 extractors. Cover
every `PiiPacks` group, composite extraction, nested and repeated protobuf
fields, schema sensitivity plus content PII, each action, stable HMAC tokens,
scope-separated tokens, pseudonym consistency, missing credential failure,
format-preserving masks, `PiiRewrite` span remapping, post-result detection,
audit privacy, unknown fields, and no-op messages.

Add a capturing processor test that places unique PII sentinels in permitted
free-text fields and proves they are absent from request bytes, persisted
artifacts, transcripts, logs, exceptions, and evidence while allowed text
remains present.

## Acceptance criteria

- Alpha4 PII detection operates on selected protobuf content without exposing
  alpha implementation types to core modules.
- Rewritten messages pass protobuf validation and retain correct remapped
  annotations.
- Raw PII never appears in route or run evidence.
- Pre-disclosure failure prevents any remote or LLM invocation.
- Post-processing catches a prohibited value introduced by a processor.

## Exclusions

Do not treat regex detection as universal truth, scan unselected binary data,
persist replacement dictionaries, or make the alpha4 runtime mandatory for a
node that does not enable the PII processor profile.
