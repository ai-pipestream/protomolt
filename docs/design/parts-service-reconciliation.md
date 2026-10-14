# Parts-based repository reconciliation

Status: working design and source inventory, not implementation approval.
Inspected baseline: `61955b4f974466783f1e7af42e31ba97016a417d`.
This document belongs to `reconcile/parts-based-repository`. Main and
`ai-slop-archive-by-astra` are outside the change scope.

The completed source-level [toolkit audit](protobuf-toolkit-audit.md) and
[module/source index](protobuf-toolkit-source-index.md) supersede the preliminary
inventory below where more precise findings are recorded. Runtime qualification
and implementation remain separate; no implementation is authorized by these docs.

## Purpose and fixed requirements

Provide storage for ProtoMolt document processing through one canonical
`DocumentService`. Preserve separately addressable parts, selected chunk sets,
selective hydration, partial saves, ownership, validation and retained history.
Extend that service rather than introducing another document publication service.

Protobuf payloads and retained schema identities are authoritative. Source files
remain bytes. SQL owns ledger transactions, references and lookup metadata;
metadata projections must not become the only retained representation of a proto.
JSON is an external-format boundary, not an internal validation or storage detour.

A transport frame budget is not a document-size ceiling. Large documents require
streamed provider access and claim checks, not a larger in-memory byte array.

## Evidence rules

There are three distinct claims: a tool exists; its implementation has been
inspected; a complete repository path is qualified. Never substitute the first
two for the third. Existing tests listed below are leads, not newly executed proof.
No tests were run for this documentation pass.

Before changing a component, record its input, output, callers, authoritative
representation and failure behavior. Trace write, read and recovery. A passing
suite does not establish architectural fidelity unless its assertions establish
the required behavior.

## Existing tools and reuse decisions

Paths below are relative to the repository root.

### Descriptors, schema identity and Any

`core/descriptors/.../DescriptorLoader.java` already abstracts file and message
type lookup. `DescriptorRegistry`, `DescriptorCatalog`, `ClosedDescriptorSet` and
`DescriptorFingerprints` are existing facilities to inspect before adding any
new descriptor repository or identity scheme. Schema providers already exist in
`schema/confluent`, `schema/apicurio` and `schema/registry`.

`core/helpers/.../AnyHandler.java` decodes with `DynamicMessage.parseFrom` using
a registered descriptor. It also offers `unpackToStruct`; therefore merely using
this helper does not establish lossless behavior. Ordinary full-name lookup is
not proof of revision-pinned lookup. Historical decode must use retained schema
assets and preserve unresolved Any bytes under explicit opaque policy.

Reuse candidates for retained resolution are in `repo/admission`, including
`DocumentRetainedSchemaResolution`, `DocumentRetainedAnyMaterialization` and
`DocumentSchemaArtifactCache`. Their integration and cache identity remain to be
traced. Do not replace them with live-registry-only historical lookup.

### Mapping, selectors and projections

`transform/mapper/core/.../ProtoFieldMapper.java` already exposes get, set,
append, clear and mapping on Message/Builder. Its presence-aware get overload
distinguishes implicit defaults from unset explicitly present fields.
`transform/mapper/cel` contains `CelProtoMapper` and selector tests.

`transform/projection/.../MessageProjection.java` reads descriptor annotations,
uses the mapper and CEL, and derives source and target FieldMasks. It is a typed
derived-message tool, not a substitute for retaining the source document.
Its option-decoding catch paths return original options; determine reachability
and failure semantics before classifying this as safe or defective. Do not
silently rewrite these semantics during repository integration.

Inspect existing mapper regression, CEL contract/selector and projection semantics
tests for unknown fields, field presence, repeated/map behavior and exact numeric
values. Missing assertions must be identified explicitly.

### Validation and transport boundaries

`protobuf/validation/.../ProtoValidator.java` owns the descriptor walk and rule
compilation. Annotation dialects enter through `ValidationRuleSource` providers;
CEL support exists in `core/cel`. Reuse this engine, including its distinction
between violations, rule compilation failures and evaluation failures.

`surface/grpc/validation/.../ValidatingServerInterceptor.java` already validates
incoming messages. It does not by its existence prove repository composition
installs it, validates assembled partial updates, or validates success responses.
Those are separate wiring checks. `protobuf/validation-conformance` is existing
qualification infrastructure, not evidence that every repository boundary uses it.

### Parts, manifests and admission

`repo/codec` contains `PartLayouts`, `DocumentPartCodec`,
`DocumentRevisionAssembly` and fragment-confinement helpers. Preserve their
descriptor-based partitioning rather than inventing another document shape.

Confirmed current mismatch: `DocumentPartCodec.manifestToJson/manifestFromJson`
are called by `repo/container/.../ledger/DocumentRecord.java` to store and recover
the part manifest. The parser uses ignoringUnknownFields. This is an actual
authoritative representation path requiring repair, not merely an external JSON
utility. Preserve protobuf manifest bytes; keep SQL lookup projections separately.

`repo/engine/.../DocumentOperations.java` remains the canonical save composition.
Its managed partial-save path calls `managedParts.readSourceSlots` for carried
parts. Inspect provider capabilities and retained references to eliminate needless
read/re-upload without changing selected-part or ownership semantics.

`repo/admission/DocumentSchemaAdmission` and container retention/commit machinery
are reuse candidates for admission and receipts. Their existence does not mean
SaveDocument already provides their guarantees.

Confirmed buffered constraint: `repo/spi/.../DocumentPublicationInput.java`
enforces an 8 MiB aggregate upload limit and 10 MiB envelope limit. Do not route
the unified parts service through this buffered request to obtain receipts.

### Other protobuf facilities that still need inventory

Reflection and invocation (`surface/grpc/invoke`), stub generation
(`core/codegen`), schema federation and Git persistence (`schema/registry/git`),
metadata and annotation helpers (`protobuf/metadata`), embedding/index hints
(`search/index`), JSON Schema generation (`surface/http/jsonschema`) and MCP/ACP
exposure must be mapped to their callers. They are not all qualified by this pass.
Boundary conversion remains permitted; it must not creep into stored documents.

## Proposed single save composition

1. Authenticate the caller; validate request annotations with the existing engine.
2. Pin the base revision and selected source-part revisions; check access and
   expected revision. Retries must not silently select a newer base.
3. Stage changed bytes through provider streams. Reuse authorized immutable
   references for untouched content where supported. Verify length and digest.
4. Assemble the candidate's logical parts and schema bindings. Validate the
   resulting document, including rules spanning changed and unchanged parts.
5. Use the retained-schema admission machinery to bind proof to that candidate.
   Unresolved opaque Any is not a successful typed-validation result.
6. Atomically commit revision references, evidence, idempotent outcome and outbox
   using the existing ledger primitives where suitable. Provider uploads occur
   outside the SQL transaction; staged-object cleanup must remain recoverable.
7. Return the recorded receipt and revision. Read/history APIs resolve that
   immutable revision, not whichever data is current later.

Unary and streaming requests use this same composition. Bulk streaming has
per-document outcomes; it does not implicitly promise a global batch transaction.
Cancellation before commit and cancellation after durable commit have different
outcomes. Replay must recover the recorded outcome rather than guessing success.

Whole-message CEL may require materialization. Streaming transfer does not make
every validation rule streaming. Resource admission and explicit failures must
be designed without silently skipping rules or inventing a document ceiling.

## Bounded sequence, subject to implementation approval

1. Finish the inventory above and finalize the existing DocumentService contract
   changes. Specify part identity, empty/omitted/deleted behavior and receipts.
2. Repair authoritative manifest storage and qualify round-trip/restart fidelity.
3. Connect one partial-save path to retained validation and durable receipts.
   Remove superseded orchestration only after mapping every retained protection.
4. Implement streamed part transfer on the same service and remove the buffered
   envelope dependency. Qualify large transfers and bounded memory.
5. Qualify history, selective hydration, partial-save concurrency and recovery
   through the canonical service. Remove obsolete guides and alternate entry paths.

Each step must name reused components and exact acceptance assertions. New public
services, altered part semantics, weakened validation and new size ceilings are
design changes requiring discussion, not implementation conveniences.

## Acceptance evidence required before completion

- Selected-part updates leave untouched parts intact and do not re-upload them
  unnecessarily; empty, omitted and deleted are tested separately.
- Unknown fields, Any bytes, explicit presence and exact 64-bit values survive
  save/read/restart. Historical definitions cannot be replaced by current ones.
- Invalid assembled documents cannot commit. Annotation errors and state-dependent
  authorization/revision checks are distinguished and tested.
- Same operation plus same input replays the receipt; changed input is rejected.
  Cancellation and process failure around commit do not produce ambiguous success.
- A 600 MB source file and the requested 2 GB case use real provider streaming;
  memory, backpressure and failure cleanup are measured. Define whether each case
  is raw source, aggregate document or structured part rather than conflating them.
- The public contract has one document service. Validation and receipt machinery
  is reached from that service, not demonstrated only through internal test APIs.

This is not a declaration that the audit is complete or the branch is ready to land.
