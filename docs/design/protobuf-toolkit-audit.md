# Protobuf toolkit audit and repository repair boundary

Source audit completed 2026-10-13 against reconciliation HEAD
`61955b4f974466783f1e7af42e31ba97016a417d`. This is a source-level design audit,
not a declaration of runtime qualification or a line-by-line security review of
every application. No production code, tests, contracts or migrations were changed.
No tests were executed for this audit. Existing test sources are identified below;
their existence is not a new passing result.

## Conclusion

The protobuf toolkit still exists and contains substantial reusable implementations.
The mapper, CEL engine, descriptor compiler, parts codec, retained-schema machinery,
action contract gates and binary receipts are not facades over JSON.

There are nevertheless concrete representation defects and missing integration.
The canonical parts save does not inherit retained admission and operation receipts
merely because those facilities exist elsewhere. The original path also contains
authoritative JSON persistence. Keeping it does not mean keeping those defects.

Build on one DocumentService and its parts model. Repair the storage representation,
compose existing admission/commit primitives into that save path, and replace
buffered payload dependencies with provider streams and immutable references.
Do not rebuild the toolkit or restore the deleted publication transport.

## Scope and evidence

The audit covers descriptor loading/identity, source compilation, mapping and CEL
selectors, projections, annotations, validation, compatibility, registries and
federation, code generation, dynamic gRPC, JSON-facing adapters, Kafka protobuf
integration, search hints, parts storage, retained schemas and receipt integration.
The accompanying [source index](protobuf-toolkit-source-index.md) enumerates the
relevant modules, production sources/contracts and test sources. Enumeration does
not mean every method was individually verified.

Workflow execution, agent scheduling, every connector's implementation, deployment
security and performance qualification are not silently included in this conclusion.
Previously reported JSON persistence in those applications is not declared fixed.
The separate archive branch's repairs are not assumed present here.

Source references use repository-relative paths and class/method names so they
remain useful after line numbers move. Links in the index resolve exact files.

## What is already available

### 1. Descriptor loading, identity and wire inspection

`core/descriptors` supplies DescriptorLoader, DescriptorRegistry, its thin alias
DescriptorCatalog, classpath and Google loaders, descriptor/message fingerprints,
ClosedDescriptorSet and MessageWireBudget. ClosedDescriptorSet checks a supplied
artifact's complete imports and duplicate identities without remote substitution.
MessageWireBudget provides wire/depth controls distinct from a document ceiling.
MessageFingerprints serializes deterministically, but allocates a full byte array;
use it for bounded control records, not as the large-object streaming implementation.

Confirmed concern: DescriptorRegistry.register replaces the descriptor for a full
name. resolveOnDemand catches DescriptorLoadException, tries other loaders, then
negative-caches the miss. An unavailable registry can therefore appear to be a
missing definition. This registry is a discovery convenience, not a retained-version
identity boundary. See repair R5.

Existing tests: DescriptorRegistryBehaviorTest, DescriptorRegistryLoaderTest,
DescriptorLoaderContractTest, ClosedDescriptorSetTest, DescriptorFingerprintsTest,
MessageWireBudgetTest.

### 2. Source compilation and schema gathering

`core/sources/ProtoSourceCompiler` uses Wire to link sources, then rebuilds options
through LinkedOptionsRepair/LinkedOptionsEncoder. Option stripping is an intermediate
structural encoding step, not intended deletion of validation annotations. Errors
become ProtoCompilationException. Preserve the option-repair tests when reusing it.
`acquire/gather` already supplies filesystem, JAR, Git, Maven and composite sources,
plus GatheringDescriptorLoader. No new generic source loader is needed.

Existing tests: ProtoSourceCompilerOptionsRepairTest, ProtoSourceCompilerOptionsTest,
ProtoSourceCompilerMapTest and ProtoSourceCompilerStructureTest. These are particularly
important for retaining validation and other custom options during the "molt" step.

### 3. Field mapping and CEL selectors

`transform/mapper/core/ProtoFieldMapperImpl` reads/writes MessageOrBuilder through
FieldDescriptor and uses AnyHandler.unpack for typed Any traversal. Failed Any decode
throws MappingException; it does not call unpackToStruct to traverse ordinary protos.
It supports set, append, clear and textual mappings, with an explicit includeDefaults
mode. `transform/mapper/cel/CelProtoMapper` evaluates filters/selectors against the
progressive protobuf builder and applies results through the mapper. Evaluation
failures propagate. A filter that returns false is an intentional non-match.

Keep these tools. Do not invent JSON path mapping for the repository. A mapping is a
declared transformation, not a lossless storage codec; new output builders need not
copy fields the mapping did not select. That distinction must be explicit at callers.

Existing tests: IncludeDefaultsTest, PathErrorSemanticsTest, SetValueConversionTest,
ProtoFieldMapperRegressionTest, CelFilterSelectorTest, CelProtoMapperContractTest.

### 4. Descriptor-driven projections

`transform/projection/MessageProjection` compiles target annotations, invokes the
mapper/CEL, produces DynamicMessage and derives source/target FieldMasks. It is
usable for deliberate derived views. SourceMask.complete is false when CEL makes
the field dependency set incomplete: callers must not prune reads using that mask.
Map-valued target fields explicitly throw ProjectionException; map projection is
not currently a supported feature to advertise or emulate through Struct.

Two hazards need bounded treatment: evaluatorFor caches by full type name, so a
shared instance is not a safe assumption across different revisions of that name;
option-reparse failures return original options. The latter is an unproven defect,
not a demonstrated lost annotation: test malformed declared options before changing
it. See R5 and the qualification list.

Existing tests: MessageProjectionTest and ProjectionSemanticsTest.

### 5. Any helpers, conversions and message comparison

`core/helpers/AnyHandler.unpack` uses DynamicMessage with a descriptor.
PayloadCodec exposes that facility. Missing descriptors prevent typed decode, but
the repository can still preserve the original Any without calling unpack.
MessageDiff compares fields using each side's descriptors and reports unknown-field
differences; retain it for semantic comparison, not schema equivalence certification.

The same helper package also exposes messageToStruct/structToMessage, packStruct,
unpackToStruct and unpackSafe. TypeConverter.structToMessage skips unknown field names;
messageToStruct cannot retain protobuf unknown fields. Its field-aware LONG encoding
uses strings, so it is incorrect to claim that every long here becomes a double.
Generic toValue(Number), however, uses doubleValue. unpackSafe returns null on decode
failure. These are not acceptable authoritative round trips. The caller search found
helper-to-helper references for these named convenience methods, not use by the core
mapper's ordinary Any traversal. See R7; do not claim widespread caller damage from
the mere presence of these APIs.

### 6. Validation rules and conformance infrastructure

`protobuf/validation/ProtoValidator` walks descriptors and compiles rules from
ValidationRuleSource. `validation-protovalidate` implements the Buf annotation
dialect; OptionReparse throws RuleCompilationException for malformed declared option
encoding. `core/cel` supplies typed environments/evaluators. `core/formats` supplies
value-format checks. Keep these implementations rather than adding parallel validators.

`repo/admission/DocumentSchemaAdmission` explicitly selects ProtomoltRuleSource and
ProtovalidateRuleSource, independent of ServiceLoader state, and declares an empty
taxonomy/postal catalog profile. That is a real profile limitation: rules needing
external catalogs cannot be represented as supported by assumption.

The upstream-shaped conformance runner is in `protobuf/validation-conformance`.
This audit does not establish conformance to a newer upstream spec. Existing tests
include RetainedDescriptorValidationTest, NumericRulesTest, GoogleTypeRuleSourceTest,
ConformanceHarnessTest and ConformanceMainTest.

### 7. Validation at entry and exit

`surface/actions/ActionCatalog.dispatch` calls CatalogContract.checkedRequest and
checkedResponse around execution. Its streaming dispatcher uses ContractStreamEmitter.
Those paths already perform typed contract checks. Reuse the behavior where appropriate.

`surface/grpc/validation` server and client interceptors validate incoming server
requests and outgoing client requests respectively. They do not validate successful
server responses. `ReflectedMethodAction` separately validates upstream responses.
None of these facts establishes a gate on DocumentService: DocumentGrpcService calls
DocumentOperations, which calls SaveResolution and the managed/legacy save compositions.
SaveResolution performs structural/identity checks; DocumentAttemptWriter explicitly
leaves schema admission to the host. See R2.

### 8. Schema compatibility, registry storage and federation

`core/compat` supplies schema/service/enum/oneof diffs and CompatibilityChecker with
wire, optional JSON, optional source and transitive-history policies. Compatibility
is not record validation and not permission to reinterpret a historical schema.

`schema/registry/core` supplies stores and write gates. `schema/registry/git` stores
proto text and content-addressed descriptor `.pb` artifacts; GitSchemaRegistryStore
validates artifact identity on reads/writes. RegistryFederation imports schemas
through the registration/compatibility path and imports descriptor bytes. Its report
records rejected subjects rather than presenting them as imported. Some registry
metadata is JSON; that is distinct from converting descriptor bytes to JSON.
Its workflow JSON persistence is a separate application-state repair concern, not
proof that the descriptor artifact store is JSON.

Confluent and Apicurio adapters exist. ConfluentSchemaRegistryLoader.loadDescriptors
reports skipped subjects for selected failures while aborting on connection/auth/server
failures. Do not mistake partial discovery for a complete schema inventory. Authoritative
exact resolution must retain the failure rather than turn it into optional absence.

Existing tests include CompatibilityChecker tests, ConfluentSchemaRegistryLoaderTest,
GitSchemaRegistryStoreTest and registry integration suites. No new external registry
compliance claim is made here.

### 9. Runtime schemas, reflection and generated source

`surface/grpc/invoke` supplies ReflectionClient, DynamicGrpcCalls and streaming
helpers. DynamicGrpcCalls constructs method marshallers from descriptors and parses
DynamicMessage directly. This supports calls without generating Java classes.
The finite server-stream call intentionally caps responses; openServerStream is the
flow-controlled path. Do not use the capped call as an exhaustive document transfer.

`core/codegen/GenerateStubsAction` runs bundled WasmProtoc generators and returns
generated files. The checked enum contains eight message-language generators
(Java, Kotlin, Python, C++, C#, Ruby, PHP, Objective-C) plus grpc-java. This is not
evidence for twelve bundled generators. GenerateStubsResponse contains files, not
a ZIP field. ZIP helpers and OKF ZIP output exist in `sink/emit`, but a stub-ZIP
endpoint was not found in this baseline. Do not invent it or claim it was never
present in another branch/version.

ClasspathDescriptorLoader and Kafka GeneratedMessages load already available classes.
No production JavaCompiler/defineClass/URLClassLoader path compiling incoming schema
source into newly loadable client classes was found. Wasm code generation and dynamic
descriptor invocation are real; automatic Java source compilation/loading is a
different capability and is not established here.

### 10. MCP, ACP, REST and schema rendering

MCP transports expose ActionCatalog actions through JSON protocol boundaries.
ACP CatalogLineRunner also invokes the catalog; reflection/service-workspace actions
bridge discovered gRPC methods into it. ReflectedMethodAction keeps its request and
reply as protobuf internally. Its convenience server-stream behavior returns the
first response; exhaustive streaming belongs to the appropriate streaming action.

`surface/http/rest`, `jsonschema` and `openapi` already exist. ProtoJsonSchemaGenerator
exposes runtime-only constraints and CEL through vendor keywords. OpenApiValidation
lowers supported constraints into OAS 3.0 and discloses unsupported property-name
constraints. OpenApiRuntimeGapTest covers several previously problematic translations.
The earlier broad claim that OpenAPI validation translation is wholly missing would
be wrong for this baseline. Generated schemas still cannot enforce arbitrary CEL,
authorization or state-dependent revision checks.

### 11. Metadata, quality, prompts, shapes and receipts

`protobuf/metadata` supplies descriptor metadata and sensitivity masking, including
an unresolved-Any report. Unresolved masking must not be treated as safe output.
`protobuf/quality` evaluates descriptor-declared CEL scores and records failed
dimensions. Quality measurements are not contract validity or semantic truth.
`protobuf/llm` and `protobuf/prompt` supply descriptor-based guidance and boundary
response shaping. `protobuf/types`, `metric` and `seo` supply reusable contracts
and annotations, not alternate document storage models.

`transform/shapes` supplies ShapeSynthesizer; `transform/pipeline` compiles workflows
into typed pipelines with validation. `transform/delegation/contract` supplies
deliverable gates. `transform/receipt` serializes and verifies protobuf WorkRecord
containers; these signed work receipts are distinct from repository commit receipts.
Do not replace either with a new generic JSON receipt.

### 12. Kafka inputs and protobuf wire format

`sink/kafka/wire` implements Confluent framing. `serde` performs protobuf parsing,
mapping and optional runtime validation. `connect` has explicit PROTOBUF, CONFLUENT
and JSON input/output modes. JSON as an external mode is allowed, but silently
discarding fields is not: ValueCodec's JSON parser uses ignoringUnknownFields.

Confirmed unsafe identity behavior: SchemaIds returns unresolved on registry failure,
and a pinned deserializer accepts its packaged descriptor when the frame index path
matches. Matching an index is not proof of schema identity. GeneratedMessages.parse
can additionally choose a class by full name and parse under its compiled descriptor;
the deserializer then validates that resulting message, not necessarily the writer
descriptor it just resolved. Preserve the binary wire path, repair these choices (R6).

### 13. Search, embedding and output tools

`search/index/spi` supplies descriptor-derived IndexMapping, pluggable hint sources,
AnyPayloadGate, chunking policy and EmbeddingSpec. Field annotations include model,
dimensions, similarity and normalization. `search/index/protobuf` supplies the typed
indexer and declared-rule validator. Lucene, OpenSearch, Solr, Qdrant and NDJSON
adapters are present. Engine-specific JSON output is a derived external search view,
not permission to persist the repository source as JSON. Annotation support alone
does not establish that repository saves automatically produce embeddings/indexes.

AnyPayloadGate supports a declared validation opt-out on fields. That search policy
must never silently become repository admission policy. Search wiring remains last,
as requested. Existing mapping/search adapters should be reused then.

### 14. Parts, revisions, providers and retained schemas

`repo/codec/DocumentPartCodec.split` starts CORE from doc.toBuilder and clears fields
assigned elsewhere, keeping protobuf unknown fields in that copy. Other fragments
carry their typed fields; assemble merges binary fragments. PartLayouts,
DocumentFragmentConfinement, DocumentChunkSequence and DocumentRevisionAssembly
already model partitions/order. Do not rewrite them through JSON.

DocumentOperations preserves selected-part and selected-chunk logic. Its managed
save reads carried parts and ManagedDocumentSave uploads them into a new attempt.
The legacy path instead calls provider copy. The re-upload problem is confirmed
and localized, not proof that the parts API has disappeared.

`repo/blob` has SPI, S3, Redis, gRPC and cache modules. Azure is not an implemented
provider in this subtree. BlobStore has InputStream upload and provider-copy APIs;
buffered GetResult and DocumentPartReader remain. Redis explicitly caps values at
512 MiB and buffers stream input; it cannot be advertised as an unrestricted large-
document backend. Provider limits must remain separate from protocol capabilities.

`repo/admission` supplies preparation, assessment, retained replay/materialization,
occurrence identities, resource reservations and a SHA-keyed artifact cache.
`repo/schema/registry/RegistrySchemaResolver` authorizes/selects each occurrence,
including cache hits; it does not cache verdicts. V120 and RepositorySchemaArtifacts
normalize retained descriptor payloads instead of embedding copies in every document.
Schema-artifact budgets are not document budgets.

DocumentAnyRootInventory explicitly handles `Document.structured_data` (field 4,
CORE) and `Document.parser_results[key].document.shape` (field 5 -> map value ->
field 7 -> field 1, PARSED). These are both Any root declarations in the current
document.proto; this audit did not find an additional declared Document Any root
being omitted. Nested payload discovery is separate in DocumentSchemaOccurrences.
The inventory rejects unknown fields along the strict root access path. This is
not a general policy for every future Document field, nor a silent opaque fallback.
Preserve its failures while specifying opaque storage versus typed admission.

The document.proto comment promises byte-for-byte assembly. The codec actually
parses/merges/reserializes messages, so preserving logical fields and unknown fields
is not a general guarantee of identical arbitrary original protobuf wire ordering.
Preserve original retained payload bytes when byte identity is required; correct
that broad documentation claim rather than treating serialization as canonical.

DocumentPublicationCommit already commits binary result bytes with revisions and
schema retention. Preserve its useful atomicity/recovery checks, while adapting
the canonical save composition rather than restoring its removed public transport.

## Bounded repair list

These are the identified work units, not permission to implement them now.

### R1. Restore authoritative protobuf metadata storage

Confirmed: DocumentRecord.read/writeManifest -> DocumentPartCodec JSON; security
read/write -> JsonFormat; ArchiveManifests -> JSONB; DocumentHistoricalMetadata
reconstructs typed history from JSON. Store authoritative protobuf bytes and retain
query columns as projections. Migrate/read/restore together. Acceptance: unknown
fields, presence, security, part ordering and historical bytes survive restart and
backup restoration. Do not discard archives or useful SQL lookup columns.

### R2. Wire admission and operation receipts into the canonical save

Missing integration: SaveResolution checks are not retained-schema validation;
DocumentAttemptWriter leaves admission to its host. SaveDocumentResponse lacks the
proposed durable operation receipt. Reuse ProtoValidator/admission/commit/outcome
machinery in one handler composition. Acceptance: invalid combined partial document
cannot advance or alter current revision; request, success response, replay identity,
authorization and commit cancellation boundaries are asserted through DocumentService.

### R3. Remove buffered payload dependence

Confirmed: DocumentPublicationInput has aggregate 8 MiB / envelope 10 MiB limits;
codec and reader materialize fragment arrays; Redis buffers and has its own ceiling.
Use existing provider streaming and a reference-based internal command; do not just
increase constants. Acceptance: the 600 MB source and requested 2 GB scenario have
explicit payload semantics, bounded memory, backpressure and cancellation/recovery
evidence. Whole-message CEL resource needs remain explicit, never silently bypassed.

### R4. Stop re-uploading unchanged managed parts

Confirmed: DocumentOperations.savePartial -> readSourceSlots -> ManagedDocumentSave
collects reused bytes and writes them into new attempt keys. Reuse immutable physical
references or a qualified provider copy. Acceptance: same untouched bytes/provenance,
no unnecessary download/upload, concurrent source change cannot alter the pinned
revision, and retention/cleanup cannot reclaim a still-referenced version.

### R5. Preserve schema identity and failures

Confirmed general-registry error-to-miss behavior and full-name replacement; projection
CEL cache also keys by full name. Use retained identity-scoped resolution for repository
work; repair misleading discovery failure semantics separately. Acceptance: outage,
missing type, corruption and conflicting definitions remain distinguishable; same
name with different definitions cannot share a validation/projection environment.

### R6. Fix Kafka schema selection and validation identity

Confirmed packaged-descriptor fallback and generated-class selection described above.
Do not validate an unknown writer under a convenient local schema. Acceptance: registry
failure cannot certify a foreign schema ID; two schemas sharing name/index but different
constraints are validated against the declared writer identity. Locally packaged classes
may be used only without changing that validation identity.

### R7. Restrict lossy convenience helpers

Confirmed unknown-field omission in Struct conversion and null-on-failure unpackSafe.
Remove them from authoritative use and either remove unused public conveniences or
isolate explicitly requested external conversions with strict errors. Acceptance:
malformed versus unresolved Any remains distinguishable; no storage or validation
round trip traverses Struct/Value as a substitute for the original message. Generic
numeric conversions must not silently round exact integers.

### R8. Make boundary input rejection explicit

Confirmed Kafka ValueCodec JSON ignores unknown fields. Retain JSON mode but reject
unrecognized input by default or require an explicitly documented, caller-selected
loss policy; never quietly drop it. Acceptance: misspelled/unknown fields fail with
a useful error and valid external JSON still converts once into typed protobuf.

### R9. Close the Any coverage contract before declaring admission complete

This is a contract/qualification item, not a demonstrated omitted current root:
current discovery covers both declared CORE/PARSED roots and nested occurrences
under those roots. Record that exact inventory in the final contract, require an
explicit decision for new locations, and mark typed versus opaque treatment.
Acceptance must cover each listed location, revision-pinned definitions, unknown
fields on root paths and opaque byte retention. Do not claim a second document
model or arbitrary schema substitution is needed to close this item.

## Things not to rewrite and claims to correct

- Keep descriptor compilation/option repair, typed mapper/CEL, parts layout, binary
  receipts, retained artifacts and cache. Add missing integration and focused fixes.
- Keep JSON at MCP/ACP/REST/registry HTTP/search-output boundaries. Do not mechanically
  delete JSON libraries or confuse deliberate derived output with source storage.
- Do not promise map projections until supported. Honor incomplete CEL source masks.
- Do not advertise twelve bundled code generators, a stub ZIP endpoint or runtime
  Java source loading based on this baseline. These are not required to repair storage.
- Keep current OpenAPI translation; document runtime-only rules rather than replacing it.
- Quality failure reports, partial federation reports and explicit filter non-matches
  are not swallowed exceptions if the caller preserves and acts on those outcomes.

## Qualification questions, not demonstrated defects

Malformed projection-option reparse behavior needs a targeted test; determine whether
failure can hide a declared annotation. Strict root-inventory unknown-field rejection
needs a stated policy, not blind weakening. Exact historical schema round trips,
cross-part CEL and large-object behavior need end-to-end tests on the unified service.
Existing test names in the index do not settle these questions by being green.

## Implementation order after approval

First finalize the single-service contract and per-location Any policy. Then R1,
R2, R4 and R3 as narrow reviewed changes with R9 acceptance alongside them. Apply R5
where the unified path uses discovery/projection, preserving existing retained resolver
semantics. R6-R8 are separately bounded toolkit/integration repairs and must not turn
the repository work into a rewrite of Kafka or agent orchestration. No new feature
is required to complete this repair list. No estimate in days is justified until
the contract and streaming validation resource model are fixed.

Main/archive are unchanged. Audit completion means the source inventory, integration
distinctions and repair boundary are recorded; it does not certify all toolkit behavior
or authorize implementation, push, merge or deployment.
