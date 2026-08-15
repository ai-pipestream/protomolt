# ProtoMolt PipeStream protobuf mesh

## Purpose

ProtoMolt can act as a contract-driven processing node in a distributed
PipeStream mesh. Nodes exchange arbitrary protobuf messages as
`google.protobuf.Any`, resolve their exact descriptors, apply registered
validation and transformation policy, and route them to local or remote
processors. A processor may be a gRPC service, an LLM, a deterministic Java
component, an OpenNLP analysis pipeline, or another mesh node.

This design aligns with
[`draft-krickert-pipestream-03`](https://www.ietf.org/archive/id/draft-krickert-pipestream-03.txt).
It does not redefine the QUIC wire protocol. ProtoMolt supplies a protobuf
application profile and node runtime. A gRPC bidirectional transport provides
the first interoperable implementation. A future QUIC adapter can map the same
entity, scope, status, barrier, and claim-check semantics onto the draft's
native control and entity streams.

## Product boundary

The mesh is not an LLM task broker. It is a typed entity-processing fabric.
LLM-assisted software generation is one processor profile on that fabric.

The core runtime owns:

- entity identity, parentage, scopes, status, and deadlines;
- exact schema identity and descriptor resolution;
- authenticated node and processor capability advertisements;
- deterministic routing and transformation selection;
- validation before and after every processing boundary;
- recursive scatter, checkpoint barriers, and rehydration;
- content-addressed claim checks and evidence; and
- replayable, auditable execution records.

Application protobufs own their domain data and instructions. ProtoMolt does
not require every integration to translate into one universal task message.

## Existing ProtoMolt foundation

The design composes existing behavior rather than introducing parallel
implementations:

| Requirement | Existing primitive |
| --- | --- |
| Runtime descriptors | `DescriptorRegistry`, service reflection, registry loaders |
| Stable remote service identity | `ServiceProfile` and service workspace |
| Dynamic invocation | `grpc-invoke` and `DynamicGrpcClient` |
| Client creation | `generate-stubs` and descriptor-backed code generation |
| Arbitrary typed payloads | `AnyHandler` and `google.protobuf.Any` |
| Structural validation | `validate.v1`, protovalidate, `ProtoValidator` |
| Mapping | scoped text rules and typed CEL selectors |
| Projection | descriptor-declared `MessageProjection` |
| Static checking | `RuleChecker`, `WorkflowVerifier`, `PipelineChecker` |
| Streaming execution | `PipelineExecutor` and all four gRPC method shapes |
| Fan-out | bounded virtual-thread fan-out and explicit collect |
| Structured LLM output | `StructuredGenerator` and inference providers |
| Evidence | workflows, run evidence, artifact references, offline replay |
| Sensitivity controls | metadata annotations and `SensitivityMasker` |
| Agent lifecycle | delegation contract, leases, checkpoints, evidence review |
| Storage | Git registry, filesystem artifacts, RustFS deployment |

## Layering

```text
PipeStream transport semantics
  entity ids, scopes, status, barriers, claim checks, rehydration

ProtoMolt protobuf mesh profile
  Any, schema fingerprints, annotations, routing, transforms, validation

Processor profiles
  dynamic gRPC, LLM software generation, OpenNLP PII, deterministic transforms

Application contracts
  customer and product protobuf messages
```

The generic mesh remains useful without an LLM provider. An LLM processor
uses the same contracts and policy gates as every deterministic processor.

## Entity model

The initial gRPC transport carries an application representation of a
PipeStream entity:

```proto
message EntityEnvelope {
  EntityHeader header = 1;
  SchemaReference schema = 2;
  google.protobuf.Any payload = 3;
}

message SchemaReference {
  string type_name = 1;
  string descriptor_fingerprint = 2;
  string registry_reference = 3;
  ArtifactReference descriptor_artifact = 4;
}
```

`Any.type_url` identifies the protobuf type. The descriptor fingerprint binds
that name to exact schema bytes. A receiver rejects a type URL that does not
match the resolved descriptor or a descriptor set whose canonical fingerprint
does not match the envelope.

Large payloads may use a typed claim-check message as the `Any` body. The
claim check names a content-addressed artifact and retains the intended
protobuf type and descriptor fingerprint.

### Header fields

The application header mirrors the draft's transport-visible identity:

- entity id;
- optional parent entity id;
- scope id and depth;
- application-defined data layer;
- content type and payload length;
- payload checksum;
- route and processing profile references;
- deadline and completion policy;
- security posture reference and digest; and
- trace and evidence correlation identifiers.

The QUIC adapter maps native PipeStream header fields directly. Profile fields
that do not fit the core wire header remain in a typed extension.

## Contract annotations

Strongly typed protobuf options describe default processing semantics. They do
not grant authority and do not contain endpoint addresses or credentials.

### Message options

A message may declare:

- processing profile;
- required processor capabilities;
- expected result type;
- default route profile;
- maximum recursive depth;
- scatter and rehydration profile references;
- whether LLM execution is allowed;
- whether content PII inspection is required; and
- approval and evidence requirements.

### Field options

A field may declare one or more roles:

- routing key;
- instruction;
- grounding or context;
- attachment or claim check;
- scatter source;
- result;
- evidence;
- PII scan target; and
- prohibited from remote disclosure.

Existing metadata, validation, LLM, quality, indexing, and projection options
remain authoritative in their domains. The mesh options reference them rather
than copying their fields.

Free-form metadata labels remain descriptive. Execution behavior uses typed
options and versioned profile contracts.

## Node and processor advertisements

A node opens an authenticated bidirectional session and advertises:

- stable node identity and trust domain;
- supported mesh protocol revisions and limits;
- processor capabilities;
- accepted input and output protobuf types;
- supported descriptor fingerprints or registry namespaces;
- local execution limits;
- service endpoints available through the node; and
- whether an endpoint is directly reachable or must be invoked through the
  advertising node.

Advertisements have an expiry and are refreshed by heartbeat. They do not
create permanent registry entries by themselves.

### Reflected service quick win

An origin node can advertise a gRPC endpoint that supports reflection. The
receiving node:

1. authenticates the advertising node;
2. applies outbound host, port, transport, and trust policy;
3. reflects the service through the direct endpoint or advertising-node relay;
4. canonicalizes and fingerprints the descriptor set;
5. compares the fingerprint with the advertisement when one was supplied;
6. registers a TTL-bound `ServiceProfile` in its service workspace;
7. publishes the descriptor artifact to the configured registry or artifact
   repository;
8. exposes methods through MCP resources and typed actions;
9. generates client code on demand; and
10. invokes the service dynamically with validated `Any` messages.

This turns service advertisement into a live integration handshake. No shared
generated client library is required before the nodes connect.

An advertisement is not permission to connect to an arbitrary address. Target
policy must reject disallowed schemes, hosts, ports, resolved addresses, and
trust configurations. Redirects are not followed during reflection.

## Routing

Routing uses a versioned protobuf `RouteProfile`. It may select processors by:

- payload type and descriptor fingerprint;
- annotated routing keys extracted from the message;
- required capability;
- data layer;
- trust domain and security posture;
- environment, locality, cost, or model constraints;
- current capacity and advertised limits; and
- a CEL predicate compiled against the payload descriptor and normalized
  execution context.

Route profiles are trusted registry configuration. A request may supply values
used by a route but cannot submit executable CEL or select an unrestricted
transformation.

Every accepted route produces a `RouteDecision` containing the matched rule,
processor, endpoint, schema fingerprints, transform fingerprints, and policy
digest. Evidence stores the decision without credential material.

## Transform boundaries

The router adapts an entity to a processor contract through an approved,
immutable transform plan:

1. unpack and validate the source `Any`;
2. select declared sources;
3. apply descriptor-checked text and CEL mappings;
4. project to the consumer-visible type;
5. apply sensitivity and PII policy;
6. validate the delivered value;
7. invoke the processor;
8. validate the result; and
9. map or project it into the declared mesh result type.

Transform plans reuse `TypedEdge` semantics and are identified by a canonical
SHA-256 fingerprint. Inline plans are allowed only from an authenticated
authoring surface with explicit approval. Normal entity traffic references a
registered plan.

Validation failure occurs before remote invocation. Result validation failure
cannot advance a parent barrier or satisfy rehydration.

## Recursive scatter and rehydration

A processor may dehydrate one entity into bounded child entities. Each child
has a stable parent id, scope, typed payload, route decision, and evidence
record. A child may recursively dehydrate again within the declared depth and
amplification limits.

The scope runtime maintains an assembly manifest containing:

- parent and child identities;
- child terminal states;
- completion policy;
- expected result types;
- transform fingerprints;
- claim-check references; and
- the rehydration profile.

Rehydration starts only when the checkpoint barrier is satisfied. Completion
policies include strict, lenient, best effort, and quorum behavior. Bounds
cover depth, child count, payload bytes, active streams, wall time, retries,
and total processor cost.

The first implementation reuses pipeline fan-out, collect, and virtual-thread
execution. The state contract remains independent of one executor so a QUIC
transport and durable coordinator can use the same reducer.

## Security model

Transport authentication establishes the peer. Entity data never establishes
its own authority.

### Transport context

gRPC metadata or QUIC TLS supplies:

- bearer or mTLS identity;
- trace context;
- session id;
- node id; and
- optional credential references resolved by the host.

Credential and key material never enters an entity, advertisement, route
profile, transcript, or evidence record.

### Entity security posture

A typed security extension carries non-secret posture:

- tenant and trust domain;
- data classification;
- required clearance;
- allowed processing environments;
- allowed processor profiles;
- maximum delegation depth;
- retention and disclosure policy references;
- PII handling policy; and
- a canonical policy digest.

The receiving node derives the effective posture from authenticated identity,
local policy, and the entity request. It can only preserve or reduce authority
when forwarding.

TLS protects each hop. Meshes crossing administrative boundaries also need an
end-to-end signed security capsule over the canonical header, payload digest,
scope, issuer, audience, and expiry. The header carries the capsule or its
content-addressed reference, never a private key.

### Privacy

Header metadata is minimized because entity type, child count, size, and
timing can reveal processing structure. Sensitive deployments may use opaque
content types, padded status intervals, fixed decomposition sizes, and relayed
endpoints.

## OpenNLP PII processor

Schema-declared sensitivity protects known protobuf fields. OpenNLP adds
content inspection for PII embedded inside free text.

The optional processor targets the `ai.pipestream:opennlp-api` and
`ai.pipestream:opennlp-runtime` `0.1.0-alpha4-SNAPSHOT` family. A narrow
ProtoMolt adapter keeps alpha implementation types out of the mesh contracts.
The alpha4 integration includes:

- `PiiExtractor` and `PiiMention`;
- `CompositePiiExtractor` and `PiiPacks`;
- contact, payment, network, secret, crypto, US identity, EU identity,
  Canadian identity, and device packs;
- `PiiAnnotator` document layers;
- type-aware `MaskPolicy` and `MaskPolicies`;
- `HmacTokenizer` for deterministic tokens;
- `Pseudonymizer` for consistent replacements;
- `PiiRewrite` with offset and annotation remapping; and
- `PiiAuditReport` with counts and privacy-safe samples.

PII processing is policy-driven:

- detect only;
- reject remote disclosure;
- remove;
- mask while preserving format or trailing characters;
- tokenize with a trust-domain-scoped HMAC key; or
- pseudonymize within a declared correlation scope.

Keys arrive through host credential references. Detection output stores type,
span, confidence or normalization where available, action, and a protected
token. Raw matched text is not stored in route evidence.

The PII processor runs before any LLM or remote processor when policy requires
it. A post-processor scan verifies that prohibited PII did not reappear in the
result.

## LLM software-generation processor

An LLM processor consumes any contract whose annotations permit LLM execution.
The descriptor supplies:

- message and field instructions;
- validation constraints;
- sensitivity policy;
- accepted grounding fields;
- expected typed result; and
- required completion evidence.

The processor uses the existing structured-generation coordinator for typed
forms. Software integration work may additionally produce content-addressed
source bundles, build manifests, commits, and check evidence.

A service advertisement can trigger an integration workflow:

1. reflect and register the advertised service;
2. inspect its methods and method policy;
3. generate or compile a client in the requested language;
4. ask an LLM processor to implement the integration around generated code;
5. run required builds and tests through deterministic processors;
6. request revisions when evidence is incomplete or checks fail; and
7. return an integration artifact whose schema and service dependencies are
   fingerprinted.

The existing delegation lifecycle becomes an LLM profile over mesh entities.
Hello, admission, leases, progress, checkpoints, cancellation, completion
candidates, revision requests, and evidence acceptance remain explicit. The
work and result payloads become typed entities rather than prose-only tasks.

## Persistence and recovery

The runtime separates interfaces from adapters:

- `EntityRepository` stores current entity and scope state;
- `TranscriptRepository` stores append-only control events;
- `AdvertisementRepository` stores expiring node and service advertisements;
- `ArtifactRepository` stores payloads, checkpoints, descriptors, and outputs;
- `RouteProfileRepository` stores immutable routing and transform profiles;
  and
- `LeaseRepository` fences active processor ownership.

An in-memory implementation supports unit and conformance tests. PostgreSQL is
the durable state and lease adapter. RustFS is the S3-compatible artifact
adapter. The Git registry stores schemas and immutable processing profiles.

Reducers validate every transition before persistence. Recovery rebuilds
active state from the durable record and reoffers expired work from the latest
valid checkpoint. At-least-once delivery is safe because frames and entity
results have idempotency keys and immutable content fingerprints.

## Future features

- [Encrypted shared workspace and tiered search](11-encrypted-shared-workspace.md):
  synchronize client-encrypted, content-addressed agent artifacts through the
  repository service and S3. Search indexes can be stored remotely, unloaded
  locally, and hydrated by claim check when needed.
- Durable agent-session recovery: checkpoint a live provider conversation and
  revive it after an HTTP/2 disconnect within the advertised resume window.
- Multi-agent workspace exchange: allow delegated workers to publish bounded
  outputs and evidence into a shared manifest while Git remains authoritative
  for source history.

## Surfaces

The mesh is exposed through:

- a typed gRPC node service;
- an outbound worker or peer bidirectional stream;
- MCP tools for submit, inspect, watch, cancel, and approve;
- MCP resources for nodes, advertisements, entities, scopes, routes,
  artifacts, and evidence; and
- REST/OpenAPI projections of the same action catalog where streaming is not
  required.

MCP bootstrap instructions describe the safe path: inspect node capabilities,
resolve the contract, validate a sample, submit the entity, monitor the scope,
and accept only evidence-backed results.

## Initial end-to-end acceptance

The first full acceptance uses two in-process nodes and custom test protobufs:

1. Node A advertises a reflected gRPC service.
2. Node B authenticates A, reflects the endpoint, fingerprints the descriptors,
   and creates a TTL-bound service profile.
3. B generates a client bundle from the reflected schema.
4. B receives a custom `Any`, validates it, and selects a route with CEL.
5. The route maps and projects the message into the advertised method request.
6. A PII processor masks a prohibited value before remote delivery.
7. The service result becomes grounding for a scripted LLM processor.
8. The LLM processor scatters deterministic build and test checks.
9. The parent waits at a checkpoint barrier and rehydrates the evidence.
10. B returns a validated typed result and offline-replayable execution record.

The default test uses no container, GPU, external model, or public endpoint.
Separate opt-in tests cover TLS routes, Keycloak, PostgreSQL, RustFS, and a live
LLM provider.

## Work packages

The files in this directory define independent implementation boundaries:

1. [Core contracts and annotations](01-core-contracts-and-annotations.md)
2. [Service advertisement and schema exchange](02-service-advertisement.md)
3. [Routing and transform engine](03-routing-and-transforms.md)
4. [Recursive scope runtime](04-recursive-scope-runtime.md)
5. [Security and policy](05-security-and-policy.md)
6. [LLM software-generation profile](06-llm-software-generation.md)
7. [OpenNLP PII processor](07-opennlp-pii-processor.md)
8. [Persistence and recovery](08-persistence-and-recovery.md)
9. [MCP and node surfaces](09-mcp-and-node-surfaces.md)
10. [Conformance and mesh acceptance](10-conformance-and-acceptance.md)

The core contract is the dependency gate. After it is reviewed, service
advertisement, routing, security policy, PII, persistence interfaces, and LLM
profile adaptation can proceed in parallel. The recursive runtime composes
those seams. Conformance owns the final cross-package acceptance.

### Suggested agent assignment

| Start order | Package | Coordination boundary |
| --- | --- | --- |
| First | Core contracts and annotations | Owns shared wire types and options; all other agents review this contract before implementation |
| Parallel after core review | Service advertisement | Owns reflection, TTL registration, and generated-client handshake |
| Parallel after core review | Routing and transforms | Owns route profiles, static compilation, selection, and edge adaptation |
| Parallel after core review | Security and policy | Owns posture derivation, signed capsules, and enforcement interfaces |
| Parallel after core review | OpenNLP PII processor | Owns the optional alpha4 adapter and privacy-safe PII evidence |
| Parallel after core review | Persistence and recovery | Owns repository interfaces, conformance suite, and durable adapters |
| Parallel after core review | LLM software generation | Owns the processor profile and delegation adaptation, not generic mesh state |
| After routing and persistence seams | Recursive scope runtime | Owns reducers, barriers, scheduling, and rehydration |
| After catalog and runtime seams | MCP and node surfaces | Owns gRPC frames, MCP actions, resources, and bootstrap discovery |
| Last integration lane | Conformance and acceptance | Owns shared vectors and cross-package scenarios, not feature implementation |

Each agent should work in its named package boundary, add its own in-process
tests, and avoid editing another package's contracts without a focused review.

Each package must add validation and sensitivity metadata to every persisted
contract field. Index annotations belong only on fields intentionally exposed
through a search index. Operational state, credentials, transcripts, and
provenance are not automatically searchable.

## Non-goals

- Replacing the PipeStream QUIC specification with gRPC
- Defining one universal application task message
- Trusting service advertisements without reflection and policy checks
- Allowing request-supplied CEL or arbitrary outbound endpoints
- Sending credentials or raw PII in entities or evidence
- Making an LLM the authority for validation, routing, or completion
- Requiring a model, GPU, container, or external endpoint for normal tests
