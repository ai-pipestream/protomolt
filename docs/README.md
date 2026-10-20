# ProtoMolt documentation

The [project README](../README.md) has the build and first-run instructions.
The guides below follow the repository's module layout.

## Protobuf toolkit overview

The [library catalog](../README.md#1-libraries) lists individual modules and
entry points. These are the reusable layers to check before adding another helper:

- **Descriptors and schemas:** [descriptor loading](core/descriptor-sources.md),
  [source gathering](acquire/gathering.md), compilation, [compatibility checking](schema/compatibility.md)
  and the [registry](schema/registry.md) supply definitions for generated messages
  and runtime `DynamicMessage` values. Name lookup is not a substitute for pinning
  an immutable schema artifact when reading retained data.
- **Mapping and selection:** [field mapping and CEL](transform/mapping.md) operate
  on protobuf fields and builders. Use them for declared transformations rather
  than converting a message to JSON to traverse it.
- **Projections and shapes:** [projections](transform/projection.md) and
  [derived shapes](transform/join-shapes.md) build typed outputs. Projection map
  fields are currently refused; a source mask marked incomplete must not be used
  to discard fields needed by CEL.
- **Validation and annotations:** [runtime validation](transform/validation.md),
  [metadata](schema/metadata.md), [masking](transform/masking.md) and
  [quality scoring](transform/quality.md) interpret descriptor options. Quality
  scores do not establish contract validity. A service must explicitly wire its
  validation boundary; library availability alone does not enforce it.
- **Protocol adapters:** [actions](surface/actions.md), [dynamic gRPC](surface/grpc-service.md),
  [MCP](surface/mcp.md), [ACP](surface/acp.md) and [REST/OpenAPI](surface/rest-gateway.md)
  expose typed behavior to callers. JSON belongs at protocols that require it.
  JSON Schema/OpenAPI translate supported validation constraints; runtime-only
  rules still require the validator.
- **Execution and evidence:** [pipelines](transform/pipeline.md),
  [workflows](transform/workflows.md), [delegation](transform/delegation.md) and
  [signed work records](design/receipts.md) provide distinct execution and evidence
  tools. A signed work record and a repository operation receipt are not the same
  contract.
- **Storage and derived output:** the [repository](../repo/README.md) supplies
  parts, provider interfaces and retained-schema machinery;
  [search indexing](search/indexing.md) consumes indexing/embedding hints, and
  [Kafka](sink/kafka-serde.md) and [bundle emitters](sink/emitting.md) provide
  integration boundaries. These capabilities do not imply automatic wiring into
  every save path.

For a runnable introduction, use the [standalone toolkit example](../examples/protobuf-toolkit/README.md).
For the October 13 reconciliation findings and known defects, see the
[source audit](design/protobuf-toolkit-audit.md) and
[source/test index](design/protobuf-toolkit-source-index.md). Those records describe
source inspection, not a new runtime certification. The
[parts-based repository requirements](design/parts-based-repository-requirements.md)
are a draft for review, not a description of completed repairs.

## Acquire

- [Stream connectors](acquire/connector.md): bounded, flow-controlled inputs
- [Pull connectors](acquire/pull-connectors.md): S3 and JDBC pulls through the intake service, watermark in and out
- [Gathering proto sources](acquire/gathering.md): filesystem, jar, Git, and Maven sources
- [Microsoft Graph](acquire/msgraph.md): OneDrive, SharePoint, and Copilot connectors

## Core and schema

- [Descriptor sources](core/descriptor-sources.md): descriptor loading and hygiene checks
- [Cluster directory](core/cluster-directory.md): memory-resident discovery with encrypted restart persistence
- [Distributed config](core/config.md): typed protobuf config documents behind a pluggable `ConfigSource`
- [Core utilities](core/helpers.md): `Any`, `Struct`, conversion, and message comparison
- [Compatibility checking](schema/compatibility.md): wire, JSON, and source compatibility
- [JSON Schema generation](schema/json-schema.md): draft 2020-12 output from protobuf descriptors
- [Schema metadata](schema/metadata.md): ownership, sensitivity, descriptions, and extraction
- [Publishing schemas](schema/publishing.md): Confluent and Apicurio publishers
- [Registry](schema/registry.md): Git storage, compatibility gates, registry protocols, and federation

## Transform and execution

- [Field mapping](transform/mapping.md): path rules and CEL expressions
- [Field masking](transform/masking.md): remove, redact, encrypt, and decrypt policies
- [Projections](transform/projection.md): descriptor-declared message projections
- [Validation](transform/validation.md): rule dialects, protovalidate, and gRPC enforcement
- [Quality scoring](transform/quality.md): CEL-based dimensions and weighted scores
- [Joins and derived shapes](transform/join-shapes.md): joins, unions, merges, and generated shapes
- [Workflow manager](transform/workflow-manager.md): checked serial compositions of gRPC calls
- [Workflows and run evidence](transform/workflows.md): record, replay, promotion, and structured steps
- [Pipelines](transform/pipeline.md): checked execution across all gRPC streaming shapes
- [Agent delegation](transform/delegation.md): coordinator and worker streaming contract

## Search and sinks

- [Search indexing](search/indexing.md): index mappings and Lucene, OpenSearch, and Solr output
- [Chunking](search/chunking.md): deterministic chunking policies executed by the sentence-packed chunker
- [Text embeddings](search/embeddings.md): Model2Vec, TEI, and OVMS providers
- [The search service](search/service.md): the query surface, its workflow-driven indexing RPC, and the console page
- [Reranking](search/rerank.md): TEI and OVMS rerank providers
- [Metrics](metric/metrics.md): descriptor-declared measures and dimensions, the query compiler and its refusals, and the Lucene engine
- [Emitting bundles](sink/emitting.md): directory, Git, zip, OKF, and Parquet sinks
- [Apache Iceberg](sink/iceberg.md): descriptor-driven tables and snapshot appends
- [Kafka Connect](sink/kafka-connect.md): gRPC source, sink, and protobuf transforms
- [Kafka Connect: OpenSearch](sink/kafka-connect-opensearch.md): hint-driven documents from topic records
- [Kafka serde](sink/kafka-serde.md): Confluent wire format with schema enforcement

## Repository

- [Save and read document parts](repo/publication.md): the repository guide to the canonical parts-based `DocumentService`

## Surfaces

- [Actions](surface/actions.md): the self-describing verb catalog
- [MCP server](surface/mcp.md): tools, resources, service discovery, and invocation
- [ACP agent](surface/acp.md): stdio integration for ACP clients
- [gRPC service](surface/grpc-service.md): typed RPCs, reflection, REST, OpenAPI, and launcher
- [REST gateway](surface/rest-gateway.md): JSON transcoding and server hosts
- [Service workspaces](surface/service-workspace.md): persistent service profiles and descriptors
- [Framework integrations](surface/framework-integrations.md): Spring Boot and Quarkus
- [gRPC tools over MCP](apps/grpc-mcp.md): a stdio application that reflects and invokes one gRPC endpoint

The [generated action inventory](generated/action-inventory.json) lists each
built-in action exposed by the standalone and full catalogs.

## Applications and operations

- [Command line](apps/cli.md): invoke actions from a shell or interactive console
- [Console](apps/console.md): schemas, workflows, services, search, metrics, and receipts in a browser
- [Agent host](apps/agent-host.md): attach resumable Codex and Kimi processes to delegation
- [Task console](apps/task-console.md): inspect and guide durable multi-agent tasks in a browser
- [Record verifier](apps/record-verifier.md): zero-dependency offline verification of signed work records
- [Docker](apps/docker.md): container images and Compose setup
- [macOS (.dmg)](apps/serve-macos.md): protomolt-serve as a self-contained Apple-silicon app with an embedded runtime
- [Document platform](apps/document-platform.md): the one-container document pipeline (repo, intake, parse, jobs, registry, search, console, playground)
- [Bounded Redis archive](apps/bounded-archive.md): run or embed managed archive entries and retained versions on Redis and PostgreSQL
- [Role nodes](apps/role-nodes.md): the same binary as specialized nodes via PROTOMOLT_ROLES and remote role targets
- [Coding workers](apps/coding-workers.md): Java and C++ agent environments, state, credentials, and transport boundaries
- [Building and testing](operations/building.md): builds, tests, linting, and publishing
- [Archive backup and cold restore](operations/archive-backup-and-restore.md): the qualified offline backup and cold-restore procedure for the archive stack
- [Environment variables](operations/environment-variables.md): every variable each service reads, with defaults and failure behavior
- [Nano1 ARM64 node](../deploy/nano1/README.md): native image builds and GPU inference boundary
- [Outbound gRPC policy](operations/grpc-channel-policy.md): target, transport, deadline, and concurrency limits

## Tutorials

- [Run your first workflow](tutorials/first-workflow.md): published Compose bundle,
  browser launch, expected results, and failure diagnosis
- [Use the protobuf toolkit](../examples/protobuf-toolkit/README.md): a standalone
  consumer for mapping, selectors, projections, validation, registry, and OpenAPI
- [Connect a gRPC service](tutorials/connect-grpc-service.md): register, inspect,
  and invoke the same service workspace through MCP and remote ACP
- [Project data and run an accepted workflow](tutorials/projected-workflow.md):
  local projection and validation followed by asynchronous service execution
- [Optional Kafka and JDBC inputs](tutorials/optional-inputs.md): runnable
  integration checks, deployment references, and contract boundaries
- [OpenVINO from an AI agent](tutorials/openvino.md)
- [Python clients without protoc](tutorials/python.md)
- [Streaming through the ACP agent](tutorials/streaming.md)
- [Earning a signed receipt from a delegated task](tutorials/delegation-receipt.md)
- [Recorded correction with a verifiable assessment](tutorials/recorded-correction.md)
- [Forward an accepted workflow launch through Kafka](tutorials/kafka-accepted-workflow.md): an optional local broker bridge to the published coordinator
- [Adoption walkthrough checklist](tutorials/adoption-checklist.md): an uncoached first-use test for someone new to ProtoMolt

## Architecture and records

These design documents describe current behavior or current plans:

- [Planned work](design/planned-work.md): open product and hardening work
- [Work tiers and skill tags](design/work-tags.md): the vocabulary offers and workers use to describe delegated work; the `protomolt-worker` skill under `.claude/skills/` walks a session through both roles
- [Metric mappings](design/metric-mapping.md): governed aggregate queries on a mapping subject (Cube comparison, option dialect, `query-metrics`)
- [Signed work records](design/receipts.md): portable, signed, offline-verifiable receipts projected from existing evidence
- [The archive](design/archive.md): entries with named renditions, retained versions via entry-local content addressing, and three upload doors
- [Asset formats and characterization](design/asset-formats.md): typed formats with strict rules, classification as a state machine, content classes, and format bridging
- [Authorization scopes](design/authorization-scopes.md): named principals, a closed scope vocabulary, and per-operation refusals
- [Intake and parsing](design/intake-and-parsing.md): platform ingestion architecture
- [Document platform](design/document-platform.md): repository and account service architecture
- [Optional action providers](architecture/action-providers.md): runtime composition of the action catalog through `ActionProvider`
- [SPI composition and dependency boundaries](architecture/spi-modularity.md): module extraction and the dependencies each SPI may carry
- [Config distribution](design/config-distribution.md): how configuration moves between nodes in a scaled deployment
- [Metric joins](design/metric-joins.md): joins on the metric surface
- [Screening](design/screening.md): model-driven PII and entity detection
- [Well-known types](design/well-known-types.md): the validated typed commons, wire-plain and server-strict
- [Archive version metadata](design/archive-version-metadata.md): descriptive metadata snapshots for archive versions
- [Worker harness guide](design/worker-harness-guide.md): failing and working invocations for delegation workers
- [Website feature inventory](website-feature-inventory.md): capability inventory prepared for the website team

The repository design source of record is
[parts-based repository requirements](design/parts-based-repository-requirements.md).

## Design history and work records (non-normative)

Everything in this section is a historical planning, audit or review
artifact. These documents record proposals and the state of the tree at the
time they were written; they name types and components that may no longer
exist (or never existed) and are not a description of the current code. The
live guides above and the code itself take precedence on any disagreement.

- [PipeStream protobuf mesh](design/pipestream-protobuf-mesh/README.md):
  a design proposal for contract-driven `Any` routing, service
  advertisement, recursive processing, LLM software generation, and OpenNLP
  PII policy across mesh nodes; only parts of it are built
- [Repo service contract and component mapping](design/repo-v2-contract-draft/component-mapping.md):
  a draft that mapped the v2 contract onto the components of its time
- [Source audit](design/protobuf-toolkit-audit.md) and
  [source/test index](design/protobuf-toolkit-source-index.md): the dated
  October 13 reconciliation records
- [Plans](plans): goal plans, inventories and verification work logs
- [Review records](reviews): dated correctness and security reviews
- [Evidence](evidence): captured outputs backing specific past goals
