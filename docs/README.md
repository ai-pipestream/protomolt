# ProtoMolt documentation

The [project README](../README.md) has the build and first-run instructions.
The guides below follow the repository's module layout.

## Acquire

- [Stream connectors](acquire/connector.md): bounded, flow-controlled inputs
- [Gathering proto sources](acquire/gathering.md): filesystem, jar, Git, and Maven sources
- [Microsoft Graph](acquire/msgraph.md): OneDrive, SharePoint, and Copilot connectors

## Core and schema

- [Descriptor sources](core/descriptor-sources.md): descriptor loading and hygiene checks
- [Cluster directory](core/cluster-directory.md): memory-resident discovery with encrypted restart persistence
- [Core utilities](core/helpers.md): `Any`, `Struct`, conversion, and message comparison
- [Compatibility checking](schema/compatibility.md): wire, JSON, and source compatibility
- [JSON Schema generation](schema/json-schema.md): draft 2020-12 output from protobuf descriptors
- [Schema metadata](schema/metadata.md): ownership, sensitivity, descriptions, and extraction
- [Publishing schemas](schema/publishing.md): Confluent and Apicurio publishers
- [Registry](schema/registry.md): Git storage, compatibility gates, and registry protocols

## Transform and execution

- [Field mapping](transform/mapping.md): path rules and CEL expressions
- [Field masking](transform/masking.md): remove, redact, encrypt, and decrypt policies
- [Projections](transform/projection.md): descriptor-declared message projections
- [Validation](transform/validation.md): rule dialects, protovalidate, and gRPC enforcement
- [Quality scoring](transform/quality.md): CEL-based dimensions and weighted scores
- [Joins and derived shapes](transform/join-shapes.md): joins, unions, merges, and generated shapes
- [Chain manager](transform/chain-manager.md): checked serial compositions of gRPC calls
- [Recipes and run evidence](transform/recipes.md): record, replay, promotion, and structured steps
- [Pipelines](transform/pipeline.md): checked execution across all gRPC streaming shapes
- [Agent delegation](transform/delegation.md): coordinator and worker streaming contract

## Search and sinks

- [Search indexing](search/indexing.md): index plans and Lucene, OpenSearch, and Solr output
- [Text embeddings](search/embeddings.md): Model2Vec, TEI, and OVMS providers
- [Reranking](search/rerank.md): TEI and OVMS rerank providers
- [Emitting bundles](sink/emitting.md): directory, Git, zip, OKF, and Parquet sinks
- [Apache Iceberg](sink/iceberg.md): descriptor-driven tables and snapshot appends
- [Kafka Connect](sink/kafka-connect.md): gRPC source, sink, and protobuf transforms
- [Kafka serde](sink/kafka-serde.md): Confluent wire format with schema enforcement

## Surfaces

- [Actions](surface/actions.md): the self-describing verb catalog
- [MCP server](surface/mcp.md): tools, resources, service discovery, and invocation
- [ACP agent](surface/acp.md): stdio integration for ACP clients
- [gRPC service](surface/grpc-service.md): typed RPCs, reflection, REST, OpenAPI, and launcher
- [REST gateway](surface/rest-gateway.md): JSON transcoding and server hosts
- [Service workspaces](surface/service-workspace.md): persistent service profiles and descriptors
- [Framework integrations](surface/framework-integrations.md): Spring Boot and Quarkus

The [generated action inventory](generated/action-inventory.json) lists each
built-in action exposed by the standalone and full catalogs.

## Applications and operations

- [Command line](apps/cli.md): invoke actions from a shell or interactive console
- [Agent host](apps/agent-host.md): attach resumable Codex and Kimi processes to delegation
- [Task console](apps/task-console.md): inspect and guide durable multi-agent tasks in a browser
- [Docker](apps/docker.md): container images and Compose setup
- [Coding workers](apps/coding-workers.md): Java and C++ agent environments, state, credentials, and transport boundaries
- [Building and testing](operations/building.md): builds, tests, linting, and publishing
- [Nano1 ARM64 node](../deploy/nano1/README.md): native image builds and GPU inference boundary
- [Outbound gRPC policy](operations/grpc-channel-policy.md): target, transport, deadline, and concurrency limits

## Tutorials

- [OpenVINO from an AI agent](tutorials/openvino.md)
- [Python clients without protoc](tutorials/python.md)
- [Streaming through the ACP agent](tutorials/streaming.md)

## Architecture and records

- [PipeStream protobuf mesh](design/pipestream-protobuf-mesh/README.md):
  contract-driven `Any` routing, service advertisement, recursive processing,
  LLM software generation, and OpenNLP PII policy across mesh nodes
- [Planned work](design/planned-work.md): open product and hardening work
- [Intake and parsing](design/intake-and-parsing.md): platform ingestion architecture
- [Document platform](design/document-platform.md): repository and account service architecture
- [Review records](reviews): dated correctness and security reviews
