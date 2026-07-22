# Operating an OpenVINO server from an AI agent

This tutorial takes you from *"I have an OpenVINO Model Server running
somewhere"* to *"my AI agent introspects its models and runs inference through
it as first-class tools"* — with no protoc installed, no generated stubs
checked in, and no schema written by hand.

It is a concrete walkthrough of [the gRPC agent workflow](../mcp.md#the-grpc-agent-workflow):
reflect the server, fall back to its published schema when reflection is off,
register that schema once, then introspect, generate a client, and infer.

Every command and every output below was run against a real OpenVINO Model
Server (`OpenVINO Model Server 2026.1.0`) serving a MiniLM sentence-embedding
pipeline over the KServe v2 gRPC API.

## What you need

- An **OpenVINO Model Server** reachable over gRPC. OVMS speaks the KServe v2
  / Open Inference Protocol (`inference.GRPCInferenceService`). This tutorial
  uses a host:port of `ovms-host:9000`; substitute your own.
- **JDK 21+** to build and run ProtoMolt.
- **[`grpcurl`](https://github.com/fullstorydev/grpcurl)** — a handy
  command-line gRPC client, used here to show the reflection result plainly
  before we bring the agent in.

## 1. Get ProtoMolt

Clone and build the MCP server that an agent will talk to:

```shell
git clone https://github.com/ai-pipestream/protomolt.git
cd protomolt
./gradlew :protomolt-mcp:installDist
```

The launcher is now at
`surface/mcp/build/install/protomolt-mcp/bin/protomolt-mcp`. Pick a directory
for it to use as a registry — it does not need to exist; the store creates
and initializes the Git repository on first use.

## 2. Try reflection first

The best case is that the server describes itself. Ask it with `grpcurl`:

```shell
grpcurl -plaintext ovms-host:9000 list
```

```
Failed to list services: server does not support the reflection API
```

OVMS — like NVIDIA Triton and many other production servers — does not enable
gRPC server reflection. ProtoMolt's `reflect` verb reports the same thing, but
as a structured result rather than an error, which is exactly what lets an
agent decide to fall back:

```json
// reflect { "target": "ovms-host:9000" }
{ "ok": false, "error": "Reflection stream failed: UNIMPLEMENTED" }
```

That `ok: false` is the fork in the road. When reflection works, you already
have the schema. When it does not, you fetch the schema from where the project
publishes it — which for KServe is a Git repository.

## 3. Bring the schema in from Git

The KServe gRPC contract lives in the
[`kserve/open-inference-protocol`](https://github.com/kserve/open-inference-protocol)
repository as a single self-contained `.proto`. ProtoMolt gathers `.proto`
sources straight from Git — this is the "point it at the repo" step. In Java:

```java
ProtoGatherer gatherer = GitProtoGatherer.builder()
        .repo("https://github.com/kserve/open-inference-protocol.git")
        .ref("main")
        .paths("specification/protocol/open_inference_grpc.proto")
        .build();

// Publish it into your registry so every consumer shares one copy.
var store  = GitSchemaRegistryStore.builder().repositoryDir(Path.of("/srv/schemas.git")).build();
var server = new SchemaRegistryServer(SchemaRegistryServerConfig.defaults(), store,
        ActionCatalog.defaults(ActionContext.create()));
server.start();
new ConfluentSchemaPublisher(URI.create("http://localhost:8081"))
        .publish(gatherer.gather(), PublishOptions.defaults())
        .throwIfFailed();
```

The KServe schema is now a subject in your registry, versioned like any other.
An agent reads it as a resource; a human resolves it by type name. You never
wrote a line of it.

> If you just want to try the flow without a registry, you can also paste the
> `.proto` text inline as the `sources` schema on any tool call — the registry
> is the durable, shareable version of the same thing.

## 4. Start the agent

Point your MCP client at the launcher with the registry mounted. For Claude
Code:

```shell
claude mcp add protomolt -- \
  /path/to/protomolt/surface/mcp/build/install/protomolt-mcp/bin/protomolt-mcp \
  --registry-git /srv/schemas.git
```

The agent now has twenty-two tools and can browse your registry — including the
KServe schema you just published — as resources.

## 5. Introspect the server and its models

First confirm the server is alive and identify it (`grpc-invoke` with the
KServe schema resolved from the registry):

```json
// grpc-invoke { "target": "ovms-host:9000",
//               "method": "inference.GRPCInferenceService/ServerMetadata",
//               "schema": { "type": "inference.ServerMetadataRequest" ... },
//               "request": {} }
{ "ok": true, "status": "OK",
  "responses": [{ "name": "OpenVINO Model Server", "version": "2026.1.0..." }] }
```

Then ask a model to describe its own tensor interface — this is where the
agent learns what the model actually takes and returns:

```json
// grpc-invoke ... "method": ".../ModelMetadata", "request": { "name": "embedding_minilm" }
{ "ok": true, "responses": [{
  "name": "embedding_minilm", "platform": "OpenVINO", "versions": ["1"],
  "inputs": [
    { "name": "attention_mask", "datatype": "INT64", "shape": ["-1", "-1"] },
    { "name": "input_ids",      "datatype": "INT64", "shape": ["-1", "-1"] }],
  "outputs": [
    { "name": "sentence_embedding", "datatype": "FP32", "shape": ["-1", "384"] },
    { "name": "token_embeddings",   "datatype": "FP32", "shape": ["-1", "-1", "384"] }]
}]}
```

The agent now knows the embedding model takes token tensors and returns a
384-dimensional vector — discovered live, from the running server.

## 6. Generate a native client

For anything tensor-heavy, hand-authoring message JSON is the wrong tool.
`generate-stubs` produces a real client from the same schema, no protoc
required:

```json
// generate-stubs { "schema": { "type": "inference.ModelInferResponse" ... },
//                  "generators": ["python"] }
{ "ok": true, "files": [{ "name": "..._pb2.py", "generator": "python", "content": "..." }] }
```

The same call with `["java", "grpc-java"]` yields a complete Java gRPC client;
`cpp`, `csharp`, `ruby`, `php`, `kotlin`, and `objc` are all available. This is
[quarkus-grpc-zero](https://github.com/ai-pipestream/quarkus-grpc-zero) as a
live call instead of a build step.

## 7. Run inference: text to embedding

The MiniLM pipeline is two models — a tokenizer and the embedder:

- `tokenizer_minilm`: a string in (`Parameter_1`, `BYTES`) → `input_ids`,
  `attention_mask`, `token_type_ids` (`INT64`).
- `embedding_minilm`: those token tensors in → `sentence_embedding`
  (`FP32`, 384) out.

An agent chains them with two `ModelInfer` calls. Tokenize first:

```json
// grpc-invoke ... "method": ".../ModelInfer",
//   "request": { "model_name": "tokenizer_minilm",
//     "inputs": [{ "name": "Parameter_1", "datatype": "BYTES", "shape": ["1"],
//                  "contents": { "bytes_contents": ["<base64 of your text>"] } }] }
{ "ok": true, "responses": [{
  "outputs": [
    { "name": "input_ids",      "datatype": "INT64", "shape": ["1", "13"] },
    { "name": "attention_mask", "datatype": "INT64", "shape": ["1", "13"] }, ... ],
  "rawOutputContents": [ "<base64 packed int64s>", ... ] }]}
```

Then feed `input_ids` and `attention_mask` into the embedder and read
`sentence_embedding`. The result, for the text
`"ProtoMolt makes any gRPC service agent-native"`:

```
sentence_embedding: shape [1, 384], 384 dims
first 6 dims: [-0.0568, -0.0415, -0.0492, -0.1137, 0.0086, -0.0807]
L2 norm: 0.9999
```

A real, normalized MiniLM sentence embedding — produced end to end through the
MCP server, against the running OpenVINO backend.

### One honesty note on tensors

KServe servers return tensor data in `rawOutputContents`: little-endian packed
bytes, ordered to match the `outputs` list, not decoded into JSON arrays. Over
`grpc-invoke` you unpack them yourself (an `int64` is 8 bytes, an `fp32` is 4).
That unpacking is precisely what a **generated client does for you
automatically** — which is why, past a quick introspection or a one-off call,
the natural move is step 6: generate the stub and do tensor I/O in a real
client (Python with NumPy, Java with the generated messages, and so on).

## Where you land

With the KServe schema registered once, the OpenVINO server is a first-class
citizen of your agent's world:

- **Introspection** — the agent reads any model's tensor contract from the
  live server.
- **Inference** — the agent calls `ModelInfer` directly, or generates a native
  client to do it at scale.
- **Reuse** — the same registered schema gives every other language in your
  fleet a generated client, and every registered gRPC service the same
  treatment.

Nothing here was OpenVINO-specific past the model names: any KServe or Triton
server works identically, and any gRPC service at all — reflection-enabled or
schema-published — becomes agent-operable the same way.

## Related

- [MCP server](../mcp.md) — the tool surface and the general gRPC-agent workflow
- [Gathering proto sources](../gathering.md) — the Git gatherer used in step 3
- [The registry](../registry.md) — where the KServe schema lives after step 3
