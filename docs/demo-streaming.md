# Demo: streaming a gRPC call live through the ACP agent

This demo shows the streaming path end to end: a server-streaming gRPC method
is invoked through the catalog, and each response arrives as its own ACP chunk
while the call is still running. Three moving parts, one command each.

- `DemoSearch` (`samples`): a server-streaming search on `localhost:9777` that
  emits five hits, best first, 400 ms apart — slow enough to watch.
- `protomolt-acp`: the action catalog as an ACP agent; `grpc-invoke` streams
  per response through it.
- `AcpStreamingDemo` (`samples`): a driver that launches the agent, prompts
  it, and prints every chunk with its arrival time.

## Run it

Terminal 1 — install the agent and start the demo server:

```shell
./gradlew :protomolt-acp:installDist :samples:runDemoSearch
```

Terminal 2 — drive the agent:

```shell
./gradlew :samples:runAcpStreamingDemo
```

Expected output (times are since launch, proto3 JSON field names are camelCase):

```
prompt: grpc-invoke demo.search.v1.DemoSearch/Search (5 hits, 400ms apart)
[+  1.0s] {   "docId" : "doc-1",   "score" : 0.98,   "text" : "approximate nearest neighbor search with HNSW graphs" }
[+  1.4s] {   "docId" : "doc-2",   "score" : 0.91,   "text" : "vector quantization for billion-scale indexes" }
[+  1.8s] {   "docId" : "doc-3",   "score" : 0.84,   "text" : "recall at high k: merging partial results from many shards" }
[+  2.2s] {   "docId" : "doc-4",   "score" : 0.77,   "text" : "cross-encoder reranking of the top candidates" }
[+  2.6s] {   "docId" : "doc-5",   "score" : 0.7,   "text" : "embedding equivalence across serving runtimes" }
[+  3.1s] {   "ok" : true,   "status" : "OK" }
```

The 400 ms gaps are the point: chunks land while the gRPC stream is still
open, and the terminal `{"ok": true, "status": "OK"}` document marks the end
of the run.

## The same thing in an IDE

Point a custom ACP agent (JetBrains AI chat, or Zed) at
`acp/build/install/protomolt-acp/bin/protomolt-acp`, then prompt:

```
grpc-invoke {"target":"localhost:9777","method":"demo.search.v1.DemoSearch/Search","schema":{"sources":{"demo.proto":"syntax = \"proto3\"; package demo.search.v1; service DemoSearch { rpc Search(SearchRequest) returns (stream SearchHit); } message SearchRequest { string query = 1; int32 hits = 2; int32 delay_ms = 3; } message SearchHit { string doc_id = 1; float score = 2; string text = 3; }"}},"request":{"query":"nearest neighbor search","hits":5,"delayMs":400}}
```

The hits render one by one in the chat, exactly like the terminal output.

## The real search

The demo shape is not a toy: the knn-node `Search` RPC is server-streaming
too. When a search node is up, the same `grpc-invoke` prompt against its
`Search` streams real hits into the IDE with no code changes — swap the
target, method, and schema.

## How it works

- `DemoSearch` emits each hit as a gRPC stream message.
- `grpc-invoke` (a `StreamingAction`) emits one document per message and a
  terminal status document at the end.
- The ACP agent sends each emitted document as its own session chunk.
