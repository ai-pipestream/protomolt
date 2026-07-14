# Python clients without protoc

This tutorial takes a Python developer from *"there's a gRPC server at this
address"* to *"my Python code calls it natively"* — with no protoc installed,
no `grpcio-tools`, and no generated stubs checked in. ProtoMolt discovers the
schema and mints the Python message modules; your side needs exactly two pip
packages:

```shell
pip install grpcio requests
```

Every command and output below was run against a live server. The target
here is ProtoMolt's own gRPC service (so the tutorial is self-contained —
one process is both the toolkit and the demo target), but nothing is
specific to it: any reflection-enabled gRPC server works identically, and
servers without reflection work from a registered schema instead (see
[the gRPC agent workflow](../mcp.md#the-grpc-agent-workflow)).

## 1. Start ProtoMolt

```shell
docker run -p 8080:8080 -p 9090:9090 ghcr.io/ai-pipestream/protomolt-serve --demo
```

(Or run the launcher from a [release zip or a clone](../grpc-service.md#running-everything-protomolt-serve).)
`--demo` seeds a sample order-management schema, so every call below has
material to work with.

## 2. Reflect the server

Its address is all we know. `Reflect` returns the service names and a
descriptor set:

```python
import requests

BASE = "http://localhost:8080"

def verb(name, body):
    r = requests.post(f"{BASE}/grpc-json/ProtoMoltService/{name}", json=body)
    r.raise_for_status()
    return r.json()

reflected = verb("Reflect", {"target": "localhost:9090"})
print(reflected["services"])
```

```
['ai.pipestream.protomolt.v1.ProtoMoltService', 'grpc.reflection.v1.ServerReflection']
```

## 3. Generate the Python message modules

The reflected descriptor set is a schema input to every other verb —
including the code generator, which runs protoc's own Python generator as
WebAssembly on the server:

```python
import pathlib

generated = verb("GenerateStubs", {
    "schema": {"descriptorSetBase64": reflected["descriptorSetBase64"]},
    "generators": ["python"],
})

out = pathlib.Path("gen")
for f in generated["files"]:
    path = out / f["name"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f["content"])
```

```
gen/ai/pipestream/protomolt/v1/protomolt_service_pb2.py
```

Real `protoc --python_out` output, from a server, on demand.

## 4. Call the server with plain grpcio

`grpcio` can invoke any method given the path and the message classes — the
`*_pb2_grpc.py` convenience stubs are optional sugar, not a requirement:

```python
import sys
sys.path.insert(0, "gen")

import grpc
from ai.pipestream.protomolt.v1 import protomolt_service_pb2 as pb2

channel = grpc.insecure_channel("localhost:9090")
list_types = channel.unary_unary(
    "/ai.pipestream.protomolt.v1.ProtoMoltService/ListTypes",
    request_serializer=pb2.ListTypesRequest.SerializeToString,
    response_deserializer=pb2.ListTypesResponse.FromString,
)

response = list_types(pb2.ListTypesRequest(filter="demo.shop"))
print([t.full_name for t in response.types])
```

```
['demo.shop.v1.Customer', 'demo.shop.v1.LineItem', 'demo.shop.v1.Order',
 'demo.shop.v1.Order.Status', 'demo.shop.v1.GetOrderRequest',
 'demo.shop.v1.ListOrdersRequest', 'demo.shop.v1.OrderService']
```

Native protobuf over gRPC, typed end to end, from Python — and the message
classes were minted moments ago from a reflected schema.

## 5. Keep going

The same channel drives every verb. Compile a brand-new proto through the
typed surface:

```python
compile_rpc = channel.unary_unary(
    "/ai.pipestream.protomolt.v1.ProtoMoltService/Compile",
    request_serializer=pb2.CompileRequest.SerializeToString,
    response_deserializer=pb2.CompileResponse.FromString,
)
result = compile_rpc(pb2.CompileRequest(sources={
    "tutorial/v1/greeting.proto":
        'syntax = "proto3";\npackage tutorial.v1;\nmessage Greeting { string text = 1; }',
}))
print(result.ok, list(result.files))   # True ['tutorial/v1/greeting.proto']
```

Validate messages against the demo schema's declared rules, diff schema
versions, render JSON Schema — all thirteen verbs speak the same envelopes
whether you arrive over gRPC, REST, or MCP.

## One honesty note on service stubs

`generate-stubs` runs protoc's built-in generators, which for Python produce
message modules (`*_pb2.py`) — the `*_pb2_grpc.py` service-stub files come
from the separate `grpc_python` plugin, which ProtoMolt does not ship yet.
The `channel.unary_unary(...)` pattern above needs only the message classes
and is exactly what those stubs wrap. When the gRPC plugin series lands, the
same call mints both files and step 4 collapses into
`pb2_grpc.ProtoMoltServiceStub(channel)`.

## Related

- [The gRPC service](../grpc-service.md) — the typed surface this tutorial calls
- [MCP server](../mcp.md) — the same verbs as agent tools
- [Operating an OpenVINO server](openvino.md) — the same workflow against a
  production inference server
