# inference/

One typed contract in front of every model backend: the provider SPI and
model catalog, OpenAI-compatible chat transports for OpenVINO Model Server
and `/v1` backends (Ollama, vLLM, llama.cpp), the gRPC service, and
schema-constrained structured generation.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-inference-proto` | `inference/proto` | The inference contract: `InferenceService`, the catalog `ModelEntry`, provenance-carrying envelopes |
| `:protomolt-inference-spi` | `inference/spi` | The provider SPI, the thread-safe model catalog, the credential resolver and the shared resolution facade |
| `:protomolt-inference-openvino` | `inference/openvino` | The OpenVINO provider plus the shared OpenAI-compatible chat transport (unary and SSE) |
| `:protomolt-inference-openai` | `inference/openai` | The `openai` profile of that transport for `/v1` backends |
| `:protomolt-inference-service` | `inference/service` | The gRPC surface around the SPI facade, requests validated before any provider is touched |
| `:protomolt-inference-structured` | `inference/structured` | Fill a protobuf message with a model: prompt packet, JSON Schema decoder constraint, validate, bounded retry |

There is no `docs/inference` chapter; the closest guide is the
[OpenVINO tutorial](../docs/tutorials/openvino.md).
