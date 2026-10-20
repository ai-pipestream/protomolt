# protobuf/

The annotation dialects and their readers: option namespaces that extend
`google.protobuf` field and message options so a schema declares its own
validation rules, metadata, quality dimensions, LLM instructions, metric
members and structured-data mapping, plus the engines that read them.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-protobuf-validation` | `protobuf/validation` | The `validate.v1` dialect and the validation engine (`ProtoValidator`), with a neutral rule-source SPI |
| `:protomolt-protobuf-validation-protovalidate` | `protobuf/validation-protovalidate` | The vendored `buf.validate` dialect as a ServiceLoader-registered rule source |
| `:protomolt-protobuf-validation-conformance` | `protobuf/validation-conformance` | Drives `ProtoValidator` against buf's protovalidate conformance suite |
| `:protomolt-protobuf-metadata` | `protobuf/metadata` | The `meta.v1` dialect: description, owner, sensitivity class, labels |
| `:protomolt-protobuf-quality` | `protobuf/quality` | The `quality.v1` dialect: CEL-scored dimensions composed as a weighted mean |
| `:protomolt-protobuf-llm` | `protobuf/llm` | The `llm.v1` dialect: fill instructions, safeguards, volatility |
| `:protomolt-protobuf-prompt` | `protobuf/prompt` | Renders a descriptor into a complete LLM form-filling briefing |
| `:protomolt-protobuf-metric` | `protobuf/metric` | The `metric.v1` dialect: measures and dimensions declared on schemas (proto only) |
| `:protomolt-protobuf-seo` | `protobuf/seo` | Search-metadata standard: Dublin Core and schema.org structured data |
| `:protomolt-protobuf-types` | `protobuf/types` | Well-known structural types whose invariants ride the schema (ranges, taxonomies, access policies) |

Docs: [validation](../docs/transform/validation.md),
[quality scoring](../docs/transform/quality.md),
[schema metadata](../docs/schema/metadata.md),
[well-known types](../docs/design/well-known-types.md); the annotation family
overview is in the [root README](../README.md#the-nine-annotation-families).
