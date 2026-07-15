# Actions

`protomolt-actions` is a catalog of verbs over the toolkit — compile,
validate, diff, check, render, evaluate — with one JSON envelope in and one
out. It exists for the edges: the registry console drives it over HTTP, and
an MCP mount can expose the same catalog as tools for LLM-driven schema
work. JSON is deliberately confined to this layer; every action wraps a
descriptor-native library underneath, and machine-to-machine paths should
prefer the binary endpoints (see [The registry](registry.md)).

```java
var catalog = ActionCatalog.defaults(ActionContext.create());
ObjectNode result = catalog.execute("check-compat", input);
```

Each action declares a name, a description written for tool use, and a JSON
Schema for its input, so `list()` is a complete, self-describing tool
manifest. Failures are structured — `{error, message, details?}` with
stable kebab-case codes (`unknown-type`, `invalid-input`, `compile-failed`,
`invalid-expression`, `mapping-failed`, …) — distinct codes for distinct
repair strategies, which matters when the caller is a model.

## The catalog

| Action | Does |
|---|---|
| `compile` | Compile inline `.proto` sources; returns file names and a base64 descriptor set |
| `list-types` | Enumerate messages, enums, and services with fields — the introspection/grounding verb |
| `validate-message` | Validate a JSON message against the rules on its schema; returns structured violations |
| `diff-schemas` | Typed change list between two schemas (rule, path, impacts) |
| `check-compat` | Compatibility verdict under a mode, with violations and the full change list |
| `render-json-schema` | JSON Schema (2020-12) for a message type |
| `render-index-mappings` | OpenSearch mappings / Solr schema / Lucene field specs from indexing hints |
| `eval-cel` | Evaluate a CEL expression against a message |
| `map-message` | Apply text and CEL mapping rules to a message |
| `synthesize-shape` | Derive a join/union output type (envelope, projection, or oneof union) from named sources; returns registrable proto source and implied rules |
| `join-messages` | Join named source messages into an authored target or a synthesized shape with scoped rules and CEL |
| `merge-schemas` | Merge two or more message types into one new type: clash report, caller-decided resolutions, then merged proto + join/union rulesets in one move |
| `check-rules` | Statically validate mapping rules and CEL (filters must be bool) against descriptors; sample messages upgrade the check to a dry run |
| `run-chain` | Execute an inline chain: serial unary gRPC calls, each request mapped from the chain input and prior steps' responses |
| `check-chain` | Verify a chain without running it: methods, step scopes, gates, and every mapping type-checked |
| `infer-schema` | Struct-to-proto: reverse-engineer a message type from JSON sample documents; returns registrable source + descriptor set |
| `mask-message` | Mask fields by their schema-declared sensitivity classes: remove, redact, or encrypt/decrypt (AES-GCM, field-bound versioned envelope), recursively including map values |
| `extract-metadata` | The declared metadata bag for a type |

Wherever an action takes a schema it accepts exactly one of three forms —
`{"type": "fully.qualified.Name"}` (resolved from the context's descriptor
registry), inline `{"sources": {...}, "root": ...}` (compiled per call), or
`{"descriptorSetBase64": ...}`. Inline and binary schemas are re-parsed
with the toolkit's option extensions registered, so validation rules,
metadata, and indexing hints behave identically however the schema arrived.

## The HTTP mount

Constructing the registry server with a catalog mounts it under the native
prefix: `GET /protomolt/actions` lists the manifest, and
`POST /protomolt/actions/{name}` executes — `unknown-action` maps to 404,
`invalid-input` to 400, other action errors to 422.

```java
var server = new SchemaRegistryServer(config, store,
        ActionCatalog.defaults(ActionContext.create()));
```
