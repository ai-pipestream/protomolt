# JSON Schema generation

`protomolt-jsonschema` renders any message type as a JSON Schema
(draft 2020-12) describing its canonical proto3 JSON form.

```java
Map<String, Object> schema = ProtoJsonSchemaGenerator.create().generate(Person.getDescriptor());
String json = ProtoJsonSchemaGenerator.create().generateJson(Person.getDescriptor());
```

Because the generator consumes the same neutral constraint model the
[validator](validation.md) evaluates, declared validation rules land as real
JSON Schema keywords — for any `ValidationRuleSource` dialect, including
protovalidate annotations:

| Validation rule | JSON Schema keyword |
|---|---|
| `min_len` / `max_len` | `minLength` / `maxLength` |
| Numeric bounds (`gt`, `gte`, `lt`, `lte`) | `minimum`, `maximum`, `exclusiveMinimum`, `exclusiveMaximum` |
| `in` | `enum` |
| `pattern` | `pattern` |
| `min_items` / `max_items` / `unique` | `minItems` / `maxItems` / `uniqueItems` |
| Map `keys` | `propertyNames` |
| `email`, `uuid`, `hostname`, `uri`, `ipv4`, `ipv6` | the corresponding `format` values |

CEL rules cannot be expressed in JSON Schema; they are surfaced verbatim
under the `x-pipestream-cel` vendor extension so consumers can display or
enforce them separately.

Structural details follow proto3 JSON conventions: message types are defined
once under `$defs` with recursion-safe `$ref`s; 64-bit integers accept both
the JSON number and string spellings; enums accept declared names or
numbers. Two rule families are intentionally not mapped, as JSON Schema has
no faithful equivalent: bytes length rules and timestamp/duration bounds
(both are documented in the generator's Javadoc).
