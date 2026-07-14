# Schema metadata

ProtoMolt supports two complementary kinds of metadata: descriptive metadata
declared on schemas as descriptor options, and runtime metadata extracted
from message contents with CEL selectors.

## Declared metadata (`protomolt-protobuf-metadata`)

The metadata standard defines `FieldOptions` / `MessageOptions` extensions
under `ai.pipestream.proto.meta.v1` for descriptive and operational
metadata — ownership, sensitivity, descriptions:

```protobuf
import "ai/pipestream/proto/meta/v1/metadata.proto";

message Doc {
  option (ai.pipestream.proto.meta.v1.message) = {
    owner: "search-platform"
    sensitivity: "internal"
  };
  string doc_id = 1 [(ai.pipestream.proto.meta.v1.field) = {
    description: "Stable id"
    sensitivity: "public"
  }];
}
```

`DescriptorMetadata` reads the options back at runtime. When descriptors are
parsed from a `FileDescriptorSet`, register the extensions first so the
options materialize:

```java
DescriptorMetadata.registerExtensions(extensionRegistry);
Map<String, Object> bag = DescriptorMetadata.asBag(Doc.getDescriptor());
```

## Extracted metadata (`protomolt-metadata`)

`MetadataExtractor` builds a named metadata bag from a message instance
using CEL selector expressions — for example, pulling routing keys or audit
fields out of arbitrary payloads. Selectors are validated eagerly against a
typed CEL environment per descriptor, results are cached per descriptor,
and environments warm up in parallel on virtual threads.

Declared metadata describes the schema; extracted metadata describes a
message. They compose: a pipeline can annotate documents with both the
schema's ownership tags and values selected from the message itself.
