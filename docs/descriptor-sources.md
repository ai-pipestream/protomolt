# Descriptor sources

Every ProtoMolt feature consumes descriptors through one abstraction:
`DescriptorRegistry` (in `protomolt-descriptors`), fed by implementations of
the `DescriptorLoader` SPI. The registry aggregates any number of loaders,
caches descriptors by full and simple type name, resolves types on demand,
and negative-caches misses. Google's well-known types are registered at
construction.

```java
public interface DescriptorLoader {
    List<FileDescriptor> loadDescriptors() throws DescriptorLoadException;
    FileDescriptor loadDescriptor(String fileName) throws DescriptorLoadException;
    default FileDescriptor loadDescriptorForType(String fullTypeName) throws DescriptorLoadException;
    boolean isAvailable();
    String getLoaderType();
}
```

```java
var registry = DescriptorRegistry.create();
registry.addLoader(loader);
Descriptor desc = registry.findDescriptorByFullName("com.example.Person");
```

| Loader | Artifact | Source |
|---|---|---|
| `ClasspathDescriptorLoader` | `protomolt-descriptors` | Generated protobuf classes on the classpath |
| `GoogleDescriptorLoader` | `protomolt-descriptors` | Binary `FileDescriptorSet` as a classpath resource |
| `ApicurioDescriptorLoader` | `protomolt-schema-apicurio` | Apicurio Registry v3 |
| `ConfluentSchemaRegistryLoader` | `protomolt-schema-confluent` | Confluent Schema Registry subjects API |
| `ConfluentDescriptorSource` | `protomolt-schema-confluent` | Binary `FileDescriptorSet` over HTTP or from the classpath |
| `GatheringDescriptorLoader` | `protomolt-gather` | Any [gatherer](gathering.md): local directories, jars, Git repositories, Maven coordinates |

Text-based sources (the registry loaders, every gatherer) share one
compilation pipeline: `ProtoSourceCompiler` in `protomolt-proto-sources`,
which links `.proto` text with Square Wire and builds runtime descriptors —
no `protoc` binary involved. The write-side counterpart of this SPI is
[schema publishing](publishing.md).

## Classpath classes

`ClasspathDescriptorLoader` resolves a type name as a Java class name via
`Class.forName` and calls the generated `getDescriptor()` reflectively. It is
on-demand only: `loadDescriptors()` deliberately returns nothing, because
scanning the entire classpath is expensive and rarely wanted.

## Descriptor sets on the classpath

`GoogleDescriptorLoader` reads a compiled `FileDescriptorSet` from a
classpath resource — by default `META-INF/grpc/services.dsc`, the path
Quarkus gRPC codegen (and the
[quarkus-grpc-gatherer](https://github.com/ai-pipestream/quarkus-grpc-gatherer)
build plugin) emits. `searchPaths(...)` accepts alternatives and uses the
first that exists. The loader links file dependencies itself, detects
dependency cycles (failing with the offending chain), and falls back to
bundled well-known types when a set omits them.

The static `GoogleDescriptorLoader.fromDescriptorSet(FileDescriptorSet)`
builds runtime descriptors from an in-memory set and is reused by the
Confluent sources.

## Apicurio Registry

```groovy
implementation 'ai.pipestream:protomolt-schema-apicurio'
```

`ApicurioDescriptorLoader` speaks Apicurio Registry's native v3 API through
the Apicurio SDK. Bulk loading searches a group for `PROTOBUF` artifacts
(paged); single-type lookup tries the artifact ID within the configured
group, then a dotted `group/artifact` split, then the `default` group.

```java
var loader = ApicurioDescriptorLoader.builder()
    .registryUrl("http://localhost:8080/apis/registry/v3")
    .groupId("default")
    .registryClient(client)
    .build();
registry.addLoader(loader);
```

Registry artifact references (protos importing other registered artifacts)
are resolved recursively with cycle protection. An artifact whose references
cannot be resolved is skipped with a warning rather than failing the load.

Under Quarkus, the loader is wired from configuration:

```properties
pipestream.proto.apicurio.enabled=true
pipestream.proto.apicurio.registry-url=http://localhost:8080/apis/registry/v3
pipestream.proto.apicurio.group-id=default
pipestream.proto.apicurio.auto-load-on-startup=false
```

### Parsing without the registry

`ApicurioProtobufParseFallback` handles the case where the registry is down
but the payload type is known: it strips the Apicurio/Confluent Kafka
wire-format prefix (magic byte, content ID, message indexes) and parses the
remaining bytes with the given generated type.

```java
Struct msg = ApicurioProtobufParseFallback.forType(Struct.class).parse(wireBytes);
```

For Kafka consumers on the Apicurio serde, prefer the serde's own
configuration flag where available.

## Confluent-compatible registries

```groovy
implementation 'ai.pipestream:protomolt-schema-confluent'
```

The module ships two independent sources.

`ConfluentSchemaRegistryLoader` speaks the Confluent Schema Registry subjects
REST protocol, which is also served by Apicurio's ccompat facade. It lists
subjects, fetches `PROTOBUF` schema text for the latest version of each,
resolves schema references (`{name, subject, version}`) recursively with
cycle protection, and compiles the resulting source graph to runtime
`FileDescriptor`s using Square Wire. Non-protobuf subjects are ignored.
A subject that cannot be loaded — dangling reference, unparseable text,
missing version — is skipped with a warning (the count is exposed via
`lastSkippedSubjectCount()`), while authentication failures, server errors,
and I/O failures abort the load. Type lookups are cached briefly (30 s) to
keep repeated resolution cheap.

`ConfluentDescriptorSource` is the binary path: it consumes an
already-compiled `FileDescriptorSet` from an HTTP endpoint or a classpath
resource, with configurable connect and request timeouts. Use it when a build
pipeline publishes descriptor sets and you do not want runtime compilation.

## Schema hygiene

`protomolt-helpers` provides pre-flight checks for schemas headed to a
registry:

```java
ProtoFqnConflictDetector.validateAndAssertNoConflicts(Map.of(
    "ref-a", fileDescriptorProtoA,
    "ref-b", fileDescriptorProtoB));
BinaryProtobufIdentifierValidator.validate("upload", fileDescriptorProto);
```

These reject illegal identifiers in binary descriptors and cross-file
fully-qualified-name conflicts that would change wire shape.

## Testing against real registries

Both registry modules include integration tests that run against live
registries; see [Building and testing](building.md) for the Docker Compose
setup and endpoint configuration.

## Current limitations

- `ApicurioDescriptorLoader` resolves references between registered
  artifacts, but Confluent loaders and Apicurio use different reference
  models; a subject or artifact with references outside its registry is
  skipped rather than fetched cross-registry.
- The registry loaders speak anonymous HTTP; authenticated registry access
  is limited to what the Apicurio SDK client is configured with and the Git
  gatherer's token/basic auth.
