# Embedded protoc wrapper provenance

ProtoMolt bundles `protoc-wrapper-v4.wasm` so code generation can run inside
the JVM without a native protoc installation. The binary is not an opaque
download: it is the output of the protobuf4j build recorded below.

## Source

- Repository: `https://github.com/ai-pipestream/protobuf4j.git`
- Source and build commit: `359ad92e3e6ba46b59d99ce51417ac35730a9abb`
- Build target: `make build-v4`
- Output: `wasm/protoc-wrapper-v4.wasm`
- SHA-256: `d313a23b806a089c39b493049f4c94c7be938ba6a62c76b0b6f8c8fa02017baf`

That source revision builds the wrapper in an Ubuntu 24.04 container with:

- Protocol Buffers `v34.1`
- grpc-java compiler plugin `v1.79.0`
- WASI SDK `25.0`
- Binaryen `123`

It contains protoc's Java, Kotlin, Python, C++, C#, Ruby, PHP, and Objective-C
generators together with the grpc-java plugin. ProtoMolt executes the module
directly with Chicory; protobuf4j is a build-time provenance source, not a
ProtoMolt runtime dependency.

## Verification and update procedure

The `verifyEmbeddedWasm` Gradle task is part of `check` and fails if the
bundled bytes do not match the recorded checksum. To reproduce an update:

```shell
bash core/codegen/provenance/rebuild-protoc-wrapper-v4.sh
./gradlew :protomolt-codegen:verifyEmbeddedWasm
./gradlew :protomolt-codegen:test
```

The rebuild script checks out the exact source revision above, builds the v4
module through protobuf4j's containerized toolchain, copies it into ProtoMolt,
and verifies the result. A deliberate source or toolchain update must change
the pinned commit, the component versions in this record, and the checksum in
the same review.

The source recipe pins component versions but its Ubuntu base image and
downloaded tool archives are not yet pinned by digest and checksum. The build
therefore has a recorded and verifiable result, but is not claimed to be a
fully hermetic build. License and NOTICE attribution for every component in
the combined binary remains a separate release audit.
