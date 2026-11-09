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

## Pinned generator inputs (#828)

The upstream recipe at the pinned commit names versions but floats on digests:
a mutable image tag, unchecksummed tool archives, and git tags. The rebuild
script therefore applies `pin-generator-inputs.patch` to the checked-out
protobuf4j tree before building. The patch is reviewed against the exact
source commit above; if it stops applying, the pins must be re-derived in the
open, not worked around. Nothing in the patch changes what is built — only
which exact bytes may be built from:

| Input | Identity | Pin |
|---|---|---|
| Docker base image | `ubuntu:24.04` (OCI index) | `sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852` |
| apt packages (git, patch, curl, wget, cmake) | Ubuntu snapshot `20261024T120000Z` (the pinned base image's creation date, 2026-10-24 — update the two together). The snapshot host redirects to HTTPS and the base image ships no CA bundle, so the patch disables apt TLS peer verification; integrity and origin authentication come from apt's signed Release files, as with the plain-HTTP archive the recipe used before | `http://snapshot.ubuntu.com/ubuntu/20261024T120000Z/` |
| WASI SDK 25.0 `x86_64-linux` tarball | `wasi-sdk-25.0-x86_64-linux.tar.gz` | `52640dde13599bf127a95499e61d6d640256119456d1af8897ab6725bcf3d89c` |
| WASI SDK 25.0 `arm64-linux` tarball | `wasi-sdk-25.0-arm64-linux.tar.gz` | `47fccad8b2498f2239e05e1115c3ffc652bf37e7de2f88fb64b2d663c976ce2d` |
| Binaryen 123 `x86_64-linux` tarball | `binaryen-version_123-x86_64-linux.tar.gz` (matches upstream's published `.sha256`) | `e959f2170af4c20c552e9de3a0253704d6a9d2766e8fdb88e4d6ac4bae9388fe` |
| grpc-java `v1.79.0` source tarball | `github.com/grpc/grpc-java/archive/refs/tags/v1.79.0.tar.gz` (tag commit `381593f39e37b530177faad3c6035cfce4536ec7`) | `ca53210ee823a15d9b1ebe86a36b564d6834fd0c049e207469803180b7f8a20e` |
| Protocol Buffers `v34.1` | git clone; HEAD must equal the tag's commit before any use | `4b0c3aacf0657fbf38253b38918d3358dd4319ec` |
| Abseil `20250512.1` (CMake FetchContent in protobuf) | `GIT_TAG` rewritten from the tag name to its commit | `76bb24329e8bf5f39704eb10d21b9a80befa7c81` |

Archive SHA-256 values were computed from the archives as served by their
upstreams on 2026-11-08 (Binaryen's also matches its published `.sha256`
sidecar); the image digest is the Docker Hub OCI index digest for
`ubuntu:24.04` on the same date. In the patched recipe every archive is
checksum-verified before extraction, the protobuf checkout is verified
before its build system runs, and a wrong image digest fails the pull — in
each case before the input can execute. Protobuf's git submodules are pinned
by the verified commit's tree.

The pins make the rebuild hermetic with respect to its declared inputs. The
final `sha256sum --check` in the rebuild script and the `verifyEmbeddedWasm`
gate remain the arbiters of the output bytes: if a pinned-input rebuild ever
produces different bytes, the checksum, this record, and the review must move
together. Verified for #828 on 2026-11-08: a clean rebuild through
`rebuild-protoc-wrapper-v4.sh` with these pins reproduced the recorded
SHA-256 above byte-for-byte, with each archive's checksum verified in-build
before extraction.

## License and notice material

Components compiled into the binary are attributed in the repository `NOTICE`
(Protocol Buffers, utf8_range, Abseil, the grpc-java plugin, the LLVM runtime
libraries, and wasi-libc with its derived portions), with full license texts
under `licenses/` at the repository root. The `protomolt-codegen` jar packages
`LICENSE`, `NOTICE`, and those texts under `META-INF/` beside the binary.

## Verification and update procedure

The `verifyEmbeddedWasm` Gradle task is part of `check` and fails if the
bundled bytes do not match the recorded checksum. To reproduce an update:

```shell
bash core/codegen/provenance/rebuild-protoc-wrapper-v4.sh
./gradlew :protomolt-codegen:verifyEmbeddedWasm
./gradlew :protomolt-codegen:test
```

The rebuild script checks out the exact source revision above, applies
`pin-generator-inputs.patch`, builds the v4 module through protobuf4j's
containerized toolchain, copies it into ProtoMolt, and verifies the result. A
deliberate source or toolchain update must change the pinned commit, the
component versions and input pins in this record, and the checksum in the
same review.
