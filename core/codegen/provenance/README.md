# Embedded protoc wrapper provenance

ProtoMolt bundles `protoc-wrapper-v4.wasm` so code generation can run inside
the JVM without a native protoc installation. The binary is not an opaque
download: it is the output of the protobuf4j build recorded below.

## Source

- Repository: `https://github.com/ai-pipestream/protobuf4j.git`
- Source and build commit: `359ad92e3e6ba46b59d99ce51417ac35730a9abb`
- Build target: `make build-v4`
- Output: `wasm/protoc-wrapper-v4.wasm`
- SHA-256: `0c1a98a6e731bd8553b31867dda88c80231c2625b622d182d7a2dd028715f03f`

That source revision, with `pin-generator-inputs.patch` applied, builds the wrapper
in an Ubuntu 24.04 container with:

- Protocol Buffers `v36.2`
- grpc-java compiler plugin `v1.85.1`
- WASI SDK `34.0` (LLVM 23.1.0, wasi-libc `2e6fb9d8ee0c`)
- Binaryen `133`

It contains protoc's Java, Kotlin, Python, C++, C#, Ruby, PHP, and Objective-C
generators together with the grpc-java plugin. ProtoMolt executes the module
directly with Chicory; protobuf4j is a build-time provenance source, not a
ProtoMolt runtime dependency.

## Pinned generator inputs

The upstream recipe at the pinned commit names versions but floats on digests:
a mutable image tag, unchecksummed tool archives, and git tags. The rebuild
script therefore applies `pin-generator-inputs.patch` to the checked-out
protobuf4j tree before building. The patch is reviewed against the exact
source commit above; if it stops applying, the pins must be re-derived in the
open, not worked around. The patch moves Protocol Buffers and the grpc-java
plugin to the versions the repository runtime uses (protobuf-java 4.36.2, grpc
1.85.1; the recipe's own version files name v34.1 and v1.79.0), moves the WASI
SDK and Binaryen to their current releases with the two recipe changes described
under "Toolchain changes for WASI SDK 34" below, and otherwise changes only
which exact bytes may be built from:

| Input | Identity | Pin |
|---|---|---|
| Docker base image | `ubuntu:24.04` (OCI index), `linux/amd64` platform | `sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852` |
| Bootstrap CA bundle `.deb`s (`openssl`, `ca-certificates`) | Ubuntu `noble-updates`/`noble-security` packages from snapshot `20261024T120000Z`, fetched by BuildKit `ADD --checksum` over host-verified HTTPS before `apt-get` runs | `openssl_3.0.13-0ubuntu3.16_amd64.deb`: `675b84971ffd4467707008c25ef7520f90ea7c23ef27b7a76b0dccf1d7c4dc3f`; `ca-certificates_20260816ubuntu0.24.04.1_all.deb`: `8a0a754aaafed72cc8efb8c1731808133284a84fbfc76a4fcc73fce9e01b4187` |
| apt packages (`ca-certificates`, `git`, `patch`, `curl`, `wget`, `cmake`) | Ubuntu snapshot `20261024T120000Z`, the pinned base image's creation date (update the image digest, snapshot timestamp, and bootstrap `.deb` pins together) | `https://snapshot.ubuntu.com/ubuntu/20261024T120000Z/` |
| WASI SDK 34.0 `x86_64-linux` tarball | `wasi-sdk-34.0-x86_64-linux.tar.gz` (upstream publishes no checksum; computed from the release asset) | `b761e3a0721dbae9c09a0059e5fdb2bf917d1b4a8a7b430fb3b5aafb0984b2c4` |
| Binaryen 133 `x86_64-linux` tarball | `binaryen-version_133-x86_64-linux.tar.gz` (matches upstream's published `.sha256`) | `2dc9c7813f5375db93d96ead4b78222fcc3e2677bbb832297af4797782a37489` |
| grpc-java `v1.85.1` | git clone of the tag; HEAD must equal the tag's commit before any use | `54c634aed4b5c78a29d0fa51d9282e9538722543` |
| Protocol Buffers `v36.2` | git clone of the tag; HEAD must equal the tag's commit before any use | `2c74169b34066ceb8ddb6b882fcb3fb32d737a55` |
| Abseil `20250512.1` (CMake FetchContent in protobuf) | `GIT_TAG` rewritten from the tag name to its commit | `76bb24329e8bf5f39704eb10d21b9a80befa7c81` |

Git-sourced inputs are pinned by commit rather than by the SHA-256 of GitHub's
generated source archives, because GitHub does not keep those archive bytes
stable. Tool archives are release assets and are pinned by SHA-256, computed
from the archives as served by their upstreams (Binaryen's also matches its
published `.sha256` sidecar). The image digest is the Docker Hub OCI index
digest for `ubuntu:24.04`.

### Toolchain changes for WASI SDK 34

The upstream recipe was written against WASI SDK 25 (LLVM 19). Two changes in
the toolchain need matching recipe changes, both in `prepare_build.sh`:

1. Threads target. wasi-sdk PR #637 ("Remove `wasm32-wasi` and
   `wasm32-wasi-threads` targets", first released in wasi-sdk-34) sets
   `WASI_SDK_TARGETS` to `wasm32-wasip1;wasm32-wasip2;wasm32-wasip3;wasm32-wasip1-threads`
   (`CMakeLists.txt:14` at tag `wasi-sdk-34`), so the sysroot has
   `lib/wasm32-wasip1-threads/` but no `lib/wasm32-wasi-threads/`. WASI SDK 25
   shipped both. The shipped `share/cmake/wasi-sdk-pthread.cmake` still sets
   `set(triple wasm32-wasi-threads)` (line 6, unchanged on upstream main), and
   `cmake/wasi-sdk-sysroot.cmake:157-159` copies only the compiler-rt builtins
   under the old name, not libc or `crt1.o`. CMake's compiler check therefore
   fails with `wasm-ld: error: cannot open crt1.o` and `unable to find library -lc`.
   The recipe derives `wasi-sdk-p1-threads.cmake` next to the shipped file,
   differing in the triple line only (checked by `diff`), and passes it as
   `CMAKE_TOOLCHAIN_FILE`. Threads support, `-pthread` and `--import-memory` are
   unchanged.
2. Stack placement. LLVM 22 changed wasm-ld's `--stack-first` to default on
   (`lld/wasm/Driver.cpp:598` at the SDK's LLVM commit `895aa2c896ad`; LLVM
   19 to 21 read `args.hasArg(OPT_stack_first)`). With stack-first, a
   `--global-base` below the 64 KiB stack is rejected
   (`lld/wasm/Writer.cpp:367-373`: `--global-base cannot be less than stack size
   when --stack-first is used`). The recipe's `-Wl,--global-base=1024` only
   restated the old non-stack-first default (`Driver.cpp:725-732`), so it is
   removed and the stack-first layout is kept: the same 64 KiB stack, placed
   below static data so an overflow traps instead of overwriting data.

Clang 23 reports about 2,900 `-Wdeprecated-attributes` warnings for Abseil
20250512.1's `lifetime_capture_by(this)`; that Abseil release is the one protobuf
v36.2 pins, so they clear when protobuf moves to a newer Abseil.

The build runs on `linux/amd64` only. The Ubuntu snapshot service serves the
amd64 archive but not `ubuntu-ports`, so an arm64 image could not pin its
packages; the Dockerfile requests the amd64 platform, an arm64 host runs it
under emulation, and the WASI SDK script refuses any architecture without a
pinned checksum. The WASM output does not depend on the build architecture.

The minimal `ubuntu:24.04` base image ships `apt`'s HTTPS transport and
`libgnutls30t64` (`libssl3t64` `3.0.13-0ubuntu3.16`) but no `/etc/ssl/certs`
bundle. Before `apt-get` runs, BuildKit fetches `openssl` and `ca-certificates`
from the pinned `20261024T120000Z` snapshot over host-verified HTTPS and checks
their SHA-256 digests (`ADD --checksum`), and `dpkg -i` populates
`/etc/ssl/certs/ca-certificates.crt`. With that CA bundle present, the
Dockerfile rewrites `ubuntu.sources` directly to
`https://snapshot.ubuntu.com/ubuntu/20261024T120000Z/`, checks that no
unpinned archive host or plain-HTTP `URIs:` entry remains, and runs
`apt-get --error-on=any update` with `Verify-Peer`, `Verify-Host`,
`Check-Valid-Until`, and signed `InRelease` verification all enabled.

In the patched recipe every archive is checksum-verified before extraction,
each git checkout is verified before its contents are used, and a wrong image
digest fails the pull: in each case before the input can execute. Protobuf's
git submodules are pinned by the verified commit's tree.

The pins make the rebuild hermetic with respect to its declared inputs. The
final `sha256sum --check` in the rebuild script and the `verifyEmbeddedWasm`
gate remain the arbiters of the output bytes: if a pinned-input rebuild ever
produces different bytes, the checksum, this record, and the review must move
together. Consecutive `docker build --no-cache` runs of the patched recipe
each produce exactly the SHA-256 above, with every archive checksum and git
commit verified in-build before use.

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
