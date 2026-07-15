# Architecture & Trust-Boundary Review Response (2026-07-15)

An external architecture review of commit `dca13b5` assessed the codebase across
security boundaries, metadata semantics, runtime correctness, release safety, and
API hardening. Its core observation: the breadth is real, but several conceptual
features behaved differently across surfaces (authentication covering some
listeners and not others, the published REST contract diverging from the router,
static chain verification permitting states execution could not produce).

This document records the disposition of every numbered finding. Items marked
**fixed** landed the same day with regression tests; items marked **deferred**
are design efforts tracked in the roadmap's hardening backlog rather than quick
fixes, and are listed there with their intended shape.

## Priority 0 — security and policy integrity

| # | Finding | Disposition |
|---|---|---|
| 1 | Registry listener ignored `--host` and `--api-token` | **Fixed.** The registry honors the configured bind address and requires the shared token on every route except health; the gRPC listener also gained host binding, so `--host` constrains all four listeners. `TokenAuthTest` and `SecurityBoundaryTest` enumerate the behavior. Scoped authorization (schema-read vs schema-write vs execute) is deferred. |
| 2 | Console partially secured in token mode | **Fixed** (by disabling): in token mode the console and its proxies answer 503 with one clear sentence, and the startup summary says why. A real session/BFF flow is deferred. |
| 3 | Shape operations drop policy metadata | **Deferred** — an option-propagation contract (transferable / contextual / derived / prohibited, conflict detection on merge) is a design effort; see the backlog. |
| 4 | `SensitivityMasker` skipped protobuf maps | **Fixed.** Message-valued map entries are traversed (paths reported as `field[key].nested`), and a sensitivity class on a map field applies to every entry value. Tests cover scalar maps, message maps, maps-in-maps, repeated messages, and whole-field annotations. |
| 5 | Encryption lacked an envelope and context binding | **Fixed.** Versioned envelope (version byte, nonce, ciphertext) with the field identity (containing message full name + field number) bound as AES-GCM AAD: ciphertext moved to another field refuses to decrypt. One `SecureRandom` instance. Ciphertext is deliberately not portable across fields; re-encrypt to move data. |
| 6 | Uncapped request handlers; no execution budgets | **Partially fixed:** the MCP HTTP handler and console proxy now read bodies through one bounded reader (16 MiB, 413 past the cap), matching the REST hosts and registry. Per-action timeouts/quotas, Git clone budgets, and outbound-network policy are deferred (see backlog, with #22). |

## Priority 1 — runtime correctness

| # | Finding | Disposition |
|---|---|---|
| 7 | REST routing and OpenAPI disagreed (verb defaults, custom paths) | **Fixed.** Undeclared verbs default to POST everywhere; the OpenAPI generator delegates to the same `allowedHttpVerbs()` the gateway enforces (parity test included). The unrouted per-method path override was removed; declaring one fails at startup. |
| 8 | Silent route replacement on duplicate registration | **Fixed.** Duplicate `service/method` registrations throw at startup, naming both protobuf service full names when descriptors are present. |
| 9 | Verifier permitted references to skipped conditional steps | **Fixed** (by defining the semantics): a skipped step binds its name to its output type's default instance at run time, so the verifier's static scope and the runtime value map always agree. Documented in both classes and covered by a regression test. |
| 10 | Keyed stream joins could strand or mis-evict matches | **Fixed.** Completed joins park in a ready queue (a full output batch never strands a pair), eviction follows true global arrival order, poll slices respect the remaining deadline, and key paths are validated at construction (existence, singular scalar, matching types, server-streaming shape). Missing-key messages are dropped; duplicate keys queue FIFO. |
| 11 | Git registry writes are not transactional | **Deferred** — JGit plumbing (blob/tree/commit + atomic ref advance) is the right shape; see backlog. |
| 12 | Registry reference aliases could conflict or vanish | **Fixed.** Traversal identity and emitted import paths are tracked separately: one schema referenced under two names materializes under both, and one import path resolving to different content raises `ReferenceConflictException` (422 at the API). |
| 13 | Backend exception text leaked to clients | **Fixed.** Registry 500s and MCP internal errors return a correlation id and generic text; the stack trace is logged server-side under the same id. Leak tests assert no exception class or message reaches the wire. |
| 14 | MCP protocol hand-rolled subset | **Partially fixed:** an empty JSON-RPC batch now gets the spec's invalid-request error. The session state machine and inspector-driven conformance fixtures are deferred. |

## Priority 1 — release and distribution safety

| # | Finding | Disposition |
|---|---|---|
| 15 | Tag and publish ran before full verification | **Fixed.** The release workflow runs console tests, typecheck, and build plus the full JVM suite before axion tags anything; snapshot publication gained the same gate. Environment approval and signed provenance are deferred. |
| 16 | Image labeled MIT, ran as root, no health check | **Fixed.** Apache-2.0 label (plus OCI revision/version), dedicated non-root uid 10001, `/health`-based HEALTHCHECK via bash `/dev/tcp` (no curl layer). Verified locally: builds, starts `--demo`, reports healthy as 10001. Digest pinning deferred until update automation exists. |
| 17 | BOM missing modules; catalog not consumer-facing | **Fixed** (BOM): `protomolt-shapes` and `protomolt-chain` constrained, and `checkBomCompleteness` (wired into `check`) fails the build when a published module is neither constrained nor deliberately excluded with a reason. A consumer-facing catalog with ProtoMolt aliases is deferred. |
| 18 | CI gaps: no typecheck, one JDK, no API compat check | **Fixed** (first two): CI runs `vue-tsc` and a test-JDK matrix where `-PtestJdk=21` runs every test JVM on the declared Java 21 baseline. Binary-compatibility checking (japicmp/Revapi) starts making sense after the first release exists to compare against; deferred until then. |

## Priority 2 — API and architecture hardening

| # | Finding | Disposition |
|---|---|---|
| 19 | No API lifecycle / stability classification | **Deferred** — pre-1.0; see backlog. |
| 20 | Action catalog silently replaced; misdocumented ordering | **Fixed.** `register()` rejects duplicates, `replace()` is the explicit override path, `names()` returns a `List` in true registration order. |
| 21 | Embedded protoc WASM lacks provenance | **Deferred** — reproducible build script + checksum + provenance record; see backlog. |
| 22 | No centralized outbound gRPC channel policy | **Deferred** — `ChainRunner.ChannelFactory` is the existing seam to generalize; see backlog. |
| 23 | Join/shape docs described unimplemented behavior | **Fixed.** `docs/design/join-shapes.md` now states the implemented joiner semantics exactly and marks richer unmatched-entry policies as future options. |
| 24 | Encryption + vector indexing threat model | **Already documented** (docs/indexing.md names the neighborhood and inversion channels explicitly and treats vectors as confidential material). Policy checks that refuse unsafe sensitive-field vectorization without explicit authorization are deferred. |

## Not adopted (with reasons)

- **Serving full-name gRPC-style routes in REST hosts** — the simple-name URL
  segment is the public contract; duplicate detection now names full names in
  the error, which addresses the diagnosability concern without doubling the
  route surface.
- **Digest-pinned base images today** — pinning without update automation rots;
  it lands together with automated digest bumps.
