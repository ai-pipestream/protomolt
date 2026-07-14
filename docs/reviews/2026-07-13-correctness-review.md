# Correctness & Error-Handling Review (2026-07-13)

> **Status: all findings below (P0, P1, P2) were fixed on 2026-07-13**, merged to
> main through commit `223a2de` with regression tests. Conformance re-verified at
> 2872/2872 after the fixes. This document is retained as the review record.

Five parallel review passes over the whole tree: servers/HTTP, validation engine,
formats/core, generation/indexing, and mapper/schema/integrations. Findings are
triaged below; file references are to current `main` (post-ProtoMolt rename).

Conformance context: the validation engine is proven at 2872/2872 against
protovalidate v1.2.2, so findings there concern behavior the suite does not
exercise (malformed rules, unsigned extremes, hot-path perf, API failure modes).

---

## P0 — Silent security / silent no-op (fix before any release)

### 1. Validator silently validates nothing without the extension registry
`protobuf/validation-protovalidate/.../ProtovalidateRuleSource.java:91`
`fieldConstraints`/`messageConstraints` use `options.hasExtension(...)` with no
unknown-field fallback. Descriptors built from a `FileDescriptorSet` parsed
without `ValidateProto.registerAllExtensions` (schema registry, reflection,
protoc plugin output) carry every `buf.validate` rule as unknown fields —
`validate()` returns OK for everything, silently. `PredefinedRules` already
demonstrates the fix (reparse options against our own registry). A validator's
worst failure mode is "silently validates nothing."

### 2. Insecure-by-default API token validation
- `http/rest/ProtoRestGateway.java:33-35`: the 2-arg constructor defaults to
  `ProtoApiTokenValidator.acceptNonBlank()` — any junk token passes a
  `required=true` method.
- `integrations/quarkus/.../ProtoToolsProducer.java:74-80` and
  `integrations/spring/.../ProtoToolsAutoConfiguration.java:49-55` wire that
  same validator as the DI default; the Quarkus producer lacks `@DefaultBean`,
  so an app cannot even override it without `AmbiguousResolutionException`.
The dev validator must be opt-in, and the DI defaults overridable.

### 3. Error responses leak internal exception messages
`servers/common/ProtoRestHttpSupport.java:112-125` + `ProtoRestGateway.java:92-94`
Any invoker `RuntimeException` message (connection strings, internal hostnames)
is sent verbatim in the 500 body on every host. 4xx messages are fine; 5xx
bodies should be generic.

### 4. Predefined rules are silently unenforced in the library
`ProtovalidateRuleSource.java:241-280`
`(buf.validate.predefined)` extensions are only handled by the package-private
conformance post-pass (`PredefinedRules`), not the library — a user schema with
predefined rules gets silent non-enforcement while conformance passes. Also the
javadoc coverage note is stale. Promote `PredefinedRules` into the library.

---

## P1 — Major correctness bugs

### Servers / HTTP layer
- **Netty blocks the I/O event loop**: `NettyProtoRestServer.java:241` runs
  `gateway.invoke` on the worker loop; slow backends stall all connections incl.
  `/health`. Vert.x uses `executeBlocking`, JDK uses virtual threads — Netty is
  the outlier. Fix: offload to a virtual-thread executor (fits the VT-default story).
- **Netty leaks event loops on bind failure**: `NettyProtoRestServer.java:111,123`
  `.sync()` sneaky-throws `BindException` (checked, uncaught by
  `catch (RuntimeException)`) → `shutdownGroups` never runs.
- **Malformed %-encoding → 500/connection drop instead of 400**:
  `ProtoRestHttpSupport.java:138` + `JdkProtoRestServer.java:143` (parseQuery
  outside the try; sun-httpserver kills the exchange).
- **No body-size limit on JDK/Vert.x hosts** (`readAllBytes()` / default
  `BodyHandler`) — OOM vector; Netty caps at 16 MB. Unify.
- **Declared `httpMethods` never enforced by any host** — a POST-only mutation
  is invocable via GET, contradicting the published OpenAPI.

### Validation engine (beyond conformance)
- **CEL bindings skip unsigned conversion for singular/repeated/map values**
  (`ProtoValidator.java:217-221,281-282,325-332`): `uint64` ≥ 2^63 binds as a
  negative signed long in custom CEL rules → wrong verdicts.
- **`string.pattern` recompiles the regex per value evaluation** and reports an
  uncompilable pattern as a per-value violation, not a compile error
  (`ProtoValidator.java:497-503,731-737`). Also `java.util.regex` on
  schema-supplied patterns is the known ReDoS surface (ties to the re2j plan).
- **CEL compile errors surface lazily** — only when a rule is first evaluated
  against a populated field (`ProtoValidator.java:986-988`). No eager
  compile-all path exists.
- **No caching of the translated rule model** — every `validate()` rebuilds the
  full constraints tree per field, and `isMessageOneofMember` re-runs message
  constraint translation per field per message (`ProtoValidator.java:163-169,
  1057-1071`). A per-descriptor compiled-rule cache fixes this, hosts eager
  compilation (prev item), and bounds the evaluator-cache growth.
- **Runtime CEL errors throw bare `IllegalStateException`**, discarding all
  collected violations (`ProtoValidator.java:989-992`). Needs a typed
  `RuleEvaluationException`.
- **Unbounded descriptor-keyed caches** (`messageCelByType`, CelEvaluator
  program cache, `ConformanceRunner.byType` — the latter a plain `HashMap` on a
  public API): leaks under dynamic-schema churn.
- **Conformance harness swallows predefined-rule failures**
  (`PredefinedRules.java:303-306,353-355` bare `return` on any
  `RuntimeException`) — the 100% baseline could mask regressions in that area.

### Core (descriptors/helpers)
- **`GoogleDescriptorLoader` has no cycle guard** (`:145-179`): a cyclic
  dependency in an untrusted `FileDescriptorSet` → `StackOverflowError`.
- **`DescriptorRegistry.resolveOnDemand` is broken and quadratic** (`:143-168`):
  passes a type name to a file-name API (never matches for
  `GoogleDescriptorLoader`) and never negative-caches, so every `Any` unpack
  miss re-parses the whole descriptor set.
- **`TypeConverter.singleValueToField` silently coerces mismatched kinds to
  proto defaults** (`:249-272`): `"8080"` → int32 `0`, number → string `""`,
  `"true"` → bool `false`. Silent data corruption; the LONG path already does
  this right.

### Generation / indexing
- **OpenAPI generator emits wrong schemas for well-known types**
  (`ProtoOpenApiGenerator.java:282-284`): Timestamp/Duration/Struct/Any/wrappers
  described as object-of-internal-fields while the gateway serves proto3 JSON
  strings. The jsonschema module already has the correct WKT table — port it.
- **JSON Schema int64 `const`/`enum` constraints reject canonical documents**
  (`ProtoJsonSchemaGenerator.java:401-424`): JsonFormat prints int64 as string;
  numeric `const: 42` fails `"42"`. Needs `anyOf` over both spellings.
- **Solr mapper stores quoted JSON scalars for WKTs** (`SolrDocumentMapper.java:118-125`):
  DATE-hinted Timestamp lands as `"..."` with embedded quotes; NDJSON writer
  guards this exact case, Solr doesn't. `IndexFieldKind` is ignored.
- **OpenSearch mapper ignores DATE hint and mangles nested numerics**
  (`OpenSearchDocumentMapper.java:135-137`): Timestamp → `{seconds,nanos}`
  object; nested int32 → double, nested maps → entry arrays. Inconsistent with
  Lucene/NDJSON.
- **`NdjsonOptions.omitWhitespace(false)` produces invalid NDJSON/bulk output**
  (`ProtoNdjsonWriter.java:52-80`): pretty-printed multi-line JSON breaks the
  line protocol. Force compact in line-oriented paths or reject the combo.

### Integrations / schema loaders
- **Spring "auto-configuration" isn't**: no `AutoConfiguration.imports`, no
  `@AutoConfiguration`, no conditionals (`ProtoToolsAutoConfiguration.java`).
- **Apicurio loader conflates auth/network failure with not-found**
  (`ApicurioDescriptorLoader.java:119-146` triple `catch (Exception ignored)` →
  null): registry outage reads as "type does not exist."
- **Apicurio `RegistryClient` produced without `@Disposes`** — leaked transport
  threads per injection point / dev-mode reload.
- **Apicurio loader registered twice** when both extensions are present
  (producer adds all loader beans + installer re-adds; no dedupe in
  `DescriptorRegistry.addLoader`) — double network sweeps.
- **Confluent loader: no request timeouts** on data calls (hangs startup
  indefinitely) and per-subject `catch (Exception)` → warn-and-skip returns
  silently partial descriptor sets on flaky/unauthorized registries.

---

## P2 — Minor (worth batching, not urgent)

Servers: JDK host prefix-matches `/health*`/`/openapi.json*` (200 on
`/healthzzz`); trailing-slash routing divergence; timing-unsafe secret compare
(`equals` vs `MessageDigest.isEqual`); `toLowerCase()` without `Locale.ROOT` in
token header lookup (Turkish-locale 401s); first-vs-last multi-value
header/query divergence across hosts; Spring-only 415 on missing content-type;
Vert.x OpenAPI cache lacks invalidation + no double-start guard; JDK executor
never shut down + abrupt `stop(0)`; Netty close doesn't await graceful
shutdown; channel errors logged at DEBUG; 405s lack `Allow` header.

Validation: `RuleCompilationException` drops cause; boxed-equality `in/not_in/
unique` NaN/±0.0 edge cases; `-0.0` treated as absent for `required`;
`DateTimeException` on extreme Timestamps; no recursion depth guard;
unescaped `"`/`\` in map-key subscript paths; wrong-return-type CEL reported as
violation not compile error; `format()` unsigned/exponent edge cases;
silent no-op on unknown oneof names from third-party rule sources;
`ValidationResult.validate(Message)` builds a full validator per call.

Formats/core: `(int)` cast aliases `isIp` versions (2^32+4 → v4); RFC 6874
zone-id charset slightly loose; `searchPaths()` empty-varargs AIOOBE;
`DescriptorRegistry.clear()` doesn't reset `autoLoadAttempted`; simple-name
collision overwrites silently; `"user."` path validates as `"user"`;
unknown enum name silently skipped in `structToMessage`.

Indexing/gen: NDJSON bulk pair not atomic on IO failure; OpenAPI swallows
`ReflectiveOperationException` (omits requestBody silently); enum schemas
reject numeric enum JSON; Lucene bytes fields with `indexed=true,stored=false`
produce nothing; Lucene OBJECT/VECTOR kinds degrade to `String.valueOf` (proto
text format / map-entry toString junk); mis-hinted numeric kinds throw raw
`ClassCastException`; proto3 default values (false/0/"") never indexed with no
include-defaults option; mixed json/proto naming in flattened field names.

Mapper/schema: CEL compile failures silently swallowed per-message in soft
mode (and recompiled every message); `build()` vs `buildPartial()` breaks
proto2 required-field mapping; Confluent `loadDescriptor` simple-name-vs-FQN
precedence contradicts javadoc + no caching; `HttpClient`s never closed;
Apicurio builder accepts `registryUrl` it never uses; network I/O inside
`computeIfAbsent`; nullable Kiota unboxing NPE; varint fallback parser
truncates on overflow.

---

## Solid (explicitly verified)

- De-regexed format scanners (Emails, Hostnames, IpAddresses, Rfc3986,
  Identifiers): no crash inputs, no bounds bugs, linear time; only two minor
  spec nits found.
- Unsigned-64 handling in the standard integral validation path; FieldPaths
  port; DescriptorSets linking; HttpHeaderRule scanners.
- Recursion guards in OpenAPI/jsonschema/IndexingPlanFactory generators.
- JSON policy compliance: no hand-rolled JSON anywhere; JsonFormat/Jackson
  split respected.
- Thread safety of transcoder, registries, generators; ApicurioReferenceResolver
  (cycle guard, memoization); wire-format framing defaults verified against
  upstream serde sources; dependency hygiene (api vs implementation) clean.
