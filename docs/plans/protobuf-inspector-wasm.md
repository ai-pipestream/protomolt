# Protobuf inspection and local WASM previews

Implementation branch: `feat/protobuf-inspector-wasm`, worktree
`/work/worktrees/protomolt/codex-protobuf-inspector-wasm`, starting main
`8e0fa531dab144fae8bb0ae7c400bd6de4b37aa9`. The worktree started clean.

## Required behavior and reuse

Document parts expand nested Any values using their revision's exact retained
occurrence bindings. Reuse `AnySchemas` from revision comparison, `decodePart`,
`retainedRegistry`, Buf reflection, and `ProtoMessageNode`. Extract the binding
utility instead of duplicating it. Opaque values remain opaque; missing bindings,
identity mismatch, descriptor fetch failure and malformed payloads stay distinct.
Expansion is lazy and bounded for display, without changing repository limits.

The read path is RepositoryDocumentView -> ReadDocument at the selected revision
-> decodePart/GetSchemaDescriptor -> shared occurrence resolver -> inspector.
There is no write/storage change or live registry substitution.

A user-selected local WASM module can preview a typed part in a dedicated browser
worker. Reuse protobuf Any, FileDescriptorSet and StringValue as the binary ABI;
no new public document API, proto contract, JSON conversion or server dependency.
The module gets the original part bytes wrapped in Any and the verified fragment
descriptor artifact. It returns a protobuf StringValue displayed as plain text.
Parts containing opaque occurrences are not passed to preview modules.
The worker has a deadline and cancellation, a bounded imported memory and no host
functions. Modules cannot define extra memories. Input/output/module limits are
preview budgets only; existing raw downloads and repository storage are unchanged.

WasmProtoc remains the source generator. This slice supplies a browser preview
host, not a new Java provider loader or a server-side WASM execution endpoint.
The ABI is suitable for a future Chicory interpreter host; native execution is
not claimed by the browser tests.

## Acceptance before publication

- Exact occurrence paths, nested Any, repeated/map ancestors, same-byte opaque
  siblings, historical schema selection and failed retained-schema reads.
- Unknown fields, exact int64 values and original preview input bytes preserved.
- Lazy tree expansion pages collections and has explicit depth-limit messages.
- Real WASM executes against protobuf input; wrong imports, extra memories,
  malformed modules/results, output bounds and traps are refused.
- Worker timeout, cancellation, replacement/unmount and late results cannot show
  a preview for a different part/revision. Guest text is never HTML or a URL.
- Console typecheck, tests and build, plus real-browser worker proof; root guards
  serialized under flock /work/.gradle-krick.lock with --max-workers=2.

## Verification result

- `npm test -- --maxWorkers=2`: 65 files, 582 tests passed, none skipped.
- `npm run typecheck` and `npm run build`: passed.
- `npm run test:preview-browser`: four Chromium tests passed, including the
  actual document screen, unknown-field-preserving download, WASM execution,
  infinite-loop deadline, cancellation and recovery. Screenshot inspected.
- `scripts/proto-stubs.sh --check`: passed with the repository-pinned Buf 1.70.0.
  The ambient Buf 1.71.0 was rejected as designed; no generated schema changed.
- `flock /work/.gradle-krick.lock ./gradlew checkZeroRegex checkTestNaming
  --max-workers=2`: passed. No Java or proto implementation changed, so there is
  no new native-image or live-provider qualification claim.
