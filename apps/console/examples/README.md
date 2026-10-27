# WASM protobuf previews

In Repository, open a document revision, read a typed part, and expand its
protobuf fields. `Any` envelopes open only through that revision's retained
occurrence binding; opaque payloads are not decoded. Repeated and map branches
expand on demand in pages of 200 entries; expand “more entries” to continue.
Inspection stops at 64 message levels with an explicit notice. The
Download protobuf button saves the complete original part bytes, including
unknown fields; display limits do not change stored content.

Under Custom preview, choose a local `.wasm` file and click Run preview.
The example download displays the input type URL. Results are literal text,
never HTML, Markdown, script, or navigable URLs. No module or part is uploaded
by this feature. Cancel, changing the part/revision, or leaving the view stops
the worker and discards its result. Parts with opaque occurrences do not offer
custom previews. A preview is derived output, not schema validation.

## Preview ABI v1

The guest is a WebAssembly v1 module with exactly one import:

```wat
(import "env" "memory" (memory 16 256))
```

The host supplies 16 initial 64 KiB pages, with a hard maximum of 256 pages
(16 MiB of linear memory). Do not define additional memories, tables, a start
function, or GC/reference types. This ABI accepts numeric function signatures.
No WASI or host functions are provided: modules have no filesystem, network,
DOM, clock, repository credentials, or repository write API.

Export these functions:

```wat
(func (export "pm_preview_abi") (result i32)) ;; returns 1
(func (export "pm_alloc") (param i32) (result i32))
(func (export "pm_preview") (param i32 i32 i32 i32) (result i64))
```

1. The host calls `pm_preview_abi` and requires version 1.
2. It calls `pm_alloc(length)` twice. Return disjoint in-bounds addresses for
   the input and schema. `memory.grow` is available within the host maximum.
3. The host writes a binary `google.protobuf.Any` into the first allocation.
   Its `type_url` is the retained fragment type URL, and its `value` contains
   the exact bytes read from the pinned revision, without reserialization.
   The host encodes `type_url` before `value` (the example uses that ordering).
4. The second allocation receives the exact verified binary
   `google.protobuf.FileDescriptorSet` retained for the fragment. It is not a
   union of all nested Any schemas: those may have conflicting definitions and
   are independently bound by occurrence in the inspector. A guest that inspects
   nested payloads must not assume the fragment closure defines them.
5. `pm_preview(input_ptr, input_len, schema_ptr, schema_len)` returns an i64:
   high 32 bits are the result address, low 32 bits are its byte length.
6. Those result bytes encode `google.protobuf.StringValue`. Unknown fields or
   invalid protobuf fail the preview. The result may reuse an input allocation.

Each invocation gets a fresh worker, module instance, and memory. Traps and ABI
errors appear as failures. Execution, including compilation and initialization,
has a three-second deadline; the host terminates the worker even if the guest
loops forever. The guest module is limited to 2 MiB, combined encoded inputs to
4 MiB, and encoded output to 256 KiB. These are local preview budgets, not
repository document limits or claims about total browser-process memory.

The worker wrapper uses structured-clone messages within the browser. The guest
ABI carries protobuf bytes only; no protobuf-to-JSON conversion is involved.

## Rebuilding the example

`type-url-preview.wat` is the source of the downloadable module. Build it with
pinned WABT 1.0.39 (Node/npm required):

```sh
bash apps/console/examples/build.sh
```

It reads the input Any's type URL and returns that field as a StringValue.
Both fields have the same protobuf wire tag, so this example can reuse the
original bytes directly. Real preview modules can implement domain-specific
summaries using their protobuf runtime and the supplied descriptor artifact.

`WasmProtoc` remains the existing server-side source generator. Generating
source code does not automatically compile it into a preview module. This
change does not add server-side plugin execution, Java SPI hot loading, or
prove GraalVM execution of this ABI. A later Chicory interpreter host can use
the same protobuf boundary without loading new JVM classes.

## Verification

```sh
cd apps/console
npm ci
npm test -- --maxWorkers=2
npm run typecheck
npm run build
npx playwright install chromium
npm run test:preview-browser
```

The browser suite exercises real workers, deadline/cancellation and recovery,
and the document screen through binary Connect test handlers. It does not
claim live catalog/provider or native-image qualification. The screenshot is
written to `test-results/protobuf-inspector.png`.
