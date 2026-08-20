# MCP server

`protomolt-mcp` exposes the toolkit to AI agents over the Model Context
Protocol. Every [action](actions.md) becomes an MCP tool with no translation
layer. The catalog manifest's `{name, description, inputSchema}` entries are
already the shape MCP requires, and the input schemas are JSON Schema in both
worlds. An always-present workspace bootstrap identifies the live catalog;
schema registries and durable gRPC service workspaces are optionally served as
additional MCP resources, so an agent can browse their contracts without
spending tool calls.

Together with the gRPC verbs (`reflect`, `grpc-invoke`), this makes any gRPC
service agent-operable. Given a registered, pasted, or reflected schema, an
agent can inspect its types, call its methods,
process the responses, and generate native clients for it in eight languages,
every step machine-verified. See [The gRPC agent workflow](#the-grpc-agent-workflow).

The implementation is deliberately plain Java: JSON-RPC 2.0 over stdio,
newline-delimited, on Jackson and the JDK. No framework, no reactive runtime.
`McpServer.handle(JsonNode)` is the pure message-in/message-out core; the
stdio loop, the tests, and any future transport drive it the same way.

## Running

```shell
./gradlew :protomolt-mcp:installDist
surface/mcp/build/install/protomolt-mcp/bin/protomolt-mcp \
  [--registry-git <path>] [--service-workspace <path>] \
  [--workflow-workspace <path>]
```

Register it with an MCP client, for example Claude Code:

```shell
claude mcp add protomolt -- \
  /path/to/protomolt-mcp/bin/protomolt-mcp \
  --registry-git /srv/schemas.git \
  --service-workspace /srv/protomolt-services \
  --workflow-workspace /srv/protomolt-workflows
```

`--registry-git` adds git-backed schema resources. `--service-workspace` adds
durable service profiles, reflected descriptor storage, and service/method
resources. The five service tools remain discoverable without the latter but
answer `unavailable` with the configuration remedy.

`--workflow-workspace` stores content-addressed redacted fixtures and immutable
run evidence. Workflow tools remain discoverable without storage: compilation
and mapping suggestions still work, while record and replay answer
`unavailable` with the remedy. `promote-workflow` requires `--registry-git`.

The workbench flow advertised during initialization is: inspect or reflect
the services, use `suggest-mappings` where useful, verify and
`compile-workflow`, then `record-workflow-run`, `replay-workflow`, and finally
`promote-workflow`.

Initialization and `tools/list` both carry the same tool count and SHA-256
catalog fingerprint in `_meta`. Read `protomolt://workspace` for that identity,
the exact live tool names, and the safe workflow without loading every tool
schema. If a client exposes a different count or fingerprint, reconnect it so
it performs fresh MCP discovery against the deployed server.

### Streamable HTTP

The same server is also reachable over MCP's streamable HTTP transport, with
no local install: [`protomolt-serve`](grpc-service.md) mounts it at
`/mcp` next to the gRPC and REST surfaces, so one running process makes
every agent on the network gRPC-aware. That mount carries the full catalog;
the generated [action inventory](../generated/action-inventory.json) records its
relationship to the standalone binary:

```shell
claude mcp add --transport http protomolt http://host:8080/mcp
# with the launcher's --api-token:
claude mcp add --transport http protomolt http://host:8080/mcp \
  --header "api_token: <secret>"
```

With an [access policy](../design/authorization-scopes.md) mounted, a
credential the policy names also authenticates; the session is pinned to that
principal at `initialize`, `tools/list` serves only the tools whose scope the
caller holds, and a call outside the scope is an `isError` result naming the
missing scope.

Each HTTP `initialize` request creates a bounded server-side session and returns
an `Mcp-Session-Id` response header. Send that header and the negotiated
`MCP-Protocol-Version` header on every subsequent request. Send
`notifications/initialized` before operating; `notifications/cancelled` can
cancel an in-flight tool request. `DELETE` closes a session. Server-initiated
streams are not used, and browser requests from non-local origins are refused
(the specification's DNS-rebinding guard). Registry resources ride along when
the launcher mounts a registry. POST requests must use `Content-Type:
application/json` and advertise both `application/json` and
`text/event-stream` in `Accept`.

The stdio transport uses the same lifecycle but keeps its session inside the
child process. Closing stdin closes the session; tool requests already read
from stdin are allowed a bounded completion window, while cancellation stops
their response and interrupts the action where possible. Each session admits
at most 64 in-flight tool calls. There is no custom `shutdown` or `exit`
JSON-RPC method.

## Tools

The standalone binary registers the built-in [actions](actions.md), the
host-independent gRPC/codegen tools, and the service-workspace tools. It prints
the generated catalog size to stderr at startup; see the [action inventory](../generated/action-inventory.json)
for the exact names.

| Tool | Does |
|---|---|
| `compile` | Compile inline `.proto` sources; returns file names and a base64 descriptor set |
| `list-types` | Enumerate messages, enums, and services with fields: the grounding verb |
| `validate-message` | Validate a JSON message against the rules on its schema |
| `diff-schemas` | Typed change list between two schemas (rule, path, impacts) |
| `check-compat` | Compatibility verdict under a mode, with violations and change list |
| `render-json-schema` | JSON Schema (2020-12) for a message type |
| `render-prompt` | Render a descriptor-grounded LLM prompt for a message type |
| `render-index-mappings` | OpenSearch / Solr / Lucene field specs from indexing hints |
| `eval-cel` | Evaluate a CEL expression against a message |
| `map-message` | Apply text and CEL mapping rules to a message |
| `synthesize-shape` | Derive a join/union output type (envelope, projection, or oneof union) from named sources |
| `join-messages` | Join named source messages into an authored target or a synthesized shape |
| `merge-schemas` | Merge two or more message types into one: clash report, caller-decided resolutions, merged proto |
| `check-rules` | Statically validate mapping rules and CEL against descriptors; sample messages upgrade the check to a dry run |
| `infer-schema` | Reverse-engineer a message type from JSON sample documents |
| `mask-message` | Mask fields by their schema-declared sensitivity classes: remove, redact, or encrypt/decrypt |
| `extract-metadata` | The declared metadata bag for a type |
| `reflect` | Discover a live gRPC server's schema from its address (server reflection) |
| `grpc-invoke` | Call a unary or server-streaming gRPC method, no generated stubs |
| `generate-stubs` | Generate client/message code in eight languages (protoc as WebAssembly) |
| `gather-git` | Gather `.proto` sources from a git repository (branch, tag, or commit) and compile them to a descriptor set |
| `service-register` | Reflect a gRPC endpoint into a durable service profile and content-addressed descriptor artifact |
| `service-list` | List registered service identities and descriptor fingerprints |
| `service-inspect` | Read a registered service's methods and request/response field shapes without returning descriptor bytes |
| `service-refresh` | Re-reflect a registered endpoint and report whether its schema fingerprint changed |
| `service-invoke` | Invoke a registered method without sending its descriptor or target on each call |

Workflows, jobs, inference, and `emit-okf` are not in the standalone binary's
catalog because they require host-side wiring. `protomolt-serve` mounts the
full catalog over HTTP, plus the live delegation surface: one coordinator per
server, adapted to catalog verbs by the delegation bridge, in-memory by
default and durable through the repository service when
`--delegation-repo-endpoint` is configured.

The delegation verbs let one agent run the coordinator role and another the
worker role over two independent MCP sessions. `delegation-watch` long-polls
the coordinator's event feed from a caller-owned cursor, so a worker session
that disconnects can reconnect and resume with no lost or duplicated frames.
A worker whose stream failed, or whose server restarted over a durable
transcript, re-registers with `delegation-worker-register`: the replacement
stream resumes the recorded sequence scopes instead of being rejected.
See [Agent delegation](../transform/delegation.md) for the lifecycle and the
session model.

| Tool | Does |
|---|---|
| `delegation-worker-register` | Register this agent as a worker: hello, admission decision |
| `delegation-worker-list` | Discover registered workers and their capabilities |
| `delegation-offer` | Offer a bounded task and lease to an admitted worker |
| `delegation-accept` | Take the open offer for a task's current attempt |
| `delegation-progress` | Report one monotonic progress note on the lease |
| `delegation-checkpoint` | Record one resumable checkpoint with a resume token |
| `delegation-candidate` | Submit an evidence-carrying completion candidate for review |
| `delegation-review` | Accept the open candidate or request a revision with feedback |
| `delegation-cancel` | Cancel the task's open attempt; terminal on emission |
| `delegation-message` | Send a non-transitioning question, answer, guidance, or note |
| `delegation-watch` | Long-poll the event feed from a cursor; returns a bounded batch and the resumption cursor |
| `delegation-transcript` | Read the recorded transcript from a cursor, bounded per call |


Wherever a tool takes a schema it accepts exactly one of `{"type": "fully.qualified.Name"}`
(resolved from the registry), `{"sources": {...}}` (inline `.proto`, compiled
per call), or `{"descriptorSetBase64": ...}` (a serialized `FileDescriptorSet`).
The `reflect` verb returns the third form, so its output is a schema input to
every other verb.

## Resources

`protomolt://workspace` is always present. With `--registry-git`, the registry
is browsable as MCP resources. With `--service-workspace`, registered gRPC
service contracts are resources too. These reads do not spend tool calls:

| URI | Contents |
|---|---|
| `protomolt://workspace` | Server identity, safe workflow, exact tool names, count, and catalog fingerprint |
| `protomolt://registry/subjects` | All subjects plus the global compatibility mode |
| `protomolt://registry/subjects/{subject}` | Version index, per-subject mode, latest schema |
| `protomolt://registry/subjects/{subject}/versions/{n}` | One exact version with references |
| `protomolt://services` | Service identities, endpoint names, and descriptor fingerprints |
| `protomolt://services/{profile}` | One connection profile plus its reflected service and method contracts |
| `protomolt://services/{profile}/methods/{full-method}` | One method's streaming mode, request/response types, and top-level fields |
| `protomolt://delegation/workers` | Registered delegation workers: identity, admission, capabilities (serve mount) |
| `protomolt://delegation/tasks` | Delegation task states as the lifecycle reducer sees them (serve mount) |
| `protomolt://delegation/tasks/{taskId}/transcript` | One task's recorded frames in cursor order, bounded (serve mount) |

Subject, profile, and method names are URL-encoded in URIs. All resource
contents are JSON. Descriptor bytes never appear in service resources.
`resources/list` stays small by listing the service root and profiles only;
method URIs are addressable after reading the selected profile contract.

`resources/templates/list` advertises templates for registry subjects and
versions plus service profiles and individual methods, and, on the serve
mount, delegation task transcripts. This lets a client form
one exact URL-encoded deep link without enumerating every schema version or
method as a separate resource.

## The gRPC agent workflow

The gRPC tools compose into a single capability: **point an agent at a running
gRPC service and let it operate the service.** For work that should survive the
current conversation, the preferred path is:

1. **`service-register` once.** Supply a stable profile name and one or more
   endpoints. ProtoMolt validates the caller-authored profile before opening a
   connection, reflects the selected endpoint, and stores the descriptor set
   in the schema registry under its SHA-256 identity.

2. **`service-inspect` or read its resources.** Ground method selection and
   request construction in the persisted contract. The profile remains useful
   after the target is offline and after ProtoMolt restarts.

3. **Invoke and verify.** Use `service-invoke` with the profile name, exact
   method, and proto3 JSON request. ProtoMolt resolves the endpoint and pinned
   descriptor internally. Check `ok` and gRPC status before using the result.

4. **`service-refresh` explicitly.** Re-reflect only when the deployed schema
   may have changed. The returned `changed` flag compares schema fingerprints.

For one-off exploration without a workspace:

1. **`reflect` the address.** If the server enables gRPC server reflection,
   this returns its service names and a descriptor set with no schema needed in
   advance. Feed that descriptor set straight to the next steps.

2. **Fall back to a schema when reflection is off.** Many production servers
   (OpenVINO Model Server, NVIDIA Triton, and others) do not enable
   reflection; `reflect` returns `ok: false` so the agent knows to get the
   schema elsewhere: read it from the registry, or use `gather-git` to pull
   the service's `.proto` from its Git repository, which returns a descriptor
   set in the same form `reflect` does. Either way the agent now holds a
   schema.

3. **`list-types` to ground.** Enumerate the services and messages so the
   agent knows the exact method and message names before calling.

4. **`grpc-invoke` to call.** Unary and server-streaming methods, request and
   responses as proto3 JSON, no generated stubs on either side. gRPC status
   failures come back as `ok: false` with the status name: an outcome to
   reason about, not an input to repair.

5. **`generate-stubs` for a native client.** When the agent (or the human)
   wants a real client rather than JSON-over-MCP calls, generate compilable
   source in java, kotlin, python, cpp, csharp, ruby, php, or objc, plus
   grpc-java service stubs. This is the right move for tensor-heavy or
   high-throughput services, where hand-authoring message JSON is impractical.

A worked end-to-end example against a real OpenVINO Model Server: reflect,
fall back to the KServe schema, introspect the models, and run a text →
embedding inference: is in [Operating an OpenVINO server](../tutorials/openvino.md).

## Semantics

- Tool execution failures are MCP tool errors (`isError: true`) carrying the
  action error envelope (`{error, message, details?}`) as structured content,
  so a calling model sees the stable error code and can repair its input.
  Protocol-level problems (unknown method, malformed params) are JSON-RPC
  errors.
- Results carry both `structuredContent` (the action's JSON document) and a
  `text` block with the same document serialized, for clients without
  structured-output support.
- Protocol revisions `2025-06-18`, `2025-03-26`, and `2024-11-05` are accepted
  during initialization; unknown requested versions negotiate down to the
  latest supported.
- Stdout carries protocol traffic only; diagnostics go to stderr, as the stdio
  transport requires.

## Framework hosts

Spring AI and the Quarkus MCP server extension both accept programmatic tool
registration, so the same catalog can be mounted in an existing framework MCP
host without this module's transport. The adapter logic is the thin part;
nothing needs rewriting to move between hosts.

## Related

- [Actions](actions.md): the verb catalog this server exposes
- [Agent delegation](../transform/delegation.md): the live coordinator/worker surface on the serve mount
- [The gRPC service](grpc-service.md): the same verbs as typed RPCs and JSON/REST
- [The registry](../schema/registry.md): the store behind the resource URIs
- [Operating an OpenVINO server](../tutorials/openvino.md): a full gRPC-agent walkthrough
- [Roadmap](../design/planned-work.md): per-method dynamic tools and registry-backed type
  resolution extend this surface next
