# MCP Inspector compatibility lane

Drives ProtoMolt's MCP servers with the official
[MCP Inspector](https://github.com/modelcontextprotocol/inspector) CLI, so
protocol framing, negotiation and error behaviour are checked by a client we
did not write. The `mcp-inspector` CI job runs it on pull requests and main.

`package-lock.json` pins `@modelcontextprotocol/inspector` and its whole
dependency tree; `run.mjs` refuses to run when the installed CLI reports a
different version. Each run prints the Node and inspector versions it used.

## Running it

```shell
./gradlew :protomolt-mcp:installDist :protomolt-serve:installDist --max-workers=2
cd tests/mcp-inspector
npm ci --ignore-scripts
node run.mjs --self-test
node run.mjs \
  --stdio ../../surface/mcp/build/install/protomolt-mcp/bin/protomolt-mcp \
  --serve ../../apps/serve/build/install/protomolt-serve/bin/protomolt-serve
```

A full run ends with `PASS (failed: 0, passed: 18)`. CI requires that exact
line, so a removed check fails the job as surely as a failing one. Change the
expected count in `.github/workflows/ci.yml` together with the checks.

## What it covers

stdio (`protomolt-mcp`):

- `initialize`: negotiated protocol version, server identity, tools capability.
- `tools/list --strict`: the inspector's schema portability check reports no
  error-severity problem, names are unique, and every `inputSchema` is an
  object schema.
- `tools/call`: a valid `compile` call; malformed arguments come back as an
  `isError` result naming `invalid-input`; an unknown tool is refused.
- `resources/list` and `resources/read`: the workspace bootstrap is listed,
  is JSON, and counts the same tools `tools/list` served; an unknown URI is an
  error. `resources/templates/list` answers with a list.
- Prompts: the server advertises no prompts capability and `prompts/list` is
  empty. Adding prompts fails this check on purpose, so prompt checks get
  written with them.

streamable HTTP (`protomolt-serve --api-token ... --access-policy ...`):

- The same `initialize` and `tools/list --strict` checks with the operator token.
- No credential and an unknown credential are both refused (`auth_required`).
- A principal holding only `schema-read` lists a strict subset of the
  operator's tools, cannot call one outside it, is denied a resource
  collection outside its scope (`permission-denied:`), and reads a workspace
  bootstrap that counts its own tools.
- The operator calls `compile` over HTTP.

## Self-test

`--self-test` runs the base checks against `fixtures/broken-server.mjs`. The
correct baseline (`MCP_BREAK=none`) must pass, and each break must fail:
an unsupported protocol version, `"jsonrpc": "1.0"`, a response id the client
never sent, and a tool whose `inputSchema` is not an object schema.

## Not covered here

The inspector CLI has no way to cancel a request in flight or to send raw
malformed JSON-RPC, so cancellation, parse errors, batches and session-header
rules stay covered by `McpHttpTest` and the
`surface/mcp` tests. This lane adds an outside client; it replaces none of
those tests.
