# Connect a gRPC service to MCP and ACP

ProtoMolt can reflect an existing gRPC service, store its contract in a service
workspace, and invoke its methods through MCP or ACP. The external service does
not need a Java implementation or a ProtoMolt-specific service interface.
Registration currently requires gRPC server reflection. The service must be
reachable from the ProtoMolt coordinator, which makes the outgoing connection.

This walkthrough uses the separate example gRPC service shipped in the
[first-workflow bundle](first-workflow.md). It is named `fixture` on that Compose
network. You can then substitute your own reflected endpoint.

## Connect an MCP client

Start the first-workflow bundle. Configure your MCP client with:

- Transport: streamable HTTP.
- URL: `http://127.0.0.1:8080/mcp` (use your configured HTTP port).
- Header: `api_token` with an authorized coordinator API credential.

For this local demonstration, retrieve the operator credential on the Docker
host with `docker compose exec -T serve cat /run/operator/token`. This credential
has broader authority than the browser launch token; use a separately scoped
principal for a deployed integration. Keep credentials in the client's secret
configuration, not in tool arguments or chat messages.

After initialization, read `protomolt://workspace` to see the live tool catalog.
An MCP client handles session initialization and transport headers. A raw HTTP
client must follow the [MCP transport instructions](../surface/mcp.md#streamable-http).

## Register once, then inspect

Call the `service-register` tool with:

```json
{
  "profile": {
    "name": "my-service",
    "endpoints": [{
      "name": "local",
      "host": "fixture",
      "port": 9778,
      "transport": "TRANSPORT_PLAINTEXT"
    }]
  },
  "endpoint": "local",
  "deadlineMs": 5000
}
```

Expect `ok: true`, a stored profile with a descriptor fingerprint, and method
summaries for `NormalizeText` and `WriteRecord`. Inspect it without reflecting
again by calling `service-inspect` with `{"name":"my-service"}`.

The workspace stores the descriptor and profile. Subsequent calls carry the
profile name rather than the full schema. The Compose bundle persists that
workspace across coordinator restarts.

## Invoke and check failure behavior

Call `service-invoke`:

```json
{
  "name": "my-service",
  "endpoint": "local",
  "method": "ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureService/NormalizeText",
  "request": {"text":"  Hello from MCP  "},
  "deadlineMs": 5000
}
```

Expect `ok: true`, `status: "OK"`, and `responses: [{"text":"Hello from MCP"}]`.
This example makes a unary call and does not write a record.

Repeat with `"request":{"text":""}`. The reflected input contract requires
nonempty text. MCP reports `isError: true` and an `invalid-input` error; do not
interpret HTTP 200 as a successful tool call. An endpoint failure and a contract
failure are different outcomes; neither is a successful response.

## Use the same workspace through ACP

Install the ACP adapter from a source checkout with JDK 25:

```sh
./gradlew :protomolt-acp-agent:installDist
```

Configure an ACP client to launch the absolute path to
`surface/acp/build/install/protomolt-acp-agent/bin/protomolt-acp-agent` with arguments
`--remote-target 127.0.0.1:9090`. Supply the same authorized API credential through
the child process's `PROTOMOLT_API_TOKEN` environment variable. Use your configured
gRPC port if different. Remote TLS connections also take `--tls`.

This launcher speaks JSON-RPC over stdio; it is not an interactive shell prompt.
In the connected ACP session, submit:

```text
service-inspect {"name":"my-service"}
```

Then submit:

```text
service-invoke {"name":"my-service","endpoint":"local","method":"ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureService/NormalizeText","request":{"text":"  Hello from ACP  "},"deadlineMs":5000}
```

Expect the same descriptor fingerprint and `Hello from ACP` in the response.
The remote ACP adapter forwards to the coordinator's workspace. Running the
adapter without `--remote-target` creates a local catalog instead; that is not
the same workspace. Empty text is refused through ACP as well, with an error
message rather than a successful result. MCP and ACP have different envelopes;
check the result inside the transport response.

## Substitute your service

Change the profile name, endpoint host/port/transport, and exact reflected method
name. Inspect the input shape before constructing a request. The sample starter
limits outbound gRPC to `fixture:9778`; deliberately configure its
`PROTOMOLT_GRPC_ALLOWED_HOSTS`, `PROTOMOLT_GRPC_ALLOWED_PORTS`, and TLS settings for
your endpoint before using another target. See [outbound policy](../operations/grpc-channel-policy.md).

The host is resolved inside the coordinator container: `127.0.0.1` points at that
container, not your laptop. Use a Compose service name for a peer container or a
host address reachable from the coordinator for an external process. Configure
TLS for the endpoint's actual transport. Custom trust and credential references
require a host resolver; the default host refuses unsupported references.

When your deployed schema changes, call `service-refresh` with
`{"name":"my-service","endpoint":"local","deadlineMs":5000}`, inspect the
new contract, and adapt the request. Do not assume an old stored descriptor has
automatically changed.

Registered invocation supports unary and server-streaming methods and makes one
attempt. This walkthrough verifies unary calls only. Client-streaming and
bidirectional methods, automatic retry, and automatic health probing are not
promised here. Method policies requiring approval are refused by this invocation
path; a tool call cannot grant itself approval. See the complete
[service workspace boundary](../surface/service-workspace.md#current-boundary).

## Reproduce the protocol check

The [acceptance script](../../scripts/service-workspace-smoke.mjs) registers the
fixture through MCP, checks valid and invalid calls, then launches the installed
ACP adapter and checks the same profile and both outcomes. It requires Node 22+
and an existing starter, plus `PROTOMOLT_API_TOKEN`, `ACP_LAUNCHER`, and optionally
`HTTP_BASE` and `GRPC_TARGET`. It creates a uniquely named service profile; run it
against a disposable tutorial workspace. No provider model is called.

The contracts are in
[protomolt_service.proto](../../surface/grpc/service-contract/src/main/resources/ai/protomolt/proto/grpc/service/v1/protomolt_service.proto).
The ACP forwarding implementation is
[RemoteCatalogLineRunner](../../surface/acp/src/main/java/ai/protomolt/proto/acp/agent/RemoteCatalogLineRunner.java).
