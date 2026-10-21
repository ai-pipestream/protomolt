# protomolt-http-connect

The [Connect protocol](https://connectrpc.com/docs/protocol) for gRPC services, on the
JDK `HttpServer`, with binary protobuf only. A browser using Connect-ES (`connect-web`
with `useBinaryFormat: true`) calls the services same-origin, with no proxy and no
transcoding.

```java
ConnectGateway gateway = new ConnectGateway(
        services,                                   // List<ServerServiceDefinition>
        Set.of("pkg.v1.Things/ListThings"),         // the procedures a client may call
        RequestCallerResolver.bearer(operatorToken, policyResolver),
        new ConnectLimits(10 * 1024 * 1024, 10 * 1024 * 1024));
httpServer.createContext(ConnectGateway.PATH, gateway);   // /connect
```

A client then calls `POST /connect/<package.Service>/<Method>`.

## What it serves

Only the procedures the gateway is built with. A registered service's other methods answer
`unimplemented` without reaching the service, so a host lists exactly what its front end
calls.

| Call | Request | Answer |
|---|---|---|
| Unary | `application/proto`, one message | `200 application/proto`, one message |
| Server streaming | `application/connect+proto`, one enveloped message | `200 application/connect+proto`, enveloped messages, then the end-of-stream record |
| Client streaming, bidirectional | refused by name, `unimplemented` | |

- A JSON content type (`application/json`, `application/connect+json`), or any other type,
  answers plain HTTP 415 with `Accept-Post` naming the binary type, and a verb other than
  POST answers 405 with `Allow: POST`, as the protocol specifies for these refusals. Nothing
  is transcoded.
- Compression other than `identity` answers `unimplemented` with the accepted encoding.
- `connect-protocol-version`, when sent, must be `1`.
- A browser request whose `Sec-Fetch-Site` is not `same-origin` or `none` answers
  `permission_denied` before any credential is read. No CORS headers are sent, and both
  binary content types need a preflight the gateway never answers, so a cross-site page
  cannot call it.
- `connect-timeout-ms` becomes the call's gRPC deadline; an expired call answers
  `deadline_exceeded` and the service sees the cancellation.
- A client that disconnects mid-stream cancels the call.
- Custom request headers become call metadata (`-bin` headers as bytes from base64). The
  service's response headers come back as HTTP headers, unary trailers as `trailer-`
  prefixed headers, and stream trailers in the end-of-stream record's `metadata`.
  `authorization`, `api_token` and `cookie` stop at the caller resolver, protocol, transport
  and `sec-` headers stop at the gateway, and `grpc-` keys stay the transport's own.
- gRPC status codes map to Connect codes, and unary errors to HTTP statuses, by the
  protocol's table (`unauthenticated` 401, `permission_denied` 403, `unavailable` 503, and
  so on). `google.rpc.Status` details on the trailers become the error's `details`.

On a server stream the HTTP status is 200 once the stream starts, so a client reads the
outcome from the end-of-stream record. Switch on the Connect error code, never on the HTTP
status.

## Callers

Every request is resolved by the `RequestCallerResolver` the gateway is built with (from
`protomolt-authz`). Empty means `unauthenticated`; an `IllegalStateException` from the
resolver means the credential store is down and answers `unavailable`, never 401. A
resolved caller runs under the `CallerContexts.CALLER` and `AUTHENTICATED_CALLER` keys, the
same ones `ApiTokenServerInterceptor` sets, so service-side scope and account checks apply
unchanged. Pass the service definitions without a transport credential interceptor.

## Mount path

`ConnectGateway.PATH` (`/connect`) by default; the five-argument constructor takes another
base path, empty for the server root.

## How it dispatches

The gateway starts a private in-process gRPC server (a random name, reached only by the
gateway) holding the registered definitions, and forwards each call as bytes. Each
service's own interceptors, validation and status mapping run as on any gRPC transport,
and the gateway itself parses no message.

## JSON

The Connect protocol defines its error record and its end-of-stream record as JSON
whatever the message codec. Those two records are the only JSON this module writes, by
hand with string escaping and no JSON library; every message is binary protobuf, and
error details travel as base64 protobuf bytes inside the record.

## Conformance

`ConnectConformanceIT` runs the [Connect conformance suite](https://github.com/connectrpc/conformance)
(`connectconformance` v1.0.6, fetched once into `build/connect-conformance` and checked against
its published SHA-256) in server mode against `ConformanceServer`, with the features in
`src/test/resources/.../conformance/server-config.yaml`: Connect over HTTP/1.1, binary protobuf,
identity compression, unary and server streaming. All 78 selected cases pass. The suite's
service protos are vendored under `src/test/proto/connectrpc` (Apache License 2.0) and compiled
for the tests only.
