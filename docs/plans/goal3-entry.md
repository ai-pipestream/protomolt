# Goal 3: runnable starter and protocol integration

This is the implementation contract for Goal 3. The Goal 2 candidate and its NAS
proof are recorded separately in `nas-candidate-verification.md`. Items below
remain in progress until their acceptance evidence is recorded.

## Entry point

A versioned, image-only Compose package starts the coordinator and the existing
correction service. No host JDK, Gradle build, database, object store, GPU or model
account is required for the labeled deterministic fixture. Real model use remains
an explicit configuration with separate evidence. Persist the coordinator's schema
and service workspaces and the correction service's signing identity, artifacts,
run records and outcomes. Generate independent API, console and correction
credentials once; retain them across restart and never embed them in images.

The console guides the user from source data through correction and assessment to
recorded evidence. It uses a scoped HttpOnly session and a bounded correction API;
it never receives the coordinator's operator token or a generic privileged proxy.
Show actionable errors and explicit fixture/provider identity. Protocol connection
instructions distinguish gRPC, MCP streamable HTTP, and an ACP stdio process.

## Existing contracts and host obligations

Reuse `CorrectionService.RunCorrection/GetCorrection`, `ServiceProfile`, the
service-workspace actions and `ProtoMoltService.ServiceInvoke`. Do not add another
structured-generation RPC. The profile `correction`, endpoint `default`, points
to the host-configured correction target. Its persisted credential reference is
`env:PROTOMOLT_CORRECTION_API_TOKEN`; the secret itself stays in host configuration.

- The host resolver approves an exact profile name, endpoint name, host, port,
  transport and credential reference. Editing a profile cannot redirect its
  credential to a different server or read arbitrary environment variables.
- Resolve credentials privately at each call. Reflection and invocation use the
  same resolver. Unsupported references, custom trust and client-certificate
  references fail closed until a corresponding resolver exists.
- Reflected verbs and explicit profile invocation enforce the same approval and
  deadline policies. Preserve the current single-call retry policy.
- Validate the parsed nested callee request before network side effects and each
  successful upstream response before exposing it as success. A bad request is
  `invalid-input`; an invalid upstream response is `invalid-upstream-response`,
  mapped to a server-side gRPC failure rather than blaming the caller.
- ACP remote mode connects to the running coordinator with host credentials;
  it must not silently substitute a fresh local workspace. It is stdio, not an
  additional listening HTTP service.

Remote ACP lists the static coordinator RPC commands; registered upstream methods
are discovered with service-inspect and called through service-invoke. Direct
reflected catalog verbs are tested separately over MCP. The current ACP host does
not implement session/cancel, and closing a browser request does not cancel an
already forwarded correction. The caller must retain its run ID and retrieve the
outcome; neither path promises automatic retry or resume. The configured upstream
deadline remains the bound on work. Direct gRPC cancellation retains the Goal 2
service's documented best-effort behavior.

The starter's correction methods are unary. Existing `service-invoke` can collect
a bounded number of server-streaming replies; its streaming action emits a
terminal status and validates each reply. Direct reflected verbs return the first
reply only. Client-streaming and bidirectional upstream methods are not exposed
through these single-request paths. The starter does not claim streaming parity.

Host configuration uses `PROTOMOLT_CORRECTION_TARGET` and
`PROTOMOLT_CORRECTION_API_TOKEN`; the existing `PROTOMOLT_API_TOKEN` protects
protocol clients and `PROTOMOLT_TASK_CONSOLE_TOKEN` establishes browser sessions.
ACP remote mode uses `--remote-target` and `PROTOMOLT_API_TOKEN`.

## Validation schema publication

Preserve OpenAPI 3.0.3 and its protobuf JSON encodings. Reuse the JSON Schema
module's neutral validation-rule translation for required fields, numeric and
string bounds, membership, repeated items and collection bounds, and map values
and size bounds. Convert numeric exclusive bounds to OpenAPI's boolean form and
constant values to single-value enums. Keep reference constraints inside `allOf`
because OpenAPI 3.0 ignores siblings of a reference.

CEL is descriptive `x-protomolt-cel`. Byte and temporal constraints, conditional
presence/ignore rules, message oneofs, Any resolution, field-mask rules, taxonomy
checks and other unrepresented rules are explicitly runtime obligations. OpenAPI
3.0 map-key constraints are retained as `x-protomolt-property-names`. No client
validation parity is claimed for these extensions, formats that a client ignores,
or state-dependent checks. Runtime validation remains the acceptance authority.
Required implicit-presence scalars and nonempty required collections also retain
runtime markers: an OpenAPI required property alone cannot express protobuf zero
value presence. Reversed numeric bounds describe the allowed outside range with
alternatives. For skipped nested inspection, published schemas do not retain a
reference that would incorrectly impose the child's validation rules.

`ProtoOpenApiValidationParityTest` checks representative strings, numeric bounds,
membership, repeated items, maps, 64-bit encodings, nested responses, and runtime
extensions against the validator and a schema evaluator.
`OpenApiRuntimeGapTest` checks reversed bounds, skipped nested validation and the
implicit-zero disclosure. The schema evaluator converts OpenAPI's boolean
exclusive-bound form to JSON Schema's numeric form; separate assertions check the
actual OpenAPI output. These tests establish the listed coverage, not complete
equivalence for every annotation or third-party OpenAPI client.

## Acceptance evidence to collect

1. Required hosted checks pass for the Goal 2 candidate and it is merged and
   synchronized to both main remotes.
2. A credential-protected upstream fixture exercises profile registration,
   reflection, refresh, invocation, policy denial, retargeting denial and
   invalid nested request/response rejection without leaking credentials.
3. The same correction contract succeeds and rejects invalid input through gRPC,
   MCP tools/call and the ACP remote command path. Test reflected verbs separately
   from ServiceInvoke wrappers. Document supported streaming modes explicitly.
4. Generated OpenAPI accepts/rejects representable fixtures consistently with
   ProtoMolt's runtime validator, including nested responses and exclusive bounds.
5. A clean Docker host pulls versioned images and starts the fixture from Compose;
   authenticated console login reaches a recorded correction without a local build.
6. Restart preserves identity and completed outcomes. Export public evidence and
   verify a receipt independently. Missing dependencies and unauthorized calls
   produce useful failures.
7. Published manifests and startup evidence establish each advertised platform.
   Image publication, local tests, hosted CI and live deployment are separate facts.

The separate delegation revision/attempt review-binding patch is still required
before enabling automatic external delegation review; this starter does not
implicitly enable it.
