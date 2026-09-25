# Goal 3 verification

Local evidence recorded on 2026-09-25. Publication and hosted checks remain
separate gates; this record does not claim that the starter is released.

## Source and local checks

Goal 2 merged through GitHub PR #319 as
`9cf4f229771467076de3d05e7e2a12cd8d9c70e6`, synchronized to Forgejo main.
Goal 3 implementation is recorded in the following focused commits (before
rebasing onto the Goal 2 squash merge):

- `c0fbdf1b`: validation schema translation and scoped protobuf Any resolution.
- `a7501cad`: authenticated reflection/invocation, validation boundaries and ACP.
- `ae041d5b`: scoped correction console and host wiring.

The full `./gradlew build :protomolt-serve:installDist
:protomolt-correction:installDist :protomolt-acp-agent:installDist --console=plain`
passed (1,495 tasks, including the root lint gate). `buf build` with complete
imports and `buf breaking --against '.git#branch=github/main'` passed against the
merged Goal 2 contracts. Console tests passed: 29 files, 171 tests; type checking
and the production Vite build passed. The starter Compose static checks passed.
The separate `:protomolt-acp-agent:acpProtocolTest` task and repository deployment
static checks also passed.

Regression coverage includes credential-bound reflection and refresh, endpoint
retargeting refusal, method policy enforcement, nested request and response
validation, deadline bounds, remote ACP authentication, reflected Any JSON
parsing/rendering, and OpenAPI rule coverage described in `goal3-entry.md`.

## Local image integration

The local Compose project `protomolt-goal3-smoke` used `goal3-local` image tags,
HTTP port 29802 and gRPC port 29803. These were locally built images, not registry
pull evidence. It used generated, separate operator, console and correction
credentials and fresh persistent volumes.

The following actual transport calls passed:

- Browser-session HTTP: correction `compose-contact-20260925-2` accepted;
  retrieval returned the same outcome, reuse of the run ID returned 409,
  an invalid uppercase run ID returned 400, and an unauthenticated call
  returned 401.
- Coordinator gRPC `ServiceInvoke`: `grpc-contact-20260925-3` accepted;
  an invalid nested request was refused before invocation.
- MCP streamable HTTP: `service-invoke` accepted
  `mcp-contact-20260925-3`; reflected `correction-run-correction` accepted
  `mcp-reflected-contact-20260925-3`. Both paths rejected invalid nested input.
- ACP over an actual Compose stdio process: remote `service-invoke` accepted
  `acp-contact-20260925-3` and rejected invalid nested input.
- Headless Chrome: console-token login, HttpOnly session authentication, and
  correction `contact-def9f4c6-d30d-411b-8024-453144e14878` reached `ACCEPTED`.
  The rendered page was visually inspected. The initial browser test had selected
  an extension target; selecting the page target fixed the test without a UI fix.

Typed Any evidence first exposed failures in generic invocation serialization
and direct reflected MCP serialization. Both were fixed, covered by regression
tests, and the transport checks above reran successfully.

`verify-receipt.sh compose-contact-20260925-2` restarted the services, compared
receipt/execution/trust bytes before and after restart, then verified the exported
records using the standalone verifier with Docker networking disabled. Execution
verification rehashed four artifacts; assessment verification rehashed fourteen.
The exported bundle contains public trust and evidence, not the signing identity.

## Remaining release gates

The committed smoke script will repeat protocol acceptance on each native image
architecture. Hosted PR checks, public versioned manifest digests, anonymous pull,
release-bundle startup and restart verification must be recorded before Goal 3
is complete. The running NAS Goal 2 candidate is a separate deployment; none of
these local tests upgrades it.
