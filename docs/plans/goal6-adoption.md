# Goal 6: reproducible adoption

Status: in progress. Baseline main is
`fd068852514771cf93c59822ecc320c1eab94d30` (2026-10-01).

Prove that a new user can adopt ProtoMolt as a gRPC integration platform or
protobuf toolkit, complete useful work, and understand failures without help
from its authors. Jev comparison is removed from this goal; integration is
deferred. Data correction, agent coordination, and reusable workflow authoring
remain adoption paths. Setup effort, custom integration code, latency, cost,
correction outcomes, recovery, and receipt verification remain measurements.

## Work and acceptance criteria

1. **First run.** Follow the published image-only bundle from an empty directory
   and fresh volumes. Record platform, source, image identities, prerequisites,
   download/pull time, startup time, time to first completed task, manual actions,
   and every undocumented decision. Distinguish warm image caches from cold
   downloads. Check browser results, retained state after restart, and shutdown
   instructions. Existing automated qualification is supporting evidence, not a
   substitute for an unfamiliar user's walkthrough.
2. **Bring a service.** Supply a reproducible external gRPC example and guide
   registration, reflection, inspection, and invocation through supported MCP
   and ACP paths. Exercise invalid input, unreachable endpoint, and changed
   schema. Explain container networking, authentication, persistence, deadlines,
   and supported streaming shapes. Verify actual interface behavior rather than
   assuming identical capabilities from a shared action catalog.
3. **Use libraries.** Provide runnable mapping, selector, projection, validation,
   schema registry, and OpenAPI examples with exact dependencies, imports,
   commands, expected results, and failure examples. Verify a consumer can use
   the libraries without starting the platform. A monorepo test alone does not
   prove dependency resolution for an external consumer. Record runtime-only
   validation constraints separately from generated schema coverage.
4. **Complete an integration.** Demonstrate input, projection, validation,
   service invocation, and output, then asynchronous execution and recovery.
   Verify receipts where configured. Preserve the correction and coordination
   walkthroughs alongside authoring. Kafka and connectors are optional
   extensions with their own prerequisites. Measure latency and failure outcomes;
   report fixture provider cost separately from live-provider and hosting costs.
5. **Publish and test the instructions.** Link tutorials to implementation and
   meaningful tests. Record each capability as demonstrated, limited, or planned.
   An unfamiliar user follows the final instructions; record assistance required
   and fix the problems found. An agent rehearsal is labeled as such and does not
   count as observed human usability. Recheck website claims against the resulting
   evidence before publication.

## Initial findings

- The main README leads with an unpinned single-container demo and source-build
  Compose instructions. The qualified authoring bundle is linked only from its
  deployment documentation. Users need an explicit choice between a published
  walkthrough, service integration, and in-process libraries.
- The authoring release is a scripted NormalizeText / WriteRecord example. It
  proves a complete execution path, not arbitrary live-model workflow authoring.
- The current authoring starter requires a separate browser token retrieved on
  the Docker host. Its console address is loopback; remote hosts need an explicit
  access path. Neither fact should be left for a user to infer.
- Existing mapping, projection, validation, registry, and surface guides contain
  substantial implementation detail. Their snippets need to become an accessible
  set of runnable examples before claiming a complete library onboarding path.
- Service workspace registration currently requires reflection. It supports
  unary and server-streaming invocation; uploaded descriptors, automatic retries,
  and health-probe execution have separate limitations documented in the guide.

## Evidence boundaries

The [Goal 5 release report](goal5-release-qualification.md) records the native
AMD64/ARM64 publication and scripted browser/crash/review checks. Goal 6 adds
adoption evidence; it does not relabel those checks as a fresh-user study.
New observations must identify the exact release, environment, commands,
results, and remaining gaps. No live provider, NAS deployment, or paid-service
quality result is implied by a local rehearsal.
