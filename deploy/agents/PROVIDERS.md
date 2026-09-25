# Provider evidence

The default worker is scripted. An agent-host adapter, a passing adapter test,
and a live provider task are different evidence. This matrix describes the
coordination starter qualification, not every historical ProtoMolt deployment.

| Provider | Evidence for this starter | Packaged entry |
| --- | --- | --- |
| Fixture | Real AgentHost/MCP lifecycle, typed report, revision, conversation, rejection, restart, signed export and offline artifact checks | Default Compose worker; no model account |
| Kimi CLI | One bounded live typed report, follow-up answer, inspected acceptance, and offline verification on local AMD64 development images | Optional same-host Compose profile; operator supplies authenticated CLI |
| Codex | Existing adapter and scripted tests; no live task in this qualification | Separate coding-worker deployment |
| Cursor | Existing ACP adapter and scripted tests; no live task in this qualification | Separate coding-worker deployment |
| OpenAI-compatible endpoint | Existing adapter and scripted tests; no live task in this qualification | Separate configured endpoint |
| Muse | Existing adapter and scripted tests; no live task in this qualification | Separate provider deployment |
| Antigravity | Existing adapter and scripted tests; no live task in this qualification | Separate provider deployment |

Adapters are selected in `apps/agent-host/src/main/java/ai/protomolt/proto/agenthost/AgentHostMain.java`;
provider tests live alongside `AgentHostTest` under `apps/agent-host/src/test`.
Historical Codex/Kimi or workstation provider demonstrations do not establish
coverage for the downloadable starter. No Claude adapter is included in this
matrix solely because an earlier experiment used Claude.

## Bounded live Kimi run, 2026-09-25

- Kimi Code CLI 2.1.1, existing operator authentication; model identity was not
  captured independently, so this is not a model-specific result.
- Task `eb99ba3c-335f-4a2f-95e4-453e29a834b5`, attempt 1, candidate revision 1.
- The offered `CoordinationReport` required a bounded headline, one or two
  findings, a matching count, and a materialized protobuf artifact. It forbade
  source edits and build/test/commit claims. This was not a coding benchmark.
- The actual 445-byte protobuf file decoded to the submitted typed report and
  hashed to `5bf0b2e99db3fba81c99ac005e9a575b2e7cf361645dd880486dbd045b196a1f`.
- The worker answered one recorded question about its CLI checks. The reviewer
  independently read, decoded, counted and hashed the artifact, then accepted
  the exact attempt and revision. The report's contents matched the task scope.
- The normal exporter initially could not read the worker-owned file. Its
  group was corrected without changing bytes; Compose now explicitly shares
  group 10001. Tasks must require group-readable evidence, including temporary
  files renamed into the evidence directory. Export does not bypass permissions.
- The accepted record and both referenced artifacts verified using the separate
  verifier in a container with networking disabled. Manifest digest:
  `a6d6729e9724a7cb83adef65447b11d7bbba101f680391e0979038bea3953e97`.
- Local development serve image:
  `sha256:382aab5015f69dcb1c8c0b69c662e74608f1fe861be0598859fffffa4027adfc`;
  agent image:
  `sha256:3c5114c14cf34ebdb5b90ce11445a522a368c553aef3a53d9a5ea7c0ccda0fd7`.
  These were built during development, before the final release commit. They
  do not prove native ARM64 Kimi execution or qualification of a later image.

Release qualification must record the source revision and immutable image
digests separately. Its fixture gates cannot silently upgrade this bounded live
Kimi evidence into claims about other providers, architectures, or code execution.
