# Agent coordination starter

This is the second ProtoMolt starter template. The fixture worker uses the real
agent-host cursor and MCP command path, but its provider is scripted. It does not
use a model account or execute code. The package is ready for use only after a
release supplies a digest-pinned `.env` for serve, repo-service, agent-host,
ACP, and their external runtime images. `SOURCE.json` records the source
revision, Compose hashes, and image digests used to assemble the ZIP.
The image tag placeholders in this source checkout are development inputs, not
a published starter bundle.

The starter agent image uses `apps/agent-host/Dockerfile.starter`: a Java runtime
with AgentHost and the fixture provider. Optional Kimi is supplied at runtime.
It does not bundle other provider CLIs or language build toolchains; those
remain in the separate coding-worker images.

From the extracted release directory:

```sh
docker compose up -d
docker compose ps --all
```

The four initializers (`bootstrap`, `signing-init`, `rustfs-init`, and
`repo-init`) should show `Exited (0)`; the long-lived services should become
healthy or running. Compose's `--wait` reports a successful one-shot exit as a
failed wait on some versions, so use `ps --all` to inspect initialization.

Docker Compose initializes separate persistent credentials, an encrypted
delegation transcript store (repo-service, PostgreSQL, and RustFS), worker
cursor/workspace, and an Ed25519 signing identity. Keep the project name and
volumes across upgrades. `docker compose down -v` deletes that state. The
identity initializer refuses an incomplete or mismatched existing key/trust
set instead of silently creating a new signer.

Open <http://127.0.0.1:8080/console/tasks> and sign in with the browser-only
token. Retrieve it on the same host with:

```sh
docker compose exec -T serve cat /run/console/token
```

The console can offer the bundled `CoordinationReport` sample or upload a
caller-owned complete protobuf descriptor set and type name. The fixture worker
supports only the bundled report contract and rejects unsupported work with a
recorded reason. Its completion checks are fixture reports to inspect, not
independently run tests or a claim that a model performed the work. The task
console records conversation, candidate evidence, revision feedback, and the
human verdict. A terminal task can export a signed record. Keep the exported
record, the exact transcript bytes from the same export, public trust snapshot,
and every referenced artifact together for offline verification; a signature
proves integrity and attribution, not the truth of the worker's claims.

HTTP/MCP listens only on `127.0.0.1:8080`, and gRPC on `127.0.0.1:9090`.
Override `PROTOMOLT_HTTP_PORT` and `PROTOMOLT_GRPC_PORT` in `.env` for other
loopback ports. The operator token, browser token, and coordination token live
in separate named volumes. The trusted agent-host and optional ACP adapter
receive the coordination token. The `worker-coordinate` scope covers delegation operations including
review and does not authenticate a specific worker identity; do not give this
token to arbitrary clients. The worker receives neither the operator token nor
the signing seed. Model/provider credentials are never stored in this bundle.

An ACP-capable IDE can launch the optional stdio adapter on demand:

```sh
docker compose --profile acp run --rm -T acp-agent
```

The adapter connects to `serve:9090` and receives the trusted
coordination-scope token. It has no operator, browser, database, or signing
credentials and starts no background ACP service. The bundled protocol smoke
qualifies its delegation methods, including `delegation/ReadTranscript` and
`delegation/SubmitCandidate`.

All three interfaces share coordinator state: native gRPC exposes the existing
`ai.protomolt.proto.delegation.v1.DelegationService` methods, MCP exposes the
`delegation-*` tools at `/mcp`, and ACP accepts `delegation/<RpcName>` commands.
Requests use the same catalog authorization and validation. Candidate results
must pass the offered protobuf contract and required-evidence checks before
they become available for review. The reviewer then decides whether the
contract-valid result actually satisfies the task.

To run a separate Kimi worker on the **same host**, supply an existing
authenticated Kimi configuration directory, then start its optional profile.
The process runs with that directory's numeric owner so it can read private
login files and persist its provider session. Its primary group is the shared
evidence group (10001); the host group is supplementary. This keeps report files
readable by the exporter even when a provider renames a temporary file into the
evidence directory. The tested Kimi Code 2.1.1
installation keeps its executable and configuration under `~/.kimi-code`;
no separate `~/.kimi` mount was needed. Its container home, cursor state,
and workspace each have their own persistent volume. Use a binary for the
host architecture:

```sh
KIMI_CODE_DIR="$HOME/.kimi-code" \
KIMI_AGENT_UID="$(id -u)" KIMI_AGENT_GID="$(id -g)" \
  docker compose -f compose.yml -f compose.kimi.yml --profile kimi up -d
```

An offered task that produces evidence should require the worker to place the
actual bytes at `/workspace/artifacts/<sha256>` with group-read permission
(`chmod 0640` when necessary). Private temporary files can otherwise remain
unreadable after a rename. The exporter refuses unreadable files; it does not
escalate permissions or infer their content from a submitted digest.

That provider uses Kimi's existing ACP CLI/login and writes separate cursor
state. The configuration mount is host-owned and must not be added to the
published ZIP. For a worker on another host, use the documented agent-host MCP
connection and transfer referenced artifact bytes explicitly; the two hosts do
not share this Compose evidence volume. Real Kimi behavior requires its own
labeled live test and is not proven by the fixture.

The trust snapshot is public data at `/run/identity/trust.binpb` inside serve.
The raw private seed is at `/run/identity/seed.bin`; never export it with task
evidence. The content-addressed report bytes are stored in the `evidence-data`
volume, mounted read-only into serve. Always export and verify a *selected*
record's referenced digests rather than copying the whole evidence directory.

Export one terminal task with its exact signed transcript and local artifact bytes:

```sh
./export-record.sh <task-uuid> task.zip
```

The exporter authenticates the record before reading artifact paths and checks
every referenced digest and size before writing the ZIP. Missing, changed, or
symlinked evidence fails the export. The ZIP contains `record.binpb`, public
`trust.binpb`, and an `artifacts/` directory named by SHA-256. It contains no
private signing seed or credentials. Use the independent record verifier on
these files for the separate offline verification step. This helper does not
fetch remote artifacts; copy a remote worker's claimed bytes into local custody
under their digest before exporting.
