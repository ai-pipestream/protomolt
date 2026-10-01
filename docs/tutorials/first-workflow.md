# Run your first workflow

This walkthrough runs the published workflow authoring prerelease on a local
Docker host. A scripted author discovers an example gRPC service, prepares a
NormalizeText / WriteRecord workflow, and submits it for independent review.
You then launch the accepted workflow as an asynchronous job in the browser.
No model-provider account, host JDK, or local image build is needed.

This is a fixed example. It demonstrates contract checks and execution, not a
live model writing an arbitrary workflow. For your own gRPC endpoint, continue
with [connecting a gRPC service](connect-grpc-service.md).

## Download and start

You need Docker with Compose, access to GitHub releases and the image registries,
and a browser on the Docker host. The shell commands below use `curl`, `unzip`,
and `sha256sum` on Linux. On macOS use `shasum -a 256 -c` for the checksum step.
The release is qualified for Linux AMD64 and ARM64 containers; a Windows shell
walkthrough has not been verified.

In a new directory:

```sh
curl --fail --location --remote-name https://github.com/ai-pipestream/protomolt/releases/download/authoring-starter-d209f0551566/protomolt-authoring-starter-authoring-starter-d209f0551566.zip
curl --fail --location --remote-name https://github.com/ai-pipestream/protomolt/releases/download/authoring-starter-d209f0551566/protomolt-authoring-starter-authoring-starter-d209f0551566.zip.sha256
sha256sum -c protomolt-authoring-starter-authoring-starter-d209f0551566.zip.sha256
unzip protomolt-authoring-starter-authoring-starter-d209f0551566.zip
cd protomolt-authoring-starter-authoring-starter-d209f0551566
docker compose config --quiet
docker compose pull
docker compose up -d --pull never
docker compose ps --all
```

Continue only if the checksum reports `OK`. The bundle contains its pinned
image configuration in `.env`. Keep that file and the same directory when
restarting the stack.

The bootstrap, signing-init, rustfs-init, repo-init, and policy-seed services
should exit with code 0. They initialize persistent state and are not supposed
to remain running. The other seven services should remain running. Check
`http://127.0.0.1:8080/health` for `{"status":"UP"}`.

If ports 8080 or 9090 are occupied, set `PROTOMOLT_HTTP_PORT` and
`PROTOMOLT_GRPC_PORT` in the bundle's `.env` before starting, then use your chosen
HTTP port in the browser. Addresses bind to loopback. On a remote Docker host,
use an SSH tunnel to its HTTP port; your laptop's localhost is a different host.

## Complete a task

Retrieve the browser credential on the Docker host:

```sh
docker compose exec -T serve cat /run/browser/token
```

Paste it into the sign-in form at `http://127.0.0.1:8080/console/tasks` and select
**Connect**. Keep this credential private. The general console token is a
different credential and cannot launch this example.

1. Wait for **scripted-author** to appear online. Use **Refresh tasks** if needed.
2. Select **Author a workflow**, then **Start workflow authoring**.
3. Wait for the task to be **accepted**. The launch form appears after review.
4. Enter this input under **Launch accepted workflow**:

   ```json
   {"operationId":"94181e0a-a892-4d68-96db-5a8701d7a72b","content":"  Hello from ProtoMolt  "}
   ```

5. Select **Prepare input**, then **Launch workflow**.
6. Select **Refresh status** until the execution completes. Submission alone
   does not mean the job finished.

The saved launch shows its job ID, input fingerprint, authorization fingerprint,
and execution status. The task also shows independently verified contract checks
and a signed-record download. Signature verification establishes record integrity
and attribution; it is not proof that arbitrary model output is factually correct.

See the [completed example screen](../evidence/goal6/first-workflow.png) for the
accepted task, launch status, and contract checks. Your identifiers will differ.

Use a new lowercase UUID as `operationId` for a new logical record. Reusing an operation identity
with changed content can produce a conflict instead of a second write.

## When something fails

- **An initializer exits nonzero:** inspect `docker compose logs SERVICE`, using
  the failing service name from `docker compose ps --all`. Do not replace keys or
  delete storage to hide a persistent-state mismatch.
- **No author appears:** inspect `docker compose logs author serve`. This starter
  already includes its scripted worker; you do not need to configure an LLM.
- **Input is refused:** read the field/rule error, fix the JSON, and prepare it
  again. A request that violates the contract must not proceed as a valid input.
- **Review fails:** inspect the recorded failure. Retry review when its stated
  cause has been resolved. A failed review is not permission to launch.
- **A job fails:** inspect the saved job status and service logs. Preserve its job
  and operation IDs when diagnosing recovery; issuing a new ID is a new operation.

Stop and start while preserving data:

```sh
docker compose stop
docker compose start
```

Keep the same Compose project and named volumes. `docker compose down -v` deletes
the stack's stored state and is not a restart procedure.

## Source and evidence

- [Release qualification](../plans/goal5-release-qualification.md): native image,
  browser, review-failure, crash-recovery, and anonymous-download checks.
- [Deployment configuration](../../deploy/authoring/compose.yml).
- [Browser acceptance script](../../deploy/authoring/browser-smoke.mjs).
- [Scripted author](../../samples/src/main/java/ai/protomolt/proto/samples/AuthoringWorker.java).
- [Goal 6 observations](../plans/goal6-first-run-observations.md): adoption
  rehearsal and remaining measurement gaps.
