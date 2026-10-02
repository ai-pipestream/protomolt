# Your first ProtoMolt workflow

## About ProtoMolt workflows

A workflow describes a job as a series of steps: what each step does, what data
it needs, and where its result goes next. ProtoMolt coordinates those steps by
calling services over gRPC. The services can be written in different languages;
you do not need to put them all into one application.

Workflows are useful when a task involves several services and you want to run
it again without connecting the pieces by hand. A developer can define the
services and their data requirements, an agent can help assemble a workflow,
and someone using the browser console can supply the input and follow a run.
The data requirements are the **contracts**: they describe the accepted fields
and the validation rules those fields must pass.

For a small, working example, suppose you want to clean up text before saving
it. This tutorial uses two services:

1. **NormalizeText** removes spaces and line breaks from the beginning and end
   of the text.
2. **WriteRecord** saves the cleaned text under a record ID.

You will send `"  Hello from ProtoMolt  "`. The workflow will save
`"Hello from ProtoMolt"`. It is a simple job, but it gives you a complete path
through creating, reviewing, and running a workflow.

## Run your first workflow

We will start ProtoMolt on your computer, open the browser console, and run the
text-cleanup example. By the end, you will have a saved record and a completed
run you can return to in the console.

The starter includes a scripted author that prepares this particular workflow.
You do not need an AI-provider account or API key, and you do not need Java
installed. Later, you can [connect your own gRPC service](connect-grpc-service.md).

### 1. Start ProtoMolt

You need Docker running with Docker Compose available, plus `curl` and `unzip`.
The script below works in a Linux or macOS terminal and needs Internet access
to download the starter and its container images. The first download may take
a while.

Copy this whole block into your terminal. It creates a `protomolt-first-workflow`
folder in your current directory; choose a location where you can keep it.

```sh
(
  set -eu
  mkdir protomolt-first-workflow
  cd protomolt-first-workflow

  release=authoring-starter-d209f0551566
  bundle="protomolt-authoring-starter-${release}"
  url="https://github.com/ai-pipestream/protomolt/releases/download/${release}"

  curl --fail --location --remote-name "${url}/${bundle}.zip"
  curl --fail --location --remote-name "${url}/${bundle}.zip.sha256"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c "${bundle}.zip.sha256"
  else
    shasum -a 256 -c "${bundle}.zip.sha256"
  fi
  unzip "${bundle}.zip"
  cd "${bundle}"

  docker compose config --quiet
  docker compose pull
  docker compose up -d --pull never
)
```

**What this does:** it downloads a specific starter release, checks that the
archive matches its published checksum, and starts the included services.
Docker runs ProtoMolt, its storage, and the example services for you. There is
nothing to compile. If a command fails, the script stops; use the error message
and the troubleshooting notes below before continuing.

The script runs in a subshell, so your terminal stays in its original directory.
Move into the downloaded starter now. Run the remaining terminal commands from
this directory:

```sh
cd protomolt-first-workflow/protomolt-authoring-starter-authoring-starter-d209f0551566
```

Open [the health page](http://127.0.0.1:8080/health). When it shows
`{"status":"UP"}`, ProtoMolt is ready. If the page does not open immediately,
give the services a moment to start and reload it.

### 2. Open the console

The starter creates a private credential for signing in. Display it in your
terminal:

```sh
docker compose exec -T serve cat /run/browser/token
```

Open [the task console](http://127.0.0.1:8080/console/tasks), paste that credential
into the sign-in form, and select **Connect**. Keep the credential private.

You should see **scripted-author** online. This is the included worker that
prepares the example workflow. If it has not appeared yet, select
**Refresh tasks**.

### 3. Prepare the workflow

Select **Author a workflow**, then **Start workflow authoring**.

The author connects the text-cleanup and record-writing steps and submits the
workflow for review. Wait until the task is **accepted**. The console will then
show **Launch accepted workflow**.

There are two separate actions here: first you prepare and review the workflow;
then you run it with some input. Acceptance means the workflow passed review.
It does not mean your text has been processed yet.

### 4. Give it some text and run it

Paste this into the input field under **Launch accepted workflow**:

```json
{
  "operationId": "94181e0a-a892-4d68-96db-5a8701d7a72b",
  "content": "  Hello from ProtoMolt  "
}
```

`content` is the text to clean up. `operationId` identifies the record being
saved. You can use the value above for this first run.

Select **Prepare input**. ProtoMolt checks the input against the workflow's
contract before allowing it to run. If there is an error, the console tells you
which field needs attention; correct it and prepare the input again.

Once the input is ready, select **Launch workflow**. The run happens in the
background. Select **Refresh status** until its execution status shows that it
has completed.

### 5. Check the result

A completed run means both steps finished: the example service cleaned the text
and saved `Hello from ProtoMolt` under the supplied record ID. The console shows
the run's status and job ID. Keep that ID if you need help finding or diagnosing
this run later.

Compare your console with the [completed example screen](../evidence/goal6/first-workflow.png).
Your task and job IDs will be different.

To save another piece of text, use a **new lowercase UUID** for `operationId`,
then prepare and launch the new input. The record ID also protects retries:
sending the same ID and content again returns the existing record. Changing the
content while keeping the same ID produces a conflict rather than overwriting
what you saved.

## Stop now and come back later

From the starter directory, stop the services with:

```sh
docker compose stop
```

When you are ready to continue, return to that same directory and run:

```sh
docker compose start
```

Keep the starter folder, including its `.env` file, and the Docker volumes.
They contain the configuration and stored data needed to continue. Do not use
`docker compose down -v` unless you intend to delete the stored data.

Return to the console using the same browser profile and URL, including the
port, then select your previous task and refresh its saved launch status.
The console remembers launch entries in that browser. If you clear its site
data or use a different browser, those entries may disappear even though the
server still has the jobs. Do not launch a new run just to look up an old one;
keep the original job ID when asking for help.

## If you get stuck

### ProtoMolt does not start

From the starter directory, check the services:

```sh
docker compose ps --all
docker compose logs --tail=100
```

Some containers do setup work and then stop normally. `bootstrap`,
`signing-init`, `rustfs-init`, `repo-init`, and `policy-seed` should show exit code
0. The other seven services should remain running. A nonzero exit code indicates
a failure; check that service's log before deleting or recreating any storage.

If the error says port 8080 or 9090 is already in use, edit `.env` in the starter
directory. Set `PROTOMOLT_HTTP_PORT` and `PROTOMOLT_GRPC_PORT` to unused ports,
then run `docker compose up -d --pull never` again. Use the new HTTP port in
the browser URL.

If a download failed, keep the folder and resume the download commands inside
it after fixing the reported problem. Running the whole setup block again will
stop at `mkdir` because that folder already exists.

### The author does not appear, or review fails

Check the worker and coordinator logs:

```sh
docker compose logs --tail=100 author serve
```

This example already includes its author; adding an AI-provider account is not
necessary. If review fails, read the failure in the task, address its cause,
and retry review. The launch form becomes available after acceptance.

### The run fails

Read the saved execution status and check the service logs. Keep the job ID
and the `operationId` with any error report so the original run can be found.
A new record ID starts a different operation; it does not repair the failed one.

<details>
<summary>Other machines and platforms</summary>

The commands above assume the browser and Docker are on the same computer.
The starter binds its ports to loopback. If Docker runs on a remote machine,
use an SSH tunnel to its HTTP port; your laptop's `127.0.0.1` does not refer to
the remote machine.

The release has been qualified with Linux AMD64 and ARM64 containers. The
script handles Linux and macOS checksum tools, but a Windows shell walkthrough
has not been verified.

</details>

## Where to go next

- [Connect your own gRPC service](connect-grpc-service.md) to work with an endpoint
  beyond this example.
- [Project data into workflow input](projected-workflow.md) to prepare input
  from a different data shape.

<details>
<summary>How this example works, and what has been tested</summary>

The task's contract checks and signed record describe the workflow authoring
and review process. That record is separate from the job you launch afterward.
A signature verifies record integrity and attribution; it does not establish
that arbitrary model-generated content is factually correct.

- [Scripted author](../../samples/src/main/java/ai/protomolt/proto/samples/AuthoringWorker.java).
- [Deployment configuration](../../deploy/authoring/compose.yml).
- [Browser acceptance script](../../deploy/authoring/browser-smoke.mjs).
- [Release qualification](../plans/goal5-release-qualification.md).
- [Adoption rehearsal observations](../plans/goal6-first-run-observations.md).

The automated rehearsal verified retained server task state after a restart.
Restoring the saved launch in the browser remains part of the fresh-user
walkthrough. The console's storage behavior is defined in the
[launch intent store](../../apps/console/src/services/workflowLaunch.ts).

</details>
