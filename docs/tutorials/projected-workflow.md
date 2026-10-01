# Project data and run an accepted workflow

This example connects the standalone protobuf libraries to the published
authoring starter: source contact → descriptor-declared projection → runtime
validation → prepared workflow input → asynchronous gRPC steps → stored output.
Projection happens in the caller before launch. The workflow maps its validated
input into NormalizeText and WriteRecord requests; it does not put `projectTo`
on an ordinary gRPC edge.

## Prepare the two paths

Follow [Run your first workflow](first-workflow.md) through acceptance of the
scripted author's task. Leave the stack running. You also need the
[standalone toolkit prerequisites](../../examples/protobuf-toolkit/README.md):
JDK 25 and access to the public dependency repositories. The Java program here
is an example client; the coordinator still accepts protobuf clients in other
languages.

From the repository root, create a source file:

```sh
mkdir -p /tmp/protomolt-contact-example
cat > /tmp/protomolt-contact-example/contact.json <<'EOF'
{"fullName":"Ada Lovelace","email":"ada@example.org"}
EOF
./gradlew -p examples/protobuf-toolkit workflowInput \
  -Psource=/tmp/protomolt-contact-example/contact.json \
  -Poperation=ed7c3e68-e417-47c5-a905-e4945c8fb88b \
  -Poutput=/tmp/protomolt-contact-example/workflow-input.json
```

Choose a new lowercase UUID for another logical record. Keep the same UUID and
content when recovering that record. The program refuses to replace an existing
output file, so rerunning it cannot silently change an input you already saved.

The [contact schema](../../examples/protobuf-toolkit/src/main/proto/contact.proto)
declares how `full_name` becomes `name` and how email populates both email fields.
The runtime checks the minimum name length, email syntax, and cross-field
confirmation rule. The
[client](../../examples/protobuf-toolkit/src/main/java/example/WorkflowInputExample.java)
then creates this launch input:

```json
{"operationId":"ed7c3e68-e417-47c5-a905-e4945c8fb88b","content":"Ada Lovelace <ada@example.org>"}
```

The projection contract and workflow input contract are separate gates. Passing
the first does not skip validation of the second by the coordinator.

## Launch and inspect

In the accepted task's **Launch accepted workflow** form, paste the generated
JSON, select **Prepare input**, then **Launch workflow**. Refresh status until
execution completes. Save the job ID separately from the operation ID: the job
identifies execution, while the operation ID identifies the fixture's record.

An operator with `service-invoke` authority can inspect that job using the
`get-job` MCP tool with `{"jobId":"YOUR-JOB-ID"}`. The general starter operator
credential can do this; do not give it to an untrusted client. The browser launch
credential intentionally has narrower access and the launch-status response
does not expose raw input or output.

Check `ok: true`, `job.status: COMPLETED`, and the two checkpoints. NormalizeText
should return `Ada Lovelace <ada@example.org>`. WriteRecord should return the same
operation ID and this SHA-256 of the UTF-8 content:

```text
9b27b5f0692616aade7b6ec572062132fa781efec24f5d7dca9a8f78246bdb00
```

This checks the actual transformation and write identity in addition to message
validity. It is a fixed service example with no model-provider calls.

## Refusal and recovery

Change the source name to `A` and email to `broken`, choose a new output filename,
and rerun the preparation command. It must fail on `string.min_len` and
`string.email` without creating that output file. Restore valid source data
before continuing. Unknown source fields are also refused by the JSON parser.

If a launch reply is lost, reuse the exact saved launch request. Do not create a
new launch identity to find out whether the first one ran. The coordinator's
launch API binds the launch UUID to the accepted candidate and prepared input.
An exact retry returns the existing job; changed intent under that UUID conflicts.
The fixture independently binds its operation ID to the content it stored.

Use the first-workflow guide's stop/start commands and the same browser profile
and URL to inspect the saved launch again. Server jobs and browser launch entries
have different storage. A process restart is not a reason to delete either.

The console's signed task record describes authoring and review evidence. Do not
present that record as a receipt for this new job's output. For the separately
configured execution and assessment receipt path, follow
[recorded correction](recorded-correction.md), including independent verification
of the stored bytes and the stated limits of its demonstration trust.

## Evidence

The automated rehearsal used the same public starter with the client above and
the typed GetAcceptedWorkflow, PrepareWorkflowLaunchInput,
LaunchAcceptedWorkflow, GetWorkflowLaunchStatus, and GetJob RPCs. It checked
invalid local input, completed checkpoints and output hash, exact launch retry,
and status after restarting the coordinator and fixture. It did not exercise the
new input through the browser or establish unfamiliar-user usability.
The [retained observation](../evidence/goal6/projected-workflow.json) records the
job, checkpoints, output hash, and status after restart.

Kafka and connector inputs remain [optional extensions](optional-inputs.md).
