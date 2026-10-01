# Forward an accepted workflow launch through Kafka

This optional local example sends a saved launch intent to a broker, then runs
the existing bridge to submit it to the published coordinator. It does not add
Kafka to the default starter or use the named-workflow request topic.

Complete [the projected workflow setup](projected-workflow.md) through creation
of `workflow-input.json`, with an accepted task in the running starter. You need
JDK 25, Docker, `grpcurl`, and `jq`. Run the commands from the repository root.
Keep JSON artifacts in a separate directory:

```sh
KAFKA_DEMO_DIR=$(mktemp -d)
cp /tmp/protomolt-contact-example/workflow-input.json "$KAFKA_DEMO_DIR/"
```

## Save the launch intent

Set `TASK_ID` to the accepted task, `LAUNCH_ID` to a new lowercase UUID, and
`GRPC_TARGET` to the coordinator's loopback gRPC address, normally
`127.0.0.1:9090`. Export `PROTOMOLT_API_TOKEN` from a private credential file with
workflow-launch authority. Keep credentials out of shell history and artifacts.

```sh
grpcurl -plaintext -max-time 30 -expand-headers \
  -H 'api_token: ${PROTOMOLT_API_TOKEN}' -d "{\"taskId\":\"$TASK_ID\"}" \
  "$GRPC_TARGET" ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringService/GetAcceptedWorkflow > "$KAFKA_DEMO_DIR/accepted.json"
jq -n --slurpfile acceptance "$KAFKA_DEMO_DIR/accepted.json" --rawfile input "$KAFKA_DEMO_DIR/workflow-input.json" \
  '{acceptance:$acceptance[0],inputJson:($input|@base64)}' > "$KAFKA_DEMO_DIR/prepare.json"
grpcurl -plaintext -max-time 30 -expand-headers \
  -H 'api_token: ${PROTOMOLT_API_TOKEN}' -d @ "$GRPC_TARGET" \
  ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchInputService/PrepareWorkflowLaunchInput < "$KAFKA_DEMO_DIR/prepare.json" > "$KAFKA_DEMO_DIR/prepared.json"
jq --arg id "$LAUNCH_ID" '{launchId:$id,acceptance,input}' "$KAFKA_DEMO_DIR/prepared.json" > "$KAFKA_DEMO_DIR/launch.json"
```

Check every command succeeds before continuing. Keep `launch.json` unchanged
after publishing. For TLS endpoints, remove `-plaintext` and configure the
appropriate trust material. The local starter browser token has launch authority
but also coordination authority; a deployed bridge should receive a dedicated
credential with only `workflow-launch` authority.

## Start the optional broker and bridge

The pinned broker version below is the existing integration-test fixture. This
is a loopback development setup, not a production broker configuration.

```sh
docker run -d --name protomolt-kafka-example -p 127.0.0.1:19093:19093 \
  docker.redpanda.com/redpandadata/redpanda:v22.2.1 \
  redpanda start --overprovisioned --smp 1 --memory 512M --reserve-memory 0M \
  --node-id 0 --check=false --kafka-addr PLAINTEXT://0.0.0.0:19093 \
  --advertise-kafka-addr PLAINTEXT://127.0.0.1:19093
docker exec protomolt-kafka-example rpk topic create accepted-launches --brokers 127.0.0.1:19093
./gradlew :samples:installAcceptedWorkflowKafkaBridge
```

If the broker is still starting, wait for readiness before creating the topic.
In a separate terminal, start the bridge with the same working-directory context:

```sh
export PROTOMOLT_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19093
export PROTOMOLT_KAFKA_ACCEPTED_LAUNCH_TOPIC=accepted-launches
export PROTOMOLT_KAFKA_ACCEPTED_LAUNCH_GROUP=accepted-launch-example
export PROTOMOLT_COORDINATOR_TARGET="$GRPC_TARGET"
export PROTOMOLT_WORKFLOW_LAUNCH_TOKEN_FILE=/absolute/path/to/private-launch-token
export PROTOMOLT_COORDINATOR_PLAINTEXT=true
samples/build/install/accepted-workflow-kafka-bridge/bin/accepted-workflow-kafka-bridge
```

The target and token file must be available in that terminal. Do not put the
token value in the environment variable that expects a file path.

## Publish and check completion

Run the producer from the [bridge guide](../../samples/ACCEPTED_WORKFLOW_KAFKA_BRIDGE.md)
with topic `accepted-launches` and path `"$KAFKA_DEMO_DIR/launch.json"`.
Then inspect the bound job:

```sh
jq '{request:.}' "$KAFKA_DEMO_DIR/launch.json" | grpcurl -plaintext -max-time 30 -expand-headers \
  -H 'api_token: ${PROTOMOLT_API_TOKEN}' -d @ "$GRPC_TARGET" \
  ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchStatusService/GetWorkflowLaunchStatus
docker exec protomolt-kafka-example rpk group describe accepted-launch-example --brokers 127.0.0.1:19093
```

Submission and a committed offset do not prove execution completed. Repeat the
read-only status call until the job is terminal. For this example, expect
`WORKFLOW_LAUNCH_JOB_STATE_COMPLETED`; inspect output using the operator's
`get-job` tool as described in the projected-workflow guide.

Publish the same saved file again. The consumer offset should advance, while
the launch UUID still identifies the same job and result. Changing the request
under that UUID is a conflict, not a request for a second job. A bridge error
stops processing without deliberately committing the failing record; diagnose
the error before restarting the same consumer group. Do not reset offsets merely
to hide it.

Stop the bridge with Ctrl-C when finished. Remove only the disposable broker
created for this walkthrough with `docker rm -f protomolt-kafka-example`.
Do not remove the starter's persistent volumes.

## Evidence and deployment boundaries

The [local rehearsal](../evidence/goal6/kafka-accepted-workflow.json) records a
real broker and bridge forwarding to the published starter, completed service
steps, exact replay with unchanged job state, and both committed offsets.
Invalid producer input failed before publishing. The rehearsal used the starter
browser credential, loopback plaintext, and a scripted workflow. It does not
qualify Kafka ACLs, TLS/SASL, a dedicated least-privilege principal, throughput,
or multi-host operations. Topic write permission grants use of the bridge's
launch credential; configure topic access accordingly for a deployed system.
