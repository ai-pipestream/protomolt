# Accepted-workflow Kafka bridge sample

This optional sample consumes `WorkflowAuthoringLaunchRequest` protobuf bytes from a dedicated Kafka topic and calls the existing authenticated `WorkflowAuthoringService.LaunchAcceptedWorkflow` RPC. It is a separate process; no Kafka launch ingress is mounted in ProtoMolt Serve. A successful RPC establishes a matching job, not completed execution.

Build the standalone launcher with `./gradlew :samples:installAcceptedWorkflowKafkaBridge`. The script is under `samples/build/install/accepted-workflow-kafka-bridge/bin/accepted-workflow-kafka-bridge`. Configure it with:

| Environment variable | Meaning |
| --- | --- |
| `PROTOMOLT_KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap addresses. |
| `PROTOMOLT_KAFKA_ACCEPTED_LAUNCH_TOPIC` | Dedicated launch-intent topic, separate from the `WorkflowRunRequest` request topic. |
| `PROTOMOLT_KAFKA_ACCEPTED_LAUNCH_GROUP` | Stable consumer group ID retained across restarts. |
| `PROTOMOLT_COORDINATOR_TARGET` | gRPC target of the coordinator with workflow authoring mounted. |
| `PROTOMOLT_WORKFLOW_LAUNCH_TOKEN_FILE` | Path to a mounted credential file whose principal has only the `workflow-launch` scope. |
| `PROTOMOLT_KAFKA_CLIENT_PROPERTIES_FILE` | Optional mounted Java properties file for Kafka TLS/SASL settings. |
| `PROTOMOLT_COORDINATOR_PLAINTEXT` | Set to `true` only for a local trusted test channel; gRPC TLS is the default. |
| `PROTOMOLT_LAUNCH_RPC_TIMEOUT_SECONDS` | Per-launch deadline, 1–120 seconds; defaults to 30. |

Publish the serialized `WorkflowAuthoringLaunchRequest` as the record value and its canonical lowercase `launch_id` UUID as the Kafka key. The request contains the server-computed accepted-candidate identity and the prepared input artifact reference. The coordinator reloads trusted acceptance and input evidence, independently verifies the workflow, writes a keyed authorization, and inserts the job. Producers must reuse the exact same request and UUID after uncertainty. A changed request under that UUID conflicts.

The installed distribution also contains a producer for a saved protobuf-JSON
launch request. It validates the envelope before connecting and serializes the
protobuf bytes with the matching key:

```sh
java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
  -cp 'samples/build/install/accepted-workflow-kafka-bridge/lib/*' \
  ai.protomolt.proto.samples.PublishAcceptedWorkflowLaunch \
  127.0.0.1:19093 accepted-launches /absolute/path/to/launch.json
```

It reads optional Kafka TLS/SASL settings from the same
`PROTOMOLT_KAFKA_CLIENT_PROPERTIES_FILE` environment variable as the bridge.
The acknowledgement proves broker delivery only. Retain `launch.json` and use
the launch-status RPC to observe execution. See the
[local end-to-end walkthrough](../docs/tutorials/kafka-accepted-workflow.md).

Protect the topic with Kafka ACLs so only operators authorized to request launches can produce records. The bridge uses its own mounted `workflow-launch` credential for every RPC. **Topic write permission grants use of that service credential; the bridge does not authenticate individual producers.** Keep the Kafka client properties and credential file out of logs and images. Configure Kafka TLS/SASL through the mounted properties file and any required JVM truststore through the process environment.

The bridge disables auto commit and polls one record at a time. It commits only that record's partition offset after a validated launch result with the requested job UUID. A lost RPC response or uncertain offset commit may cause the same launch intent to be retried; the keyed authorization and job ID make this replay idempotent. Invalid bytes, an unknown field, a key mismatch, a denied credential, conflicting content, or a transport/storage failure stops the bridge without deliberately committing that record. Inspect and correct a poison record before restarting its consumer group; the bridge does not skip or dead-letter it. Do not send these values to the existing `WorkflowRunRequest` topic, whose named-workflow resolver has different semantics.

This sample is not deployed by the checked-in Compose stacks. Existing accepted-workflow gRPC/REST and job-status operations remain the supported launch and observation surfaces.
