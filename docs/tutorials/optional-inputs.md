# Optional Kafka and JDBC inputs

The first-workflow starter needs neither Kafka nor an external source database.
Add an input integration when your application needs it. ProtoMolt has separate
contracts for named-workflow requests, accepted-workflow launch intents, and
connector pulls; their envelopes are not interchangeable.

The commands here run executable development examples from a source checkout
with JDK 25 and Docker. Testcontainers creates disposable infrastructure. They
do not install Kafka or a connector into the published starter. Deployment
requires the configuration described in the linked guides.

## Kafka to a named workflow

Run:

```sh
./gradlew :protomolt-jobs-service:test --tests '*WorkflowRunKafkaIT'
```

This starts PostgreSQL, Redpanda, a local TCP gRPC fixture, the workflow worker,
and its event relay. A producer sends a typed `WorkflowRunRequest`. The worker
resolves the named workflow, submits and executes the job, and publishes lifecycle
events. The consumer validates the typed events on read. The failure case names
an unknown workflow and requires a failed job and failed event rather than
silently dropping the request.

The [test source](../../jobs/service/src/test/java/ai/protomolt/proto/jobs/service/worker/WorkflowRunKafkaIT.java)
contains the producer, serializer configuration, workflow repository, worker,
and event consumer. The [jobs guide](../../jobs/README.md) explains the request
and event contracts. This path resolves a workflow name; it does not establish
agent review or accepted-candidate identity.

## Kafka to an already accepted workflow

The optional
[accepted-workflow bridge](../../samples/ACCEPTED_WORKFLOW_KAFKA_BRIDGE.md) consumes
a `WorkflowAuthoringLaunchRequest` with a pinned accepted candidate and prepared
input, then calls the authenticated launch RPC. It uses a separate topic and
credential. Do not send this request to the named-workflow topic above.

Run its broker-level behavior checks:

```sh
./gradlew :samples:test --tests '*AcceptedWorkflowKafkaBridgeTest'
```

The broker is real; this test's launch ledger is simulated. It checks offset
handling after a lost response, replay of the same request, conflicts, malformed
records, and denied launches. It does not prove the bridge-to-coordinator path as
one complete deployed system. The coordinator's accepted-launch and recovery
checks are separate evidence in the
[authoring release report](../plans/goal5-release-qualification.md).

For deployment, build the bridge launcher and supply the bootstrap addresses,
topic, stable group, coordinator target, and scoped token file described in its
guide. Successful submission establishes a job; inspect job status to determine
completion. A poison record stops the bridge without deliberately committing its
offset; fix the cause before restarting the same group.

## JDBC to intake and storage

Run:

```sh
./gradlew :protomolt-acquire-jdbc:test --tests '*JdbcPullIT'
```

This starts a PostgreSQL source table, repository storage using PostgreSQL and
LocalStack S3, and the real intake/repository implementations over in-process
gRPC. It verifies first pull, incremental watermark binding, updates retaining
document identity, deduplication on unchanged input, and refusal of contradictory
or unordered queries. The network and credentials of a deployed source database
are outside this example.

The [test source](../../acquire/jdbc/src/test/java/ai/protomolt/proto/acquire/jdbc/JdbcPullIT.java)
contains the SQL setup and incremental query. For a deployed connector, follow
[pull connectors](../acquire/pull-connectors.md): configure the source JDBC URL,
credentials, intake identity, and query. The caller owns the returned watermark
and decides when to request another pull. Connectors do not schedule themselves.

## Check the result

Check the JUnit XML under each module's `build/test-results/test/`, not just
Gradle's exit status. The expected test counts are 2 for `WorkflowRunKafkaIT`,
3 for `AcceptedWorkflowKafkaBridgeTest`, and 6 for `JdbcPullIT`, with zero skipped
and zero failed tests. These classes can skip when Docker is unavailable.

These examples use fixed fixtures and no model-provider calls. They demonstrate
integration behavior, not live-model quality, production throughput, independent
receipt trust, or a single combined Kafka-to-reviewed-workflow deployment.
