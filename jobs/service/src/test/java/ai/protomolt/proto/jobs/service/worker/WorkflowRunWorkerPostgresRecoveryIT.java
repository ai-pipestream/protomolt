package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.jobs.v1.WorkflowRunEvent;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import io.grpc.ServerServiceDefinition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PostgreSQL-backed checkpoint recovery through a newly constructed store and worker.
 * This is an orderly reconstruction test, not a process-kill qualification.
 */
@Testcontainers(disabledWithoutDocker = true)
class WorkflowRunWorkerPostgresRecoveryIT {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PROTO = """
            syntax = "proto3";
            package jobs.recovery;
            message Input { string text = 1; string request_key = 2; }
            message Prepared { string text = 1; string request_key = 2; }
            message EffectRequest { string request_key = 1; string content = 2; }
            message Result { string value = 1; }
            service Prepare { rpc Run(Input) returns (Prepared); }
            service Effect { rpc Apply(EffectRequest) returns (Result); }
            """;

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static WorkflowRunDatabase database;
    private static JdbcWorkflowRunStore store;
    private static ActionContext context;
    private static CompiledProtos protos;
    private static String serverName;
    private static Server server;
    private static ManagedChannel fixtureChannel;
    private static Fixture fixture;

    @BeforeAll
    static void startDatabaseAndFixture() throws Exception {
        database = newDatabase();
        store = new JdbcWorkflowRunStore(database);
        context = ActionContext.create();
        protos = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("jobs/recovery/recovery.proto", PROTO, "test").build());
        fixture = new Fixture(protos);
        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(fixture.prepareService())
                .addService(fixture.effectService())
                .build().start();
        fixtureChannel = InProcessChannelBuilder.forName(serverName).build();
    }

    @AfterAll
    static void stop() {
        if (fixtureChannel != null) fixtureChannel.shutdownNow();
        if (server != null) server.shutdownNow();
        if (database != null) database.close();
    }

    @BeforeEach
    void clean() {
        database.inTransaction(c -> {
            try (var statement = c.createStatement()) {
                statement.execute("DROP TRIGGER IF EXISTS fail_effect_checkpoint "
                        + "ON workflow_run_events_outbox");
                statement.execute("DROP FUNCTION IF EXISTS fail_effect_checkpoint()");
                statement.execute("DELETE FROM workflow_run_events_outbox");
                statement.execute("DELETE FROM workflow_run");
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        fixture.reset();
    }

    @Test
    void reconstructedWorkerResumesAfterRemoteSuccessBeforeCheckpointCommit() throws Exception {
        UUID jobId = UUID.randomUUID();
        String key = jobId + "/effect";
        ObjectNode input = JSON.createObjectNode().put("text", "payload").put("request_key", key);
        ObjectNode workflow = workflow();
        var submitted = new WorkflowRunSubmitter(store, null, 3).submit(
                workflow, null, input, jobId.toString(), context);
        assertThat(submitted.ok()).as("submit failed: %s", submitted.error()).isTrue();

        installBoundaryFailure();
        WorkflowRunWorker firstWorker = worker(store, "worker-before-restart");
        assertThat(firstWorker.workOnce()).isTrue();

        WorkflowRunRecord retry = store.get(jobId).orElseThrow();
        assertThat(retry.status).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(retry.attempt).isEqualTo(1);
        var firstCheckpoint = JSON.readTree(retry.checkpoints);
        assertThat(firstCheckpoint).hasSize(1);
        assertThat(firstCheckpoint.get(0).get("name").asText()).isEqualTo("prepare");
        assertThat(firstCheckpoint.get(0).get("response").get("requestKey").asText())
                .isEqualTo(key);
        assertThat(fixture.prepareCalls.get()).isEqualTo(1);
        assertThat(fixture.effectRequests.get()).isEqualTo(1);
        assertThat(fixture.effectExecutions.get()).isEqualTo(1);

        // Rebuild both database pool/Flyway wrapper and the worker after the retry row is durable.
        firstWorker.close();
        database.close();
        database = newDatabase();
        JdbcWorkflowRunStore restartedStore = new JdbcWorkflowRunStore(database);
        dropBoundaryFailure();
        // Backoff is production behavior; move only the durable eligibility timestamp forward so
        // this recovery assertion does not sleep for a real second.
        restartedStore.requeue(jobId, Duration.ZERO);
        WorkflowRunWorker restartedWorker = worker(restartedStore, "worker-after-restart");
        assertThat(restartedWorker.workOnce()).isTrue();

        WorkflowRunRecord completed = restartedStore.get(jobId).orElseThrow();
        assertThat(completed.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(completed.attempt).isEqualTo(2);
        var checkpoints = JSON.readTree(completed.checkpoints);
        assertThat(checkpoints).hasSize(2);
        assertThat(checkpoints.get(0)).isEqualTo(firstCheckpoint.get(0));
        assertThat(checkpoints.get(0).get("name").asText()).isEqualTo("prepare");
        assertThat(checkpoints.get(1).get("name").asText()).isEqualTo("effect");
        assertThat(JSON.readTree(completed.result).get("value").asText())
                .isEqualTo("stored:payload");

        // The completed first step was replayed from PostgreSQL, while the uncheckpointed second
        // RPC repeated with the same job+step key and was deduplicated by the fixture service.
        assertThat(fixture.prepareCalls.get()).isEqualTo(1);
        assertThat(fixture.effectRequests.get()).isEqualTo(2);
        assertThat(fixture.effectExecutions.get()).isEqualTo(1);
        assertThat(fixture.effectDuplicates.get()).isEqualTo(1);

        List<WorkflowRunEventRecord> events = restartedStore.pollPendingEvents(20);
        assertThat(events).hasSize(4);
        assertThat(events).extracting(event -> event.eventType).containsExactly(
                WorkflowRunEventRecord.TYPE_ACCEPTED,
                WorkflowRunEventRecord.TYPE_STEP_CHECKPOINT,
                WorkflowRunEventRecord.TYPE_STEP_CHECKPOINT,
                WorkflowRunEventRecord.TYPE_COMPLETED);
        List<WorkflowRunEvent> decoded = events.stream().map(event -> {
            try {
                return WorkflowRunEvent.parseFrom(event.payload);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }).toList();
        assertThat(decoded.get(1).getStep()).isEqualTo("prepare");
        assertThat(decoded.get(1).getAttempt()).isEqualTo(1);
        assertThat(decoded.get(2).getStep()).isEqualTo("effect");
        assertThat(decoded.get(2).getAttempt()).isEqualTo(2);

        DynamicMessage changed = fixture.effectRequest(key, "changed-content");
        assertThatThrownBy(() -> DynamicGrpcCalls.call(fixtureChannel,
                fixture.effectMethod(), changed,
                CallOptions.DEFAULT.withDeadlineAfter(3, java.util.concurrent.TimeUnit.SECONDS),
                new Metadata(), 1))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(error -> assertThat(((StatusRuntimeException) error).getStatus().getCode())
                        .isEqualTo(Status.Code.ALREADY_EXISTS));
        assertThat(fixture.effectExecutions.get()).isEqualTo(1);
        assertThat(fixture.effectConflicts.get()).isEqualTo(1);
        restartedWorker.close();
    }

    private static WorkflowRunDatabase newDatabase() {
        return new WorkflowRunDatabase(new WorkflowRunStoreConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }

    private static WorkflowRunWorker worker(JdbcWorkflowRunStore targetStore, String workerId) {
        WorkflowRunsConfig config = new WorkflowRunsConfig(workerId, 1,
                Duration.ofMinutes(2), Duration.ofMillis(10), 1, 3, 4,
                null, "workflow-run-events", null, null);
        WorkflowRunner runner = new WorkflowRunner(
                step -> InProcessChannelBuilder.forName(serverName).build());
        return new WorkflowRunWorker(targetStore, context, null, runner, config);
    }

    private static ObjectNode workflow() {
        String descriptorSet = Base64.getEncoder()
                .encodeToString(protos.descriptorSet().toByteArray());
        ObjectNode workflow = JSON.createObjectNode().put("name", "restartable-effect");
        workflow.put("validateContract", true);
        workflow.putObject("schema").put("descriptorSetBase64", descriptorSet);
        workflow.put("inputType", "jobs.recovery.Input");
        var steps = workflow.putArray("steps");
        steps.addObject().put("name", "prepare").put("target", "fixture:1")
                .put("method", "jobs.recovery.Prepare/Run")
                .putArray("rules").add("text = input.text").add("request_key = input.request_key");
        steps.addObject().put("name", "effect").put("target", "fixture:1")
                .put("method", "jobs.recovery.Effect/Apply")
                .putArray("rules").add("request_key = prepare.request_key")
                .add("content = prepare.text");
        workflow.putObject("output").put("type", "jobs.recovery.Result")
                .putArray("rules").add("value = effect.value");
        return workflow;
    }

    private void installBoundaryFailure() {
        database.inTransaction(c -> {
            try (var statement = c.createStatement()) {
                statement.execute("""
                        CREATE FUNCTION fail_effect_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN
                          IF NEW.event_type = 'STEP_CHECKPOINT' AND EXISTS (
                            SELECT 1 FROM workflow_run r
                             WHERE r.job_id::text = NEW.kafka_key
                               AND jsonb_array_length(r.checkpoints) >= 2
                               AND r.checkpoints->1->>'name' = 'effect'
                          ) THEN
                            RAISE EXCEPTION 'injected checkpoint persistence failure';
                          END IF;
                          RETURN NEW;
                        END
                        $$
                        """);
                statement.execute("CREATE TRIGGER fail_effect_checkpoint BEFORE INSERT "
                        + "ON workflow_run_events_outbox FOR EACH ROW "
                        + "EXECUTE FUNCTION fail_effect_checkpoint()");
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private void dropBoundaryFailure() {
        database.inTransaction(c -> {
            try (var statement = c.createStatement()) {
                statement.execute("DROP TRIGGER IF EXISTS fail_effect_checkpoint "
                        + "ON workflow_run_events_outbox");
                statement.execute("DROP FUNCTION IF EXISTS fail_effect_checkpoint()");
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private static final class Fixture {
        private final com.google.protobuf.Descriptors.FileDescriptor file;
        private final com.google.protobuf.Descriptors.MethodDescriptor prepareMethod;
        private final com.google.protobuf.Descriptors.MethodDescriptor effectMethod;
        private final AtomicInteger prepareCalls = new AtomicInteger();
        private final AtomicInteger effectRequests = new AtomicInteger();
        private final AtomicInteger effectExecutions = new AtomicInteger();
        private final AtomicInteger effectDuplicates = new AtomicInteger();
        private final AtomicInteger effectConflicts = new AtomicInteger();
        private final Map<String, String> idempotencyLedger = new ConcurrentHashMap<>();

        private Fixture(CompiledProtos protos) {
            file = protos.descriptorFor("jobs/recovery/recovery.proto").orElseThrow();
            prepareMethod = file.findServiceByName("Prepare").findMethodByName("Run");
            effectMethod = file.findServiceByName("Effect").findMethodByName("Apply");
        }

        private ServerServiceDefinition prepareService() {
            var wireMethod = DynamicGrpcCalls.methodDescriptor(prepareMethod);
            var service = io.grpc.ServiceDescriptor.newBuilder(prepareMethod.getService().getFullName())
                    .addMethod(wireMethod).build();
            return ServerServiceDefinition.builder(service)
                    .addMethod(wireMethod, ServerCalls.asyncUnaryCall((request, out) -> {
                        prepareCalls.incrementAndGet();
                        var output = DynamicMessage.newBuilder(prepareMethod.getOutputType())
                                .setField(prepareMethod.getOutputType().findFieldByName("text"),
                                        request.getField(prepareMethod.getInputType()
                                                .findFieldByName("text")))
                                .setField(prepareMethod.getOutputType()
                                                .findFieldByName("request_key"),
                                        request.getField(prepareMethod.getInputType()
                                                .findFieldByName("request_key")))
                                .build();
                        out.onNext(output);
                        out.onCompleted();
                    })).build();
        }

        private ServerServiceDefinition effectService() {
            var wireMethod = DynamicGrpcCalls.methodDescriptor(effectMethod);
            var service = io.grpc.ServiceDescriptor.newBuilder(effectMethod.getService().getFullName())
                    .addMethod(wireMethod).build();
            return ServerServiceDefinition.builder(service)
                    .addMethod(wireMethod, ServerCalls.asyncUnaryCall((request, out) -> {
                        effectRequests.incrementAndGet();
                        String key = (String) request.getField(
                                effectMethod.getInputType().findFieldByName("request_key"));
                        String content = (String) request.getField(
                                effectMethod.getInputType().findFieldByName("content"));
                        String previous = idempotencyLedger.putIfAbsent(key, content);
                        if (previous != null && !previous.equals(content)) {
                            effectConflicts.incrementAndGet();
                            out.onError(Status.ALREADY_EXISTS
                                    .withDescription("request key reused with different content")
                                    .asRuntimeException());
                            return;
                        }
                        if (previous == null) effectExecutions.incrementAndGet();
                        else effectDuplicates.incrementAndGet();
                        String stored = previous == null ? content : previous;
                        out.onNext(DynamicMessage.newBuilder(effectMethod.getOutputType())
                                .setField(effectMethod.getOutputType().findFieldByName("value"),
                                        "stored:" + stored).build());
                        out.onCompleted();
                    })).build();
        }

        private DynamicMessage effectRequest(String key, String content) {
            return DynamicMessage.newBuilder(effectMethod.getInputType())
                    .setField(effectMethod.getInputType().findFieldByName("request_key"), key)
                    .setField(effectMethod.getInputType().findFieldByName("content"), content)
                    .build();
        }

        private com.google.protobuf.Descriptors.MethodDescriptor effectMethod() {
            return effectMethod;
        }

        private void reset() {
            prepareCalls.set(0);
            effectRequests.set(0);
            effectExecutions.set(0);
            effectDuplicates.set(0);
            effectConflicts.set(0);
            idempotencyLedger.clear();
        }
    }
}
