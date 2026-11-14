package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.actions.CompleteStepAction;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.store.ClaimLostException;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.ParkedCompletion;
import ai.protomolt.proto.jobs.service.store.WorkerClaim;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.pipeline.DynamicPipelineTransport;
import ai.protomolt.proto.pipeline.PipelineExecutor;
import ai.protomolt.proto.pipeline.PipelineExecutor.Checkpoint;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.GrpcCallStep;
import ai.protomolt.proto.pipeline.v1.MethodShape;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.StringValue;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
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
 * PostgreSQL-backed durability qualification for pipeline external-completion jobs:
 * store/worker reconstruction before and after {@code complete-step}, lost-response
 * duplicate completion, altered-intent refusal, expired lease sweep, worker reassignment,
 * and {@link ClaimLostException} claim fencing with operation-idempotency verification.
 */
@Testcontainers
class PipelineJobPostgresRecoveryIT {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Context.Key<String> OP_KEY = Context.key("pipeline-op-key");

    private static final String PROTO = """
            syntax = "proto3";
            package jobs.pipeline.recovery;
            import "ai/protomolt/proto/validate/v1/validate.proto";
            import "google/protobuf/any.proto";
            import "google/protobuf/wrappers.proto";

            message RequestInput {
              string request_id = 1;
              bytes payload_hash = 2;
              uint64 revision = 3;
              google.protobuf.Any envelope = 4;
            }
            message StagedPayload {
              string request_id = 1;
              bytes payload_hash = 2;
              uint64 revision = 3;
              google.protobuf.Any envelope = 4;
            }
            message ExternalSignoff {
              string approver = 1 [(ai.protomolt.proto.validate.v1.field) = {
                required: true
                string: {min_len: 3}
              }];
              bytes token = 2;
            }
            message ApplyCommand {
              string request_id = 1;
              string approver = 2;
              bytes payload_hash = 3;
              bytes token = 4;
              uint64 revision = 5;
              google.protobuf.Any envelope = 6;
            }
            message AppliedOutcome {
              string committed_key = 1;
              bytes payload_hash = 2;
              bytes token = 3;
              uint64 revision = 4;
              google.protobuf.Any envelope = 5;
            }

            service Stage { rpc Run(RequestInput) returns (StagedPayload); }
            service Signoff { rpc Wait(StagedPayload) returns (ExternalSignoff); }
            service Ledger { rpc Apply(ApplyCommand) returns (AppliedOutcome); }
            """;

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static WorkflowRunDatabase database;
    private static JdbcWorkflowRunStore store;
    private static ActionContext context;
    private static FileDescriptor file;
    private static List<FileDescriptor> files;
    private static String descriptorFp;
    private static String serverName;
    private static Server server;
    private static ManagedChannel channel;

    private static final AtomicInteger stageCalls = new AtomicInteger();
    private static final AtomicInteger ledgerRequests = new AtomicInteger();
    private static final AtomicInteger ledgerExecutions = new AtomicInteger();
    private static final AtomicInteger ledgerDuplicates = new AtomicInteger();
    private static final Map<String, DynamicMessage> ledgerOutcomes = new ConcurrentHashMap<>();

    @BeforeAll
    static void startInfrastructure() throws Exception {
        database = newDatabase();
        store = new JdbcWorkflowRunStore(database);
        context = ActionContext.create();

        CompiledProtos protos = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/protomolt/proto/validate/v1/validate.proto",
                        resource("ai/protomolt/proto/validate/v1/validate.proto"), "test")
                .add("google/protobuf/any.proto",
                        resource("google/protobuf/any.proto"), "test")
                .add("google/protobuf/wrappers.proto",
                        resource("google/protobuf/wrappers.proto"), "test")
                .add("jobs/pipeline/recovery/recovery.proto", PROTO, "test")
                .build());
        file = protos.descriptorFor("jobs/pipeline/recovery/recovery.proto").orElseThrow();
        files = List.of(file);
        descriptorFp = ServiceProfileValidation.sha256(
                FileDescriptorSet.newBuilder().addFile(file.toProto()).build().toByteArray());

        ServerInterceptor opInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                String op = headers.get(DynamicPipelineTransport.OPERATION_ID_HEADER);
                return Contexts.interceptCall(
                        Context.current().withValue(OP_KEY, op == null ? "" : op),
                        call, headers, next);
            }
        };
        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerInterceptors.intercept(stageService(), opInterceptor))
                .addService(ServerInterceptors.intercept(ledgerService(), opInterceptor))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName).build();
    }

    @AfterAll
    static void stopInfrastructure() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
        if (database != null) {
            database.close();
        }
    }

    @BeforeEach
    void resetState() {
        database.inTransaction(c -> {
            try (var stmt = c.createStatement()) {
                stmt.execute("DELETE FROM workflow_run_events_outbox");
                stmt.execute("DELETE FROM workflow_run");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        stageCalls.set(0);
        ledgerRequests.set(0);
        ledgerExecutions.set(0);
        ledgerDuplicates.set(0);
        ledgerOutcomes.clear();
    }

    @Test
    void survivesRestartBeforeAndAfterExternalCompletionLostResponseAndAlteredIntent()
            throws Exception {
        Pipeline pipeline = buildPipeline();
        ByteString hash = ByteString.copyFrom(new byte[] {0x00, 0x7f, (byte) 0x80, (byte) 0xff});
        long unsignedRev = -12345L;
        Any packedEnvelope = Any.pack(StringValue.of("durable-payload"));
        UnknownFieldSet unknownInput = UnknownFieldSet.newBuilder()
                .addField(888, UnknownFieldSet.Field.newBuilder()
                        .addLengthDelimited(ByteString.copyFromUtf8("wire-ext"))
                        .build())
                .build();
        DynamicMessage input = buildInput("req-pg-1", hash, unsignedRev, packedEnvelope,
                unknownInput);

        UUID jobId = UUID.randomUUID();
        PipelineJobCoordinator coordinator1 = new PipelineJobCoordinator(store, 4);
        assertThat(coordinator1.submit(pipeline, files, input, jobId.toString()).ok()).isTrue();

        WorkflowRunWorker worker1 = newWorker(store, "worker-before-park-restart");
        assertThat(worker1.workOnce()).isTrue();
        assertThat(store.get(jobId).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_WAITING);
        assertThat(stageCalls.get()).isEqualTo(1);

        // --- RESTART #1: Close worker & DB pool BEFORE complete-step ---
        worker1.close();
        database.close();
        database = newDatabase();
        store = new JdbcWorkflowRunStore(database);
        PipelineJobCoordinator coordinatorAfterRestart1 = new PipelineJobCoordinator(store, 4);

        PipelineJobCoordinator.ParkedStep parked =
                coordinatorAfterRestart1.parkedStep(jobId).orElseThrow();
        assertThat(parked.stepName()).isEqualTo("signoff");
        assertThat(parked.checkpoints()).hasSize(1);
        assertThat(PipelineJobCoordinator.readDefinition(store.get(jobId).orElseThrow())
                .input().getUnknownFields()).isEqualTo(unknownInput);

        // Altered-intent completion with mismatched fingerprint is refused against Postgres!
        ByteString signoffToken = ByteString.copyFrom(new byte[] {0x11, 0x22, 0x33});
        DynamicMessage signoffMsg = buildSignoff("reviewer-1", signoffToken);
        assertThat(coordinatorAfterRestart1.completeStep(
                jobId, "signoff", "wrong-fp", parked.inputFingerprint(), List.of(signoffMsg)))
                .isEqualTo(new ParkedCompletion.Conflict(WorkflowRunRecord.STATUS_WAITING));

        // Complete through the real CompleteStepAction verb, then simulate lost response (retry)
        // and altered-intent conflict against Postgres row lock.
        CompleteStepAction completeAction = new CompleteStepAction(store);
        ObjectNode completeJson = JSON.createObjectNode();
        completeJson.put("jobId", jobId.toString());
        completeJson.put("stepName", "signoff");
        completeJson.putObject("response")
                .put("approver", "reviewer-1")
                .put("token", Base64.getEncoder().encodeToString(signoffToken.toByteArray()));

        ObjectNode firstReply = ActionCatalog.defaults(ActionContext.create())
                .replace(completeAction)
                .execute("complete-step", completeJson);
        assertThat(firstReply.path("ok").asBoolean()).isTrue();
        assertThat(firstReply.path("status").asText()).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);

        // Lost response: identical retry returns ok=true, status=QUEUED with no extra outbox row!
        int outboxBeforeRetry = store.pollPendingEvents(50).size();
        ObjectNode retryReply = ActionCatalog.defaults(ActionContext.create())
                .replace(completeAction)
                .execute("complete-step", completeJson);
        assertThat(retryReply.path("ok").asBoolean()).isTrue();
        assertThat(retryReply.path("status").asText()).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(store.pollPendingEvents(50)).hasSize(outboxBeforeRetry);

        // Altered response on completed step returns conflict without mutating Postgres state!
        ObjectNode alteredJson = completeJson.deepCopy();
        alteredJson.withObject("response").put("approver", "intruder");
        ObjectNode conflictReply = ActionCatalog.defaults(ActionContext.create())
                .replace(completeAction)
                .execute("complete-step", alteredJson);
        assertThat(conflictReply.path("ok").asBoolean()).isFalse();
        assertThat(conflictReply.path("error").asText()).contains("conflict");

        // --- RESTART #2: Close DB pool AFTER complete-step, reconstruct store & worker ---
        database.close();
        database = newDatabase();
        store = new JdbcWorkflowRunStore(database);
        WorkflowRunWorker workerAfterRestart2 = newWorker(store, "worker-after-complete-restart");
        try {
            assertThat(workerAfterRestart2.workOnce()).isTrue();
        } finally {
            workerAfterRestart2.close();
        }

        // Stage step was NOT re-executed; Ledger step ran once; final binary values match!
        assertThat(stageCalls.get()).isEqualTo(1);
        assertThat(ledgerRequests.get()).isEqualTo(1);
        assertThat(ledgerExecutions.get()).isEqualTo(1);

        WorkflowRunRecord completed = store.get(jobId).orElseThrow();
        assertThat(completed.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);

        PipelineJobCoordinator finalCoordinator = new PipelineJobCoordinator(store, 4);
        List<Checkpoint> checkpoints = finalCoordinator.checkpoints(jobId);
        assertThat(checkpoints).hasSize(3);
        DynamicMessage finalMsg = checkpoints.get(2).messages().getFirst();
        Descriptor outcomeType = finalMsg.getDescriptorForType();
        assertThat(outcomeType.getFullName()).isEqualTo("jobs.pipeline.recovery.AppliedOutcome");
        assertThat(finalMsg.getField(outcomeType.findFieldByName("committed_key")))
                .isEqualTo("req-pg-1:reviewer-1");
        assertThat(finalMsg.getField(outcomeType.findFieldByName("payload_hash"))).isEqualTo(hash);
        assertThat(finalMsg.getField(outcomeType.findFieldByName("token")))
                .isEqualTo(signoffToken);
        assertThat(finalMsg.getField(outcomeType.findFieldByName("revision")))
                .isEqualTo(unsignedRev);
        DynamicMessage finalAny = (DynamicMessage) finalMsg.getField(
                outcomeType.findFieldByName("envelope"));
        assertThat(Any.parseFrom(finalAny.toByteString()).unpack(StringValue.class).getValue())
                .isEqualTo("durable-payload");

        List<WorkflowRunEventRecord> events = store.pollPendingEvents(50);
        assertThat(events).extracting(e -> e.eventType).containsExactly(
                WorkflowRunEventRecord.TYPE_ACCEPTED,
                WorkflowRunEventRecord.TYPE_STEP_CHECKPOINT,
                WorkflowRunEventRecord.TYPE_WAITING,
                WorkflowRunEventRecord.TYPE_STEP_CHECKPOINT,
                WorkflowRunEventRecord.TYPE_STEP_CHECKPOINT,
                WorkflowRunEventRecord.TYPE_COMPLETED);
    }

    @Test
    void expiredPostgresLeaseAndWorkerReassignmentFenceStaleWorkerAndDeduplicateSideEffect()
            throws Exception {
        Pipeline pipeline = buildPipeline();
        DynamicMessage input = buildInput("req-pg-lease", ByteString.copyFromUtf8("h"), 77L,
                Any.pack(StringValue.of("lease-payload")), UnknownFieldSet.getDefaultInstance());
        UUID jobId = UUID.randomUUID();

        PipelineJobCoordinator coordinator = new PipelineJobCoordinator(store, 4);
        assertThat(coordinator.submit(pipeline, files, input, jobId.toString()).ok()).isTrue();

        WorkflowRunWorker workerA = newWorker(store, "worker-a");
        assertThat(workerA.workOnce()).isTrue();
        workerA.close();

        assertThat(coordinator.completeStep(jobId, "signoff",
                List.of(buildSignoff("lead-reviewer", ByteString.copyFromUtf8("tok")))))
                .isInstanceOf(ParkedCompletion.Completed.class);

        // Worker A claims resumed job (attempt 2), executes in-flight Ledger RPC, then its
        // Postgres lease expires BEFORE checkpoint/completion commits!
        WorkflowRunRecord claimedByA = store.claim("worker-a", Duration.ofMinutes(2)).orElseThrow();
        WorkerClaim staleClaimA = WorkerClaim.from(claimedByA);
        assertThat(staleClaimA.attempt()).isEqualTo(2);

        PipelineJobCoordinator.Definition def = PipelineJobCoordinator.readDefinition(claimedByA);
        List<Checkpoint> prior = PipelineJobCoordinator.readCheckpoints(claimedByA, def);
        PipelineExecutor directExecutor = new PipelineExecutor(
                new DynamicPipelineTransport(dep -> channel));

        // Execute the live RPC as Worker A, then expire lease_until in Postgres inside the
        // checkpoint observer right BEFORE Worker A calls saveCheckpoint!
        assertThatThrownBy(() -> directExecutor.runSegment(
                def.pipeline(), def.files(), def.input(), prior,
                new PipelineExecutor.ExecutionIdentity(jobId.toString(), staleClaimA.attempt()),
                cp -> {
                    expireLeaseInPostgres(jobId);
                    assertThat(store.requeueExpiredLeases()).isEqualTo(1);
                    // Worker B claims the requeued job (attempt 3) before Worker A's observer writes!
                    WorkflowRunWorker workerB = newWorker(store, "worker-b");
                    try {
                        assertThat(workerB.workOnce()).isTrue();
                    } finally {
                        workerB.close();
                    }
                    // Stale Worker A now tries to save its checkpoint under expired attempt 2!
                    store.saveCheckpoint(staleClaimA, "[]",
                            WorkflowRunEventFactory.stepCheckpoint(claimedByA, cp.outcome().name()));
                }))
                .isInstanceOfSatisfying(PipelineExecutor.PipelineExecutionException.class,
                        e -> assertThat(e.getCause()).isInstanceOf(ClaimLostException.class));

        // Verify Stage ran once, Ledger received 2 requests with the SAME x-protomolt-operation-id
        // (<jobId>/apply) and executed side effect ONCE, and Postgres holds 1 COMPLETED outcome!
        assertThat(stageCalls.get()).isEqualTo(1);
        assertThat(ledgerRequests.get()).isEqualTo(2);
        assertThat(ledgerExecutions.get()).isEqualTo(1);
        assertThat(ledgerDuplicates.get()).isEqualTo(1);

        WorkflowRunRecord settled = store.get(jobId).orElseThrow();
        assertThat(settled.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(settled.attempt).isEqualTo(3);
        assertThat(store.pollPendingEvents(50).stream()
                .filter(e -> WorkflowRunEventRecord.TYPE_COMPLETED.equals(e.eventType))
                .count()).isEqualTo(1);
    }

    private static void expireLeaseInPostgres(UUID jobId) {
        database.inTransaction(c -> {
            try (var ps = c.prepareStatement(
                    "UPDATE workflow_run SET lease_until = clock_timestamp() - interval '5 seconds'"
                            + " WHERE job_id = ?")) {
                ps.setObject(1, jobId);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private static WorkflowRunDatabase newDatabase() {
        return new WorkflowRunDatabase(new WorkflowRunStoreConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }

    private static WorkflowRunWorker newWorker(JdbcWorkflowRunStore targetStore, String workerId) {
        WorkflowRunsConfig config = new WorkflowRunsConfig(
                workerId, 1, Duration.ofMinutes(2), Duration.ofMillis(15),
                1, 4, 4, null, "workflow-run-events", null, null);
        PipelineExecutor pipelineExecutor = new PipelineExecutor(
                new DynamicPipelineTransport(dep -> channel));
        return new WorkflowRunWorker(
                targetStore, context, null,
                new WorkflowRunner(step -> channel),
                pipelineExecutor, config);
    }

    private static Pipeline buildPipeline() {
        String endpoint = "in-process." + serverName;
        return Pipeline.newBuilder()
                .setName("pg-durable-pipeline")
                .setInputType("jobs.pipeline.recovery.RequestInput")
                .setDescriptorFingerprint(descriptorFp)
                .setMaxStreamMessages(32)
                .setDeadline(com.google.protobuf.Duration.newBuilder().setSeconds(30).build())
                .addDependencies(ServiceDependency.newBuilder()
                        .setAlias("stage")
                        .setServiceProfile("stage")
                        .setEndpoint(endpoint)
                        .setDescriptorFingerprint(descriptorFp)
                        .build())
                .addDependencies(ServiceDependency.newBuilder()
                        .setAlias("signoff")
                        .setServiceProfile("signoff")
                        .setEndpoint(endpoint)
                        .setDescriptorFingerprint(descriptorFp)
                        .build())
                .addDependencies(ServiceDependency.newBuilder()
                        .setAlias("ledger")
                        .setServiceProfile("ledger")
                        .setEndpoint(endpoint)
                        .setDescriptorFingerprint(descriptorFp)
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("stage")
                        .setDependency("stage")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("jobs.pipeline.recovery.Stage/Run")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(TypedEdge.newBuilder()
                                        .addSources("input")
                                        .setProduceType("jobs.pipeline.recovery.RequestInput")
                                        .addRules("request_id = input.request_id")
                                        .addRules("payload_hash = input.payload_hash")
                                        .addRules("revision = input.revision")
                                        .addRules("envelope = input.envelope")
                                        .build())
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("signoff")
                        .setDependency("signoff")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("jobs.pipeline.recovery.Signoff/Wait")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_EXTERNAL)
                                .setValidateResponse(true)
                                .build())
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("apply")
                        .setDependency("ledger")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("jobs.pipeline.recovery.Ledger/Apply")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(TypedEdge.newBuilder()
                                        .addSources("stage")
                                        .addSources("signoff")
                                        .setProduceType("jobs.pipeline.recovery.ApplyCommand")
                                        .addRules("request_id = stage.request_id")
                                        .addRules("approver = signoff.approver")
                                        .addRules("payload_hash = stage.payload_hash")
                                        .addRules("token = signoff.token")
                                        .addRules("revision = stage.revision")
                                        .addRules("envelope = stage.envelope")
                                        .build())
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .build();
    }

    private static DynamicMessage buildInput(String id, ByteString hash, long revision,
                                             Any envelope, UnknownFieldSet unknown)
            throws Exception {
        Descriptor t = file.findMessageTypeByName("RequestInput");
        return DynamicMessage.newBuilder(t)
                .setField(t.findFieldByName("request_id"), id)
                .setField(t.findFieldByName("payload_hash"), hash)
                .setField(t.findFieldByName("revision"), revision)
                .setField(t.findFieldByName("envelope"),
                        DynamicMessage.parseFrom(Any.getDescriptor(), envelope.toByteString()))
                .setUnknownFields(unknown)
                .build();
    }

    private static DynamicMessage buildSignoff(String approver, ByteString token) {
        Descriptor t = file.findMessageTypeByName("ExternalSignoff");
        return DynamicMessage.newBuilder(t)
                .setField(t.findFieldByName("approver"), approver)
                .setField(t.findFieldByName("token"), token)
                .build();
    }

    private static ServerServiceDefinition stageService() {
        MethodDescriptor m = file.findServiceByName("Stage").findMethodByName("Run");
        var wire = DynamicGrpcCalls.methodDescriptor(m);
        var svc = io.grpc.ServiceDescriptor.newBuilder("jobs.pipeline.recovery.Stage")
                .addMethod(wire).build();
        Descriptor in = m.getInputType();
        Descriptor out = m.getOutputType();
        return ServerServiceDefinition.builder(svc)
                .addMethod(wire, ServerCalls.asyncUnaryCall((req, obs) -> {
                    stageCalls.incrementAndGet();
                    DynamicMessage.Builder b = DynamicMessage.newBuilder(out)
                            .setField(out.findFieldByName("request_id"),
                                    req.getField(in.findFieldByName("request_id")))
                            .setField(out.findFieldByName("payload_hash"),
                                    req.getField(in.findFieldByName("payload_hash")))
                            .setField(out.findFieldByName("revision"),
                                    req.getField(in.findFieldByName("revision")));
                    if (req.hasField(in.findFieldByName("envelope"))) {
                        b.setField(out.findFieldByName("envelope"),
                                req.getField(in.findFieldByName("envelope")));
                    }
                    obs.onNext(b.build());
                    obs.onCompleted();
                }))
                .build();
    }

    private static ServerServiceDefinition ledgerService() {
        MethodDescriptor m = file.findServiceByName("Ledger").findMethodByName("Apply");
        var wire = DynamicGrpcCalls.methodDescriptor(m);
        var svc = io.grpc.ServiceDescriptor.newBuilder("jobs.pipeline.recovery.Ledger")
                .addMethod(wire).build();
        Descriptor in = m.getInputType();
        Descriptor out = m.getOutputType();
        return ServerServiceDefinition.builder(svc)
                .addMethod(wire, ServerCalls.asyncUnaryCall((req, obs) -> {
                    ledgerRequests.incrementAndGet();
                    String opKey = OP_KEY.get();
                    DynamicMessage existing = ledgerOutcomes.get(opKey);
                    if (existing != null) {
                        ledgerDuplicates.incrementAndGet();
                        obs.onNext(existing);
                        obs.onCompleted();
                        return;
                    }
                    DynamicMessage created = ledgerOutcomes.computeIfAbsent(opKey, k -> {
                        ledgerExecutions.incrementAndGet();
                        String reqId = (String) req.getField(in.findFieldByName("request_id"));
                        String approver = (String) req.getField(in.findFieldByName("approver"));
                        DynamicMessage.Builder b = DynamicMessage.newBuilder(out)
                                .setField(out.findFieldByName("committed_key"),
                                        reqId + ":" + approver)
                                .setField(out.findFieldByName("payload_hash"),
                                        req.getField(in.findFieldByName("payload_hash")))
                                .setField(out.findFieldByName("token"),
                                        req.getField(in.findFieldByName("token")))
                                .setField(out.findFieldByName("revision"),
                                        req.getField(in.findFieldByName("revision")));
                        if (req.hasField(in.findFieldByName("envelope"))) {
                            b.setField(out.findFieldByName("envelope"),
                                    req.getField(in.findFieldByName("envelope")));
                        }
                        return b.build();
                    });
                    obs.onNext(created);
                    obs.onCompleted();
                }))
                .build();
    }

    private static String resource(String path) throws Exception {
        try (InputStream in = PipelineJobPostgresRecoveryIT.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
