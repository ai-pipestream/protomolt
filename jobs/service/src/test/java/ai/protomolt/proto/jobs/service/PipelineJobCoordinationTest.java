package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.jobs.service.actions.CompleteStepAction;
import ai.protomolt.proto.jobs.service.actions.GetJobAction;
import ai.protomolt.proto.jobs.service.actions.ListJobsAction;
import ai.protomolt.proto.jobs.service.actions.SubmitWorkflowAction;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.store.ClaimLostException;
import ai.protomolt.proto.jobs.service.store.InMemoryWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.ParkedCompletion;
import ai.protomolt.proto.jobs.service.store.WorkerClaim;
import ai.protomolt.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.pipeline.DynamicPipelineTransport;
import ai.protomolt.proto.pipeline.PipelineExecutor;
import ai.protomolt.proto.pipeline.PipelineExecutor.Checkpoint;
import ai.protomolt.proto.pipeline.v1.CollectStep;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.GrpcCallStep;
import ai.protomolt.proto.pipeline.v1.MethodShape;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineOutput;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Duration;
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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end unit tests for durable pipeline job coordination over {@link InMemoryWorkflowRunStore}
 * and in-process gRPC services: external completion, binary protobuf checkpoint/evidence fidelity,
 * duplicate completion, lost-response retry, altered-intent refusal, validation verdicts,
 * lease expiry, and worker reassignment.
 */
class PipelineJobCoordinationTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Context.Key<String> OP_ID_CTX = Context.key("op-id");

    private static final String SCHEMA = """
            syntax = "proto3";
            package jobs.pipeline.test;
            import "ai/protomolt/proto/validate/v1/validate.proto";
            import "google/protobuf/any.proto";
            import "google/protobuf/wrappers.proto";

            message OrderInput {
              string order_id = 1;
              bytes digest = 2;
              uint64 sequence_no = 3;
              google.protobuf.Any detail = 4;
              repeated BranchItem items = 5;
            }
            message BranchItem {
              string label = 1;
            }
            message BranchBag {
              repeated BranchItem items = 1;
            }
            message PreparedOrder {
              string order_id = 1;
              bytes digest = 2;
              uint64 sequence_no = 3;
              google.protobuf.Any detail = 4;
            }
            message ApprovalDecision {
              string reviewer = 1 [(ai.protomolt.proto.validate.v1.field) = {
                required: true
                string: {min_len: 3}
              }];
              string decision = 2 [(ai.protomolt.proto.validate.v1.field) = {
                required: true
                string: {min_len: 4}
              }];
              bytes signature = 3;
              uint64 signed_at_tick = 4;
            }
            message ApprovalBatch {
              repeated ApprovalDecision decisions = 1;
            }
            message CommitRequest {
              string order_id = 1;
              string decision = 2;
              bytes digest = 3;
              bytes signature = 4;
              uint64 sequence_no = 5;
              google.protobuf.Any detail = 6;
            }
            message CommitOutcome {
              string receipt = 1;
              bytes digest = 2;
              bytes signature = 3;
              uint64 sequence_no = 4;
              google.protobuf.Any detail = 5;
            }

            service PrepareService {
              rpc Prepare(OrderInput) returns (PreparedOrder);
              rpc CheckBranch(BranchItem) returns (BranchItem);
            }
            service ApprovalDesk {
              rpc Approve(PreparedOrder) returns (ApprovalDecision);
              rpc StreamApprovals(PreparedOrder) returns (stream ApprovalDecision);
            }
            service CommitService {
              rpc Commit(CommitRequest) returns (CommitOutcome);
            }
            """;

    private static FileDescriptor file;
    private static List<FileDescriptor> files;
    private static String descriptorFp;
    private static String serverName;
    private static Server server;
    private static ManagedChannel channel;

    private static final AtomicInteger prepareCalls = new AtomicInteger();
    private static final AtomicInteger branchCalls = new AtomicInteger();
    private static final AtomicInteger commitRequests = new AtomicInteger();
    private static final AtomicInteger commitSideEffects = new AtomicInteger();
    private static final Map<String, DynamicMessage> commitLedger = new ConcurrentHashMap<>();

    private InMemoryWorkflowRunStore store;
    private ActionContext context;
    private PipelineJobCoordinator coordinator;
    private CompleteStepAction completeStepAction;
    private GetJobAction getJobAction;
    private ListJobsAction listJobsAction;

    @BeforeAll
    static void setUpFixture() throws Exception {
        CompiledProtos protos = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/protomolt/proto/validate/v1/validate.proto",
                        resource("ai/protomolt/proto/validate/v1/validate.proto"), "test")
                .add("google/protobuf/any.proto",
                        resource("google/protobuf/any.proto"), "test")
                .add("google/protobuf/wrappers.proto",
                        resource("google/protobuf/wrappers.proto"), "test")
                .add("jobs/pipeline/test/coord.proto", SCHEMA, "test")
                .build());
        file = protos.descriptorFor("jobs/pipeline/test/coord.proto").orElseThrow();
        files = List.of(file);
        descriptorFp = ServiceProfileValidation.sha256(
                FileDescriptorSet.newBuilder().addFile(file.toProto()).build().toByteArray());

        serverName = InProcessServerBuilder.generateName();
        ServerInterceptor opInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                String opId = headers.get(DynamicPipelineTransport.OPERATION_ID_HEADER);
                return Contexts.interceptCall(
                        Context.current().withValue(OP_ID_CTX, opId == null ? "" : opId),
                        call, headers, next);
            }
        };
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerInterceptors.intercept(buildPrepareService(), opInterceptor))
                .addService(ServerInterceptors.intercept(buildCommitService(), opInterceptor))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName).build();
    }

    @AfterAll
    static void tearDownFixture() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    @BeforeEach
    void reset() {
        prepareCalls.set(0);
        branchCalls.set(0);
        commitRequests.set(0);
        commitSideEffects.set(0);
        commitLedger.clear();

        store = new InMemoryWorkflowRunStore();
        context = ActionContext.create();
        coordinator = new PipelineJobCoordinator(store, 4);
        completeStepAction = new CompleteStepAction(store);
        getJobAction = new GetJobAction(store);
        listJobsAction = new ListJobsAction(store);
    }

    @Test
    void pipelineParksPreservesBinaryFidelityAndCompletesOnceThroughCompleteStep()
            throws Exception {
        Pipeline pipeline = approvalPipeline(true);
        ByteString digest = ByteString.copyFrom(new byte[] {0x01, (byte) 0xfe, 0x00, 0x42});
        long largeUint64 = -99L; // > Long.MAX_VALUE unsigned
        Any detail = Any.pack(StringValue.of("invoice-payload-77"));
        UnknownFieldSet inputUnknown = UnknownFieldSet.newBuilder()
                .addField(901, UnknownFieldSet.Field.newBuilder()
                        .addLengthDelimited(ByteString.copyFromUtf8("audit-envelope-bytes"))
                        .build())
                .build();
        DynamicMessage input = buildInput("ord-1", digest, largeUint64, detail,
                List.of("ok-1", "fail-branch", "ok-2"), inputUnknown);

        UUID jobId = UUID.randomUUID();
        var submitted = coordinator.submit(pipeline, files, input, jobId.toString());
        assertThat(submitted.ok()).isTrue();
        assertThat(submitted.jobId()).isEqualTo(jobId.toString());

        WorkflowRunWorker workerA = newWorker("worker-a");
        assertThat(workerA.workOnce()).isTrue();

        WorkflowRunRecord parkedRow = store.get(jobId).orElseThrow();
        assertThat(parkedRow.status).isEqualTo(WorkflowRunRecord.STATUS_WAITING);
        assertThat(parkedRow.outstandingStep).isEqualTo("approve");
        assertThat(prepareCalls.get()).isEqualTo(1);
        assertThat(branchCalls.get()).isEqualTo(3);
        assertThat(commitRequests.get()).isZero();

        // Inspect durable parked step & binary checkpoints before external completion.
        PipelineJobCoordinator.ParkedStep parked = coordinator.parkedStep(jobId).orElseThrow();
        assertThat(parked.stepName()).isEqualTo("approve");
        assertThat(parked.cardinality()).isEqualTo(EdgeCardinality.EDGE_CARDINALITY_ONE);
        assertThat(parked.checkpoints()).hasSize(2);
        // Fan-out CONTINUE branch evidence survived binary checkpoint serialization.
        assertThat(parked.checkpoints().get(1).branches()).hasSize(3);
        assertThat(parked.checkpoints().get(1).branches().get(1).succeeded()).isFalse();
        assertThat(PipelineJobCoordinator.readDefinition(parkedRow).input().getUnknownFields())
                .isEqualTo(inputUnknown);

        // Supply external completion through CompleteStepAction (the complete-step verb),
        // including base64 bytes and uint64 string.
        ByteString sigBytes = ByteString.copyFrom(new byte[] {(byte) 0xaa, (byte) 0xbb, 0x03});
        ObjectNode completeReq = JSON.createObjectNode();
        completeReq.put("jobId", jobId.toString());
        completeReq.put("stepName", "approve");
        completeReq.putObject("response")
                .put("reviewer", "alice")
                .put("decision", "APPROVED")
                .put("signature", Base64.getEncoder().encodeToString(sigBytes.toByteArray()))
                .put("signedAtTick", Long.toUnsignedString(-7L));
        ObjectNode completedStepReply = dispatch(completeStepAction, completeReq);
        assertThat(completedStepReply.path("ok").asBoolean()).isTrue();
        assertThat(completedStepReply.path("status").asText())
                .isEqualTo(WorkflowRunRecord.STATUS_QUEUED);

        // Resume in a newly constructed worker ("process restart" / worker handoff).
        WorkflowRunWorker workerB = newWorker("worker-b");
        assertThat(workerB.workOnce()).isTrue();

        // Step 1 and fan-out step were replayed from checkpoints without re-invoking services.
        assertThat(prepareCalls.get()).isEqualTo(1);
        assertThat(branchCalls.get()).isEqualTo(3);
        assertThat(commitRequests.get()).isEqualTo(1);
        assertThat(commitSideEffects.get()).isEqualTo(1);
        assertThat(commitLedger).containsOnlyKeys(jobId + "/commit");

        WorkflowRunRecord finished = store.get(jobId).orElseThrow();
        assertThat(finished.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(finished.verdict)
                .isEqualTo("4 steps, output jobs.pipeline.test.CommitOutcome");

        List<Checkpoint> allCheckpoints = coordinator.checkpoints(jobId);
        assertThat(allCheckpoints).hasSize(4);
        DynamicMessage commitMsg = allCheckpoints.get(3).messages().getFirst();
        Descriptor outcomeType = commitMsg.getDescriptorForType();
        assertThat(outcomeType.getFullName()).isEqualTo("jobs.pipeline.test.CommitOutcome");
        assertThat(commitMsg.getField(outcomeType.findFieldByName("receipt")))
                .isEqualTo("ord-1:APPROVED");
        assertThat(commitMsg.getField(outcomeType.findFieldByName("digest"))).isEqualTo(digest);
        assertThat(commitMsg.getField(outcomeType.findFieldByName("signature")))
                .isEqualTo(sigBytes);
        assertThat(commitMsg.getField(outcomeType.findFieldByName("sequence_no")))
                .isEqualTo(largeUint64);
        DynamicMessage unpackedAny = (DynamicMessage) commitMsg.getField(
                outcomeType.findFieldByName("detail"));
        assertThat(Any.parseFrom(unpackedAny.toByteString())
                .unpack(StringValue.class).getValue())
                .isEqualTo("invoice-payload-77");

        // Verify get-job and list-jobs render the completed pipeline run cleanly.
        ObjectNode getReply = dispatch(getJobAction,
                JSON.createObjectNode().put("jobId", jobId.toString()));
        assertThat(getReply.path("ok").asBoolean()).isTrue();
        assertThat(getReply.path("job").path("status").asText()).isEqualTo("COMPLETED");
        assertThat(getReply.path("job").path("checkpoints")).hasSize(4);
        assertThat(getReply.path("job").path("result").path("receipt").asText())
                .isEqualTo("ord-1:APPROVED");

        ObjectNode listReply = dispatch(listJobsAction,
                JSON.createObjectNode().put("status", "JOB_STATUS_COMPLETED"));
        assertThat(listReply.path("jobs")).hasSize(1);
    }

    @Test
    void duplicateCompletionLostResponseAndAlteredIntentRefusalLeaveExactlyOneOutcome()
            throws Exception {
        Pipeline pipeline = approvalPipeline(true);
        ByteString digest = ByteString.copyFromUtf8("digest-a");
        UnknownFieldSet unknownV1 = UnknownFieldSet.newBuilder()
                .addField(700, UnknownFieldSet.Field.newBuilder().addVarint(1L).build())
                .build();
        UnknownFieldSet unknownV2 = UnknownFieldSet.newBuilder()
                .addField(700, UnknownFieldSet.Field.newBuilder().addVarint(2L).build())
                .build();
        DynamicMessage input = buildInput("ord-2", digest, 5L,
                Any.pack(StringValue.of("v1")), List.of("x"), unknownV1);

        UUID jobId = UUID.randomUUID();
        assertThat(coordinator.submit(pipeline, files, input, jobId.toString()).ok()).isTrue();
        int eventsAfterSubmit = store.events().size();

        // 1. Idempotent job submission (lost submit response) succeeds without extra events.
        var retrySubmit = coordinator.submit(pipeline, files, input, jobId.toString());
        assertThat(retrySubmit.ok()).isTrue();
        assertThat(retrySubmit.conflict()).isFalse();
        assertThat(store.events()).hasSize(eventsAfterSubmit);

        // 2. Altered binary input (differing ONLY in unknown field!) on same jobId conflicts!
        DynamicMessage inputAlteredUnknown = buildInput("ord-2", digest, 5L,
                Any.pack(StringValue.of("v1")), List.of("x"), unknownV2);
        var alteredInputSubmit = coordinator.submit(
                pipeline, files, inputAlteredUnknown, jobId.toString());
        assertThat(alteredInputSubmit.ok()).isFalse();
        assertThat(alteredInputSubmit.conflict()).isTrue();

        // 3. Altered pipeline definition on same jobId conflicts!
        Pipeline alteredPipeline = pipeline.toBuilder().setMaxStreamMessages(999).build();
        var alteredPipelineSubmit = coordinator.submit(
                alteredPipeline, files, input, jobId.toString());
        assertThat(alteredPipelineSubmit.ok()).isFalse();
        assertThat(alteredPipelineSubmit.conflict()).isTrue();

        // Park job on external step "approve".
        WorkflowRunWorker worker = newWorker("worker-1");
        assertThat(worker.workOnce()).isTrue();
        PipelineJobCoordinator.ParkedStep parked = coordinator.parkedStep(jobId).orElseThrow();

        // 4. Wrong step name (naming already-checkpointed live step "prepare" or future "commit")
        // answers WrongState without touching the WAITING job.
        DynamicMessage approvalV1 = buildApproval("bob", "APPROVED", unknownV1);
        assertThat(coordinator.completeStep(jobId, "prepare", List.of(approvalV1)))
                .isInstanceOf(ParkedCompletion.WrongState.class);
        assertThat(coordinator.completeStep(jobId, "commit", List.of(approvalV1)))
                .isInstanceOf(ParkedCompletion.WrongState.class);

        // 5. Altered intent token (mismatched pipelineFingerprint or inputFingerprint)
        // is refused as Conflict while keeping the job WAITING!
        assertThat(coordinator.completeStep(jobId, "approve",
                "tampered-pipeline-fp", parked.inputFingerprint(), List.of(approvalV1)))
                .isInstanceOf(ParkedCompletion.Conflict.class);
        assertThat(coordinator.completeStep(jobId, "approve",
                parked.pipelineFingerprint(), "tampered-input-fp", List.of(approvalV1)))
                .isInstanceOf(ParkedCompletion.Conflict.class);
        assertThat(store.get(jobId).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_WAITING);

        // 6. Commit valid binary external completion (preserving unknownV1 on approval).
        assertThat(coordinator.completeStep(jobId, "approve",
                parked.pipelineFingerprint(), parked.inputFingerprint(), List.of(approvalV1)))
                .isInstanceOf(ParkedCompletion.Completed.class);
        int eventsAfterComplete = store.events().size();

        // 7. Lost response / duplicate completion while QUEUED returns AlreadyDone, zero new events!
        ParkedCompletion dupWhileQueued = coordinator.completeStep(
                jobId, "approve", List.of(approvalV1));
        assertThat(dupWhileQueued).isEqualTo(new ParkedCompletion.AlreadyDone("QUEUED"));
        assertThat(store.events()).hasSize(eventsAfterComplete);

        // 8. Altered binary completion (differing ONLY in unknown field!) while QUEUED conflicts!
        DynamicMessage approvalDifferentUnknown = buildApproval("bob", "APPROVED", unknownV2);
        assertThat(coordinator.completeStep(jobId, "approve", List.of(approvalDifferentUnknown)))
                .isEqualTo(new ParkedCompletion.Conflict("QUEUED"));
        assertThat(store.events()).hasSize(eventsAfterComplete);

        // Run second segment to COMPLETED.
        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord completedJob = store.get(jobId).orElseThrow();
        assertThat(completedJob.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        String committedCheckpoints = completedJob.checkpoints;
        int finalEventCount = store.events().size();

        // Unknown field on external completion survived replay!
        assertThat(coordinator.checkpoints(jobId).get(2).messages().getFirst().getUnknownFields())
                .isEqualTo(unknownV1);

        // 9. Duplicate completion AFTER job COMPLETED still returns AlreadyDone("COMPLETED")!
        assertThat(coordinator.completeStep(jobId, "approve", List.of(approvalV1)))
                .isEqualTo(new ParkedCompletion.AlreadyDone("COMPLETED"));

        // 10. Altered or invalid completion AFTER job COMPLETED returns Conflict("COMPLETED")
        // without mutating checkpoints, status, or events!
        DynamicMessage invalidApproval = buildApproval("x", "no", UnknownFieldSet.getDefaultInstance());
        assertThat(coordinator.completeStep(jobId, "approve", List.of(invalidApproval)))
                .isEqualTo(new ParkedCompletion.Conflict("COMPLETED"));
        assertThat(store.get(jobId).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(store.get(jobId).orElseThrow().checkpoints).isEqualTo(committedCheckpoints);
        assertThat(store.events()).hasSize(finalEventCount);
        assertThat(commitSideEffects.get()).isEqualTo(1);
    }

    @Test
    void invalidExternalResponseFailsWaitingPipelineJobAsValidationVerdict() throws Exception {
        Pipeline pipeline = approvalPipeline(true);
        DynamicMessage input = buildInput("ord-3", ByteString.EMPTY, 1L,
                Any.pack(StringValue.of("check")), List.of("a"),
                UnknownFieldSet.getDefaultInstance());
        UUID jobId = UUID.randomUUID();
        assertThat(coordinator.submit(pipeline, files, input, jobId.toString()).ok()).isTrue();

        WorkflowRunWorker worker = newWorker("worker-v");
        assertThat(worker.workOnce()).isTrue();
        assertThat(store.get(jobId).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_WAITING);

        // "no" violates min_len: 4 on ApprovalDecision.decision.
        ObjectNode badComplete = JSON.createObjectNode();
        badComplete.put("jobId", jobId.toString());
        badComplete.put("stepName", "approve");
        badComplete.putObject("response")
                .put("reviewer", "alice")
                .put("decision", "no");
        ObjectNode reply = dispatch(completeStepAction, badComplete);
        assertThat(reply.path("ok").asBoolean()).isFalse();
        assertThat(reply.path("status").asText()).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
        assertThat(reply.path("error").asText())
                .contains("VALIDATION: complete-step response failed validation")
                .contains("decision");

        WorkflowRunRecord failed = store.get(jobId).orElseThrow();
        assertThat(failed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
        assertThat(failed.outstandingStep).isNull();
        assertThat(coordinator.checkpoints(jobId)).hasSize(2);
        assertThat(store.events().stream().map(e -> e.eventType))
                .contains(WorkflowRunEventRecord.TYPE_FAILED);
    }

    @Test
    void expiredLeaseAndWorkerReassignmentFenceStaleWorkerAndCommitSingleOutcome()
            throws Exception {
        Pipeline pipeline = approvalPipeline(true);
        DynamicMessage input = buildInput("ord-lease", ByteString.copyFromUtf8("d"), 9L,
                Any.pack(StringValue.of("lease-test")), List.of("b1"),
                UnknownFieldSet.getDefaultInstance());
        UUID jobId = UUID.randomUUID();
        assertThat(coordinator.submit(pipeline, files, input, jobId.toString()).ok()).isTrue();

        // Segment 1 parks on "approve", then external completion requeues it.
        WorkflowRunWorker workerA = newWorker("worker-a");
        assertThat(workerA.workOnce()).isTrue();
        DynamicMessage approval = buildApproval(
                "carol", "APPROVED", UnknownFieldSet.getDefaultInstance());
        assertThat(coordinator.completeStep(jobId, "approve", List.of(approval)))
                .isInstanceOf(ParkedCompletion.Completed.class);

        // Simulate Worker A claiming the resumed job (attempt 2), invoking the downstream
        // side-effecting service with jobId/commit, and then losing its lease before settling!
        WorkflowRunRecord staleClaimed = store.claim("worker-a", java.time.Duration.ofMillis(10))
                .orElseThrow();
        WorkerClaim staleClaim = WorkerClaim.from(staleClaimed);
        assertThat(staleClaim.attempt()).isEqualTo(2);

        // Worker A's in-flight RPC executes on the downstream service before lease expiry.
        PipelineExecutor directExecutor = new PipelineExecutor(
                new DynamicPipelineTransport(dep -> channel));
        PipelineJobCoordinator.Definition def = PipelineJobCoordinator.readDefinition(staleClaimed);
        List<Checkpoint> prior = PipelineJobCoordinator.readCheckpoints(staleClaimed, def);
        // Force lease expiry & sweep before Worker A writes checkpoint/completion!
        staleClaimed.leaseUntil = Instant.now().minusSeconds(5);
        assertThat(store.requeueExpiredLeases()).isEqualTo(1);

        // Worker B claims the reassigned job (attempt 3) and finishes it cleanly!
        WorkflowRunWorker workerB = newWorker("worker-b");
        assertThat(workerB.workOnce()).isTrue();
        assertThat(store.get(jobId).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(store.get(jobId).orElseThrow().attempt).isEqualTo(3);

        // Now stale Worker A wakes up and finishes running its segment in memory, attempting
        // to saveCheckpoint via the observer — it MUST be fenced by ClaimLostException!
        assertThatThrownBy(() -> directExecutor.runSegment(
                def.pipeline(), def.files(), def.input(), prior,
                new PipelineExecutor.ExecutionIdentity(jobId.toString(), staleClaim.attempt()),
                cp -> store.saveCheckpoint(staleClaim, "[]",
                        WorkflowRunEventFactory.stepCheckpoint(staleClaimed, cp.outcome().name()))))
                .isInstanceOfSatisfying(PipelineExecutor.PipelineExecutionException.class,
                        e -> assertThat(e.getCause()).isInstanceOf(ClaimLostException.class));
        assertThatThrownBy(() -> store.markCompleted(staleClaim, "{}", "stale",
                WorkflowRunEventFactory.completed(staleClaimed, "stale")))
                .isInstanceOf(ClaimLostException.class);

        // Pre-park steps ran ONCE total; downstream side-effecting service received 2 requests
        // (Worker B + late Worker A) with the SAME operationId and committed 1 side effect;
        // job row has exactly 1 COMPLETED event!
        assertThat(prepareCalls.get()).isEqualTo(1);
        assertThat(branchCalls.get()).isEqualTo(1);
        assertThat(commitRequests.get()).isEqualTo(2);
        assertThat(commitSideEffects.get()).isEqualTo(1);
        assertThat(store.events().stream()
                .filter(e -> WorkflowRunEventRecord.TYPE_COMPLETED.equals(e.eventType))
                .count()).isEqualTo(1);
    }

    @Test
    void streamingExternalCompletionAndSubmitVerbWorkEndToEnd() throws Exception {
        Pipeline pipeline = streamingExternalPipeline();
        ObjectNode workflowEnvelope = PipelineRunCodec.encodeDefinition(
                pipeline, files,
                DynamicMessage.getDefaultInstance(type("OrderInput")));
        // Remove inputBase64/inputFingerprint so submit parses the caller's JSON input.
        workflowEnvelope.remove(PipelineRunCodec.INPUT_BASE64_KEY);
        workflowEnvelope.remove(PipelineRunCodec.INPUT_FINGERPRINT_KEY);

        SubmitWorkflowAction namedSubmitAction = new SubmitWorkflowAction(
                store,
                name -> "streaming-approval-pipeline".equals(name)
                        ? java.util.Optional.of(workflowEnvelope)
                        : java.util.Optional.empty(),
                4);

        ObjectNode submitReq = JSON.createObjectNode();
        submitReq.put("workflowName", "streaming-approval-pipeline");
        submitReq.putObject("input").put("orderId", "stream-ord-9");

        ObjectNode submitReply = dispatch(namedSubmitAction, submitReq);
        assertThat(submitReply.path("ok").asBoolean()).isTrue();
        String jobId = submitReply.path("jobId").asText();

        // Use the 5-arg WorkflowRunWorker constructor (verifies default in-process channel wiring!).
        WorkflowRunWorker defaultWorker = new WorkflowRunWorker(
                store, context, null,
                new WorkflowRunner(step -> channel),
                new WorkflowRunsConfig("default-worker", 1, java.time.Duration.ofSeconds(30),
                        java.time.Duration.ofMillis(20), 1, 4, 4,
                        null, "workflow-run-events", null, null));
        try {
            assertThat(defaultWorker.workOnce()).isTrue();
            assertThat(store.get(UUID.fromString(jobId)).orElseThrow().status)
                    .isEqualTo(WorkflowRunRecord.STATUS_WAITING);

            // Supply streaming external completion via CompleteStepAction {"items": [...]}.
            ObjectNode streamComplete = JSON.createObjectNode();
            streamComplete.put("jobId", jobId);
            streamComplete.put("stepName", "stream_approvals");
            var items = streamComplete.putObject("response").putArray("items");
            items.addObject().put("reviewer", "rev-1").put("decision", "APPROVE_A");
            items.addObject().put("reviewer", "rev-2").put("decision", "APPROVE_B");

            ObjectNode completeReply = dispatch(completeStepAction, streamComplete);
            assertThat(completeReply.path("ok").asBoolean()).isTrue();

            assertThat(defaultWorker.workOnce()).isTrue();
            WorkflowRunRecord done = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(done.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            JsonNode result = JSON.readTree(done.result);
            assertThat(result.path("decisions")).hasSize(2);
        } finally {
            defaultWorker.close();
        }
    }

    private WorkflowRunWorker newWorker(String workerId) {
        PipelineExecutor executor = new PipelineExecutor(
                new DynamicPipelineTransport(dep -> channel));
        WorkflowRunsConfig config = new WorkflowRunsConfig(
                workerId, 1, java.time.Duration.ofSeconds(30),
                java.time.Duration.ofMillis(20), 1, 4, 4,
                null, "workflow-run-events", null, null);
        return new WorkflowRunWorker(
                store, context, null,
                new WorkflowRunner(step -> channel),
                executor, config);
    }

    private static Pipeline approvalPipeline(boolean validateApproval) {
        String inProcessEndpoint = "in-process." + serverName;
        FanOutSpec fanOut = FanOutSpec.newBuilder()
                .setItems("items")
                .setCollectType("jobs.pipeline.test.BranchBag")
                .setCollectInto("items")
                .setMaxItems(10)
                .setMaxConcurrency(2)
                .setFailurePolicy(BranchFailurePolicy.BRANCH_FAILURE_POLICY_CONTINUE)
                .build();
        return Pipeline.newBuilder()
                .setName("order-approval-pipeline")
                .setInputType("jobs.pipeline.test.OrderInput")
                .setDescriptorFingerprint(descriptorFp)
                .setMaxStreamMessages(64)
                .setDeadline(Duration.newBuilder().setSeconds(30).build())
                .addDependencies(dependency("prepare", inProcessEndpoint))
                .addDependencies(dependency("desk", inProcessEndpoint))
                .addDependencies(dependency("commit", inProcessEndpoint))
                .addSteps(PipelineStep.newBuilder()
                        .setName("prepare")
                        .setDependency("prepare")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("jobs.pipeline.test.PrepareService/Prepare")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(TypedEdge.newBuilder()
                                        .addSources("input")
                                        .setProduceType("jobs.pipeline.test.OrderInput")
                                        .addRules("order_id = input.order_id")
                                        .addRules("digest = input.digest")
                                        .addRules("sequence_no = input.sequence_no")
                                        .addRules("detail = input.detail")
                                        .build())
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("check_branches")
                        .setDependency("prepare")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("jobs.pipeline.test.PrepareService/CheckBranch")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(TypedEdge.newBuilder()
                                        .addSources("input")
                                        .setProduceType("jobs.pipeline.test.OrderInput")
                                        .addRules("items = input.items")
                                        .build())
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .setFanOut(fanOut)
                                .build())
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("approve")
                        .setDependency("desk")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("jobs.pipeline.test.ApprovalDesk/Approve")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_EXTERNAL)
                                .setValidateResponse(validateApproval)
                                .build())
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("commit")
                        .setDependency("commit")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("jobs.pipeline.test.CommitService/Commit")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(TypedEdge.newBuilder()
                                        .addSources("prepare")
                                        .addSources("approve")
                                        .setProduceType("jobs.pipeline.test.CommitRequest")
                                        .addRules("order_id = prepare.order_id")
                                        .addRules("decision = approve.decision")
                                        .addRules("digest = prepare.digest")
                                        .addRules("signature = approve.signature")
                                        .addRules("sequence_no = prepare.sequence_no")
                                        .addRules("detail = prepare.detail")
                                        .build())
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .build();
    }

    private static Pipeline streamingExternalPipeline() {
        String inProcessEndpoint = "in-process." + serverName;
        return Pipeline.newBuilder()
                .setName("streaming-approval-pipeline")
                .setInputType("jobs.pipeline.test.OrderInput")
                .setDescriptorFingerprint(descriptorFp)
                .setMaxStreamMessages(16)
                .setDeadline(Duration.newBuilder().setSeconds(30).build())
                .addDependencies(dependency("prepare", inProcessEndpoint))
                .addDependencies(dependency("desk", inProcessEndpoint))
                .addSteps(PipelineStep.newBuilder()
                        .setName("prepare")
                        .setDependency("prepare")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("jobs.pipeline.test.PrepareService/Prepare")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(TypedEdge.newBuilder()
                                        .addSources("input")
                                        .setProduceType("jobs.pipeline.test.OrderInput")
                                        .addRules("order_id = input.order_id")
                                        .build())
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("stream_approvals")
                        .setDependency("desk")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("jobs.pipeline.test.ApprovalDesk/StreamApprovals")
                                .setMethodShape(MethodShape.METHOD_SHAPE_SERVER_STREAMING)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_MANY)
                                .setCompletion(StepCompletion.STEP_COMPLETION_EXTERNAL)
                                .setValidateResponse(true)
                                .build())
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("collect_approvals")
                        .setCollect(CollectStep.newBuilder()
                                .setSource("stream_approvals")
                                .setCollectType("jobs.pipeline.test.ApprovalBatch")
                                .setCollectInto("decisions")
                                .build())
                        .build())
                .setOutput(PipelineOutput.newBuilder()
                        .setType("jobs.pipeline.test.ApprovalBatch")
                        .addRules("decisions = collect_approvals.decisions")
                        .build())
                .build();
    }

    private static ServiceDependency dependency(String alias, String endpoint) {
        return ServiceDependency.newBuilder()
                .setAlias(alias)
                .setServiceProfile(alias)
                .setEndpoint(endpoint)
                .setDescriptorFingerprint(descriptorFp)
                .build();
    }

    private static DynamicMessage buildInput(String orderId, ByteString digest, long seq,
                                             Any detail, List<String> branches,
                                             UnknownFieldSet unknownFields) throws Exception {
        Descriptor inType = type("OrderInput");
        Descriptor itemType = type("BranchItem");
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(inType)
                .setField(inType.findFieldByName("order_id"), orderId)
                .setField(inType.findFieldByName("digest"), digest)
                .setField(inType.findFieldByName("sequence_no"), seq)
                .setField(inType.findFieldByName("detail"),
                        DynamicMessage.parseFrom(Any.getDescriptor(), detail.toByteString()))
                .setUnknownFields(unknownFields);
        for (String b : branches) {
            builder.addRepeatedField(inType.findFieldByName("items"),
                    DynamicMessage.newBuilder(itemType)
                            .setField(itemType.findFieldByName("label"), b)
                            .build());
        }
        return builder.build();
    }

    private static DynamicMessage buildApproval(String reviewer, String decision,
                                                UnknownFieldSet unknownFields) {
        Descriptor approvalType = type("ApprovalDecision");
        return DynamicMessage.newBuilder(approvalType)
                .setField(approvalType.findFieldByName("reviewer"), reviewer)
                .setField(approvalType.findFieldByName("decision"), decision)
                .setUnknownFields(unknownFields)
                .build();
    }

    private static ServerServiceDefinition buildPrepareService() {
        MethodDescriptor prepareMethod = file.findServiceByName("PrepareService")
                .findMethodByName("Prepare");
        MethodDescriptor branchMethod = file.findServiceByName("PrepareService")
                .findMethodByName("CheckBranch");
        var wirePrepare = DynamicGrpcCalls.methodDescriptor(prepareMethod);
        var wireBranch = DynamicGrpcCalls.methodDescriptor(branchMethod);
        var svc = io.grpc.ServiceDescriptor.newBuilder("jobs.pipeline.test.PrepareService")
                .addMethod(wirePrepare)
                .addMethod(wireBranch)
                .build();
        Descriptor outType = prepareMethod.getOutputType();
        return ServerServiceDefinition.builder(svc)
                .addMethod(wirePrepare, ServerCalls.asyncUnaryCall((req, obs) -> {
                    prepareCalls.incrementAndGet();
                    Descriptor inType = prepareMethod.getInputType();
                    DynamicMessage.Builder reply = DynamicMessage.newBuilder(outType)
                            .setField(outType.findFieldByName("order_id"),
                                    req.getField(inType.findFieldByName("order_id")))
                            .setField(outType.findFieldByName("digest"),
                                    req.getField(inType.findFieldByName("digest")))
                            .setField(outType.findFieldByName("sequence_no"),
                                    req.getField(inType.findFieldByName("sequence_no")));
                    if (req.hasField(inType.findFieldByName("detail"))) {
                        reply.setField(outType.findFieldByName("detail"),
                                req.getField(inType.findFieldByName("detail")));
                    }
                    obs.onNext(reply.build());
                    obs.onCompleted();
                }))
                .addMethod(wireBranch, ServerCalls.asyncUnaryCall((req, obs) -> {
                    branchCalls.incrementAndGet();
                    String label = (String) req.getField(
                            branchMethod.getInputType().findFieldByName("label"));
                    if (label.startsWith("fail")) {
                        obs.onError(Status.INVALID_ARGUMENT
                                .withDescription("bad branch " + label)
                                .asRuntimeException());
                        return;
                    }
                    obs.onNext(req);
                    obs.onCompleted();
                }))
                .build();
    }

    private static ServerServiceDefinition buildCommitService() {
        MethodDescriptor commitMethod = file.findServiceByName("CommitService")
                .findMethodByName("Commit");
        var wireCommit = DynamicGrpcCalls.methodDescriptor(commitMethod);
        var svc = io.grpc.ServiceDescriptor.newBuilder("jobs.pipeline.test.CommitService")
                .addMethod(wireCommit)
                .build();
        Descriptor inType = commitMethod.getInputType();
        Descriptor outType = commitMethod.getOutputType();
        return ServerServiceDefinition.builder(svc)
                .addMethod(wireCommit, ServerCalls.asyncUnaryCall((req, obs) -> {
                    commitRequests.incrementAndGet();
                    String opKey = OP_ID_CTX.get();
                    DynamicMessage committed = commitLedger.computeIfAbsent(opKey, key -> {
                        commitSideEffects.incrementAndGet();
                        String orderId = (String) req.getField(
                                inType.findFieldByName("order_id"));
                        String decision = (String) req.getField(
                                inType.findFieldByName("decision"));
                        DynamicMessage.Builder out = DynamicMessage.newBuilder(outType)
                                .setField(outType.findFieldByName("receipt"),
                                        orderId + ":" + decision)
                                .setField(outType.findFieldByName("digest"),
                                        req.getField(inType.findFieldByName("digest")))
                                .setField(outType.findFieldByName("signature"),
                                        req.getField(inType.findFieldByName("signature")))
                                .setField(outType.findFieldByName("sequence_no"),
                                        req.getField(inType.findFieldByName("sequence_no")));
                        if (req.hasField(inType.findFieldByName("detail"))) {
                            out.setField(outType.findFieldByName("detail"),
                                    req.getField(inType.findFieldByName("detail")));
                        }
                        return out.build();
                    });
                    obs.onNext(committed);
                    obs.onCompleted();
                }))
                .build();
    }

    private static Descriptor type(String simpleName) {
        return file.findMessageTypeByName(simpleName);
    }

    private static ObjectNode dispatch(ProtoAction verb, ObjectNode input)
            throws ActionException {
        return ActionCatalog.defaults(ActionContext.create())
                .replace(verb).execute(verb.name(), input);
    }

    private static String resource(String path) throws Exception {
        try (InputStream in = PipelineJobCoordinationTest.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
