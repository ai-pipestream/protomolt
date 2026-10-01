package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.AdmissionPolicy;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.DelegationReducer;
import ai.protomolt.proto.delegation.DelegationWorker;
import ai.protomolt.proto.delegation.InMemoryTranscriptRepository;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.ScriptedWorkerRunner;
import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.delegation.WorkerRunner;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.AgentDelegationServiceGrpc;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemRunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.receipt.*;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.workflow.RecordSigning;
import ai.protomolt.proto.workflow.WorkflowRunner;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailureReason;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration boundaries for coordinator-owned candidate preparation. */
class WorkflowCandidatePreparerTest {
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000321";
    private static final String HOLDER = "author-worker";
    private static final String PREPARATION_ID = "00000000-0000-4000-8000-000000000987";
    private static final Instant NOW = Instant.parse("2026-10-01T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final String PROTO = """
            syntax = "proto3";
            package preparer.test;
            message Text { string text = 1; }
            service Echo { rpc Say(Text) returns (Text); }
            """;
    private static final String SOURCE = """
            {"name":"prepared-echo","validateContract":true,
             "schema":{"descriptorSetBase64":"%s"},"inputType":"preparer.test.Text",
             "steps":[{"name":"echo","target":"fixture:9090","method":"preparer.test.Echo/Say",
             "validate":false,"rules":["text = input.text"]}]}
            """;

    @TempDir Path temp;
    private FileSystemArtifactRepository artifacts;
    private FileSystemRunEvidenceRepository runs;
    private CompiledProtos protos;
    private FileDescriptor file;
    private ArtifactReference policyRef;
    private WorkflowAuthoringPolicy policy;
    private WorkflowRunner runner;
    private Server service;
    private String serviceName;
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger opens = new AtomicInteger();
    private final AtomicInteger wrongResponseCall = new AtomicInteger(-1);
    private final AtomicReference<String> serviceReply = new AtomicReference<>();
    private final java.util.concurrent.atomic.AtomicBoolean serviceUnavailable =
            new java.util.concurrent.atomic.AtomicBoolean();
    private final java.util.concurrent.atomic.AtomicBoolean cancelOnServiceCall =
            new java.util.concurrent.atomic.AtomicBoolean();
    private InProcessDelegationCoordinator coordinator;
    private Server delegationServer;
    private ManagedChannel delegationChannel;
    private DelegationWorker worker;
    private InMemoryTranscriptRepository transcripts;
    private FileSystemWorkflowPreparationRepository ledger;
    private WorkflowCandidatePreparer preparer;
    private RecordSigning signing;
    private TrustSnapshot trust;

    @BeforeEach
    void setup() throws Exception {
        artifacts = new FileSystemArtifactRepository(temp.resolve("artifacts"));
        runs = new FileSystemRunEvidenceRepository(temp.resolve("runs"));
        ledger = new FileSystemWorkflowPreparationRepository(temp.resolve("preparations"));
        protos = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("preparer/test/echo.proto", PROTO, "test").build());
        file = protos.descriptorFor("preparer/test/echo.proto").orElseThrow();
        var serviceDescriptor = file.findServiceByName("Echo");
        var method = DynamicGrpcCalls.methodDescriptor(serviceDescriptor.findMethodByName("Say"));
        serviceName = InProcessServerBuilder.generateName();
        service = InProcessServerBuilder.forName(serviceName)
                .addService(ServerServiceDefinition.builder(io.grpc.ServiceDescriptor.newBuilder(
                        serviceDescriptor.getFullName()).addMethod(method).build())
                        .addMethod(method, ServerCalls.asyncUnaryCall((request, response) -> {
                            int call = calls.incrementAndGet();
                            if (cancelOnServiceCall.get()) coordinator.cancel(TASK_ID, "cancel during fixture RPC");
                            if (serviceUnavailable.get()) {
                                response.onError(Status.UNAVAILABLE.withDescription("fixture unavailable")
                                        .asRuntimeException());
                                return;
                            }
                            String override = wrongResponseCall.get() == call
                                    ? "wrong recorded result" : serviceReply.get();
                            if (override == null) response.onNext(request);
                            else response.onNext(DynamicMessage.newBuilder(file.findMessageTypeByName("Text"))
                                    .setField(file.findMessageTypeByName("Text").findFieldByName("text"), override)
                                    .build());
                            response.onCompleted();
                        })).build()).build().start();
        runner = new WorkflowRunner(step -> {
            opens.incrementAndGet();
            return InProcessChannelBuilder.forName(serviceName).build();
        });

        ArtifactReference descriptorRef = artifacts.save(protos.descriptorSet().toByteArray(),
                "application/x-protobuf", false);
        var fixture = WorkflowAcceptanceFixture.newBuilder().setName("minimum-valid")
                .setInput(artifacts.save(text("hello!").toByteArray(), "application/x-protobuf", false))
                .setExpectedOutput(artifacts.save(text("hello!").toByteArray(), "application/x-protobuf", false))
                .build();
        policy = WorkflowAuthoringPolicy.newBuilder().setDescriptors(descriptorRef).addFixtures(fixture)
                .addPermittedCalls(WorkflowPermittedCall.newBuilder().setTarget("fixture:9090")
                        .setMethod("preparer.test.Echo/Say")).build();
        policyRef = artifacts.save(policy.toByteArray(), "application/x-protobuf", false);

        KeyPair pair = RecordKeys.generate();
        signing = new RecordSigning("preparer-test", new RecordSigner("preparer-key", pair.getPrivate()));
        trust = TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder().setIssuer("preparer-test")
                .addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                .addKeys(TrustedKey.newBuilder().setKeyId("preparer-key")
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setState(KeyState.KEY_STATE_ACTIVE)
                        .setPublicKey(ByteString.copyFrom(RecordKeys.rawPublicKey(pair.getPublic()))))).build();
        startTask(CandidateReviewer.manual());
        preparer = newPreparer(transcripts, runs, () -> trust);
    }

    @AfterEach
    void stop() throws Exception {
        if (worker != null) worker.close();
        if (delegationChannel != null) delegationChannel.shutdownNow();
        if (coordinator != null) coordinator.close();
        if (delegationServer != null) delegationServer.shutdownNow();
        if (service != null) service.shutdownNow();
    }

    @Test
    void executesAndExactCompletedReplayDoesNotOpenAnotherChannelOrCallService() throws Exception {
        var request = request(SOURCE.formatted(Base64.getEncoder().encodeToString(protos.descriptorSet().toByteArray())));
        var response = preparer.prepare(request, holder());
        int afterFirst = calls.get();
        int opensAfterFirst = opens.get();
        assertThat(afterFirst).isEqualTo(2); // pinned fixture plus immutable recorded run
        assertThat(response.getBinding().getPreparationId()).isEqualTo(PREPARATION_ID);
        assertThat(preparer.prepare(request, holder())).isEqualTo(response);
        assertThat(calls.get()).isEqualTo(afterFirst);
        assertThat(opens.get()).isEqualTo(opensAfterFirst);
    }

    @Test
    void malformedSourceDoesNotReserveAndCanBeCorrected() throws Exception {
        var invalid = request("{not json");
        assertThatThrownBy(() -> preparer.prepare(invalid, holder()))
                .isInstanceOf(WorkflowPreparationException.class);
        assertThat(ledger.find(TASK_ID, 1, 1)).isEmpty();
        assertThat(calls).hasValue(0);
        assertThat(preparer.prepare(request(SOURCE.formatted(Base64.getEncoder()
                .encodeToString(protos.descriptorSet().toByteArray()))), holder()).getBinding().getTaskId())
                .isEqualTo(TASK_ID);
    }

    @Test
    void malformedRequestIsInvalidInputBeforeCallsOrReservation() throws Exception {
        assertThatThrownBy(() -> preparer.prepare(PrepareWorkflowCandidateRequest.getDefaultInstance(), holder()))
                .isInstanceOf(WorkflowPreparationException.class)
                .extracting("kind").isEqualTo(WorkflowPreparationException.Kind.INVALID_INPUT);
        assertThat(calls).hasValue(0);
        assertThat(opens).hasValue(0);
        assertThat(ledger.find(TASK_ID, 1, 1)).isEmpty();
    }

    @Test
    void wrongHolderAndExpiredLeaseFailBeforeAnyChannelOrCall() throws Exception {
        var request = request(SOURCE.formatted(Base64.getEncoder().encodeToString(protos.descriptorSet().toByteArray())));
        var wrong = Caller.scoped("somebody-else", Set.of(Scopes.WORKER_COORDINATE));
        assertThatThrownBy(() -> preparer.prepare(request, wrong))
                .isInstanceOf(WorkflowPreparationException.class)
                .extracting("kind").isEqualTo(WorkflowPreparationException.Kind.PERMISSION_DENIED);
        assertThat(opens).hasValue(0);
        assertThat(calls).hasValue(0);
        var expiredPreparer = new WorkflowCandidatePreparer(transcripts, ledger, artifacts, runs,
                runner, ActionContext.create(), policyRef, () -> trust, signing,
                Clock.offset(CLOCK, java.time.Duration.ofSeconds(31)), (workflow, json, p) -> {});
        assertThatThrownBy(() -> expiredPreparer.prepare(request, holder()))
                .isInstanceOf(WorkflowPreparationException.class);
        assertThat(opens).hasValue(0);
        assertThat(calls).hasValue(0);
    }

    @Test
    void changedIntentConflictsWithoutRepeatingExternalCalls() throws Exception {
        var source = SOURCE.formatted(Base64.getEncoder().encodeToString(protos.descriptorSet().toByteArray()));
        var request = request(source);
        preparer.prepare(request, holder());
        int before = calls.get();
        var changed = request.toBuilder().setExecutableSourceJson(ByteString.copyFromUtf8(source.replace(
                "prepared-echo", "different-echo"))).build();
        assertThatThrownBy(() -> preparer.prepare(changed, holder()))
                .isInstanceOf(WorkflowPreparationException.class)
                .extracting("kind").isEqualTo(WorkflowPreparationException.Kind.CONFLICT);
        assertThat(calls).hasValue(before);
    }

    @Test
    void incorrectPinnedFixtureIsPersistedAsTerminalAndNotRetried() throws Exception {
        // The service's identity response disagrees with the pinned hello! expectation.
        serviceReply.set("incorrect");
        var request = request(SOURCE.formatted(Base64.getEncoder().encodeToString(protos.descriptorSet().toByteArray())));
        assertThatThrownBy(() -> preparer.prepare(request, holder()))
                .isInstanceOf(WorkflowPreparationException.class);
        int after = calls.get();
        assertThat(ledger.find(TASK_ID, 1, 1).orElseThrow().hasFailed()).isTrue();
        assertThatThrownBy(() -> preparer.prepare(request, holder()))
                .isInstanceOf(WorkflowPreparationException.class);
        assertThat(calls).hasValue(after);
    }

    @Test
    void failedRecordedRunIsTerminalWithoutFixtureOrRecordedCall() throws Exception {
        var source = SOURCE.formatted(Base64.getEncoder().encodeToString(protos.descriptorSet().toByteArray()));
        // Seed the deterministic run identity with immutable failed evidence.
        String runId = "prepare-" + PREPARATION_ID;
        serviceUnavailable.set(true);
        var compiledWorkflow = ai.protomolt.proto.workflow.WorkflowJson.parse(
                (ObjectNode) new ObjectMapper().readTree(source), ActionContext.create());
        try {
            new ai.protomolt.proto.workflow.WorkflowRunRecorder(runner, artifacts, runs)
                    .record(runId, null, compiledWorkflow, text("hello!"));
        } catch (WorkflowRunner.WorkflowExecutionException expected) {
            // The repository now contains immutable failed evidence under the preparation run ID.
        }
        serviceUnavailable.set(false);
        int seededCallCount = calls.get();
        assertThatThrownBy(() -> preparer.prepare(request(source), holder()))
                .isInstanceOf(WorkflowPreparationException.class);
        int after = calls.get();
        assertThat(ledger.find(TASK_ID, 1, 1).orElseThrow().hasFailed()).isTrue();
        assertThatThrownBy(() -> preparer.prepare(request(source), holder()))
                .isInstanceOf(WorkflowPreparationException.class);
        assertThat(calls).hasValue(after);
        assertThat(after).isEqualTo(seededCallCount);
    }

    @Test
    void failedRunUnderStableIdWithDifferentWorkflowProvenanceIsCorruptNotTerminal() throws Exception {
        var source = SOURCE.formatted(Base64.getEncoder().encodeToString(protos.descriptorSet().toByteArray()));
        String runId = "prepare-" + PREPARATION_ID;
        var differentSource = source.replace("prepared-echo", "other-prepared-echo");
        var differentWorkflow = ai.protomolt.proto.workflow.WorkflowJson.parse(
                (ObjectNode) new ObjectMapper().readTree(differentSource), ActionContext.create());
        serviceUnavailable.set(true);
        try {
            new ai.protomolt.proto.workflow.WorkflowRunRecorder(runner, artifacts, runs)
                    .record(runId, null, differentWorkflow, text("hello!"));
        } catch (WorkflowRunner.WorkflowExecutionException expected) {
            // Store a valid FAILED record under the expected stable ID but with wrong provenance.
        }
        serviceUnavailable.set(false);
        int callsBefore = calls.get();
        assertThat(runs.find(runId)).isPresent();
        assertThatThrownBy(() -> preparer.prepare(request(source), holder()))
                .isInstanceOf(WorkflowPreparationException.class)
                .extracting("kind").isEqualTo(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE);
        assertThat(calls).hasValue(callsBefore);
        assertThat(ledger.find(TASK_ID, 1, 1)).isPresent();
        assertThat(ledger.find(TASK_ID, 1, 1).orElseThrow().hasFailed()).isFalse();
    }

    @Test
    void successfulRunRecoveryReusesImmutableEvidenceWithoutRerecording() throws Exception {
        var source = SOURCE.formatted(Base64.getEncoder().encodeToString(protos.descriptorSet().toByteArray()));
        var definition = ai.protomolt.proto.workflow.WorkflowJson.parse(
                (ObjectNode) new ObjectMapper().readTree(source), ActionContext.create());
        String runId = "prepare-" + PREPARATION_ID;
        RunEvidence saved = new ai.protomolt.proto.workflow.WorkflowRunRecorder(runner, artifacts, runs)
                .record(runId, null, definition, text("hello!"));
        int seededCalls = calls.get();
        var response = preparer.prepare(request(source), holder());
        assertThat(calls).hasValue(seededCalls); // neither fixtures nor recorded workflow rerun on recovery
        assertThat(runs.find(runId)).contains(saved);
        assertThat(response.getAuthored().getDeliverable().getRunId()).isEqualTo(runId);
        assertThat(ledger.find(TASK_ID, 1, 1).orElseThrow().hasCompleted()).isTrue();
    }

    @Test
    void successfulFixtureFollowedByIncorrectRecordedOutputBecomesTerminal() throws Exception {
        // The first RPC is the pinned acceptance fixture; the second creates immutable run evidence.
        wrongResponseCall.set(2);
        var request = request(SOURCE.formatted(Base64.getEncoder().encodeToString(protos.descriptorSet().toByteArray())));
        assertThatThrownBy(() -> preparer.prepare(request, holder()))
                .isInstanceOf(WorkflowPreparationException.class);
        int afterInitialAttempt = calls.get();
        assertThat(afterInitialAttempt).isEqualTo(2);
        assertThat(ledger.find(TASK_ID, 1, 1)).isPresent();
        assertThat(ledger.find(TASK_ID, 1, 1).orElseThrow().hasFailed()).isTrue();
        assertThatThrownBy(() -> preparer.prepare(request, holder()))
                .isInstanceOf(WorkflowPreparationException.class);
        assertThat(calls).hasValue(afterInitialAttempt);
    }

    @Test
    void cancellationDuringFixtureRpcIsRecheckedBeforeCompletion() throws Exception {
        cancelOnServiceCall.set(true);
        var request = request(SOURCE.formatted(Base64.getEncoder().encodeToString(protos.descriptorSet().toByteArray())));
        assertThatThrownBy(() -> preparer.prepare(request, holder()))
                .isInstanceOf(WorkflowPreparationException.class);
        assertThat(calls).hasValue(1);
        assertThat(ledger.find(TASK_ID, 1, 1)).isPresent();
        assertThat(ledger.find(TASK_ID, 1, 1).orElseThrow().hasCompleted()).isFalse();
    }

    private WorkflowCandidatePreparer newPreparer(TranscriptRepository transcriptRepository,
            RunEvidenceRepository runRepository, java.util.function.Supplier<TrustSnapshot> trustSupplier) {
        return new WorkflowCandidatePreparer(transcriptRepository, ledger, artifacts, runRepository,
                runner, ActionContext.create(), policyRef, trustSupplier, signing, CLOCK, (workflow, json, p) -> {});
    }

    private void startTask(CandidateReviewer reviewer) throws Exception {
        transcripts = new InMemoryTranscriptRepository();
        coordinator = new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(), reviewer, CLOCK, transcripts);
        String name = InProcessServerBuilder.generateName();
        delegationServer = InProcessServerBuilder.forName(name).addService(coordinator).build().start();
        delegationChannel = InProcessChannelBuilder.forName(name).build();
        var hello = WorkerHello.newBuilder().setWorkerId(HOLDER).setProtocolVersion(1)
                .setProvider("scripted").setModel("preparer-test")
                .addCapabilities(WorkerCapability.newBuilder().setName("workflow-authoring")).build();
        var workerRunner = new ScriptedWorkerRunner(hello, List.of((task, events) -> {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < until) {
                Thread.sleep(10);
                if (coordinator.state().tasks().get(TASK_ID).phase() == DelegationReducer.Phase.CANCELLED) break;
            }
            throw new IllegalStateException("worker test task complete");
        }));
        worker = new DelegationWorker(AgentDelegationServiceGrpc.newStub(delegationChannel), workerRunner);
        worker.start();
        assertThat(worker.awaitAdmission(java.time.Duration.ofSeconds(10))).isTrue();
        var closure = new LinkedHashMap<String, FileDescriptor>();
        collect(ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable.getDescriptor().getFile(), closure);
        FileDescriptorSet.Builder descriptorSet = FileDescriptorSet.newBuilder();
        closure.values().forEach(value -> descriptorSet.addFile(value.toProto()));
        var spec = TaskSpec.newBuilder().setObjective("Prepare the authored workflow")
                .setContract(DeliverableContract.newBuilder().setTypeName(
                        ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable.getDescriptor()
                                .getFullName()).setDescriptorSet(descriptorSet.build().toByteString()))
                .addContext(policyRef);
        WorkflowAuthoringReviewer.REQUIRED_CHECKS.forEach(check -> spec.addRequiredChecks(
                AcceptanceCheck.newBuilder().setName(check).setDescription(check)));
        coordinator.offer(HOLDER, TASK_ID, spec.build(), java.time.Duration.ofSeconds(30));
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < until) {
            var state = coordinator.state().tasks().get(TASK_ID);
            if (state != null && state.phase() == DelegationReducer.Phase.LEASED) return;
            Thread.sleep(5);
        }
        throw new AssertionError("task did not enter leased phase");
    }

    private static PrepareWorkflowCandidateRequest request(String json) {
        return PrepareWorkflowCandidateRequest.newBuilder().setTaskId(TASK_ID).setAttempt(1).setRevision(1)
                .setPreparationId(PREPARATION_ID).setExecutableSourceJson(ByteString.copyFromUtf8(json)).build();
    }

    private static Caller holder() { return Caller.scoped(HOLDER, Set.of(Scopes.WORKER_COORDINATE)); }

    private DynamicMessage text(String value) {
        return DynamicMessage.newBuilder(file.findMessageTypeByName("Text"))
                .setField(file.findMessageTypeByName("Text").findFieldByName("text"), value).build();
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.containsKey(file.getName())) return;
        file.getDependencies().forEach(dep -> collect(dep, files));
        files.put(file.getName(), file);
    }
}
