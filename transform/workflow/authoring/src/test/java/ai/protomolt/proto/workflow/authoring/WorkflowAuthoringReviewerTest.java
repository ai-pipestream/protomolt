package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.delegation.CandidateReviewer.ReviewContext;
import ai.protomolt.proto.delegation.CandidateReviewer.ReviewDecision;
import ai.protomolt.proto.delegation.AdmissionPolicy;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.DelegationReducer;
import ai.protomolt.proto.delegation.DelegationWorker;
import ai.protomolt.proto.delegation.InMemoryTranscriptRepository;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.ScriptedWorkerRunner;
import ai.protomolt.proto.delegation.v1.AgentDelegationServiceGrpc;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemRunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.registry.GitSchemaRegistryStore;
import ai.protomolt.proto.registry.RegistryWorkflowVersionRepository;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.receipt.*;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.workflow.WorkflowCompiler;
import ai.protomolt.proto.workflow.WorkflowJson;
import ai.protomolt.proto.workflow.WorkflowRunRecorder;
import ai.protomolt.proto.workflow.WorkflowRunner;
import ai.protomolt.proto.workflow.WorkRecordProjector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowAuthoringReviewerTest {
    private static final String PROTO = """
            syntax = "proto3";
            package workflow.test;
            message Text { string text = 1; }
            service Echo { rpc Say(Text) returns (Text); }
            """;

    @TempDir Path temp;
    private FileSystemArtifactRepository artifacts;
    private FileSystemRunEvidenceRepository runs;
    private Descriptor textType;
    private Server server;
    private WorkflowRunner runner;
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicReference<String> serviceReply = new AtomicReference<>();
    private final AtomicBoolean serviceUnavailable = new AtomicBoolean();
    private ArtifactReference policyRef;
    private ArtifactReference sourceRef;
    private ArtifactReference workflowRef;
    private ArtifactReference receiptRef;
    private ArtifactReference alternateReceiptRef;
    private WorkflowAcceptanceFixture fixture;
    private WorkflowAuthoringDeliverable authored;
    private TaskSpec spec;
    private CompletionCandidate candidate;
    private TrustSnapshot trust;
    private RunEvidence recorded;

    @BeforeEach
    void setup() throws Exception {
        artifacts = new FileSystemArtifactRepository(temp.resolve("artifacts"));
        runs = new FileSystemRunEvidenceRepository(temp.resolve("runs"));
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("workflow/test/echo.proto", PROTO, "test").build());
        var file = compiled.descriptorFor("workflow/test/echo.proto").orElseThrow();
        textType = file.findMessageTypeByName("Text");
        var service = file.findServiceByName("Echo");
        var method = DynamicGrpcCalls.methodDescriptor(service.findMethodByName("Say"));
        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerServiceDefinition.builder(io.grpc.ServiceDescriptor
                        .newBuilder(service.getFullName()).addMethod(method).build())
                        .addMethod(method, ServerCalls.asyncUnaryCall((request, response) -> {
                            calls.incrementAndGet();
                            if (serviceUnavailable.get()) {
                                response.onError(Status.UNAVAILABLE.withDescription("fixture service down")
                                        .asRuntimeException());
                                return;
                            }
                            String input = (String) request.getField(textType.findFieldByName("text"));
                            String overridden = serviceReply.get();
                            response.onNext(message(overridden == null ? input + "!" : overridden));
                            response.onCompleted();
                        })).build()).build().start();
        runner = new WorkflowRunner(step -> InProcessChannelBuilder.forName(serverName).build());

        byte[] descriptors = compiled.descriptorSet().toByteArray();
        var source = new ObjectMapper().createObjectNode();
        source.put("name", "echo-text");
        source.put("validateContract", true);
        source.putObject("schema").put("descriptorSetBase64",
                Base64.getEncoder().encodeToString(descriptors));
        source.put("inputType", "workflow.test.Text");
        var step = source.putArray("steps").addObject();
        step.put("name", "echo");
        step.put("target", "fixture:9090");
        step.put("method", "workflow.test.Echo/Say");
        step.put("validate", true);
        step.putArray("rules").add("text = input.text");
        var definition = WorkflowJson.parse(source, ActionContext.create());
        var workflow = WorkflowCompiler.compile(definition);
        workflowRef = artifacts.save(workflow.toByteArray(), "application/x-protobuf", false);
        sourceRef = artifacts.save(source.toString().getBytes(StandardCharsets.UTF_8),
                "application/json", false);
        var descriptorRef = artifacts.save(descriptors, "application/x-protobuf", false);
        fixture = WorkflowAcceptanceFixture.newBuilder().setName("echo-fixture")
                .setInput(artifacts.save(message("hello").toByteArray(), "application/x-protobuf", false))
                .setExpectedOutput(artifacts.save(message("hello!").toByteArray(),
                        "application/x-protobuf", false)).build();
        var policy = WorkflowAuthoringPolicy.newBuilder().setDescriptors(descriptorRef)
                .addFixtures(fixture).addPermittedCalls(WorkflowPermittedCall.newBuilder()
                        .setTarget("fixture:9090").setMethod("workflow.test.Echo/Say")).build();
        policyRef = artifacts.save(policy.toByteArray(), "application/x-protobuf", false);

        recorded = new WorkflowRunRecorder(runner, artifacts, runs)
                .record("authoring-run", null, definition, message("recorded"));
        var keys = RecordKeys.generate();
        trust = TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder()
                .setIssuer("authoring-test").addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                .addKeys(TrustedKey.newBuilder().setKeyId("test-key")
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setState(KeyState.KEY_STATE_ACTIVE)
                        .setPublicKey(ByteString.copyFrom(RecordKeys.rawPublicKey(keys.getPublic()))))).build();
        receiptRef = receipt(recorded, keys);
        alternateReceiptRef = receipt(recorded.toBuilder().setRunId("another-run").build(), keys);

        var checks = WorkflowAuthoringReviewer.REQUIRED_CHECKS.stream()
                .map(name -> CheckEvidence.newBuilder().setCheckName(name)
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(100))
                        .addArtifacts(sourceRef).build()).toList();
        var deliverable = WorkflowDeliverable.newBuilder().setWorkflow(workflow)
                .setWorkflowArtifact(workflowRef).setDescriptors(descriptorRef)
                .addFixtures(recorded.getInputArtifact()).addFixtures(recorded.getOutputArtifact())
                .addAllChecks(checks).setRunId(recorded.getRunId()).setReceipt(receiptRef).build();
        authored = WorkflowAuthoringDeliverable.newBuilder().setDeliverable(deliverable)
                .setExecutableSource(sourceRef).addAcceptanceFixtures(fixture).build();

        Map<String, FileDescriptor> closure = new LinkedHashMap<>();
        collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), closure);
        var set = FileDescriptorSet.newBuilder();
        closure.values().forEach(fileDescriptor -> set.addFile(fileDescriptor.toProto()));
        var contract = DeliverableContract.newBuilder()
                .setTypeName(WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                .setDescriptorSet(set.build().toByteString());
        var specBuilder = TaskSpec.newBuilder().setObjective("Author one checked workflow")
                .setContract(contract).addContext(policyRef);
        WorkflowAuthoringReviewer.REQUIRED_CHECKS.forEach(name -> specBuilder.addRequiredChecks(
                AcceptanceCheck.newBuilder().setName(name).setDescription(name)));
        spec = specBuilder.build();
        candidate = CompletionCandidate.newBuilder().setAttempt(1).setRevision(1)
                .setSummary("Authored and checked workflow")
                .addAllEvidence(checks).addArtifacts(workflowRef).addArtifacts(sourceRef)
                .addArtifacts(receiptRef).setResult(Any.pack(authored)).build();
    }

    @AfterEach
    void stop() {
        if (server != null) server.shutdownNow();
    }

    @Test
    void acceptsOnlyAfterIndependentlyRerunningCallerFixture() throws Exception {
        int before = calls.get();
        var decision = review(spec, candidate, runs);
        assertThat(decision).isInstanceOf(ReviewDecision.Accept.class);
        assertThat(((ReviewDecision.Accept) decision).verdict())
                .contains("Verified 1 caller fixtures", "run=authoring-run", "manifest=");
        assertThat(calls.get()).isEqualTo(before + 1);
    }

    @Test
    void forgedPassingChecksAndChangedExpectedFixtureCannotAuthorizeCalls() throws Exception {
        var changed = authored.toBuilder().setAcceptanceFixtures(0, fixture.toBuilder()
                .setExpectedOutput(artifacts.save(message("forged").toByteArray(),
                        "application/x-protobuf", false))).build();
        assertRevisedWithoutCalls(spec, candidate.toBuilder().setResult(Any.pack(changed)).build(), runs);
    }

    @Test
    void mismatchedInnerChecksAndUnsupportedOfferCheckCannotAuthorizeCalls() throws Exception {
        var mismatch = authored.toBuilder().setDeliverable(authored.getDeliverable().toBuilder()
                .setChecks(0, authored.getDeliverable().getChecks(0).toBuilder()
                        .setDetail("different evidence"))).build();
        assertRevisedWithoutCalls(spec, candidate.toBuilder().setResult(Any.pack(mismatch)).build(), runs);
        var unsupported = spec.toBuilder().setRequiredChecks(0, AcceptanceCheck.newBuilder()
                .setName("unsupported-check").setDescription("not independently implemented")).build();
        assertRevisedWithoutCalls(unsupported, candidate, runs);
    }

    @Test
    void absentPolicyContextAndWrongReceiptCannotAuthorizeCalls() throws Exception {
        assertRevisedWithoutCalls(spec.toBuilder().clearContext().build(), candidate, runs);
        var wrong = authored.toBuilder().setDeliverable(authored.getDeliverable().toBuilder()
                .setReceipt(alternateReceiptRef)).build();
        var withWrongReceipt = candidate.toBuilder().addArtifacts(alternateReceiptRef)
                .setResult(Any.pack(wrong)).build();
        assertRevisedWithoutCalls(spec, withWrongReceipt, runs);
    }

    @Test
    void changedAuthoritativeRunPayloadAndMissingRunCannotAuthorizeCalls() throws Exception {
        RunEvidence changed = recorded.toBuilder().setOutputArtifact(recorded.getInputArtifact()).build();
        var changedRuns = new FileSystemRunEvidenceRepository(temp.resolve("changed-runs"));
        changedRuns.save(changed);
        assertRevisedWithoutCalls(spec, candidate, changedRuns);
        var emptyRuns = new FileSystemRunEvidenceRepository(temp.resolve("empty-runs"));
        assertRevisedWithoutCalls(spec, candidate, emptyRuns);
    }

    @Test
    void changedRealServiceResponseRequiresRevision() throws Exception {
        serviceReply.set("different-result");
        int before = calls.get();
        assertThat(review(spec, candidate, runs)).isInstanceOf(ReviewDecision.Revise.class);
        assertThat(calls.get()).isEqualTo(before + 1);
    }

    @Test
    void serviceOutagePropagatesWithoutAcceptance() {
        serviceUnavailable.set(true);
        int before = calls.get();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> review(spec, candidate, runs))
                .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class);
        assertThat(calls.get()).isGreaterThan(before);
    }

    @Test
    void coordinatorAcceptsWorkerCandidateAndRestoresAcceptedRevisionFromTranscript() throws Exception {
        var transcriptStore = new InMemoryTranscriptRepository();
        var reviewer = new WorkflowAuthoringReviewer(policyRef, artifacts, runs,
                ActionContext.create(), runner, trust);
        String workerId = "authoring-worker";
        String taskId = UUID.randomUUID().toString();
        var hello = WorkerHello.newBuilder().setWorkerId(workerId).setProtocolVersion(1)
                .setProvider("scripted").setModel("deterministic")
                .addCapabilities(WorkerCapability.newBuilder().setName("workflow-authoring")).build();
        var scripted = new ScriptedWorkerRunner(hello, List.of((task, events) ->
                candidate.toBuilder().setAttempt(task.offer().getAttempt())
                        .setRevision(task.expectedRevision()).build()));
        int before = calls.get();
        try (var coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), reviewer, Clock.systemUTC(), transcriptStore)) {
            String name = InProcessServerBuilder.generateName();
            Server delegationServer = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(coordinator).build().start();
            ManagedChannel delegationChannel = InProcessChannelBuilder.forName(name)
                    .directExecutor().build();
            try (var worker = new DelegationWorker(
                    AgentDelegationServiceGrpc.newStub(delegationChannel), scripted)) {
                worker.start();
                assertThat(worker.awaitAdmission(Duration.ofSeconds(30))).isTrue();
                coordinator.offer(workerId, taskId, spec, Duration.ofSeconds(30));

                long cursor = 0;
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                InProcessDelegationCoordinator.Event accepted = null;
                while (accepted == null && System.nanoTime() < deadline) {
                    var next = coordinator.waitForEvent(taskId, cursor,
                            Duration.ofNanos(Math.max(1, deadline - System.nanoTime()))).orElse(null);
                    if (next == null) continue;
                    cursor = next.cursor();
                    if (next.entry().getCoordinatorFrame().hasAccepted()) accepted = next;
                }
                assertThat(accepted).as("reviewer acceptance event").isNotNull();
                assertThat(accepted.entry().getCoordinatorFrame().getAccepted().getAttempt()).isEqualTo(1);
                assertThat(accepted.entry().getCoordinatorFrame().getAccepted().getRevision()).isEqualTo(1);
                assertThat(coordinator.state().tasks().get(taskId).phase())
                        .isEqualTo(DelegationReducer.Phase.ACCEPTED);
                assertThat(coordinator.state().clean()).isTrue();
                assertThat(worker.streamFailure()).isEmpty();
                assertThat(calls.get()).isEqualTo(before + 1);
            } finally {
                delegationChannel.shutdownNow();
                delegationServer.shutdownNow();
            }
        }
        try (var restored = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), Clock.systemUTC(), transcriptStore)) {
            assertThat(restored.state().tasks().get(taskId).phase())
                    .isEqualTo(DelegationReducer.Phase.ACCEPTED);
            assertThat(restored.state().tasks().get(taskId).attempt()).isEqualTo(1);
            assertThat(restored.state().tasks().get(taskId).candidateRevision()).isEqualTo(1);
            assertThat(restored.transcript()).isEqualTo(transcriptStore.load().orElseThrow());
            assertThat(restored.state().clean()).isTrue();
        }
        exerciseLaunchRecovery(transcriptStore, taskId, reviewer);
    }

    private void exerciseLaunchRecovery(InMemoryTranscriptRepository transcripts, String taskId,
            WorkflowAuthoringReviewer reviewer) throws Exception {
        var ledgerPath = temp.resolve("launch-authorizations");
        var versionsPath = temp.resolve("promotions");
        var ledger = new FileSystemWorkflowLaunchAuthorizationRepository(ledgerPath);
        var storedJobs = new TestLaunchJobs();
        WorkflowAuthoringLaunchRequest request;
        int afterAuthorization;
        try (var git = GitSchemaRegistryStore.builder().repositoryDir(versionsPath).build()) {
            var versions = new RegistryWorkflowVersionRepository(git);
            var launcher = new WorkflowAuthoringLauncher(transcripts, reviewer, ledger,
                    versions, artifacts, storedJobs.store, ActionContext.create(), 3);
            request = WorkflowAuthoringLaunchRequest.newBuilder().setLaunchId(UUID.randomUUID().toString())
                    .setAcceptance(launcher.acceptedCandidate(taskId)).setInput(fixture.getInput()).build();
            serviceUnavailable.set(true);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> launcher.launch(request))
                    .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class);
            assertThat(ledger.find(request.getLaunchId())).isEmpty();
            assertThat(versions.versions(authored.getDeliverable().getWorkflow().getName())).isEmpty();
            assertThat(storedJobs.jobs).isEmpty();

            serviceUnavailable.set(false);
            var failingPromotion = new WorkflowVersionRepository() {
                public Optional<VersionedWorkflow> find(String name, String version) throws java.io.IOException {
                    return versions.find(name, version);
                }
                public List<VersionedWorkflow> versions(String name) throws java.io.IOException {
                    return versions.versions(name);
                }
                public void save(VersionedWorkflow workflow) throws java.io.IOException {
                    throw new java.io.IOException("injected promotion failure");
                }
            };
            var beforePromotion = new WorkflowAuthoringLauncher(transcripts, reviewer, ledger,
                    failingPromotion, artifacts, storedJobs.store, ActionContext.create(), 3);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> beforePromotion.launch(request))
                    .isInstanceOf(java.io.IOException.class).hasMessageContaining("injected promotion failure");
            assertThat(ledger.find(request.getLaunchId())).isPresent();
            assertThat(versions.versions(authored.getDeliverable().getWorkflow().getName())).isEmpty();
            assertThat(storedJobs.jobs).isEmpty();
            int beforeRecovery = calls.get();
            serviceUnavailable.set(true);
            var afterPromotionFailure = new WorkflowAuthoringLauncher(transcripts, reviewer,
                    new FileSystemWorkflowLaunchAuthorizationRepository(ledgerPath),
                    versions, artifacts, storedJobs.store, ActionContext.create(), 3);
            storedJobs.failBeforeInsert.set(true);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> afterPromotionFailure.launch(request))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("injected insert failure");
            assertThat(calls.get()).isEqualTo(beforeRecovery);
            assertThat(ledger.find(request.getLaunchId())).isPresent();
            assertThat(versions.versions(authored.getDeliverable().getWorkflow().getName())).hasSize(1);
            assertThat(storedJobs.jobs).isEmpty();
            afterAuthorization = calls.get();
        }

        // Reopen both durable stores. The fixture service is unavailable during recovery.
        serviceUnavailable.set(true);
        try (var git = GitSchemaRegistryStore.builder().repositoryDir(versionsPath).build()) {
            var versions = new RegistryWorkflowVersionRepository(git);
            var restoredLedger = new FileSystemWorkflowLaunchAuthorizationRepository(ledgerPath);
            var launcher = new WorkflowAuthoringLauncher(transcripts, reviewer, restoredLedger,
                    versions, artifacts, storedJobs.store, ActionContext.create(), 3);
            // A lifecycle-clean replacement still cannot change the accepted candidate
            // under an existing authorization, even if the typed result is unchanged.
            var originalTranscript = transcripts.load().orElseThrow();
            var changedTranscript = originalTranscript.toBuilder();
            for (int i = 0; i < originalTranscript.getEntriesCount(); i++) {
                var entry = originalTranscript.getEntries(i);
                if (entry.hasWorkerFrame() && entry.getWorkerFrame().hasCompletion()
                        && entry.getWorkerFrame().getTaskId().equals(taskId)) {
                    changedTranscript.setEntries(i, entry.toBuilder().setWorkerFrame(
                            entry.getWorkerFrame().toBuilder().setCompletion(
                                    entry.getWorkerFrame().getCompletion().toBuilder()
                                            .setSummary("changed accepted candidate"))));
                }
            }
            assertThat(new DelegationReducer().reduce(changedTranscript.build()).clean()).isTrue();
            transcripts.save(changedTranscript.build());
            try {
                org.assertj.core.api.Assertions.assertThatThrownBy(() -> launcher.launch(request))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("durable candidate");
                assertThat(storedJobs.jobs).isEmpty();
                assertThat(calls.get()).isEqualTo(afterAuthorization);
                assertThat(versions.versions(authored.getDeliverable().getWorkflow().getName())).hasSize(1);
            } finally {
                transcripts.save(originalTranscript);
            }
            var first = launcher.launch(request);
            assertThat(first.getJobId()).isEqualTo(request.getLaunchId());
            assertThat(launcher.launch(request)).isEqualTo(first);
            var catalog = WorkflowAuthoringActions.register(
                    ai.protomolt.proto.actions.ActionCatalog.defaults(
                            ai.protomolt.proto.actions.ActionContext.create()), launcher);
            assertThat(catalog.execute("get-accepted-workflow",
                    ai.protomolt.proto.workflow.authoring.v1.GetAcceptedWorkflowRequest.newBuilder()
                            .setTaskId(taskId).build())).isEqualTo(request.getAcceptance());
            assertThat(catalog.execute("launch-accepted-workflow", request)).isEqualTo(first);
            assertThat(calls.get()).isEqualTo(afterAuthorization);
            assertThat(storedJobs.jobs).hasSize(1);
            assertThat(storedJobs.acceptedEvents.get()).isEqualTo(1);
            assertThat(versions.versions(authored.getDeliverable().getWorkflow().getName())).hasSize(1);
            var evidence = artifacts.find(first.getAuthorization().getSha256()).orElseThrow();
            assertThat(evidence.content()).isEqualTo(WorkflowLaunchValidation.deterministicBytes(
                    restoredLedger.find(request.getLaunchId()).orElseThrow()));

            var changed = request.toBuilder().setInput(artifacts.save(message("changed").toByteArray(),
                    "application/x-protobuf", false)).build();
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> launcher.launch(changed))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("different intent");
            assertThat(calls.get()).isEqualTo(afterAuthorization);
            assertThat(storedJobs.acceptedEvents.get()).isEqualTo(1);

            // A job inserted outside this launch boundary cannot be adopted retrospectively.
            var unbound = request.toBuilder().setLaunchId(UUID.randomUUID().toString()).build();
            var unboundJob = new WorkflowRunRecord();
            unboundJob.jobId = UUID.fromString(unbound.getLaunchId());
            storedJobs.jobs.put(unboundJob.jobId, unboundJob);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> launcher.launch(unbound))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unbound");
            assertThat(restoredLedger.find(unbound.getLaunchId())).isEmpty();
            assertThat(calls.get()).isEqualTo(afterAuthorization);
            storedJobs.jobs.remove(unboundJob.jobId);

            // The job commit can succeed while its response is lost. Retry discovers that row.
            serviceUnavailable.set(false);
            var lostResponse = request.toBuilder().setLaunchId(UUID.randomUUID().toString()).build();
            storedJobs.failAfterInsert.set(true);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> launcher.launch(lostResponse))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("injected lost response");
            int afterLostResponse = calls.get();
            serviceUnavailable.set(true);
            assertThat(launcher.launch(lostResponse).getJobId()).isEqualTo(lostResponse.getLaunchId());
            assertThat(calls.get()).isEqualTo(afterLostResponse);
            assertThat(storedJobs.jobs).hasSize(2);
            assertThat(storedJobs.acceptedEvents.get()).isEqualTo(2);
            assertThat(versions.versions(authored.getDeliverable().getWorkflow().getName())).hasSize(1);

            // An unrelated job submitter can race across the separate ledger and
            // jobs stores. No success is returned, and its row is never replaced.
            serviceUnavailable.set(false);
            var raced = request.toBuilder().setLaunchId(UUID.randomUUID().toString()).build();
            storedJobs.externalClaim.set(true);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> launcher.launch(raced))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("different workflow");
            assertThat(restoredLedger.find(raced.getLaunchId())).isPresent();
            assertThat(storedJobs.jobs.get(UUID.fromString(raced.getLaunchId())).input)
                    .isEqualTo("{\"text\":\"external\"}");
            int afterConflict = calls.get();
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> launcher.launch(raced))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("different job");
            assertThat(calls.get()).isEqualTo(afterConflict);
        }
    }

    // Only the job persistence boundary is simulated here. The production submitter,
    // reviewer, protobuf validation, fixture RPCs, ledger and Git registry are exercised.
    private static final class TestLaunchJobs {
        final Map<UUID, WorkflowRunRecord> jobs = new LinkedHashMap<>();
        final AtomicInteger acceptedEvents = new AtomicInteger();
        final AtomicBoolean failBeforeInsert = new AtomicBoolean();
        final AtomicBoolean failAfterInsert = new AtomicBoolean();
        final AtomicBoolean externalClaim = new AtomicBoolean();
        final WorkflowRunStore store = (WorkflowRunStore) java.lang.reflect.Proxy.newProxyInstance(
                WorkflowRunStore.class.getClassLoader(), new Class<?>[] {WorkflowRunStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("get")) return Optional.ofNullable(jobs.get(args[0]));
                    if (method.getName().equals("insert")) {
                        if (failBeforeInsert.getAndSet(false)) throw new IllegalStateException("injected insert failure");
                        var proposed = (WorkflowRunRecord) args[0];
                        if (externalClaim.getAndSet(false)) {
                            var external = new WorkflowRunRecord();
                            external.jobId = proposed.jobId;
                            external.workflowName = proposed.workflowName;
                            external.workflowDefinition = proposed.workflowDefinition;
                            external.input = "{\"text\":\"external\"}";
                            jobs.put(external.jobId, external);
                            acceptedEvents.incrementAndGet();
                        }
                        var existing = jobs.putIfAbsent(proposed.jobId, proposed);
                        if (existing != null) return new WorkflowRunStore.InsertOutcome(existing, false,
                                !WorkflowRunStore.sameSubmission(existing, proposed));
                        acceptedEvents.incrementAndGet();
                        if (failAfterInsert.getAndSet(false)) throw new IllegalStateException("injected lost response");
                        return new WorkflowRunStore.InsertOutcome(proposed, true, false);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private ReviewDecision review(TaskSpec offer, CompletionCandidate completion,
            RunEvidenceRepository repository) throws Exception {
        var reviewer = new WorkflowAuthoringReviewer(policyRef, artifacts, repository,
                ActionContext.create(), runner, trust);
        return reviewer.review(new ReviewContext("authoring-task", "fixture-worker", offer, completion));
    }

    private void assertRevisedWithoutCalls(TaskSpec offer, CompletionCandidate completion,
            RunEvidenceRepository repository) throws Exception {
        int before = calls.get();
        assertThat(review(offer, completion, repository))
                .isInstanceOf(ReviewDecision.Revise.class);
        assertThat(calls.get()).isEqualTo(before);
    }

    private ArtifactReference receipt(RunEvidence run, java.security.KeyPair keys) throws Exception {
        var issuance = new WorkRecordProjector.Issuance("receipt-" + run.getRunId(),
                "authoring-test", "test-key", Timestamp.newBuilder().setSeconds(200).build(), "");
        var signed = new RecordSigner("test-key", keys.getPrivate())
                .sign(WorkRecordProjector.project(run, issuance));
        return artifacts.save(signed.toByteArray(), "application/x-protobuf", false);
    }

    private DynamicMessage message(String text) {
        return DynamicMessage.newBuilder(textType)
                .setField(textType.findFieldByName("text"), text).build();
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> closure) {
        if (closure.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, closure));
        closure.put(file.getName(), file);
    }
}
