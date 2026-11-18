package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.v1.DescriptorArtifact;
import ai.protomolt.proto.grpc.profile.v1.MethodPolicy;
import ai.protomolt.proto.grpc.profile.v1.Operation;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.store.InMemoryWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.worker.PipelinePolicyTestFixture.CompiledSchema;
import ai.protomolt.proto.jobs.service.worker.PipelinePolicyTestFixture.IssuedPki;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.grpc.ManagedChannel;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.TlsChannelCredentials;
import io.grpc.TlsServerCredentials;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.io.IOException;
import java.util.Optional;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static ai.protomolt.proto.jobs.service.worker.PipelinePolicyTestFixture.PROFILE_NAME;
import static ai.protomolt.proto.jobs.service.worker.PipelinePolicyTestFixture.countingFilter;
import static ai.protomolt.proto.jobs.service.worker.PipelinePolicyTestFixture.echoPipeline;
import static ai.protomolt.proto.jobs.service.worker.PipelinePolicyTestFixture.echoService;
import static ai.protomolt.proto.jobs.service.worker.PipelinePolicyTestFixture.policyBackedTlsFactory;
import static ai.protomolt.proto.jobs.service.worker.PipelinePolicyTestFixture.submitPipeline;
import static ai.protomolt.proto.jobs.service.worker.PipelinePolicyTestFixture.workerConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that durable pipeline execution in {@link WorkflowRunWorker} enforces
 * {@link OutboundChannelPolicy} target allowlists, TLS requirements, active-channel eviction,
 * and canonical per-target concurrency permits across dependency aliases.
 */
class PipelineOutboundPolicyTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    static Path pkiDir;

    private static CompiledSchema schema;
    private static IssuedPki pki;

    @TempDir
    Path workspaceDir;

    private final AtomicInteger plainTransports = new AtomicInteger();
    private final AtomicInteger plainCalls = new AtomicInteger();
    private final AtomicInteger secondaryTransports = new AtomicInteger();
    private final AtomicInteger secondaryCalls = new AtomicInteger();
    private final AtomicInteger tlsTransports = new AtomicInteger();
    private final AtomicInteger tlsCalls = new AtomicInteger();
    private final AtomicReference<String> negotiatedTlsProtocol = new AtomicReference<>();
    private final AtomicReference<Runnable> plainInvokeHook = new AtomicReference<>();

    private Server plainServer;
    private int plainPort;
    private Server secondaryServer;
    private int secondaryPort;
    private Server tlsServer;
    private int tlsPort;
    private ActionContext context;
    private FileSystemServiceProfileRepository serviceProfiles;

    @BeforeAll
    static void compileSchemaAndIssueCertificates() throws Exception {
        schema = PipelinePolicyTestFixture.compileSchema();
        pki = PipelinePolicyTestFixture.issueCertificates(pkiDir);
    }

    @BeforeEach
    void setUp() throws Exception {
        plainTransports.set(0);
        plainCalls.set(0);
        secondaryTransports.set(0);
        secondaryCalls.set(0);
        tlsTransports.set(0);
        tlsCalls.set(0);
        negotiatedTlsProtocol.set(null);
        plainInvokeHook.set(null);

        plainServer = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                .addTransportFilter(countingFilter(plainTransports))
                .addService(echoService(schema, plainCalls, null, () -> {
                    Runnable hook = plainInvokeHook.get();
                    if (hook != null) {
                        hook.run();
                    }
                }))
                .build()
                .start();
        plainPort = plainServer.getPort();

        secondaryServer = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                .addTransportFilter(countingFilter(secondaryTransports))
                .addService(echoService(schema, secondaryCalls, null, null))
                .build()
                .start();
        secondaryPort = secondaryServer.getPort();

        tlsServer = Grpc.newServerBuilderForPort(0, TlsServerCredentials.create(
                        pki.serverCertPem().toFile(), pki.serverKeyPem().toFile()))
                .addTransportFilter(countingFilter(tlsTransports))
                .addService(echoService(schema, tlsCalls, negotiatedTlsProtocol, null))
                .build()
                .start();
        tlsPort = tlsServer.getPort();

        context = ActionContext.create();
        for (FileDescriptor fd : schema.files()) {
            context.registry().registerFile(fd);
        }
        serviceProfiles = new FileSystemServiceProfileRepository(workspaceDir);
        PipelinePolicyTestFixture.saveDefaultServiceProfile(
                serviceProfiles, schema, plainPort, secondaryPort, tlsPort);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (Server srv : new Server[] {plainServer, secondaryServer, tlsServer}) {
            if (srv != null) {
                srv.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void deniedHostAndDeniedPortAreRefusedBeforeConnecting() {
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);

        OutboundChannelPolicy denyHostPolicy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("allowed.example.internal"))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(denyHostPolicy),
                denyHostPolicy, serviceProfiles, null, workerConfig("deny-host", 4, 4))) {
            String jobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain"), "hello");
            assertThat(worker.workOnce()).isTrue();

            WorkflowRunRecord failed = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(failed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(failed.error).contains("gRPC target host is not allowed: 127.0.0.1");
            assertThat(plainTransports.get()).isZero();
            assertThat(plainCalls.get()).isZero();
        }

        int otherPort = plainPort == 65535 ? 65534 : plainPort + 1;
        OutboundChannelPolicy denyPortPolicy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .allowedPorts(Set.of(otherPort))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(denyPortPolicy),
                denyPortPolicy, serviceProfiles, null, workerConfig("deny-port", 4, 4))) {
            String jobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain"), "hello");
            assertThat(worker.workOnce()).isTrue();

            WorkflowRunRecord failed = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(failed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(failed.error).contains("gRPC target port is not allowed: " + plainPort);
            assertThat(plainTransports.get()).isZero();
        }
    }

    @Test
    void tlsRequiredPolicyRefusesPlaintextAndConnectsBothSharedAndCustomTlsOnSameWorker()
            throws Exception {
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);

        OutboundChannelPolicy tlsRequiredPolicy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .allowedPorts(Set.of(plainPort, tlsPort))
                .allowPlaintext(false)
                .allowTls(true)
                .maxActiveChannels(1)
                .build();
        ChannelCredentials trustedCa = TlsChannelCredentials.newBuilder()
                .trustManager(pki.caCertPem().toFile())
                .build();
        ChannelFactory policyTlsFactory = policyBackedTlsFactory(tlsRequiredPolicy, trustedCa);
        WorkflowRunWorker.EndpointCredentials endpointCredentials =
                new WorkflowRunWorker.EndpointCredentials() {
                    @Override
                    public ChannelCredentials channelCredentials(ServiceProfile profile,
                                                                 ServiceEndpoint endpoint) {
                        if ("tls:test-ca".equals(endpoint.getTrustRef())) {
                            return trustedCa;
                        }
                        return WorkflowRunWorker.EndpointCredentials.NONE
                                .channelCredentials(profile, endpoint);
                    }
                };

        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(tlsRequiredPolicy),
                policyTlsFactory, serviceProfiles, null, endpointCredentials,
                workerConfig("tls-required", 4, 4))) {
            String plainJobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain"), "blocked");
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord plainFailed = store.get(UUID.fromString(plainJobId)).orElseThrow();
            assertThat(plainFailed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(plainFailed.error)
                    .contains("plaintext outbound channels are disabled by policy");
            assertThat(plainTransports.get()).isZero();

            String tlsJobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "secure"), "hello-tls");
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord tlsCompleted = store.get(UUID.fromString(tlsJobId)).orElseThrow();
            assertThat(tlsCompleted.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            assertThat(negotiatedTlsProtocol.get()).startsWith("TLS");
            JsonNode result = PipelineJobCoordinator.view(tlsCompleted).result();
            assertThat(result.path("echoed").asText()).isEqualTo("echo:hello-tls");
            assertThat(result.path("operationId").asText()).isEqualTo(tlsJobId + "/call_echo");

            String customTlsJobId = submitPipeline(submitter, schema,
                    echoPipeline(schema, "echo", "secure-custom-trust"), "mtls-ca");
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord customCompleted =
                    store.get(UUID.fromString(customTlsJobId)).orElseThrow();
            assertThat(customCompleted.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            assertThat(tlsCalls.get()).isEqualTo(2);
        }
    }

    @Test
    void finishedPipelineCallReleasesItsChannelSlotForOtherUsersOfThePolicy() throws Exception {
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 1);
        OutboundChannelPolicy oneSlotPolicy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .allowedPorts(Set.of(plainPort, secondaryPort))
                .maxActiveChannels(1)
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(oneSlotPolicy),
                oneSlotPolicy, serviceProfiles, null, workerConfig("slot-release", 1, 4))) {
            String jobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain"), "done");
            assertThat(worker.workOnce()).isTrue();
            assertThat(store.get(UUID.fromString(jobId)).orElseThrow().status)
                    .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);

            // A catalog or workflow call shares this policy and cannot ask the pipeline
            // transport to give anything up, so the finished call must not be holding the slot.
            ManagedChannel other = oneSlotPolicy.open(
                    "127.0.0.1:" + secondaryPort, false,
                    target -> Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                            .build());
            other.shutdownNow();
        }
    }

    @Test
    void maxActiveChannelsOneServesSequentialCallsOneChannelAtATimeAndProtectsInFlightCalls()
            throws Exception {
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 1);
        OutboundChannelPolicy oneSlotPolicy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .allowedPorts(Set.of(plainPort, secondaryPort))
                .maxActiveChannels(1)
                .build();

        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(oneSlotPolicy),
                oneSlotPolicy, serviceProfiles, null, workerConfig("one-slot", 1, 4))) {
            // Sequential calls: endpoint A twice, then endpoint B. Each call's channel closes when
            // the call finishes, so the single slot is free for the next one.
            String firstA = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain"), "a1");
            assertThat(worker.workOnce()).isTrue();
            assertThat(store.get(UUID.fromString(firstA)).orElseThrow().status)
                    .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);

            String secondA = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain"), "a2");
            assertThat(worker.workOnce()).isTrue();
            assertThat(store.get(UUID.fromString(secondA)).orElseThrow().status)
                    .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            assertThat(plainTransports.get()).isEqualTo(2);

            String firstB = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain-secondary"), "b1");
            assertThat(worker.workOnce()).isTrue();
            assertThat(store.get(UUID.fromString(firstB)).orElseThrow().status)
                    .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            assertThat(secondaryCalls.get()).isEqualTo(1);

            // Truly concurrent calls to A and B under maxActiveChannels(1): in-flight A is NOT
            // shut down underneath its RPC while B hits RESOURCE_EXHAUSTED (landing DEAD at maxAttempts=1).
            CountDownLatch callEntered = new CountDownLatch(1);
            CountDownLatch releaseCall = new CountDownLatch(1);
            plainInvokeHook.set(() -> {
                callEntered.countDown();
                try {
                    releaseCall.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            String inFlightA = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain"), "in-flight-a");
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<Boolean> runningA = pool.submit(worker::workOnce);
                assertThat(callEntered.await(5, TimeUnit.SECONDS)).isTrue();

                String concurrentB = submitPipeline(submitter, schema,
                        echoPipeline(schema, "echo", "plain-secondary"), "blocked-b");
                assertThat(worker.workOnce()).isTrue();
                WorkflowRunRecord rejectedB =
                        store.get(UUID.fromString(concurrentB)).orElseThrow();
                assertThat(rejectedB.status).isEqualTo(WorkflowRunRecord.STATUS_DEAD);
                assertThat(rejectedB.error)
                        .contains("RESOURCE_EXHAUSTED", "outbound channel concurrency limit");

                releaseCall.countDown();
                assertThat(runningA.get(5, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(store.get(UUID.fromString(inFlightA)).orElseThrow().status)
                    .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        }
    }

    @Test
    void profileReadFailureRequeuesTheRunWithoutCallingOutOrGuessingATarget() throws Exception {
        ServiceProfileRepository flaky = profilesFailingFrom(1);
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
        OutboundChannelPolicy allowLoopback = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(allowLoopback),
                allowLoopback, flaky, null, workerConfig("flaky-profiles", 4, 1))) {
            String jobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain"), "hello");
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord requeued = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(requeued.status).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
            assertThat(requeued.attempt).isEqualTo(1);
            assertThat(plainCalls.get()).isZero();
        }
    }

    @Test
    void anExternalCompletionStepParksWithoutReadingItsProfileOrTakingAPermit() throws Exception {
        ServiceProfileRepository unreadable = profilesFailingFrom(1);
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
        OutboundChannelPolicy allowLoopback = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .build();
        Pipeline live = echoPipeline(schema, "echo", "plain");
        Pipeline external = live.toBuilder()
                .setSteps(0, live.getSteps(0).toBuilder()
                        .setGrpcCall(live.getSteps(0).getGrpcCall().toBuilder()
                                .clearEdge()
                                .clearEdgeCardinality()
                                .setCompletion(StepCompletion.STEP_COMPLETION_EXTERNAL)))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(allowLoopback),
                allowLoopback, unreadable, null, workerConfig("external-parks", 4, 1))) {
            String jobId = submitPipeline(submitter, schema, external, "hello");
            assertThat(worker.workOnce()).isTrue();

            // The step calls nothing, so an unreadable profile cannot hold the run back.
            WorkflowRunRecord parked = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(parked.status).isEqualTo(WorkflowRunRecord.STATUS_WAITING);
            assertThat(parked.attempt).isEqualTo(1);
            assertThat(plainCalls.get()).isZero();
        }
    }

    @Test
    void aFalseGatedLiveStepIsSkippedWithoutReadingItsProfile() throws Exception {
        ServiceProfileRepository unreadable = profilesFailingFrom(1);
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
        OutboundChannelPolicy allowLoopback = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .build();
        Pipeline live = echoPipeline(schema, "echo", "plain");
        Pipeline gatedOff = live.toBuilder()
                .setSteps(0, live.getSteps(0).toBuilder().setWhen("input.message == 'never'"))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(allowLoopback),
                allowLoopback, unreadable, null, workerConfig("gated-off", 4, 1))) {
            String jobId = submitPipeline(submitter, schema, gatedOff, "hello");
            assertThat(worker.workOnce()).isTrue();

            // The step never calls, so its profile is never needed.
            WorkflowRunRecord done = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(done.status).as("error: %s", done.error)
                    .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            assertThat(done.attempt).isEqualTo(1);
            assertThat(plainCalls.get()).isZero();
        }
    }

    @Test
    void aCallAfterASkippedExternalStepStillTakesItsTargetPermit() throws Exception {
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch firstEntered = new CountDownLatch(1);
        plainInvokeHook.set(() -> {
            int current = inFlight.incrementAndGet();
            maxConcurrent.accumulateAndGet(current, Math::max);
            firstEntered.countDown();
            try {
                Thread.sleep(80);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inFlight.decrementAndGet();
            }
        });
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
        OutboundChannelPolicy allowLoopback = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .maxActiveChannels(4)
                .build();
        // The run's first step is an external step its gate skips; the call comes second.
        Pipeline live = echoPipeline(schema, "echo", "plain");
        Pipeline skipThenCall = live.toBuilder()
                .setSteps(0, live.getSteps(0).toBuilder()
                        .setName("approval")
                        .setWhen("input.message == 'never'")
                        .setGrpcCall(live.getSteps(0).getGrpcCall().toBuilder()
                                .clearEdge()
                                .clearEdgeCardinality()
                                .setCompletion(StepCompletion.STEP_COMPLETION_EXTERNAL)))
                .addSteps(live.getSteps(0))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(allowLoopback),
                allowLoopback, serviceProfiles, null, workerConfig("skip-then-call", 4, 1))) {
            List<String> jobs = List.of(
                    submitPipeline(submitter, schema, skipThenCall, "one"),
                    submitPipeline(submitter, schema, skipThenCall, "two"),
                    submitPipeline(submitter, schema, skipThenCall, "three"));
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<Boolean> first = pool.submit(worker::workOnce);
                assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
                Future<Boolean> second = pool.submit(worker::workOnce);
                Future<Boolean> third = pool.submit(worker::workOnce);
                assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
                assertThat(second.get(5, TimeUnit.SECONDS)).isTrue();
                assertThat(third.get(5, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(maxConcurrent.get()).isEqualTo(1);
            for (String id : jobs) {
                WorkflowRunRecord done = store.get(UUID.fromString(id)).orElseThrow();
                assertThat(done.status).as("error: %s", done.error)
                        .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            }
        }
    }

    @Test
    void timeSpentOpeningTheChannelCountsAgainstTheStepDeadline() throws Exception {
        OutboundChannelPolicy allowLoopback = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .build();
        ChannelFactory standard = ChannelFactory.standard(allowLoopback);
        // Opening the channel takes longer than the whole pipeline deadline.
        ChannelFactory slowToOpen = new ChannelFactory() {
            @Override
            public ManagedChannel open(String target, boolean tls) {
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return standard.open(target, tls);
            }

            @Override
            public void validateTarget(String target, boolean tls) {
                standard.validateTarget(target, tls);
            }

            @Override
            public void validateDeadline(long deadlineMillis) {
                standard.validateDeadline(deadlineMillis);
            }

            @Override
            public OutboundChannelPolicy policy() {
                return allowLoopback;
            }
        };
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(allowLoopback),
                slowToOpen, serviceProfiles, null, WorkflowRunWorker.EndpointCredentials.NONE,
                workerConfig("slow-channel", 4, 1))) {
            Pipeline oneSecond = echoPipeline(schema, "echo", "plain").toBuilder()
                    .setDeadline(com.google.protobuf.Duration.newBuilder().setSeconds(1))
                    .build();
            String jobId = submitPipeline(submitter, schema, oneSecond, "late");
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord after = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(plainCalls.get()).as("no call after the deadline has passed").isZero();
            assertThat(after.status).as("error: %s", after.error)
                    .isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        }
    }

    @Test
    void workerShutdownDuringACallLeavesTheRunForRecovery() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch releaseServer = new CountDownLatch(1);
        plainInvokeHook.set(() -> {
            entered.countDown();
            try {
                releaseServer.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
        OutboundChannelPolicy allowLoopback = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .build();
        WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(allowLoopback),
                allowLoopback, serviceProfiles, null, workerConfig("shutting-down", 4, 1));
        try {
            String jobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain"), "hello");
            worker.start();
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

            worker.close();

            // The call was cut short by this worker stopping, not refused by the service:
            // the run keeps its claim and is recovered when the lease expires.
            WorkflowRunRecord interrupted = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(interrupted.status).as("error: %s", interrupted.error)
                    .isEqualTo(WorkflowRunRecord.STATUS_RUNNING);
            assertThat(interrupted.error).isNull();
        } finally {
            releaseServer.countDown();
            worker.close();
        }
    }

    @Test
    void profileReadFailureAtCallTimeRequeuesTheRunAndTheRetrySucceeds() throws Exception {
        // The profile is read once per call; the first call's read fails.
        ServiceProfileRepository flaky = profilesFailingBetween(1, 1);
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
        OutboundChannelPolicy allowLoopback = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(allowLoopback),
                allowLoopback, flaky, null, workerConfig("flaky-call-read", 4, 1))) {
            String jobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain"), "hello");
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord requeued = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(requeued.status).as("error: %s", requeued.error)
                    .isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
            assertThat(plainCalls.get()).isZero();

            requeued.runAfter = java.time.Instant.now();
            assertThat(worker.workOnce()).isTrue();
            assertThat(store.get(UUID.fromString(jobId)).orElseThrow().status)
                    .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            assertThat(plainCalls.get()).isEqualTo(1);
        }
    }

    @Test
    void timeSpentResolvingTheProfileCountsAgainstTheStepDeadline() throws Exception {
        // The call's profile read takes longer than the whole pipeline deadline.
        AtomicInteger reads = new AtomicInteger();
        ServiceProfileRepository slow = new ServiceProfileRepository() {
            @Override
            public Optional<ServiceProfile> find(String name) throws IOException {
                if (reads.incrementAndGet() == 1) {
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return serviceProfiles.find(name);
            }

            @Override
            public List<ServiceProfile> list() throws IOException {
                return serviceProfiles.list();
            }

            @Override
            public void save(ServiceProfile profile) throws IOException {
                serviceProfiles.save(profile);
            }

            @Override
            public Optional<DescriptorArtifact> findDescriptorArtifact(String fingerprint)
                    throws IOException {
                return serviceProfiles.findDescriptorArtifact(fingerprint);
            }

            @Override
            public void saveDescriptorArtifact(DescriptorArtifact artifact) throws IOException {
                serviceProfiles.saveDescriptorArtifact(artifact);
            }
        };
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
        OutboundChannelPolicy allowLoopback = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(allowLoopback),
                allowLoopback, slow, null, workerConfig("slow-profile", 4, 1))) {
            Pipeline oneSecond = echoPipeline(schema, "echo", "plain").toBuilder()
                    .setDeadline(com.google.protobuf.Duration.newBuilder().setSeconds(1))
                    .build();
            String jobId = submitPipeline(submitter, schema, oneSecond, "late");
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord after = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(plainCalls.get()).as("no call after the deadline has passed").isZero();
            assertThat(after.status).as("error: %s", after.error)
                    .isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        }
    }

    /** The fixture's profiles, with reads from {@code firstFailingRead} on failing. */
    private ServiceProfileRepository profilesFailingFrom(int firstFailingRead) {
        return profilesFailingBetween(firstFailingRead, Integer.MAX_VALUE);
    }

    /** The fixture's profiles, with {@code find} failing on the given 1-based reads. */
    private ServiceProfileRepository profilesFailingBetween(int firstFailingRead,
                                                            int lastFailingRead) {
        AtomicInteger reads = new AtomicInteger();
        return new ServiceProfileRepository() {
            @Override
            public Optional<ServiceProfile> find(String name) throws IOException {
                int read = reads.incrementAndGet();
                if (read >= firstFailingRead && read <= lastFailingRead) {
                    throw new IOException("profile store unreachable");
                }
                return serviceProfiles.find(name);
            }

            @Override
            public List<ServiceProfile> list() throws IOException {
                return serviceProfiles.list();
            }

            @Override
            public void save(ServiceProfile profile) throws IOException {
                serviceProfiles.save(profile);
            }

            @Override
            public Optional<DescriptorArtifact> findDescriptorArtifact(String fingerprint)
                    throws IOException {
                return serviceProfiles.findDescriptorArtifact(fingerprint);
            }

            @Override
            public void saveDescriptorArtifact(DescriptorArtifact artifact) throws IOException {
                serviceProfiles.saveDescriptorArtifact(artifact);
            }
        };
    }

    @Test
    void distinctAliasesTargetingSameResolvedEndpointShareTargetConcurrencyLimit()
            throws Exception {
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch firstEntered = new CountDownLatch(1);
        plainInvokeHook.set(() -> {
            int current = inFlight.incrementAndGet();
            maxConcurrent.accumulateAndGet(current, Math::max);
            firstEntered.countDown();
            try {
                Thread.sleep(80);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inFlight.decrementAndGet();
            }
        });

        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
        OutboundChannelPolicy allowLoopback = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .maxActiveChannels(4)
                .build();

        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(allowLoopback),
                allowLoopback, serviceProfiles, null, workerConfig("shared-cap", 4, 1))) {
            // "plain" and "plain-mirror" are two endpoint names for one host and port.
            String firstJob = submitPipeline(
                    submitter, schema, echoPipeline(schema, "alias_alpha", "plain"), "one");
            String secondJob = submitPipeline(
                    submitter, schema, echoPipeline(schema, "alias_beta", "plain-mirror"), "two");
            String thirdJob = submitPipeline(
                    submitter, schema, echoPipeline(schema, "alias_gamma", "plain"), "three");

            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<Boolean> first = pool.submit(worker::workOnce);
                assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
                Future<Boolean> second = pool.submit(worker::workOnce);
                Future<Boolean> third = pool.submit(worker::workOnce);
                assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
                assertThat(second.get(5, TimeUnit.SECONDS)).isTrue();
                assertThat(third.get(5, TimeUnit.SECONDS)).isTrue();
            }

            assertThat(maxConcurrent.get()).isEqualTo(1);
            for (String id : new String[] {firstJob, secondJob, thirdJob}) {
                assertThat(store.get(UUID.fromString(id)).orElseThrow().status)
                        .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            }
        }
    }

    @Test
    void defaultWorkerRefusesUnconfiguredWorkspaceAndApprovalRequiredMethodPolicy()
            throws Exception {
        AtomicInteger inProcessCalls = new AtomicInteger();
        String inProcessName = InProcessServerBuilder.generateName();
        Server inProcessServer = InProcessServerBuilder.forName(inProcessName)
                .directExecutor()
                .addService(echoService(schema, inProcessCalls, null, null))
                .build()
                .start();
        try {
            InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
            WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
            try (WorkflowRunWorker worker = new WorkflowRunWorker(
                    store, context, null, new WorkflowRunner(),
                    workerConfig("default-worker", 4, 4))) {
                String smuggledJobId = submitPipeline(submitter, schema,
                        echoPipeline(schema, "echo", "in-process." + inProcessName), "smuggle");
                assertThat(worker.workOnce()).isTrue();
                WorkflowRunRecord failed =
                        store.get(UUID.fromString(smuggledJobId)).orElseThrow();
                assertThat(failed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
                assertThat(failed.error).contains("service profile repository is not configured");
                assertThat(inProcessCalls.get()).isZero();
            }
        } finally {
            inProcessServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }

        ServiceProfile base = serviceProfiles.find(PROFILE_NAME).orElseThrow();
        serviceProfiles.save(base.toBuilder()
                .addMethodPolicies(MethodPolicy.newBuilder()
                        .setMethod("jobs.pipeline.policy.EchoService/Echo")
                        .addOperation(Operation.OPERATION_APPROVAL_REQUIRED)
                        .setApprovalRequired(true)
                        .build())
                .build());
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
        OutboundChannelPolicy allowLoopback = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(allowLoopback),
                allowLoopback, serviceProfiles, null, workerConfig("approval-required", 4, 4))) {
            String jobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "echo", "plain"), "hello");
            assertThat(worker.workOnce()).isTrue();
            WorkflowRunRecord failed = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(failed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(failed.error).contains("requires approval");
            assertThat(plainCalls.get()).isZero();
        }
    }
}
