package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.AdmissionPolicy;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.contract.DelegationValidation;
import ai.protomolt.proto.delegation.lifecycle.InMemoryTranscriptRepository;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateRequest;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringRequest;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthoringEntryCoordinatorTest {
    private static final String WORKER = "scripted-author";

    @TempDir Path temp;

    @Test
    void templateAndFirstStartPinPolicyThenReplayWithoutCurrentPolicyOrWorker() throws Exception {
        var base = new FileSystemArtifactRepository(temp.resolve("artifacts"));
        ArtifactReference policy = policy(base);
        var outage = new AtomicBoolean();
        var reads = new AtomicInteger();
        ArtifactRepository artifacts = new ArtifactRepository() {
            public ArtifactReference save(byte[] content, String mediaType, boolean redacted)
                    throws IOException { return base.save(content, mediaType, redacted); }
            public java.util.Optional<StoredArtifact> find(String sha256) throws IOException {
                reads.incrementAndGet();
                if (outage.get()) throw new IOException("injected outage");
                return base.find(sha256);
            }
        };
        var transcript = new InMemoryTranscriptRepository();
        try (var coordinator = new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(),
                CandidateReviewer.manual(), Clock.systemUTC(), transcript);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello());
            var entry = new WorkflowAuthoringEntryCoordinator(bridge, policy, artifacts, 240);
            var template = entry.template(GetWorkflowAuthoringTemplateRequest.getDefaultInstance());
            assertThat(template.getTemplate().getSpec().getContextList()).containsExactly(policy);
            assertThat(template.getTemplate().getSpec().getRequiredChecksList())
                    .extracting(check -> check.getName())
                    .containsExactlyElementsOf(WorkflowAuthoringReviewer.REQUIRED_CHECKS);
            assertThat(template.getTemplate().getSpec().getContract().getTypeName())
                    .isEqualTo(WorkflowAuthoringDeliverable.getDescriptor().getFullName());
            assertThat(template.getTemplate().getSpec().getContract().getJsonSchema()).isNotBlank();
            assertThat(template.getTemplateSha256())
                    .isEqualTo(WorkflowAuthoringStartBinding.templateSha256(template.getTemplate()));

            var request = StartWorkflowAuthoringRequest.newBuilder()
                    .setTaskId(UUID.randomUUID().toString()).setWorkerId(WORKER)
                    .setTemplateSha256(template.getTemplateSha256())
                    .setObjective("Author the policy-pinned workflow.").build();
            var first = entry.start(request);
            assertThat(first.getRequest()).isEqualTo(request);
            assertThat(first.getOffer().getAttempt()).isEqualTo(1);
            assertThat(first.getOffer().getSpec().getContextList()).containsExactly(policy);
            assertThat(first.getOffer().getStartBindingSha256())
                    .isEqualTo(WorkflowAuthoringStartBinding.sha256(request,
                            first.getOffer().getSpec(), first.getOffer().getLeaseDuration()));
            int afterStart = coordinator.transcript().getEntriesCount();
            int afterReads = reads.get();
            outage.set(true);
            assertThat(entry.start(request)).isEqualTo(first);
            assertThat(reads.get()).isEqualTo(afterReads);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(afterStart);
            assertThatThrownBy(() -> entry.start(request.toBuilder().setObjective("Changed").build()))
                    .isInstanceOfSatisfying(WorkflowAuthoringEntryException.class, failure ->
                            assertThat(failure.kind()).isEqualTo(WorkflowAuthoringEntryException.Kind.CONFLICT));
            assertThatThrownBy(() -> entry.template(GetWorkflowAuthoringTemplateRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(WorkflowAuthoringEntryException.class, failure ->
                            assertThat(failure.kind()).isEqualTo(WorkflowAuthoringEntryException.Kind.UNAVAILABLE));
        }
    }

    @Test
    void corruptStoredOfferFailsReplayWithoutAppendingOrReloadingPolicy() throws Exception {
        var artifacts = new FileSystemArtifactRepository(temp.resolve("artifacts"));
        var policy = policy(artifacts);
        var repository = new InMemoryTranscriptRepository();
        StartWorkflowAuthoringRequest request;
        try (var coordinator = new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(),
                CandidateReviewer.manual(), Clock.systemUTC(), repository);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello());
            var entry = new WorkflowAuthoringEntryCoordinator(bridge, policy, artifacts, 240);
            var template = entry.template(GetWorkflowAuthoringTemplateRequest.getDefaultInstance());
            request = StartWorkflowAuthoringRequest.newBuilder()
                    .setTaskId(UUID.randomUUID().toString()).setWorkerId(WORKER)
                    .setTemplateSha256(template.getTemplateSha256()).setObjective("Author a workflow.").build();
            entry.start(request);
        }
        var corrupted = repository.load().orElseThrow().toBuilder();
        for (var entry : corrupted.getEntriesBuilderList()) {
            if (entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasOffer()) {
                entry.getCoordinatorFrameBuilder().getOfferBuilder().setUnknownFields(
                        UnknownFieldSet.newBuilder().addField(100,
                                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build());
            }
        }
        var restored = new InMemoryTranscriptRepository();
        restored.save(corrupted.build());
        ArtifactRepository unavailable = new ArtifactRepository() {
            public ArtifactReference save(byte[] content, String mediaType, boolean redacted) {
                throw new AssertionError("replay must not write artifacts");
            }
            public java.util.Optional<StoredArtifact> find(String sha256) {
                throw new AssertionError("replay must not reload policy");
            }
        };
        try (var coordinator = new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(),
                CandidateReviewer.manual(), Clock.systemUTC(), restored);
             var bridge = new DelegationBridge(coordinator)) {
            var entry = new WorkflowAuthoringEntryCoordinator(bridge, policy, unavailable, 240);
            var before = coordinator.transcript();
            assertThatThrownBy(() -> entry.start(request))
                    .isInstanceOfSatisfying(WorkflowAuthoringEntryException.class, failure ->
                            assertThat(failure.kind()).isEqualTo(WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE));
            assertThat(coordinator.transcript()).isEqualTo(before);
        }
    }

    @Test
    void staleTemplateInvalidShapeAndCorruptPolicyFailBeforeOffer() throws Exception {
        var artifacts = new FileSystemArtifactRepository(temp.resolve("artifacts"));
        ArtifactReference policy = policy(artifacts);
        var transcript = new InMemoryTranscriptRepository();
        try (var coordinator = new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(),
                CandidateReviewer.manual(), Clock.systemUTC(), transcript);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello());
            var entry = new WorkflowAuthoringEntryCoordinator(bridge, policy, artifacts, 240);
            var start = StartWorkflowAuthoringRequest.newBuilder()
                    .setTaskId(UUID.randomUUID().toString()).setWorkerId(WORKER)
                    .setTemplateSha256("a".repeat(64)).setObjective("Author a workflow.").build();
            int before = coordinator.transcript().getEntriesCount();
            assertThatThrownBy(() -> entry.start(start))
                    .isInstanceOfSatisfying(WorkflowAuthoringEntryException.class, failure ->
                            assertThat(failure.kind()).isEqualTo(WorkflowAuthoringEntryException.Kind.INACTIVE));
            assertThatThrownBy(() -> entry.start(start.toBuilder().setTaskId("invalid").build()))
                    .isInstanceOfSatisfying(WorkflowAuthoringEntryException.class, failure ->
                            assertThat(failure.kind()).isEqualTo(WorkflowAuthoringEntryException.Kind.INVALID_INPUT));
            var unknown = GetWorkflowAuthoringTemplateRequest.newBuilder()
                    .setUnknownFields(UnknownFieldSet.newBuilder().addField(100,
                            UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
            assertThatThrownBy(() -> entry.template(unknown))
                    .isInstanceOfSatisfying(WorkflowAuthoringEntryException.class, failure ->
                            assertThat(failure.kind()).isEqualTo(WorkflowAuthoringEntryException.Kind.INVALID_INPUT));
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(before);

            var wrong = policy.toBuilder().setSha256("b".repeat(64)).build();
            var corrupt = new WorkflowAuthoringEntryCoordinator(bridge, wrong, artifacts, 240);
            assertThatThrownBy(() -> corrupt.template(GetWorkflowAuthoringTemplateRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(WorkflowAuthoringEntryException.class, failure ->
                            assertThat(failure.kind()).isEqualTo(WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE));
        }
    }

    @Test
    void framePreflightReservesMaximumValidObjectiveWidth() {
        var base = TaskSpec.newBuilder().setObjective("default");
        for (int index = 0; index < 256; index++) {
            base.addConstraints("x".repeat(2048)).addAllowedScope("x".repeat(512));
        }
        for (int index = 0; index < 64; index++) {
            base.addRequiredChecks(AcceptanceCheck.newBuilder().setName("check-" + index)
                    .setDescription("x".repeat(2048)));
        }
        var contract = DeliverableContract.newBuilder().setDescriptorSet(ByteString.copyFromUtf8("x"))
                .setTypeName("test.Result");
        base.setContract(contract);
        int schemaLength = DelegationValidation.MAX_FRAME_BYTES - 10_000 - base.build().getSerializedSize();
        assertThat(schemaLength).isBetween(1, 262_144);
        base.setContract(contract.setJsonSchema("x".repeat(schemaLength)));
        TaskSpec nearLimit = base.build();
        assertThat(nearLimit.getSerializedSize()).isLessThan(DelegationValidation.MAX_FRAME_BYTES);
        assertThatThrownBy(() -> WorkflowAuthoringTemplates.preflightFrame(nearLimit, 240))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("frame exceeds");
    }

    @Test
    void policyDescriptorClosureRejectsMissingWellKnownImportAndDuplicateFilename() throws Exception {
        var artifacts = new FileSystemArtifactRepository(temp.resolve("artifacts"));
        Map<String, FileDescriptor> closure = new LinkedHashMap<>();
        collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), closure);
        String missingName = closure.values().stream().flatMap(file -> file.getDependencies().stream())
                .map(FileDescriptor::getName).filter(name -> name.startsWith("google/protobuf/"))
                .findFirst().orElseThrow();
        var missing = FileDescriptorSet.newBuilder();
        var duplicated = FileDescriptorSet.newBuilder();
        closure.values().forEach(file -> {
            if (!file.getName().equals(missingName)) missing.addFile(file.toProto());
            duplicated.addFile(file.toProto());
        });
        duplicated.addFile(closure.values().iterator().next().toProto());
        var missingImport = missing.build();
        var duplicate = duplicated.build();
        var transcript = new InMemoryTranscriptRepository();
        try (var coordinator = new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(),
                CandidateReviewer.manual(), Clock.systemUTC(), transcript);
             var bridge = new DelegationBridge(coordinator)) {
            for (var set : java.util.List.of(missingImport, duplicate)) {
                var entry = new WorkflowAuthoringEntryCoordinator(bridge,
                        policy(artifacts, set.toByteArray()), artifacts, 240);
                assertThatThrownBy(() -> entry.template(GetWorkflowAuthoringTemplateRequest.getDefaultInstance()))
                        .isInstanceOfSatisfying(WorkflowAuthoringEntryException.class, failure ->
                                assertThat(failure.kind())
                                        .isEqualTo(WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE));
            }
        }
    }

    private static ArtifactReference policy(FileSystemArtifactRepository artifacts) throws IOException {
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), files);
        var set = FileDescriptorSet.newBuilder();
        files.values().forEach(file -> set.addFile(file.toProto()));
        return policy(artifacts, set.build().toByteArray());
    }

    private static ArtifactReference policy(FileSystemArtifactRepository artifacts,
            byte[] descriptorBytes) throws IOException {
        var descriptors = artifacts.save(descriptorBytes, "application/x-protobuf", false);
        var fixture = WorkflowAcceptanceFixture.newBuilder().setName("starter")
                .setInput(artifacts.save(new byte[] {1}, "application/x-protobuf", false))
                .setExpectedOutput(artifacts.save(new byte[] {2}, "application/x-protobuf", false)).build();
        var value = WorkflowAuthoringPolicy.newBuilder().setDescriptors(descriptors)
                .addFixtures(fixture).addPermittedCalls(WorkflowPermittedCall.newBuilder()
                        .setTarget("fixture:9090")
                        .setMethod("workflow.test.Fixture/Call")).build();
        return artifacts.save(value.toByteArray(), "application/x-protobuf", false);
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, files));
        files.put(file.getName(), file);
    }

    private static WorkerHello hello() {
        return WorkerHello.newBuilder().setWorkerId(WORKER).setProtocolVersion(1)
                .setProvider("scripted").addCapabilities(
                        WorkerCapability.newBuilder().setName("workflow-authoring")).build();
    }
}
