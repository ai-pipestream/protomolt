package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.contract.DelegationValidation;
import ai.protomolt.proto.delegation.contract.DeliverableContracts;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.descriptors.GoogleDescriptorLoader;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringTemplate;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.HashSet;

/** Reads the mounted policy and constructs the sole authoritative authoring starter. */
final class WorkflowAuthoringTemplates {
    private static final String PROTOBUF_MEDIA_TYPE = "application/x-protobuf";
    private static final String DEFAULT_OBJECTIVE =
            "Author and independently verify a workflow under the pinned policy.";

    private final ArtifactReference policyReference;
    private final ArtifactRepository artifacts;
    private final int leaseSeconds;

    WorkflowAuthoringTemplates(ArtifactReference policyReference, ArtifactRepository artifacts,
            int leaseSeconds) {
        this.policyReference = Objects.requireNonNull(policyReference);
        this.artifacts = Objects.requireNonNull(artifacts);
        if (leaseSeconds < 1 || leaseSeconds > 86_400) {
            throw new IllegalArgumentException("authoring lease must be 1 to 86400 seconds");
        }
        this.leaseSeconds = leaseSeconds;
    }

    WorkflowAuthoringTemplate load() {
        WorkflowAuthoringPolicy policy = policy();
        descriptors(policy.getDescriptors());
        Map<String, FileDescriptor> closure = new LinkedHashMap<>();
        collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), closure);
        var descriptorSet = FileDescriptorSet.newBuilder();
        closure.values().forEach(file -> descriptorSet.addFile(file.toProto()));
        var contract = DeliverableContract.newBuilder()
                .setTypeName(WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                .setDescriptorSet(descriptorSet.build().toByteString()).build();
        try {
            DeliverableContracts.compile(contract);
            var spec = TaskSpec.newBuilder().setObjective(DEFAULT_OBJECTIVE)
                    .addContext(policyReference).setContract(contract);
            WorkflowAuthoringReviewer.REQUIRED_CHECKS.forEach(name -> spec.addRequiredChecks(
                    AcceptanceCheck.newBuilder().setName(name)
                            .setDescription("Independently verify " + name.replace('_', ' ') + ".")));
            TaskSpec rendered = DeliverableContracts.rendered(spec.build());
            DelegationValidation.validate(rendered);
            var template = WorkflowAuthoringTemplate.newBuilder()
                    .setSpec(rendered).setLeaseSeconds(leaseSeconds).build();
            WorkflowLaunchValidation.validate(template);
            preflightFrame(rendered, leaseSeconds);
            return template;
        } catch (RuntimeException invalid) {
            throw failure(WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE,
                    "configured authoring template is invalid", invalid);
        }
    }

    private WorkflowAuthoringPolicy policy() {
        byte[] bytes = resolve(policyReference);
        try {
            var policy = WorkflowAuthoringPolicy.parseFrom(bytes);
            WorkflowLaunchValidation.validate(policy);
            return policy;
        } catch (Exception corrupt) {
            throw failure(WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE,
                    "configured authoring policy is invalid", corrupt);
        }
    }

    private void descriptors(ArtifactReference reference) {
        byte[] bytes = resolve(reference);
        try {
            var set = FileDescriptorSet.parseFrom(bytes);
            var names = new HashSet<String>();
            for (var file : set.getFileList()) {
                if (file.getName().isBlank() || !names.add(file.getName())) {
                    throw new IllegalArgumentException("policy descriptor filenames are missing or duplicated");
                }
            }
            for (var file : set.getFileList()) {
                for (String dependency : file.getDependencyList()) {
                    if (!names.contains(dependency)) {
                        throw new IllegalArgumentException("policy descriptor closure is incomplete");
                    }
                }
            }
            if (set.getFileCount() == 0 || GoogleDescriptorLoader.fromDescriptorSet(set).isEmpty()) {
                throw new IllegalArgumentException("policy descriptor closure is empty");
            }
        } catch (Exception corrupt) {
            throw failure(WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE,
                    "configured policy descriptor closure is invalid", corrupt);
        }
    }

    private byte[] resolve(ArtifactReference reference) {
        try {
            WorkflowLaunchValidation.validate(reference);
            WorkflowValidation.validate(reference);
            if (reference.getRedacted() || !PROTOBUF_MEDIA_TYPE.equals(reference.getMediaType())
                    || Long.compareUnsigned(reference.getSizeBytes(), WorkflowLaunchValidation.MAX_BYTES) > 0) {
                throw new IllegalArgumentException("policy artifact metadata is invalid");
            }
            var stored = artifacts.find(reference.getSha256()).orElseThrow(() ->
                    new IllegalArgumentException("pinned artifact is missing"));
            byte[] bytes = stored.content();
            if (!stored.reference().equals(reference) || bytes.length != reference.getSizeBytes()
                    || !WorkRecords.sha256Hex(bytes).equals(reference.getSha256())) {
                throw new IllegalArgumentException("pinned artifact differs from its reference");
            }
            return bytes;
        } catch (IOException unavailable) {
            throw failure(WorkflowAuthoringEntryCoordinator.deadline(unavailable)
                    ? WorkflowAuthoringEntryException.Kind.DEADLINE
                    : WorkflowAuthoringEntryException.Kind.UNAVAILABLE,
                    "authoring policy storage is unavailable", unavailable);
        } catch (StatusRuntimeException unavailable) {
            throw failure(WorkflowAuthoringEntryCoordinator.deadline(unavailable)
                    ? WorkflowAuthoringEntryException.Kind.DEADLINE
                    : WorkflowAuthoringEntryException.Kind.UNAVAILABLE,
                    "authoring policy transport is unavailable", unavailable);
        } catch (RuntimeException corrupt) {
            throw failure(WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE,
                    "configured authoring artifact is invalid", corrupt);
        }
    }

    /** Reserve the maximum wire width of any valid start objective and timestamps. */
    static void preflightFrame(TaskSpec spec, int leaseSeconds) {
        TaskSpec widestSpec = spec.toBuilder().setObjective("\uD83D\uDE00".repeat(4096)).build();
        Timestamp widestTimestamp = Timestamp.newBuilder()
                .setSeconds(-62_135_596_800L).setNanos(999_999_999).build();
        TaskOffer offer = TaskOffer.newBuilder().setAttempt(1).setSpec(widestSpec)
                .setLeaseDuration(Duration.newBuilder().setSeconds(leaseSeconds))
                .setExpiresAt(widestTimestamp)
                .setStartBindingSha256("f".repeat(64)).build();
        var frame = DelegateResponse.newBuilder()
                .setFrameId("ffffffff-ffff-4fff-8fff-ffffffffffff")
                .setTaskId("ffffffff-ffff-4fff-8fff-ffffffffffff")
                .setSeq(Long.MAX_VALUE)
                .setSentAt(widestTimestamp)
                .setOffer(offer).build();
        DelegationValidation.validate(frame);
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, files));
        files.put(file.getName(), file);
    }

    private static WorkflowAuthoringEntryException failure(WorkflowAuthoringEntryException.Kind kind,
            String message, Throwable cause) {
        return new WorkflowAuthoringEntryException(kind, message, cause);
    }
}
