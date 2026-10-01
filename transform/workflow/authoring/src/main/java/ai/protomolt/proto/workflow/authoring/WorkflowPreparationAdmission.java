package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.delegation.DelegationReducer;
import ai.protomolt.proto.delegation.DelegationValidation;
import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationBinding;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Read-only admission snapshot; the handler rechecks authority before each effect. */
final class WorkflowPreparationAdmission {
    private static final int MAX_TRANSCRIPT_BYTES = 8 * 1024 * 1024;

    enum Mode { EXECUTE, REPLAY_COMPLETED }

    record Snapshot(TranscriptEntry selectedOffer, WorkflowPreparationBinding binding,
                    Timestamp leaseExpiry) {}

    private WorkflowPreparationAdmission() {}

    static Snapshot inspect(TranscriptRepository repository, PrepareWorkflowCandidateRequest request,
            Caller caller, Clock clock, Mode mode, ArtifactReference configuredPolicy) {
        Objects.requireNonNull(repository, "repository");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(configuredPolicy, "configuredPolicy");
        validateNative(request);
        validateNative(configuredPolicy);
        if (configuredPolicy.getRedacted()
                || !"application/x-protobuf".equals(configuredPolicy.getMediaType())
                || Long.compareUnsigned(configuredPolicy.getSizeBytes(), 4L * 1024 * 1024) > 0) {
            throw invalid("configured policy must be a bounded unredacted protobuf artifact");
        }

        var transcript = repository.load().orElseThrow(() -> invalid("no durable transcript"));
        if (transcript.getSerializedSize() > MAX_TRANSCRIPT_BYTES) {
            throw invalid("durable transcript exceeds preparation bound");
        }
        rejectUnknown(transcript);
        DelegationValidation.validate(transcript);
        validateNative(transcript);
        var reduced = new DelegationReducer().reduce(transcript);
        if (!reduced.clean()) throw invalid("durable transcript has lifecycle findings");
        var state = reduced.tasks().get(request.getTaskId());
        if (state == null || state.attempt() != request.getAttempt()) {
            throw invalid("preparation task or attempt is not current");
        }
        if (!state.holder().equals(caller.name())) {
            throw new SecurityException("preparation caller is not the lease holder");
        }
        boolean candidateReplay = mode == Mode.REPLAY_COMPLETED
                && state.phase() == DelegationReducer.Phase.CANDIDATE
                && state.candidateRevision() == request.getRevision();
        boolean leased = state.phase() == DelegationReducer.Phase.LEASED
                && state.candidateRevision() + 1 == request.getRevision();
        if (!leased && !candidateReplay) {
            throw invalid("preparation revision is not active");
        }

        TranscriptEntry selectedOffer = null;
        Timestamp leaseExpiry = null;
        Set<TranscriptEntry> seen = new HashSet<>();
        for (TranscriptEntry entry : transcript.getEntriesList()) {
            // The reducer permits exact frame retries; they cannot be a second
            // offer or roll a later lease renewal back to an older expiry.
            if (!seen.add(entry)) continue;
            if (!entry.hasCoordinatorFrame()) continue;
            var frame = entry.getCoordinatorFrame();
            if (!frame.getTaskId().equals(request.getTaskId())) continue;
            if (frame.hasOffer() && frame.getOffer().getAttempt() == state.attempt()) {
                if (selectedOffer != null) throw invalid("multiple offers for active attempt");
                if (!entry.getWorkerId().equals(state.holder())) {
                    throw invalid("selected offer is addressed to a different holder");
                }
                selectedOffer = entry;
                leaseExpiry = frame.getOffer().getExpiresAt();
            }
            if (frame.hasRenewal() && frame.getRenewal().getAttempt() == state.attempt()) {
                if (selectedOffer == null || !entry.getWorkerId().equals(state.holder())) {
                    throw invalid("renewal is not bound to selected offer and holder");
                }
                leaseExpiry = frame.getRenewal().getExpiresAt();
            }
        }
        if (selectedOffer == null || leaseExpiry == null) throw invalid("active offer is absent");
        var spec = selectedOffer.getCoordinatorFrame().getOffer().getSpec();
        if (!spec.hasContract() || !spec.getContract().getTypeName().equals(
                WorkflowAuthoringDeliverable.getDescriptor().getFullName())) {
            throw invalid("task does not offer the authoring contract");
        }
        Set<String> checks = new HashSet<>();
        for (var check : spec.getRequiredChecksList()) {
            if (!checks.add(check.getName())) throw invalid("duplicate authoring check");
        }
        if (spec.getRequiredChecksCount() != WorkflowAuthoringReviewer.REQUIRED_CHECKS.size()
                || !checks.equals(Set.copyOf(WorkflowAuthoringReviewer.REQUIRED_CHECKS))) {
            throw invalid("unsupported authoring checks");
        }
        if (!spec.getContextList().contains(configuredPolicy)) {
            throw invalid("configured policy is absent from selected offer");
        }
        var now = clock.instant();
        var nowTimestamp = Timestamp.newBuilder().setSeconds(now.getEpochSecond())
                .setNanos(now.getNano()).build();
        if (leased && Timestamps.compare(leaseExpiry, nowTimestamp) <= 0) {
            throw invalid("preparation lease has expired");
        }
        var binding = WorkflowPreparationBinding.newBuilder()
                .setTaskId(request.getTaskId()).setAttempt(request.getAttempt())
                .setRevision(request.getRevision()).setPreparationId(request.getPreparationId())
                .setOfferEntrySha256(WorkflowLaunchValidation.sha256(selectedOffer))
                .setSourceSha256(WorkRecords.sha256Hex(request.getExecutableSourceJson().toByteArray()))
                .build();
        validateNative(binding);
        return new Snapshot(selectedOffer, binding, leaseExpiry);
    }

    private static void validateNative(Message message) {
        rejectUnknown(message);
        var result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        if (!result.valid()) throw invalid("invalid preparation contract");
    }

    private static void rejectUnknown(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) throw invalid("unknown protocol fields");
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) rejectUnknown((Message) nested);
            } else rejectUnknown((Message) field.getValue());
        }
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }
}
