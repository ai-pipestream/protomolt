package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.SchemaResolver;
import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;
import ai.protomolt.proto.delegation.contract.DeliverableContracts;
import ai.protomolt.proto.delegation.contract.DelegationValidation;
import ai.protomolt.proto.delegation.lifecycle.TranscriptRepository;
import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.ObservedEvent;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextResponse;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsRequest;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsResponse;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorAssignmentsRequest;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorAssignmentsResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorAssignment;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Base64;

/** Trusted, bounded reads for one authenticated workflow author and task attempt. */
public final class WorkflowAuthorTaskReader {
    private static final int TRANSCRIPT_MAX = 8 * 1024 * 1024;
    private static final int ARTIFACT_MAX = 4 * 1024 * 1024;
    private static final int RESPONSE_MAX = 16 * 1024 * 1024;
    private static final int SCAN_MAX = 256;
    private static final int ASSIGNMENTS_MAX = 64 * 1024;

    private final TranscriptRepository transcripts;
    private final ArtifactRepository artifacts;
    private final ArtifactReference policyReference;
    private final Clock clock;

    public WorkflowAuthorTaskReader(TranscriptRepository transcripts, ArtifactRepository artifacts,
            ArtifactReference policyReference, Clock clock) {
        this.transcripts = Objects.requireNonNull(transcripts);
        this.artifacts = Objects.requireNonNull(artifacts);
        this.policyReference = Objects.requireNonNull(policyReference);
        this.clock = Objects.requireNonNull(clock);
    }

    public GetWorkflowAuthorContextResponse context(GetWorkflowAuthorContextRequest request,
            Caller caller) throws WorkflowPreparationException {
        validateRequest(request);
        requireCaller(caller);
        Transcript transcript = trustedTranscript();
        var reduced = new DelegationReducer().reduce(transcript);
        var state = reduced.tasks().get(request.getTaskId());
        if (state == null || state.attempt() != request.getAttempt()) {
            throw failure(WorkflowPreparationException.Kind.INACTIVE, "author task is not current");
        }
        TranscriptEntry offer = offer(transcript, request.getTaskId(), request.getAttempt());
        if (!offer.getWorkerId().equals(caller.name())) {
            throw failure(WorkflowPreparationException.Kind.PERMISSION_DENIED, "author task belongs to another worker");
        }
        if (state.phase() != DelegationReducer.Phase.OFFERED
                && state.phase() != DelegationReducer.Phase.LEASED) {
            throw failure(WorkflowPreparationException.Kind.INACTIVE, "author task is not offered or leased");
        }
        if (state.phase() == DelegationReducer.Phase.LEASED && !state.holder().equals(caller.name())) {
            throw failure(WorkflowPreparationException.Kind.PERMISSION_DENIED, "author is not lease holder");
        }
        requireSupportedOffer(offer);
        Timestamp expiry = expiry(transcript, offer, request.getTaskId(), request.getAttempt());
        var now = clock.instant();
        Timestamp current = Timestamp.newBuilder().setSeconds(now.getEpochSecond())
                .setNanos(now.getNano()).build();
        if (Timestamps.compare(expiry, current) <= 0) {
            throw failure(WorkflowPreparationException.Kind.INACTIVE, "author offer has expired");
        }
        try {
            WorkflowAuthoringPolicy policy = WorkflowAuthoringPolicy.parseFrom(
                    resolve(policyReference, "policy"));
            validate(policy, ARTIFACT_MAX);
            ArtifactReference descriptors = policy.getDescriptors();
            byte[] descriptorBytes = resolve(descriptors, "descriptors");
            requireCompleteImports(descriptorBytes);
            var response = GetWorkflowAuthorContextResponse.newBuilder()
                    .setTaskId(request.getTaskId()).setAttempt(request.getAttempt())
                    .setOfferEntry(offer).setOfferEntrySha256(WorkflowLaunchValidation.sha256(offer))
                    .setPolicy(policyReference).setDescriptors(descriptors)
                    .setDescriptorSet(com.google.protobuf.ByteString.copyFrom(descriptorBytes))
                    .addAllPermittedCalls(policy.getPermittedCallsList()).build();
            validate(response, RESPONSE_MAX);
            return response;
        } catch (InvalidProtocolBufferException malformed) {
            throw failure(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "author policy or descriptors are malformed", malformed);
        } catch (IOException storage) {
            throw failure(WorkflowPreparationException.Kind.UNAVAILABLE, "author artifacts are unavailable", storage);
        } catch (IllegalArgumentException invalid) {
            throw failure(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE, "author policy or descriptors are invalid", invalid);
        }
    }

    public ReadWorkflowAuthorEventsResponse events(ReadWorkflowAuthorEventsRequest request,
            Caller caller) throws WorkflowPreparationException {
        validateRequest(request);
        requireCaller(caller);
        Transcript transcript = trustedTranscript();
        TranscriptEntry selected = offer(transcript, request.getTaskId(), request.getAttempt());
        if (!selected.getWorkerId().equals(caller.name())) {
            throw failure(WorkflowPreparationException.Kind.PERMISSION_DENIED, "author task belongs to another worker");
        }
        requireSupportedOffer(selected);
        List<TranscriptEntry> entries = transcript.getEntriesList();
        long after = request.getAfterCursor();
        if (after > entries.size()) {
            throw failure(WorkflowPreparationException.Kind.INVALID_INPUT, "author event cursor is ahead of transcript");
        }
        var response = ReadWorkflowAuthorEventsResponse.newBuilder()
                .setTaskId(request.getTaskId()).setAttempt(request.getAttempt())
                .setAfterCursor(after).setCursor(after);
        int examined = 0;
        for (long position = after; position < entries.size() && examined < SCAN_MAX; position++) {
            TranscriptEntry entry = entries.get((int) position);
            long cursor = position + 1;
            if (entry.getWorkerId().equals(caller.name())
                    && request.getTaskId().equals(taskId(entry))
                    && attempt(entry) == request.getAttempt()) {
                validateReturnedAny(entry);
                var event = ObservedEvent.newBuilder().setCursor(cursor)
                        .setWorkerId(entry.getWorkerId()).setTaskId(request.getTaskId())
                        .setLane(entry.getLane()).setEntry(entry).build();
                var candidate = response.clone().setCursor(cursor).addEvents(event).build();
                if (response.getEventsCount() == request.getMaxEvents()
                        || candidate.getSerializedSize() > RESPONSE_MAX) {
                    break; // Preserve this authorized event for the next page.
                }
                response.addEvents(event);
            }
            response.setCursor(cursor);
            examined++;
        }
        response.setTruncated(response.getCursor() < entries.size());
        ReadWorkflowAuthorEventsResponse result = response.build();
        try {
            validate(result, RESPONSE_MAX);
        } catch (IllegalArgumentException invalid) {
            throw failure(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE, "author event projection is invalid", invalid);
        }
        return result;
    }

    /** Historical, caller-owned offers; no current lease or policy lookup is involved. */
    public ReadWorkflowAuthorAssignmentsResponse assignments(
            ReadWorkflowAuthorAssignmentsRequest request, Caller caller) throws WorkflowPreparationException {
        validateRequest(request);
        requireCaller(caller);
        Transcript transcript = trustedTranscript(true);
        List<TranscriptEntry> entries = transcript.getEntriesList();
        long after = request.getAfterCursor();
        if (after > entries.size()) {
            throw failure(WorkflowPreparationException.Kind.INVALID_INPUT,
                    "author assignment cursor is ahead of transcript");
        }
        var response = ReadWorkflowAuthorAssignmentsResponse.newBuilder()
                .setWorkerId(caller.name()).setAfterCursor(after).setCursor(after);
        Set<TranscriptEntry> seenOffers = new HashSet<>();
        // Whole-transcript validation is bounded separately (8 MiB). Index prior
        // offers so a duplicate identical frame after this cursor is not another
        // assignment; SCAN_MAX limits only new cursor positions in this page.
        for (int index = 0; index < after; index++) {
            TranscriptEntry entry = entries.get(index);
            if (entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasOffer()) seenOffers.add(entry);
        }
        int examined = 0;
        for (long position = after; position < entries.size() && examined < SCAN_MAX; position++) {
            TranscriptEntry entry = entries.get((int) position);
            long cursor = position + 1;
            if (entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasOffer()
                    && seenOffers.add(entry) && entry.getWorkerId().equals(caller.name())
                    && supportedAssignment(entry)) {
                var assignment = WorkflowAuthorAssignment.newBuilder().setCursor(cursor)
                        .setTaskId(entry.getCoordinatorFrame().getTaskId())
                        .setAttempt(entry.getCoordinatorFrame().getOffer().getAttempt())
                        .setOfferEntrySha256(WorkflowLaunchValidation.sha256(entry)).build();
                var candidate = response.clone().setCursor(cursor).addAssignments(assignment).build();
                if (response.getAssignmentsCount() == request.getMaxAssignments()
                        || candidate.getSerializedSize() > ASSIGNMENTS_MAX) {
                    break; // Never advance over an eligible offer omitted from this page.
                }
                response.addAssignments(assignment);
            }
            response.setCursor(cursor);
            examined++;
        }
        response.setTruncated(response.getCursor() < entries.size());
        var result = response.build();
        try {
            validate(result, ASSIGNMENTS_MAX);
        } catch (IllegalArgumentException invalid) {
            throw failure(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "author assignment projection is invalid", invalid);
        }
        return result;
    }

    private static boolean supportedAssignment(TranscriptEntry entry) throws WorkflowPreparationException {
        var spec = entry.getCoordinatorFrame().getOffer().getSpec();
        if (!spec.hasContract() || !spec.getContract().getTypeName().equals(
                WorkflowAuthoringDeliverable.getDescriptor().getFullName())) return false;
        Set<String> checks = new HashSet<>();
        spec.getRequiredChecksList().forEach(check -> checks.add(check.getName()));
        if (spec.getRequiredChecksCount() != WorkflowAuthoringReviewer.REQUIRED_CHECKS.size()
                || !checks.equals(Set.copyOf(WorkflowAuthoringReviewer.REQUIRED_CHECKS))
                || spec.getContextCount() == 0) {
            throw failure(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "historical authoring offer has invalid checks or policy context");
        }
        try {
            DeliverableContracts.compile(spec.getContract());
            requireCompleteImports(spec.getContract().getDescriptorSet().toByteArray());
        } catch (IOException | IllegalArgumentException invalid) {
            throw failure(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "historical authoring offer has invalid result descriptors", invalid);
        }
        return true;
    }

    private Transcript trustedTranscript() throws WorkflowPreparationException {
        return trustedTranscript(false);
    }

    private Transcript trustedTranscript(boolean allowMissing) throws WorkflowPreparationException {
        Transcript transcript;
        try {
            var loaded = transcripts.load();
            if (loaded.isEmpty() && !allowMissing) {
                throw failure(WorkflowPreparationException.Kind.INACTIVE,
                        "author task has no transcript");
            }
            transcript = loaded.orElse(Transcript.getDefaultInstance());
            validate(transcript, TRANSCRIPT_MAX);
            DelegationValidation.validate(transcript);
            if (!new DelegationReducer().reduce(transcript).clean()) {
                throw new IllegalArgumentException("transcript has lifecycle findings");
            }
        } catch (WorkflowPreparationException inactive) {
            throw inactive;
        } catch (IllegalArgumentException invalid) {
            throw failure(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE, "trusted transcript is invalid", invalid);
        } catch (RuntimeException storage) {
            throw failure(WorkflowPreparationException.Kind.UNAVAILABLE, "trusted transcript is unavailable", storage);
        }
        return transcript;
    }

    private TranscriptEntry offer(Transcript transcript, String taskId, int attempt)
            throws WorkflowPreparationException {
        TranscriptEntry selected = null;
        Set<TranscriptEntry> seen = new HashSet<>();
        for (TranscriptEntry entry : transcript.getEntriesList()) {
            if (!seen.add(entry) || !entry.hasCoordinatorFrame()) continue;
            var frame = entry.getCoordinatorFrame();
            if (frame.getTaskId().equals(taskId) && frame.hasOffer()
                    && frame.getOffer().getAttempt() == attempt) {
                if (selected != null) {
                    throw failure(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE, "multiple author offers for attempt");
                }
                selected = entry;
            }
        }
        if (selected == null) {
            throw failure(WorkflowPreparationException.Kind.INACTIVE, "author offer is absent");
        }
        return selected;
    }

    private void requireSupportedOffer(TranscriptEntry offer) throws WorkflowPreparationException {
        var spec = offer.getCoordinatorFrame().getOffer().getSpec();
        if (!spec.hasContract() || !spec.getContract().getTypeName().equals(
                WorkflowAuthoringDeliverable.getDescriptor().getFullName())) {
            throw failure(WorkflowPreparationException.Kind.INACTIVE, "unsupported author contract");
        }
        Set<String> checks = new HashSet<>();
        spec.getRequiredChecksList().forEach(check -> checks.add(check.getName()));
        if (spec.getRequiredChecksCount() != WorkflowAuthoringReviewer.REQUIRED_CHECKS.size()
                || !checks.equals(Set.copyOf(WorkflowAuthoringReviewer.REQUIRED_CHECKS))
                || !spec.getContextList().contains(policyReference)) {
            throw failure(WorkflowPreparationException.Kind.INACTIVE, "unsupported author offer");
        }
    }

    private Timestamp expiry(Transcript transcript, TranscriptEntry offer, String taskId, int attempt)
            throws WorkflowPreparationException {
        Timestamp expiry = offer.getCoordinatorFrame().getOffer().getExpiresAt();
        Set<TranscriptEntry> seen = new HashSet<>();
        for (TranscriptEntry entry : transcript.getEntriesList()) {
            if (!seen.add(entry) || !entry.hasCoordinatorFrame()) continue;
            var frame = entry.getCoordinatorFrame();
            if (frame.getTaskId().equals(taskId) && entry.getWorkerId().equals(offer.getWorkerId())
                    && frame.hasRenewal() && frame.getRenewal().getAttempt() == attempt) {
                expiry = frame.getRenewal().getExpiresAt();
            }
        }
        return expiry;
    }

    private byte[] resolve(ArtifactReference reference, String label) throws IOException {
        validate(reference, ARTIFACT_MAX);
        WorkflowValidation.validate(reference);
        if (reference.getRedacted() || !"application/x-protobuf".equals(reference.getMediaType())
                || Long.compareUnsigned(reference.getSizeBytes(), ARTIFACT_MAX) > 0) {
            throw new IllegalArgumentException(label + " reference is invalid");
        }
        var stored = artifacts.find(reference.getSha256())
                .orElseThrow(() -> new IllegalArgumentException(label + " artifact is absent"));
        if (!stored.reference().equals(reference)) {
            throw new IllegalArgumentException(label + " reference metadata differs");
        }
        byte[] bytes = stored.content();
        if (bytes.length > ARTIFACT_MAX || bytes.length != reference.getSizeBytes()
                || !WorkRecords.sha256Hex(bytes).equals(reference.getSha256())) {
            throw new IllegalArgumentException(label + " content differs from reference");
        }
        return bytes;
    }

    private static void requireCompleteImports(byte[] bytes) throws IOException {
        FileDescriptorSet set = FileDescriptorSet.parseFrom(bytes);
        Set<String> names = new HashSet<>();
        for (var file : set.getFileList()) {
            if (file.getName().isBlank() || !names.add(file.getName())) {
                throw new IllegalArgumentException("descriptor file name is empty or duplicate");
            }
        }
        if (names.isEmpty()) throw new IllegalArgumentException("descriptor set is empty");
        for (var file : set.getFileList()) {
            for (String dependency : file.getDependencyList()) {
                if (!names.contains(dependency)) {
                    throw new IllegalArgumentException("descriptor set omits an import");
                }
            }
        }
        var schemaField = CatalogContract.request("CompiledWorkflow").findFieldByName("schema");
        var schemaType = schemaField.getMessageType();
        var encodedField = schemaType.findFieldByName("descriptor_set_base64");
        Message schema = DynamicMessage.newBuilder(schemaType)
                .setField(encodedField, Base64.getEncoder().encodeToString(bytes)).build();
        try {
            SchemaResolver.resolveSource(schema, "/schema", ActionContext.create());
        } catch (ActionException invalid) {
            throw new IllegalArgumentException("descriptor set does not resolve", invalid);
        }
    }

    private static String taskId(TranscriptEntry entry) {
        return entry.hasWorkerFrame() ? entry.getWorkerFrame().getTaskId()
                : entry.hasCoordinatorFrame() ? entry.getCoordinatorFrame().getTaskId() : "";
    }

    private static void validateReturnedAny(TranscriptEntry entry)
            throws WorkflowPreparationException {
        if (!entry.hasWorkerFrame() || !entry.getWorkerFrame().hasCompletion()) return;
        if (!entry.getWorkerFrame().getCompletion().hasResult()) {
            throw failure(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "author candidate lacks its deliverable");
        }
        var packed = entry.getWorkerFrame().getCompletion().getResult();
        String expected = "/" + WorkflowAuthoringDeliverable.getDescriptor().getFullName();
        if (!packed.getTypeUrl().endsWith(expected)) {
            throw failure(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "author candidate type is invalid");
        }
        try {
            WorkflowAuthoringDeliverable deliverable = WorkflowAuthoringDeliverable.parseFrom(
                    packed.getValue());
            validate(deliverable, RESPONSE_MAX);
        } catch (InvalidProtocolBufferException | IllegalArgumentException malformed) {
            throw failure(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE,
                    "author candidate contains unknown or invalid fields", malformed);
        }
    }

    private static int attempt(TranscriptEntry entry) {
        if (entry.hasWorkerFrame()) {
            DelegateRequest frame = entry.getWorkerFrame();
            return switch (frame.getPayloadCase()) {
                case ACCEPT -> frame.getAccept().getAttempt();
                case REJECT -> frame.getReject().getAttempt();
                case HEARTBEAT -> frame.getHeartbeat().getAttempt();
                case PROGRESS -> frame.getProgress().getAttempt();
                case CHECKPOINT -> frame.getCheckpoint().getAttempt();
                case BLOCKED -> frame.getBlocked().getAttempt();
                case FAILED -> frame.getFailed().getAttempt();
                case CANCELLED -> frame.getCancelled().getAttempt();
                case COMPLETION -> frame.getCompletion().getAttempt();
                default -> 0;
            };
        }
        DelegateResponse frame = entry.getCoordinatorFrame();
        return switch (frame.getPayloadCase()) {
            case OFFER -> frame.getOffer().getAttempt();
            case RENEWAL -> frame.getRenewal().getAttempt();
            case EXPIRED -> frame.getExpired().getAttempt();
            case CANCELLATION -> frame.getCancellation().getAttempt();
            case REVISION_REQUESTED -> frame.getRevisionRequested().getAttempt();
            case ACCEPTED -> frame.getAccepted().getAttempt();
            case REVIEW_STARTED -> frame.getReviewStarted().getIdentity().getAttempt();
            case REVIEW_FAILED -> frame.getReviewFailed().getIdentity().getAttempt();
            case REVIEW_DEFERRED -> frame.getReviewDeferred().getIdentity().getAttempt();
            default -> 0;
        };
    }

    private static void requireCaller(Caller caller) throws WorkflowPreparationException {
        if (caller == null || caller.name() == null || caller.name().isBlank()) {
            throw failure(WorkflowPreparationException.Kind.PERMISSION_DENIED, "authenticated author required");
        }
    }

    private static void validateRequest(Message request) throws WorkflowPreparationException {
        try {
            if (request == null) throw new IllegalArgumentException("request required");
            validate(request, 1024);
        } catch (IllegalArgumentException invalid) {
            throw failure(WorkflowPreparationException.Kind.INVALID_INPUT, "invalid author read request", invalid);
        }
    }

    private static void validate(Message message, int maxBytes) {
        if (message.getSerializedSize() > maxBytes) throw new IllegalArgumentException("message size exceeded");
        rejectUnknown(message);
        var result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        if (!result.valid()) throw new IllegalArgumentException("message violates native contract");
    }

    private static void rejectUnknown(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("unknown protocol fields");
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) rejectUnknown((Message) nested);
            } else rejectUnknown((Message) field.getValue());
        }
    }

    private static WorkflowPreparationException failure(WorkflowPreparationException.Kind kind,
            String message) { return new WorkflowPreparationException(kind, message, null); }

    private static WorkflowPreparationException failure(WorkflowPreparationException.Kind kind,
            String message, Throwable cause) { return new WorkflowPreparationException(kind, message, cause); }
}
