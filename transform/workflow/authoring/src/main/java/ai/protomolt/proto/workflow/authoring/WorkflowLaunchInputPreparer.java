package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.descriptors.GoogleDescriptorLoader;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractResponse;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Prepares one accepted workflow's pinned input without executing fixtures or creating a job. */
public final class WorkflowLaunchInputPreparer implements WorkflowLaunchInputOperations {
    private static final int ARTIFACT_MAX = 4 * 1024 * 1024;
    private static final int RESPONSE_MAX = 8 * 1024 * 1024;
    private static final String PROTOBUF_MEDIA_TYPE = "application/x-protobuf";

    private final TranscriptRepository transcripts;
    private final WorkflowAuthoringReviewer reviewer;
    private final ArtifactRepository artifacts;

    public WorkflowLaunchInputPreparer(TranscriptRepository transcripts,
            WorkflowAuthoringReviewer reviewer, ArtifactRepository artifacts) {
        this.transcripts = Objects.requireNonNull(transcripts);
        this.reviewer = Objects.requireNonNull(reviewer);
        this.artifacts = Objects.requireNonNull(artifacts);
    }

    @Override
    public GetWorkflowLaunchInputContractResponse contract(
            GetWorkflowLaunchInputContractRequest request) throws WorkflowLaunchInputException {
        validateRequest(request);
        PinnedInput pinned = pinned(request.getAcceptance());
        var response = GetWorkflowLaunchInputContractResponse.newBuilder()
                .setAcceptance(request.getAcceptance())
                .setInputType(pinned.type().getFullName())
                .setDescriptors(pinned.reference())
                .setDescriptorSet(ByteString.copyFrom(pinned.descriptors()))
                .build();
        validateResponse(response);
        return response;
    }

    @Override
    public PrepareWorkflowLaunchInputResponse prepare(
            PrepareWorkflowLaunchInputRequest request) throws WorkflowLaunchInputException {
        validateRequest(request);
        PinnedInput pinned = pinned(request.getAcceptance());
        String json;
        try {
            json = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(request.getInputJson().toByteArray())).toString();
        } catch (CharacterCodingException malformed) {
            throw error(WorkflowLaunchInputException.Kind.INVALID_INPUT,
                    "launch input is not UTF-8", malformed);
        }
        DynamicMessage input;
        try {
            var builder = DynamicMessage.newBuilder(pinned.type());
            JsonFormat.parser().usingTypeRegistry(pinned.registry()).merge(json, builder);
            input = builder.build();
            // Native annotations, including nested message rules, are authoritative.
            WorkflowLaunchValidation.validate(input);
            validateEmbeddedAny(input, pinned.registry(), 0);
        } catch (InvalidProtocolBufferException | IllegalArgumentException malformed) {
            throw error(WorkflowLaunchInputException.Kind.INVALID_INPUT,
                    "launch input is not valid for the pinned schema", malformed);
        } catch (RuntimeException unsupported) {
            throw error(WorkflowLaunchInputException.Kind.INVALID_INPUT,
                    "launch input cannot be validated under the pinned schema", unsupported);
        }
        byte[] bytes = WorkflowLaunchValidation.deterministicBytes(input);
        ArtifactReference reference;
        try {
            reference = artifacts.save(bytes, PROTOBUF_MEDIA_TYPE, false);
            verifyStored(reference, bytes);
        } catch (IncompatibleArtifact incompatible) {
            throw error(WorkflowLaunchInputException.Kind.INCOMPATIBLE_ARTIFACT,
                    "stored input artifact has incompatible media or redaction metadata", incompatible);
        } catch (CorruptArtifact corrupt) {
            throw error(WorkflowLaunchInputException.Kind.CORRUPT_EVIDENCE,
                    "stored input artifact differs from committed bytes", corrupt);
        } catch (IOException unavailable) {
            throw error(deadline(unavailable) ? WorkflowLaunchInputException.Kind.DEADLINE
                            : WorkflowLaunchInputException.Kind.UNAVAILABLE,
                    "launch input artifact storage is unavailable", unavailable);
        } catch (StatusRuntimeException unavailable) {
            throw error(deadline(unavailable) ? WorkflowLaunchInputException.Kind.DEADLINE
                            : WorkflowLaunchInputException.Kind.UNAVAILABLE,
                    "launch input artifact transport is unavailable", unavailable);
        } catch (RuntimeException invalidStorage) {
            throw error(deadline(invalidStorage) ? WorkflowLaunchInputException.Kind.DEADLINE
                            : WorkflowLaunchInputException.Kind.CORRUPT_EVIDENCE,
                    "launch input artifact storage returned invalid evidence", invalidStorage);
        }
        var response = PrepareWorkflowLaunchInputResponse.newBuilder()
                .setAcceptance(request.getAcceptance()).setInput(reference).build();
        validateResponse(response);
        return response;
    }

    private PinnedInput pinned(ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate selector)
            throws WorkflowLaunchInputException {
        WorkflowLaunchAcceptance accepted;
        try {
            accepted = WorkflowLaunchAcceptance.inspect(transcripts, selector.getTaskId());
        } catch (WorkflowLaunchAcceptance.Failure failure) {
            var kind = failure.kind() == WorkflowLaunchAcceptance.FailureKind.INACTIVE
                    ? WorkflowLaunchInputException.Kind.INACTIVE
                    : WorkflowLaunchInputException.Kind.CORRUPT_EVIDENCE;
            throw error(kind, "workflow candidate is not usable", failure);
        } catch (IllegalArgumentException invalidEvidence) {
            throw error(WorkflowLaunchInputException.Kind.CORRUPT_EVIDENCE,
                    "accepted workflow transcript is invalid", invalidEvidence);
        } catch (RuntimeException unavailable) {
            throw error(deadline(unavailable) ? WorkflowLaunchInputException.Kind.DEADLINE
                            : WorkflowLaunchInputException.Kind.UNAVAILABLE,
                    "accepted workflow transcript is unavailable", unavailable);
        }
        if (!accepted.identity().equals(selector)) {
            throw error(WorkflowLaunchInputException.Kind.INACTIVE,
                    "accepted workflow selector has changed", null);
        }
        WorkflowAuthoringReviewer.PreparedReview prepared;
        try {
            prepared = reviewer.prepare(accepted.context());
        } catch (InvalidProtocolBufferException corrupt) {
            throw error(WorkflowLaunchInputException.Kind.CORRUPT_EVIDENCE,
                    "accepted workflow evidence is malformed", corrupt);
        } catch (StatusRuntimeException transport) {
            var kind = transport.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED
                    ? WorkflowLaunchInputException.Kind.DEADLINE
                    : WorkflowLaunchInputException.Kind.UNAVAILABLE;
            throw error(kind, "accepted workflow evidence transport failed", transport);
        } catch (StatusException transport) {
            var kind = transport.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED
                    ? WorkflowLaunchInputException.Kind.DEADLINE
                    : WorkflowLaunchInputException.Kind.UNAVAILABLE;
            throw error(kind, "accepted workflow evidence transport failed", transport);
        } catch (java.io.InterruptedIOException deadline) {
            throw error(WorkflowLaunchInputException.Kind.DEADLINE,
                    "accepted workflow evidence timed out", deadline);
        } catch (IOException unavailable) {
            throw error(deadline(unavailable) ? WorkflowLaunchInputException.Kind.DEADLINE
                            : WorkflowLaunchInputException.Kind.UNAVAILABLE,
                    "accepted workflow evidence is unavailable", unavailable);
        } catch (Exception invalid) {
            throw error(deadline(invalid) ? WorkflowLaunchInputException.Kind.DEADLINE
                            : WorkflowLaunchInputException.Kind.CORRUPT_EVIDENCE,
                    "accepted workflow evidence is invalid", invalid);
        }
        ArtifactReference descriptors = prepared.policy().getDescriptors();
        byte[] bytes;
        try {
            bytes = resolveDescriptors(descriptors);
        } catch (IOException unavailable) {
            throw error(deadline(unavailable) ? WorkflowLaunchInputException.Kind.DEADLINE
                            : WorkflowLaunchInputException.Kind.UNAVAILABLE,
                    "accepted workflow descriptors are unavailable", unavailable);
        } catch (StatusRuntimeException unavailable) {
            throw error(deadline(unavailable) ? WorkflowLaunchInputException.Kind.DEADLINE
                            : WorkflowLaunchInputException.Kind.UNAVAILABLE,
                    "accepted workflow descriptor transport is unavailable", unavailable);
        } catch (RuntimeException invalid) {
            throw error(deadline(invalid) ? WorkflowLaunchInputException.Kind.DEADLINE
                            : WorkflowLaunchInputException.Kind.CORRUPT_EVIDENCE,
                    "accepted workflow descriptors differ from pinned evidence", invalid);
        }
        PinnedSchema schema;
        try {
            schema = schema(bytes, prepared.admitted().workflow().inputType().getFullName());
            if (!schema.type().getFile().toProto().equals(
                    prepared.admitted().workflow().inputType().getFile().toProto())) {
                throw new IllegalArgumentException("input type does not match pinned descriptor file");
            }
        } catch (Exception invalid) {
            throw error(WorkflowLaunchInputException.Kind.CORRUPT_EVIDENCE,
                    "accepted workflow input descriptor is invalid", invalid);
        }
        return new PinnedInput(schema.type(), schema.registry(), descriptors, bytes);
    }

    private byte[] resolveDescriptors(ArtifactReference reference) throws IOException {
        WorkflowValidation.validate(reference);
        if (reference.getRedacted() || !PROTOBUF_MEDIA_TYPE.equals(reference.getMediaType())
                || Long.compareUnsigned(reference.getSizeBytes(), ARTIFACT_MAX) > 0) {
            throw new IllegalArgumentException("descriptor reference is invalid");
        }
        var stored = artifacts.find(reference.getSha256())
                .orElseThrow(() -> new IllegalArgumentException("descriptor artifact is absent"));
        byte[] bytes = stored.content();
        if (!stored.reference().equals(reference) || bytes.length != reference.getSizeBytes()
                || bytes.length > ARTIFACT_MAX
                || !WorkRecords.sha256Hex(bytes).equals(reference.getSha256())) {
            throw new IllegalArgumentException("descriptor artifact differs from its reference");
        }
        return bytes;
    }

    private void verifyStored(ArtifactReference reference, byte[] bytes)
            throws IOException, IncompatibleArtifact, CorruptArtifact {
        if (reference == null) throw new CorruptArtifact();
        try {
            WorkflowValidation.validate(reference);
        } catch (IllegalArgumentException invalid) {
            throw new CorruptArtifact();
        }
        if (reference.getSizeBytes() != bytes.length
                || !WorkRecords.sha256Hex(bytes).equals(reference.getSha256())) {
            throw new CorruptArtifact();
        }
        var stored = artifacts.find(reference.getSha256())
                .orElseThrow(CorruptArtifact::new);
        if (!stored.reference().equals(reference)
                || !java.util.Arrays.equals(stored.content(), bytes)) {
            throw new CorruptArtifact();
        }
        if (reference.getRedacted() || !PROTOBUF_MEDIA_TYPE.equals(reference.getMediaType())) {
            throw new IncompatibleArtifact();
        }
    }

    static PinnedSchema schema(byte[] descriptors, String inputType) throws Exception {
        List<FileDescriptor> files = GoogleDescriptorLoader.fromDescriptorSet(
                FileDescriptorSet.parseFrom(descriptors));
        Descriptor selected = null;
        var builder = JsonFormat.TypeRegistry.newBuilder();
        Set<String> visited = new HashSet<>();
        for (FileDescriptor file : files) {
            addFile(builder, file, visited);
            for (Descriptor message : file.getMessageTypes()) {
                Descriptor found = findType(message, inputType);
                if (found != null) {
                    if (selected != null) throw new IllegalArgumentException("duplicate pinned input type");
                    selected = found;
                }
            }
        }
        if (selected == null) throw new IllegalArgumentException("pinned input type is absent");
        return new PinnedSchema(selected, builder.build());
    }

    private static Descriptor findType(Descriptor message, String name) {
        if (message.getFullName().equals(name)) return message;
        for (Descriptor nested : message.getNestedTypes()) {
            Descriptor found = findType(nested, name);
            if (found != null) return found;
        }
        return null;
    }

    static void validateEmbeddedAny(Message message, JsonFormat.TypeRegistry registry, int depth) {
        if (depth > 64) throw new IllegalArgumentException("launch input nesting exceeds the supported depth");
        if (message.getDescriptorForType().getFullName().equals(Any.getDescriptor().getFullName())) {
            Any packed;
            try {
                packed = Any.parseFrom(message.toByteArray());
            } catch (InvalidProtocolBufferException malformed) {
                throw new IllegalArgumentException("embedded Any is malformed", malformed);
            }
            String typeUrl = packed.getTypeUrl();
            if (!typeUrl.startsWith("type.googleapis.com/")) {
                throw new IllegalArgumentException("embedded Any type URL is unsupported");
            }
            Descriptor type = registry.find(typeUrl.substring("type.googleapis.com/".length()));
            if (type == null) throw new IllegalArgumentException("embedded Any type is not pinned");
            try {
                DynamicMessage value = DynamicMessage.parseFrom(type, packed.getValue());
                validate(value, ARTIFACT_MAX);
                validateEmbeddedAny(value, registry, depth + 1);
            } catch (InvalidProtocolBufferException malformed) {
                throw new IllegalArgumentException("embedded Any payload is malformed", malformed);
            }
            return;
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) {
                    validateEmbeddedAny((Message) nested, registry, depth + 1);
                }
            } else validateEmbeddedAny((Message) field.getValue(), registry, depth + 1);
        }
    }

    private static void addFile(JsonFormat.TypeRegistry.Builder registry, FileDescriptor file,
            Set<String> visited) {
        if (!visited.add(file.getName())) return;
        for (FileDescriptor dependency : file.getDependencies()) addFile(registry, dependency, visited);
        registry.add(file.getMessageTypes());
    }

    private static void validateRequest(Message request) throws WorkflowLaunchInputException {
        try {
            validate(request, RESPONSE_MAX);
        } catch (RuntimeException invalid) {
            throw error(WorkflowLaunchInputException.Kind.INVALID_INPUT,
                    "launch input request is invalid", invalid);
        }
    }

    private static void validateResponse(Message response) throws WorkflowLaunchInputException {
        try {
            validate(response, RESPONSE_MAX);
        } catch (RuntimeException invalid) {
            throw error(WorkflowLaunchInputException.Kind.CORRUPT_EVIDENCE,
                    "launch input response is invalid", invalid);
        }
    }

    private static void validate(Message message, int maxBytes) {
        if (message == null || message.getSerializedSize() > maxBytes) {
            throw new IllegalArgumentException("launch input message exceeds its bound");
        }
        rejectUnknown(message);
        var result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        if (!result.valid()) throw new IllegalArgumentException("launch input contract is invalid: " + result.violations());
    }

    private static void rejectUnknown(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("unknown fields in launch input contract");
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) rejectUnknown((Message) nested);
            } else rejectUnknown((Message) field.getValue());
        }
    }

    private static WorkflowLaunchInputException error(WorkflowLaunchInputException.Kind kind,
            String message, Throwable cause) {
        return new WorkflowLaunchInputException(kind, message, cause);
    }

    private static boolean deadline(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof StatusRuntimeException status
                    && status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) return true;
            if (current instanceof StatusException status
                    && status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) return true;
            if (current instanceof java.io.InterruptedIOException
                    || current instanceof java.util.concurrent.TimeoutException) return true;
        }
        return false;
    }

    static record PinnedSchema(Descriptor type, JsonFormat.TypeRegistry registry) {}

    private record PinnedInput(Descriptor type, JsonFormat.TypeRegistry registry,
            ArtifactReference reference, byte[] descriptors) {}

    private static final class IncompatibleArtifact extends Exception {}
    private static final class CorruptArtifact extends Exception {}
}
