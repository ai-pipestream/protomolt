package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import java.util.UUID;

/** Validated immutable command identity. No caller identity is accepted from protobuf. */
public final class ArchiveMutationCommand {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private final UUID operationId;
    private final ArchiveMutationRequest request;
    private final ByteString canonical;
    private final String sha256;
    private final EntryAddress address;
    private final ArchiveMutationKind kind;

    public ArchiveMutationCommand(ArchiveMutationRequest request) {
        rejectUnknown(request);
        if (!VALIDATOR.validate(request).valid()) throw new IllegalArgumentException("Invalid archive mutation command");
        operationId = UUID.fromString(request.getOperationId());
        this.request = request.toBuilder().setOperationId(operationId.toString()).build();
        address = switch (request.getMutationCase()) {
            case DELETE_ENTRY -> request.getDeleteEntry().getAddress();
            case DELETE_RENDITION -> request.getDeleteRendition().getAddress();
            case PRUNE_VERSIONS -> request.getPruneVersions().getAddress();
            default -> throw new IllegalArgumentException("Archive mutation selection is required");
        };
        if (address.getAccountId().isBlank() || address.getEntryId().isBlank()
                || address.getAccountId().length() > 200 || address.getEntryId().length() > 500
                || (request.hasDeleteRendition() && (request.getDeleteRendition().getReason().isBlank()
                || request.getDeleteRendition().getReason().length() > 200)))
            throw new IllegalArgumentException("Invalid archive mutation scope or reason");
        kind = switch (request.getMutationCase()) {
            case DELETE_ENTRY -> ArchiveMutationKind.ARCHIVE_MUTATION_KIND_DELETE_ENTRY;
            case DELETE_RENDITION -> ArchiveMutationKind.ARCHIVE_MUTATION_KIND_DELETE_RENDITION;
            case PRUNE_VERSIONS -> ArchiveMutationKind.ARCHIVE_MUTATION_KIND_PRUNE_VERSIONS;
            default -> throw new IllegalArgumentException("Archive mutation selection is required");
        };
        var selected = request.toBuilder().clearOperationId().build();
        byte[] bytes = new byte[selected.getSerializedSize()];
        var output = CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization();
        try { selected.writeTo(output); output.checkNoSpaceLeft(); }
        catch (java.io.IOException impossible) { throw new IllegalStateException("Cannot encode archive mutation", impossible); }
        canonical = ByteString.copyFrom(bytes);
        sha256 = ArchiveManifests.sha256Hex(bytes);
    }

    public UUID operationId() { return operationId; }
    public ArchiveMutationRequest request() { return request; }
    public ByteString canonical() { return canonical; }
    public String sha256() { return sha256; }
    public EntryAddress address() { return address; }
    public ArchiveMutationKind kind() { return kind; }

    private static void rejectUnknown(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty())
            throw new IllegalArgumentException("Unknown archive mutation fields are not supported");
        message.getAllFields().forEach((field, value) -> {
            if (field.getJavaType() != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) return;
            if (field.isRepeated()) for (Object item : (java.util.List<?>) value) rejectUnknown((Message) item);
            else rejectUnknown((Message) value);
        });
    }
}
