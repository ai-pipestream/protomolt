package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Versioned, immutable publication intent and its canonical identity.
 * Performs no I/O and grants no permission, schema admission, retention or
 * publication. The coordinator/executor and durable outcome are not implemented
 * by this value. A future executor must use {@link #intent()}, not another payload.
 */
public final class DocumentPublicationCommand {
    public static final String CODEC = "document-publication";
    public static final int ENCODING_VERSION = 1;
    public static final int MAX_COMMAND_BYTES = 1024 * 1024;
    public static final int MAX_PARTS = 10_000;
    public static final int MAX_SOURCE_CHECKS = 10_000;
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private final UUID operationId;
    private final DocumentPublicationIntent intent;
    private final ByteString canonical;
    private final String sha256;

    public DocumentPublicationCommand(DocumentPublicationIntent supplied) {
        Objects.requireNonNull(supplied, "intent");
        // Bound validation and canonicalization work before traversing the model.
        // The transport must separately bound bytes before protobuf parsing.
        if (supplied.getSerializedSize() > MAX_COMMAND_BYTES)
            throw new IllegalArgumentException("Publication intent exceeds 1 MiB");
        rejectUnknownAndNul(supplied);
        var validation = VALIDATOR.validate(supplied);
        if (!validation.valid()) throw new IllegalArgumentException("Invalid publication intent");
        validateAggregate(supplied);
        operationId = UUID.fromString(supplied.getOperationId());
        intent = supplied.toBuilder().setOperationId(operationId.toString()).clearMembers()
                .addAllMembers(supplied.getMembersList().stream()
                        .sorted(Comparator.comparing(DocumentPublicationMember::getMemberId)).toList()).build();
        var semantic = intent.toBuilder().clearOperationId().build();
        byte[] bytes = new byte[semantic.getSerializedSize()];
        var output = CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization();
        try {
            semantic.writeTo(output);
            output.checkNoSpaceLeft();
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot encode publication command", impossible);
        }
        canonical = ByteString.copyFrom(bytes);
        try {
            sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    public UUID operationId() { return operationId; }
    public DocumentPublicationIntent intent() { return intent; }
    public ByteString canonical() { return canonical; }
    public String sha256() { return sha256; }

    /**
     * Validate a success result against this exact command and the authenticated
     * operation's committing principal/generation. This proves correspondence only,
     * not commit, authorization or admission. The publisher must verify durable
     * revision rows and store the result in their transaction. Replay uses the
     * originally committed generation, not a new reader's or retry worker's value.
     */
    public void requireResult(DocumentPublicationResult result, String principal, long committedGeneration) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(principal, "principal");
        if (result.getSerializedSize() > MAX_COMMAND_BYTES)
            throw new IllegalArgumentException("Publication result exceeds 1 MiB");
        rejectUnknownAndNul(result);
        if (!VALIDATOR.validate(result).valid()) throw new IllegalArgumentException("Invalid publication result");
        if (!result.getOperationId().equals(operationId.toString()) || !result.getAccountId().equals(intent.getAccountId())
                || !result.getCommandSha256().equals(sha256) || result.getCommandEncodingVersion() != ENCODING_VERSION
                || !result.getPrincipal().equals(principal) || result.getOwnerGeneration() != committedGeneration)
            throw new IllegalArgumentException("Publication result differs from operation identity");
        if (result.getMembersCount() != intent.getMembersCount())
            throw new IllegalArgumentException("Publication result has an incomplete member set");
        for (int i = 0; i < intent.getMembersCount(); i++) {
            var expected = intent.getMembers(i);
            var actual = result.getMembers(i);
            if (!actual.getMemberId().equals(expected.getMemberId())
                    || !actual.getAddress().equals(expected.getDestination().getAddress()))
                throw new IllegalArgumentException("Publication result differs from canonical member order or address");
        }
    }

    @Override public String toString() {
        return "DocumentPublicationCommand[operationId=" + operationId + ", members="
                + intent.getMembersCount() + ", sha256=" + sha256 + "]";
    }

    private static void validateAggregate(DocumentPublicationIntent intent) {
        var memberIds = new HashSet<String>();
        var destinations = new HashMap<NodeAddress, DocumentRevisionCondition>();
        var sourceRevisions = new HashMap<NodeAddress, Long>();
        int parts = 0;
        int sources = 0;
        long bytes = 0;
        for (var member : intent.getMembersList()) {
            if (!memberIds.add(member.getMemberId())
                    || destinations.putIfAbsent(member.getDestination().getAddress(), member.getDestination()) != null)
                throw new IllegalArgumentException("Duplicate publication member or destination");
            requireCanonicalUuid(member.getDriveId());
            validateOwnership(member.getOwnership());
            parts += member.getPartsCount();
            sources += member.getSourcesCount();
            var explicitSources = new HashSet<NodeAddress>();
            for (var source : member.getSourcesList()) {
                if (!explicitSources.add(source.getAddress()))
                    throw new IllegalArgumentException("Duplicate explicit source dependency");
                addSource(sourceRevisions, source);
            }
            var slots = new HashSet<DocumentPublicationSlot>();
            for (var part : member.getPartsList()) {
                if (!slots.add(part.getSlot())) throw new IllegalArgumentException("Duplicate publication slot");
                long size = 0;
                if (part.hasUpload()) size = part.getUpload().getSizeBytes();
                if (part.hasReuse()) {
                    sources++;
                    var reuse = part.getReuse();
                    addSource(sourceRevisions, reuse.getSource());
                    requireCanonicalUuid(reuse.getObject().getObjectId());
                    if (!part.getSlot().equals(reuse.getSourceSlot()))
                        throw new IllegalArgumentException("Reuse must preserve the source slot");
                    size = reuse.getObject().getSizeBytes();
                }
                try { bytes = Math.addExact(bytes, size); }
                catch (ArithmeticException overflow) {
                    throw new IllegalArgumentException("Aggregate publication byte size overflows", overflow);
                }
            }
        }
        if (parts > MAX_PARTS || sources > MAX_SOURCE_CHECKS)
            throw new IllegalArgumentException("Aggregate publication part/source limit exceeded");
        sourceRevisions.forEach((address, revision) -> {
            var destination = destinations.get(address);
            if (destination != null && (!destination.hasExpectedMutationRevision()
                    || destination.getExpectedMutationRevision() != revision))
                throw new IllegalArgumentException("Source conflicts with destination pre-change revision");
        });
    }

    private static void addSource(Map<NodeAddress, Long> revisions, DocumentRevisionCondition source) {
        var previous = revisions.putIfAbsent(source.getAddress(), source.getExpectedMutationRevision());
        if (previous != null && previous != source.getExpectedMutationRevision())
            throw new IllegalArgumentException("Conflicting source revisions");
    }

    private static void validateOwnership(OwnershipContext ownership) {
        if (ownership.hasConnectorId() && ownership.getConnectorId().isBlank())
            throw new IllegalArgumentException("Malformed ownership connector identity");
        for (var rule : ownership.getSecurity().getPermissionsList()) {
            if (rule.getIdentity().isBlank() || rule.getIdentityType().isBlank()
                    || rule.getAccess() == Access.ACCESS_UNSPECIFIED)
                throw new IllegalArgumentException("Malformed ownership access rule");
        }
        if (ownership.hasSourceOwner() && (ownership.getSourceOwner().getIdentity().isBlank()
                || ownership.getSourceOwner().getIdentityType().isBlank()))
            throw new IllegalArgumentException("Malformed ownership source principal");
    }

    private static void requireCanonicalUuid(String value) {
        if (!UUID.fromString(value).toString().equals(value))
            throw new IllegalArgumentException("Stored identity requires canonical lowercase UUID");
    }

    private static void rejectUnknownAndNul(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty())
            throw new IllegalArgumentException("Unknown publication fields are unsupported");
        message.getAllFields().forEach((field, value) -> {
            if (field.isRepeated()) {
                for (Object item : (List<?>) value) inspectValue(field, item);
            } else inspectValue(field, value);
        });
    }

    private static void inspectValue(Descriptors.FieldDescriptor field, Object value) {
        switch (field.getJavaType()) {
            case MESSAGE -> rejectUnknownAndNul((Message) value);
            case ENUM -> {
                if (((Descriptors.EnumValueDescriptor) value).getIndex() < 0)
                    throw new IllegalArgumentException("Unknown publication enum is unsupported");
            }
            case STRING -> {
                var text = (String) value;
                if (text.indexOf('\0') >= 0)
                    throw new IllegalArgumentException("NUL is unsupported in publication strings");
                // Protobuf's Java encoder replaces malformed UTF-16. Reject it
                // rather than give distinct Java intents identical stored bytes.
                for (int i = 0; i < text.length(); i++) {
                    char current = text.charAt(i);
                    if (Character.isHighSurrogate(current)) {
                        if (++i >= text.length() || !Character.isLowSurrogate(text.charAt(i)))
                            throw new IllegalArgumentException("Malformed UTF-16 in publication string");
                    } else if (Character.isLowSurrogate(current)) {
                        throw new IllegalArgumentException("Malformed UTF-16 in publication string");
                    }
                }
            }
            default -> { /* Scalar bounds are checked by the runtime validator. */ }
        }
    }
}
