package ai.pipestream.proto.helpers;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Base64;
import java.util.Objects;

/**
 * Validates binary {@link FileDescriptorProto} uploads against protobuf identifier grammar
 * via {@link Descriptors.FileDescriptor#buildFrom}.
 *
 * <p>Wire's binary→text round-trip silently drops illegal identifiers, so binary
 * descriptors must be checked with protobuf-java directly.
 */
public final class BinaryProtobufIdentifierValidator {

    /**
     * Substring markers from {@link DescriptorValidationException} messages.
     * protobuf-java does not expose typed error codes for these failures.
     */
    static final String INVALID_IDENTIFIER_MARKER = "is not a valid identifier";
    static final String MISSING_NAME_MARKER = "Missing name";

    private BinaryProtobufIdentifierValidator() {
    }

    /**
     * Validates a binary descriptor. No-op when {@code fileProto} is {@code null}.
     *
     * @throws ProtoSchemaValidationException on invalid / missing identifiers
     */
    public static void validate(String sourceName, FileDescriptorProto fileProto)
            throws ProtoSchemaValidationException {
        if (fileProto == null) {
            return;
        }
        Objects.requireNonNull(sourceName, "sourceName");
        try {
            Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0], true);
        } catch (DescriptorValidationException dve) {
            String message = dve.getMessage() == null ? "" : dve.getMessage();
            if (message.contains(INVALID_IDENTIFIER_MARKER) || message.contains(MISSING_NAME_MARKER)) {
                throw new ProtoSchemaValidationException(
                        "Rejected potentially malicious Protobuf identifier in "
                                + sourceName + ": " + message,
                        dve);
            }
            // Other build failures (unresolved types, etc.) are left to callers.
        }
    }

    /**
     * Attempts to interpret content as a base64-encoded {@link FileDescriptorProto}.
     * Text protos contain characters outside the base64 alphabet, so this is a reliable
     * binary-vs-text discriminator without keyword heuristics.
     *
     * @return the parsed descriptor, or {@code null} if content is not a binary upload
     */
    public static FileDescriptorProto tryParseBase64Descriptor(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(trimmed);
        } catch (IllegalArgumentException notBase64) {
            return null;
        }
        try {
            return FileDescriptorProto.parseFrom(decoded);
        } catch (InvalidProtocolBufferException notAFileDescriptor) {
            return null;
        }
    }

    /** Validates raw binary {@link FileDescriptorProto} bytes. */
    public static void validateBytes(String sourceName, byte[] descriptorBytes)
            throws ProtoSchemaValidationException {
        Objects.requireNonNull(descriptorBytes, "descriptorBytes");
        try {
            validate(sourceName, FileDescriptorProto.parseFrom(descriptorBytes));
        } catch (InvalidProtocolBufferException e) {
            throw new ProtoSchemaValidationException(
                    "Content is not a valid FileDescriptorProto: " + sourceName, e);
        }
    }
}
