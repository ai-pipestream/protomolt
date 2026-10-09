package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import ai.protomolt.proto.descriptors.MessageWireBudget;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Durable success payload encoding, not a commit, authorization or replay endpoint. */
public final class DocumentPublicationResultCodec {
    public static final String CODEC = "document-publication-result";
    public static final int VERSION = 1;
    public static final int MAX_BYTES = 1024 * 1024;
    // A full 64-member result needs fewer than 1024 structural field occurrences.
    public static final int MAX_WIRE_VALUES = 1024;

    private DocumentPublicationResultCodec() {}

    /** Immutable exact bytes and their digest; instances are produced only after command matching. */
    public static final class Encoded {
        private final ByteString bytes;
        private final String sha256;
        private Encoded(ByteString bytes) { this.bytes = bytes; this.sha256 = digest(bytes); }
        public ByteString bytes() { return bytes; }
        public String sha256() { return sha256; }
        @Override public String toString() { return "EncodedPublicationResult[size=" + bytes.size() + "]"; }
    }

    /** Principal/generation must come from the committing operation, not result-supplied claims. */
    public static Encoded encode(DocumentPublicationCommand command, DocumentPublicationResult result,
            String principal, long generation) {
        Objects.requireNonNull(command, "command").requireResult(result, principal, generation);
        byte[] encoded = new byte[result.getSerializedSize()];
        var output = CodedOutputStream.newInstance(encoded);
        output.useDeterministicSerialization();
        try {
            result.writeTo(output);
            output.checkNoSpaceLeft();
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot encode publication result", impossible);
        }
        return new Encoded(ByteString.copyFrom(encoded));
    }

    /**
     * Decode a stored outcome using its independently retained codec/version/digest
     * and original committing principal/generation. Does not consult live ownership,
     * create a new revision, normalize stored bytes or prove current read permission.
     */
    public static DocumentPublicationResult decode(DocumentPublicationCommand command, String principal, long generation,
            String codec, int version, ByteString bytes, String sha256) throws InvalidProtocolBufferException {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(bytes, "bytes");
        if (!CODEC.equals(codec) || version != VERSION)
            throw new IllegalArgumentException("Unsupported publication result encoding");
        if (bytes.isEmpty() || bytes.size() > MAX_BYTES)
            throw new IllegalArgumentException("Stored publication result exceeds byte bounds");
        if (sha256 == null || !sha256.matches("[0-9a-f]{64}") || !digest(bytes).equals(sha256))
            throw new IllegalArgumentException("Stored publication result digest mismatch");
        new MessageWireBudget(MAX_WIRE_VALUES, 16, () -> {}).check(bytes, DocumentPublicationResult.getDescriptor());
        var input = bytes.newCodedInput();
        input.setRecursionLimit(16);
        final DocumentPublicationResult result;
        try { result = DocumentPublicationResult.parseFrom(input); }
        catch (IOException failure) {
            if (failure instanceof InvalidProtocolBufferException invalid) throw invalid;
            throw new InvalidProtocolBufferException(failure);
        }
        input.checkLastTagWas(0);
        if (input.getTotalBytesRead() != bytes.size()) throw new InvalidProtocolBufferException("Incomplete publication result decode");
        command.requireResult(result, principal, generation);
        return result;
    }

    private static String digest(ByteString bytes) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            for (var buffer : bytes.asReadOnlyByteBufferList()) digest.update(buffer);
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
