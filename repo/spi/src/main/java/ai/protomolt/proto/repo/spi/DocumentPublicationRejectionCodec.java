package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejection;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Bounded immutable decision encoding. Encoding a value does not make it a durable receipt. */
public final class DocumentPublicationRejectionCodec {
    public static final String CODEC = "document-publication-rejection";
    public static final int VERSION = 1;
    public static final int MAX_BYTES = 4096;
    private static final int MAX_WIRE_VALUES = 32;

    private DocumentPublicationRejectionCodec() {}

    public static final class Encoded {
        private final ByteString bytes;
        private final String sha256;
        private Encoded(ByteString bytes) { this.bytes = bytes; sha256 = digest(bytes); }
        public ByteString bytes() { return bytes; }
        public String sha256() { return sha256; }
        @Override public String toString() { return "EncodedPublicationRejection[size=" + bytes.size() + "]"; }
    }

    public static Encoded encode(DocumentPublicationCommand command, DocumentPublicationRejection rejection,
            String principal, long generation) {
        Objects.requireNonNull(command).requireRejection(rejection, principal, generation);
        return new Encoded(canonical(rejection));
    }

    public static DocumentPublicationRejection decode(DocumentPublicationCommand command, String principal, long generation,
            String codec, int version, ByteString bytes, String sha256) throws InvalidProtocolBufferException {
        Objects.requireNonNull(command); Objects.requireNonNull(bytes);
        if (!CODEC.equals(codec) || version != VERSION) throw new IllegalArgumentException("Unsupported publication rejection encoding");
        if (bytes.isEmpty() || bytes.size() > MAX_BYTES) throw new IllegalArgumentException("Stored publication rejection exceeds byte bounds");
        if (sha256 == null || !sha256.matches("[0-9a-f]{64}") || !digest(bytes).equals(sha256))
            throw new IllegalArgumentException("Stored publication rejection digest mismatch");
        new MessageWireBudget(MAX_WIRE_VALUES, 4, () -> {}).check(bytes, DocumentPublicationRejection.getDescriptor());
        var rejection = DocumentPublicationRejection.parseFrom(bytes);
        command.requireRejection(rejection, principal, generation);
        if (!canonical(rejection).equals(bytes)) throw new IllegalArgumentException("Noncanonical publication rejection encoding");
        return rejection;
    }

    private static ByteString canonical(DocumentPublicationRejection rejection) {
        byte[] bytes = new byte[rejection.getSerializedSize()];
        var output = CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization();
        try { rejection.writeTo(output); output.checkNoSpaceLeft(); }
        catch (IOException impossible) { throw new IllegalStateException("Cannot encode publication rejection", impossible); }
        return ByteString.copyFrom(bytes);
    }

    private static String digest(ByteString bytes) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            for (var buffer : bytes.asReadOnlyByteBufferList()) digest.update(buffer);
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 unavailable", impossible); }
    }
}
