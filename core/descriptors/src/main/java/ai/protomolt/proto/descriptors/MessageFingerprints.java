package ai.protomolt.proto.descriptors;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Deterministic protobuf bytes and SHA-256 identities shared by protocol consumers. */
public final class MessageFingerprints {
    private MessageFingerprints() { }
    /** The message's deterministic serialization: the bytes {@link #fingerprint} hashes. */
    public static byte[] deterministicBytes(Message message) {
        byte[] bytes = new byte[message.getSerializedSize()];
        CodedOutputStream out = CodedOutputStream.newInstance(bytes);
        out.useDeterministicSerialization();
        try {
            message.writeTo(out);
            out.checkNoSpaceLeft();
        } catch (IOException e) {
            throw new IllegalStateException("in-memory serialization failed", e);
        }
        return bytes;
    }

    /**
     * SHA-256 of the message's deterministic serialization: the platform's
     * fingerprint discipline, for identifying a message by its content.
     */
    public static String fingerprint(Message message) {
        if (message == null) {
            throw new IllegalArgumentException("message must not be null");
        }
        return sha256Hex(deterministicBytes(message));
    }

    /** SHA-256 of the given bytes as lowercase hex; the manifest digest. */
    public static String sha256Hex(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes must not be null");
        }
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable in this JDK", e);
        }
    }

}
