package ai.pipestream.proto.registry;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Validation and content identity for registry-owned descriptor-set artifacts. */
final class DescriptorSetArtifacts {

    private static final int MAX_BYTES = 16 * 1024 * 1024;

    private DescriptorSetArtifacts() {
    }

    static void requireFingerprint(String fingerprint) {
        if (fingerprint == null || !fingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(
                    "descriptor fingerprint must be a lowercase SHA-256 hex string");
        }
    }

    static void validate(String fingerprint, ByteString descriptorSet) {
        requireFingerprint(fingerprint);
        Objects.requireNonNull(descriptorSet, "descriptorSet");
        if (descriptorSet.isEmpty()) {
            throw new IllegalArgumentException("descriptor set must not be empty");
        }
        if (descriptorSet.size() > MAX_BYTES) {
            throw new IllegalArgumentException("descriptor set exceeds the 16 MiB limit");
        }
        if (!fingerprint.equals(sha256(descriptorSet))) {
            throw new IllegalArgumentException(
                    "descriptor fingerprint does not match descriptor-set bytes");
        }
        try {
            FileDescriptorSet parsed = FileDescriptorSet.parseFrom(descriptorSet);
            if (parsed.getFileCount() == 0) {
                throw new IllegalArgumentException(
                        "descriptor set must contain at least one file");
            }
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("descriptor set is not a FileDescriptorSet", e);
        }
    }

    static ByteString read(Path path, String fingerprint) throws IOException {
        byte[] bytes;
        try (InputStream input = Files.newInputStream(path)) {
            bytes = input.readNBytes(MAX_BYTES + 1);
        }
        if (bytes.length > MAX_BYTES) {
            throw new IllegalArgumentException("descriptor set exceeds the 16 MiB limit");
        }
        ByteString descriptorSet = ByteString.copyFrom(bytes);
        validate(fingerprint, descriptorSet);
        return descriptorSet;
    }

    private static String sha256(ByteString bytes) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
