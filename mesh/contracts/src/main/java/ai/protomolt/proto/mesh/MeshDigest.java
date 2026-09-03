package ai.protomolt.proto.mesh;

import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Canonical protobuf hashing for the mesh contract.
 *
 * <p><b>Descriptor-set fingerprints.</b> A schema fingerprint is the lowercase SHA-256 hex of the
 * canonical encoding of a {@link FileDescriptorSet}: the {@link FileDescriptorProto}s sorted by
 * file name and serialized in that order. protobuf-java serializes each proto deterministically
 * (fields in tag order; no maps occur in descriptor protos), so the fingerprint depends on content
 * alone, never on assembly order. This matches the workflow and pipeline convention, so a mesh
 * fingerprint is directly comparable with a {@code ServiceProfile} or pipeline dependency
 * fingerprint computed over the same set.
 *
 * <p><b>Unknown-field policy.</b> Unknown fields are preserved, not stripped: an unknown field in
 * a descriptor proto is schema content the producer understood and the consumer does not, and
 * identity must include it. Parsing and re-serializing descriptor bytes keeps unknown fields, so
 * the fingerprint is stable across a wire round trip, and two descriptor sets that differ only in
 * an unknown field fingerprint differently. The same rule covers custom options an older consumer
 * has no extension for.
 *
 * <p><b>Payload digests.</b> A payload digest is the SHA-256 of the exact stored wire bytes: the
 * {@code Any.value} bytes for an inline body, the artifact bytes for a claim check. No
 * re-serialization happens at the boundary, so the digest commits to the bytes as received.
 */
public final class MeshDigest {

    private MeshDigest() {
    }

    /** Returns the lowercase SHA-256 hex of {@code bytes}. */
    public static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("JDK does not provide SHA-256", e);
        }
    }

    /**
     * Returns the canonical fingerprint of a descriptor set: its files sorted by name, then
     * serialized and hashed. Order-insensitive; content-sensitive, unknown fields included.
     *
     * @param set the descriptor set to fingerprint
     * @return the lowercase SHA-256 hex fingerprint
     */
    public static String fingerprint(FileDescriptorSet set) {
        FileDescriptorSet canonical = FileDescriptorSet.newBuilder()
                .addAllFile(set.getFileList().stream()
                        .sorted(Comparator.comparing(FileDescriptorProto::getName))
                        .toList())
                .build();
        return sha256(canonical.toByteArray());
    }

    /**
     * Returns the canonical fingerprint of the given files, assembled as a descriptor set.
     *
     * @param files the files to fingerprint
     * @return the lowercase SHA-256 hex fingerprint
     */
    public static String fingerprint(List<FileDescriptor> files) {
        return fingerprint(FileDescriptorSet.newBuilder()
                .addAllFile(files.stream().map(FileDescriptor::toProto).toList())
                .build());
    }

    /**
     * Returns the descriptor-set closure that defines {@code type}: the defining file plus its
     * transitive public and private dependencies, deduplicated by file name, in dependency-first
     * order. Feed the result to {@link #fingerprint(FileDescriptorSet)} for the schema identity
     * fingerprint a {@code SchemaReference} carries.
     *
     * @param type the message type whose closure is needed
     * @return the closure as a descriptor set
     */
    public static FileDescriptorSet closure(Descriptor type) {
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        collect(type.getFile(), byName);
        return FileDescriptorSet.newBuilder()
                .addAllFile(new ArrayList<>(byName.values()))
                .build();
    }

    /**
     * Returns the canonical fingerprint of the closure that defines {@code type}: the schema
     * identity fingerprint of a fully qualified type name.
     *
     * @param type the message type to fingerprint
     * @return the lowercase SHA-256 hex fingerprint
     */
    public static String fingerprintOf(Descriptor type) {
        return fingerprint(closure(type));
    }

    /**
     * Returns the payload digest of an inline body: SHA-256 over the exact {@code Any.value}
     * bytes as received.
     *
     * @param payload the inline payload
     * @return the lowercase SHA-256 hex digest
     */
    public static String payloadDigest(Any payload) {
        return sha256(payload.getValue().toByteArray());
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptorProto> byName) {
        if (byName.containsKey(file.getName())) {
            return;
        }
        // Dependencies first, so the assembled set is in a loadable order even before the
        // canonical sort the fingerprint applies.
        for (FileDescriptor dependency : file.getDependencies()) {
            collect(dependency, byName);
        }
        for (FileDescriptor dependency : file.getPublicDependencies()) {
            collect(dependency, byName);
        }
        byName.put(file.getName(), file.toProto());
    }
}
