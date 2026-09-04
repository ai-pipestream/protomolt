package ai.protomolt.proto.descriptors;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Exact identity for one protobuf message definition.
 *
 * <p>A full type name is necessary but insufficient: independent descriptor pools may contain
 * different definitions under the same name. ProtoMolt therefore binds the name to the SHA-256
 * fingerprint of the canonical descriptor-set closure that defines it. The closure contains the
 * defining file and every transitive dependency. Files are ordered by file name and written with
 * protobuf's deterministic encoder. Unknown fields and unresolved custom options remain part of
 * the bytes and therefore part of the identity.</p>
 *
 * <p>This is the shared identity primitive for projections, compiled pipelines, message
 * envelopes, registries, and transports. Processing code must compare this value before parsing
 * or translating application bytes.</p>
 *
 * @param typeName fully qualified protobuf message name
 * @param fingerprint lowercase SHA-256 of the canonical descriptor closure
 */
public record DescriptorIdentity(String typeName, String fingerprint) {

    private static final Pattern TYPE_NAME = Pattern.compile(
            "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*");
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

    /** Validates and creates an identity value. */
    public DescriptorIdentity {
        Objects.requireNonNull(typeName, "typeName");
        Objects.requireNonNull(fingerprint, "fingerprint");
        if (!TYPE_NAME.matcher(typeName).matches()) {
            throw new IllegalArgumentException(
                    "typeName must be a valid protobuf message full name: " + typeName);
        }
        if (!SHA256.matcher(fingerprint).matches()) {
            throw new IllegalArgumentException(
                    "fingerprint must be 64 lowercase hexadecimal characters");
        }
    }

    /** Returns the exact identity of {@code descriptor}. */
    public static DescriptorIdentity of(Descriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        return new DescriptorIdentity(descriptor.getFullName(), fingerprint(descriptor));
    }

    /** Returns whether {@code descriptor} has this exact name and descriptor closure. */
    public boolean matches(Descriptor descriptor) {
        return descriptor != null
                && typeName.equals(descriptor.getFullName())
                && fingerprint.equals(fingerprint(descriptor));
    }

    /** Returns the defining file and all transitive dependencies of {@code descriptor}. */
    public static FileDescriptorSet closure(Descriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        Map<String, FileDescriptorProto> files = new LinkedHashMap<>();
        collect(descriptor.getFile(), files);
        return FileDescriptorSet.newBuilder().addAllFile(files.values()).build();
    }

    /** Returns the canonical descriptor-set closure fingerprint for {@code descriptor}. */
    public static String fingerprint(Descriptor descriptor) {
        return fingerprint(closure(descriptor));
    }

    /** Returns the canonical fingerprint of a descriptor set. */
    public static String fingerprint(FileDescriptorSet descriptors) {
        return sha256(canonicalBytes(descriptors));
    }

    /** Returns the canonical fingerprint of exactly the supplied descriptor files. */
    public static String fingerprintFiles(Collection<FileDescriptor> files) {
        Objects.requireNonNull(files, "files");
        Map<String, FileDescriptorProto> unique = new LinkedHashMap<>();
        for (FileDescriptor file : files) {
            Objects.requireNonNull(file, "files contains null");
            FileDescriptorProto previous = unique.putIfAbsent(file.getName(), file.toProto());
            if (previous != null && !previous.equals(file.toProto())) {
                throw new IllegalArgumentException(
                        "descriptor files contain two definitions named " + file.getName());
            }
        }
        return fingerprint(FileDescriptorSet.newBuilder().addAllFile(unique.values()).build());
    }

    /**
     * Returns the canonical protobuf bytes of {@code descriptors}: files sorted by name and
     * encoded deterministically. No JSON or text normalization participates in identity.
     */
    public static byte[] canonicalBytes(FileDescriptorSet descriptors) {
        Objects.requireNonNull(descriptors, "descriptors");
        Map<String, FileDescriptorProto> unique = new LinkedHashMap<>();
        for (FileDescriptorProto file : descriptors.getFileList()) {
            FileDescriptorProto previous = unique.putIfAbsent(file.getName(), file);
            if (previous != null && !previous.equals(file)) {
                throw new IllegalArgumentException(
                        "descriptor set contains two definitions named " + file.getName());
            }
        }
        FileDescriptorSet canonical = FileDescriptorSet.newBuilder()
                .addAllFile(unique.values().stream()
                        .sorted(Comparator.comparing(FileDescriptorProto::getName))
                        .toList())
                .build();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(canonical.getSerializedSize());
        CodedOutputStream out = CodedOutputStream.newInstance(bytes);
        out.useDeterministicSerialization();
        try {
            canonical.writeTo(out);
            out.flush();
        } catch (IOException e) {
            throw new IllegalStateException("in-memory descriptor serialization failed", e);
        }
        return bytes.toByteArray();
    }

    /** Returns lowercase SHA-256 for exact binary content. */
    public static String sha256(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("JDK does not provide SHA-256", e);
        }
    }

    private static void collect(
            FileDescriptor file, Map<String, FileDescriptorProto> destination) {
        FileDescriptorProto existing = destination.get(file.getName());
        if (existing != null) {
            if (!existing.equals(file.toProto())) {
                throw new IllegalArgumentException(
                        "descriptor closure contains two definitions named " + file.getName());
            }
            return;
        }
        for (FileDescriptor dependency : file.getDependencies()) {
            collect(dependency, destination);
        }
        for (FileDescriptor dependency : file.getPublicDependencies()) {
            collect(dependency, destination);
        }
        destination.put(file.getName(), file.toProto());
    }
}
