package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * Verified schema identity for internal admission preparation. Does not certify
 * validation-rule support, Any type URLs, candidate values, authorization or durable retention.
 */
final class DocumentSchemaBinding {
    private static final ProtoValidator CONDITIONS = ProtoValidator.create(List.of(new ProtomoltRuleSource()));
    private final PublicationSchemaCondition condition;
    private final ByteString artifact;
    private final String artifactSha256;
    private final Descriptor type;

    private DocumentSchemaBinding(PublicationSchemaCondition condition, ByteString artifact,
            String artifactSha256, Descriptor type) {
        this.condition = condition;
        this.artifact = artifact;
        this.artifactSha256 = artifactSha256;
        this.type = type;
    }

    PublicationSchemaCondition condition() { return condition; }
    ByteString artifact() { return artifact; }
    String artifactSha256() { return artifactSha256; }
    Descriptor type() { return type; }

    /**
     * Accepts only the root file's complete import closure. The protocol fingerprint
     * uses existing canonical file ordering; the artifact digest covers exact bytes.
     * The caller owns memory accounting and supplies immutable bytes. Control checks
     * surround bounded linking/hashing; they do not interrupt one protobuf operation.
     */
    static DocumentSchemaBinding bind(PublicationSchemaCondition condition, ByteString artifact,
            ClosedDescriptorSet.Limits limits, Runnable control) {
        return bind(condition, artifact, limits, null, control);
    }

    static DocumentSchemaBinding bind(PublicationSchemaCondition condition, ByteString artifact,
            ClosedDescriptorSet.Limits limits, DocumentAdmissionReservations reservations, Runnable control) {
        Objects.requireNonNull(condition, "condition");
        Objects.requireNonNull(artifact, "artifact");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(control, "control");
        control.run();
        if (condition.getSerializedSize() > 4096 || !condition.getUnknownFields().asMap().isEmpty()
                || !CONDITIONS.validate(condition).valid()) {
            throw new IllegalArgumentException("invalid publication schema condition");
        }
        List<FileDescriptor> files = ClosedDescriptorSet.load(artifact, limits);
        control.run();
        Descriptor type = findType(files, condition.getTypeName(), control);
        if (type == null) throw new IllegalArgumentException("schema type is missing from artifact");
        var closure = DescriptorFingerprints.closure(type);
        if (closure.getFileCount() != files.size()) {
            throw new IllegalArgumentException("schema artifact contains files outside the selected type's import closure");
        }
        control.run();
        final String fingerprint;
        if (reservations == null) {
            fingerprint = DescriptorFingerprints.fingerprint(closure);
        } else {
            // Fingerprints sort the closure's files and serialize one exact-sized array.
            // File ordering does not change its encoded size. Linked descriptor heap
            // is separate; this lease covers only that transient serialization.
            try (var scratch = Objects.requireNonNull(reservations.reserve(closure.getSerializedSize()))) {
                control.run();
                fingerprint = DescriptorFingerprints.fingerprint(closure);
                control.run();
            }
        }
        if (!fingerprint.equals(condition.getDescriptorFingerprint())) {
            throw new IllegalArgumentException("schema descriptor fingerprint mismatch");
        }
        String digest = sha256(artifact, control);
        control.run();
        return new DocumentSchemaBinding(condition, artifact, digest, type);
    }

    private static Descriptor findType(List<FileDescriptor> files, String name, Runnable control) {
        var pending = new ArrayDeque<Descriptor>();
        for (FileDescriptor file : files) pending.addAll(file.getMessageTypes());
        while (!pending.isEmpty()) {
            control.run();
            Descriptor type = pending.removeFirst();
            if (type.getFullName().equals(name)) return type;
            pending.addAll(type.getNestedTypes());
        }
        return null;
    }

    private static String sha256(ByteString artifact, Runnable control) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            for (var bytes : artifact.asReadOnlyByteBufferList()) {
                control.run();
                digest.update(bytes);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
