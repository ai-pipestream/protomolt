package ai.protomolt.proto.grpc.workflow;

import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

/** Bounded content-addressed storage for redacted request, response, and replay fixtures. */
public interface ArtifactRepository {

    /**
     * Stores bytes after any caller-selected redaction pass and returns their verified reference.
     */
    ArtifactReference save(byte[] content, String mediaType, boolean redacted) throws IOException;

    /** Finds one artifact by its lowercase SHA-256 identity. */
    Optional<StoredArtifact> find(String sha256) throws IOException;

    /** Artifact bytes paired with the reference that describes and authenticates them. */
    record StoredArtifact(ArtifactReference reference, byte[] content) {

        /** Creates a defensively copied stored artifact. */
        public StoredArtifact {
            if (reference == null) {
                throw new IllegalArgumentException("reference must not be null");
            }
            if (content == null) {
                throw new IllegalArgumentException("content must not be null");
            }
            content = Arrays.copyOf(content, content.length);
            WorkflowValidation.validate(reference);
            if (reference.getSizeBytes() != content.length) {
                throw new IllegalArgumentException(
                        "reference size_bytes does not match artifact content");
            }
            if (!reference.getSha256().equals(ServiceProfileValidation.sha256(content))) {
                throw new IllegalArgumentException(
                        "reference sha256 does not match artifact content");
            }
        }

        /** Returns a defensive copy so repository state cannot be mutated by a caller. */
        @Override
        public byte[] content() {
            return Arrays.copyOf(content, content.length);
        }
    }
}
