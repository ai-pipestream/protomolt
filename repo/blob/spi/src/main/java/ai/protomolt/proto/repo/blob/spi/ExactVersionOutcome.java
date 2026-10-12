package ai.protomolt.proto.repo.blob.spi;

import java.util.Objects;

/** Result of one exact-version operation, including diagnostics for an uncertain outcome. */
public record ExactVersionOutcome(Status status, Phase phase, BlobStoreException failure) {
    public enum Status { DELETED, ABSENT, UNCERTAIN }
    public enum Phase { QUALIFICATION, EXISTENCE, DELETE, VERIFICATION }

    public static final ExactVersionOutcome DELETED = new ExactVersionOutcome(Status.DELETED, null, null);
    public static final ExactVersionOutcome ABSENT = new ExactVersionOutcome(Status.ABSENT, null, null);

    public ExactVersionOutcome {
        Objects.requireNonNull(status, "status");
        if (status == Status.UNCERTAIN) {
            Objects.requireNonNull(phase, "phase");
            Objects.requireNonNull(failure, "failure");
        } else if (phase != null || failure != null) {
            throw new IllegalArgumentException("Terminal success cannot carry failure diagnostics");
        }
    }

    /** Preserve internal causes; transport adapters must expose only sanitized diagnostic fields. */
    public static ExactVersionOutcome uncertain(Phase phase, BlobStoreException failure) {
        return new ExactVersionOutcome(Status.UNCERTAIN, phase, failure);
    }
}
