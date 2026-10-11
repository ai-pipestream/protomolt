package ai.protomolt.proto.repo.blob.spi;

/**
 * Terminal classification of one exact-version reclamation call. Explicit
 * failures (invalid target, authorization, unqualified namespace, unsupported
 * backend) throw instead of producing an outcome; see
 * {@link ExactVersionReclaimer}.
 */
public enum ExactVersionOutcome {
    /**
     * The exact version was observed present, the provider acknowledged its
     * deletion, and exact-version evidence confirms its absence afterwards.
     */
    DELETED,
    /**
     * Verified absence of the exact version by exact-version evidence. Absence
     * is permanent once verified: providers never reuse an immutable version id,
     * so no deletion was or will be needed.
     */
    ABSENT,
    /**
     * Neither terminal state is proven (timeout, connection failure, server
     * error, interruption, or inconclusive verification). The request may or
     * may not have been applied. Retrying the same target is safe and is
     * required to converge; the caller retains durable retry state.
     */
    UNCERTAIN
}
