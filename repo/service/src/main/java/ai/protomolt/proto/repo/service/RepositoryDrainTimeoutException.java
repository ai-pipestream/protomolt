package ai.protomolt.proto.repo.service;

/** A timed drain expired with resources retained; the owner may retry close. */
final class RepositoryDrainTimeoutException extends IllegalStateException {
    enum Phase { LIFECYCLE_WORKER, ARCHIVE_RPC, ARCHIVE_PUT, ARCHIVE_READ, PUBLICATION_RPC }

    private final Phase phase;

    RepositoryDrainTimeoutException(Phase phase, String message) {
        super(message);
        this.phase = java.util.Objects.requireNonNull(phase);
    }

    Phase phase() { return phase; }
}
