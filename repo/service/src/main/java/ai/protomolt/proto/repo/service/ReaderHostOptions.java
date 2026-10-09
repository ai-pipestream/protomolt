package ai.protomolt.proto.repo.service;

import java.util.UUID;

/**
 * Trusted supervisor identity for one service execution, never supplied by a read
 * request. Each startup needs a fresh execution UUID, including after a lost
 * registration reply. The supervisor retains this identity for failed-startup
 * recovery. This configuration neither proves termination nor installs a verifier.
 */
public record ReaderHostOptions(UUID execution, String hostIdentity, String bootIdentity) {
    public ReaderHostOptions {
        new ai.protomolt.proto.repo.container.ledger.ReaderHostTermination.Identity(
                execution, hostIdentity, bootIdentity);
    }
}
