package ai.protomolt.proto.repo.accession;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Decides, once, whether an {@code UploadDocument} call commits or is withdrawn. The call is
 * open until either the upload claims its commit or a refusal or cancellation withdraws it;
 * both are one compare-and-set from open, so exactly one of them wins. A call answered with an
 * error therefore never commits, and a committing call is never answered with a refusal.
 */
final class UploadCommitGate {
    private static final int OPEN = 0;
    private static final int COMMITTING = 1;
    private static final int WITHDRAWN = 2;

    private final AtomicInteger state = new AtomicInteger(OPEN);

    /**
     * Claims the commit for the upload.
     *
     * @return whether the upload may commit; {@code false} once the call was withdrawn
     */
    boolean claimCommit() {
        return state.compareAndSet(OPEN, COMMITTING);
    }

    /**
     * Withdraws the call before the upload claims its commit.
     *
     * @return whether the call was withdrawn now; {@code false} when the commit was claimed
     *         first or the call was already withdrawn
     */
    boolean withdraw() {
        return state.compareAndSet(OPEN, WITHDRAWN);
    }
}
