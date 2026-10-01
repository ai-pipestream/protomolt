package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.CandidateReviewIdentity;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.receipt.WorkRecords;

import java.util.Objects;

/** Derives review identity from the exact durable offer and candidate entries. */
public final class DelegationReviewBindings {
    private DelegationReviewBindings() {
    }

    public static String digest(TranscriptEntry entry) {
        return WorkRecords.fingerprint(Objects.requireNonNull(entry, "entry"));
    }

    public static CandidateReviewIdentity identity(TranscriptEntry offerEntry,
            TranscriptEntry candidateEntry, String invocationId) {
        Objects.requireNonNull(offerEntry, "offerEntry");
        Objects.requireNonNull(candidateEntry, "candidateEntry");
        if (!offerEntry.hasCoordinatorFrame() || !offerEntry.getCoordinatorFrame().hasOffer()
                || !candidateEntry.hasWorkerFrame() || !candidateEntry.getWorkerFrame().hasCompletion()) {
            throw new IllegalArgumentException("review identity requires offer and candidate entries");
        }
        var offer = offerEntry.getCoordinatorFrame();
        var candidate = candidateEntry.getWorkerFrame();
        if (!offer.getTaskId().equals(candidate.getTaskId())
                || !offerEntry.getWorkerId().equals(candidateEntry.getWorkerId())
                || offer.getOffer().getAttempt() != candidate.getCompletion().getAttempt()) {
            throw new IllegalArgumentException("review offer and candidate differ");
        }
        return CandidateReviewIdentity.newBuilder()
                .setTaskId(candidate.getTaskId())
                .setWorkerId(candidateEntry.getWorkerId())
                .setAttempt(candidate.getCompletion().getAttempt())
                .setRevision(candidate.getCompletion().getRevision())
                .setInvocationId(Objects.requireNonNull(invocationId, "invocationId"))
                .setOfferEntrySha256(digest(offerEntry))
                .setCandidateEntrySha256(digest(candidateEntry))
                .build();
    }
}
