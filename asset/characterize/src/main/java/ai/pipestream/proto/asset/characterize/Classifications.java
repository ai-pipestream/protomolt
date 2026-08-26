package ai.pipestream.proto.asset.characterize;

import ai.pipestream.proto.asset.v1.Attribution;
import ai.pipestream.proto.asset.v1.Classification;
import ai.pipestream.proto.asset.v1.ClassificationState;
import ai.pipestream.proto.asset.v1.FormatFact;
import ai.pipestream.proto.asset.v1.ObjectStoreOrigin;
import com.google.protobuf.util.Timestamps;

import java.time.Instant;

/**
 * The classification state machine's one resolution point: given what was
 * declared and what was identified, produce the stored
 * {@link Classification}. Every consumer — archive doors, re-classification,
 * bridge gating — resolves states here, so the machine has exactly one
 * definition.
 */
public final class Classifications {

    private Classifications() {
    }

    /**
     * Resolves a classification.
     *
     * <ul>
     *   <li>nothing declared, nothing identified → {@code UNCLASSIFIED};</li>
     *   <li>declared only → {@code DECLARED};</li>
     *   <li>identified only → {@code IDENTIFIED};</li>
     *   <li>both, same format → {@code VERIFIED};</li>
     *   <li>both, identification a generalization of the claim → the claim
     *   stands: {@code DECLARED}, with the evidence kept (the generalized
     *   conclusion is not a finding about the specific format, so it is not
     *   stored as one);</li>
     *   <li>both, contradiction → {@code CONFLICTED}, both facts kept.</li>
     * </ul>
     *
     * @param declared the producer's claim, or null
     * @param identification what characterization concluded and saw
     * @param classifiedBy who declared or requested this, or null when unknown
     * @param origin the object-store origin, or null when the asset has none
     * @param at when the classification is being made
     * @return the stored classification
     */
    public static Classification resolve(FormatFact declared,
                                         Characterizer.Identification identification,
                                         Attribution classifiedBy,
                                         ObjectStoreOrigin origin,
                                         Instant at) {
        Classification.Builder classification = Classification.newBuilder()
                .addAllEvidence(identification.evidence())
                .setClassifiedAt(Timestamps.fromMillis(at.toEpochMilli()));
        if (classifiedBy != null
                && (!classifiedBy.getModule().isBlank() || !classifiedBy.getActor().isBlank())) {
            classification.setClassifiedBy(classifiedBy);
        }
        if (origin != null) {
            classification.setOrigin(origin);
        }
        FormatFact identified = identification.fact();
        if (declared == null && identified == null) {
            return classification
                    .setState(ClassificationState.CLASSIFICATION_STATE_UNCLASSIFIED)
                    .build();
        }
        if (declared == null) {
            return classification
                    .setState(ClassificationState.CLASSIFICATION_STATE_IDENTIFIED)
                    .setIdentified(identified)
                    .build();
        }
        if (identified == null) {
            return classification
                    .setState(ClassificationState.CLASSIFICATION_STATE_DECLARED)
                    .setDeclared(declared)
                    .build();
        }
        if (declared.getFormatCase() == identified.getFormatCase()) {
            return classification
                    .setState(ClassificationState.CLASSIFICATION_STATE_VERIFIED)
                    .setDeclared(declared)
                    .setIdentified(identified)
                    .build();
        }
        if (!FormatCompatibility.contradicts(declared, identified)) {
            return classification
                    .setState(ClassificationState.CLASSIFICATION_STATE_DECLARED)
                    .setDeclared(declared)
                    .build();
        }
        return classification
                .setState(ClassificationState.CLASSIFICATION_STATE_CONFLICTED)
                .setDeclared(declared)
                .setIdentified(identified)
                .build();
    }
}
