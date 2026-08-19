package ai.pipestream.proto.screening;

import java.util.List;

/**
 * A model-driven detector over one text value. Implementations are
 * probabilistic and model-versioned: the same input may change verdicts on a
 * model update with no schema change anywhere, which is exactly why a
 * screening verdict never rides a validate.v1 rule (the design chapter's
 * hard line). The engine therefore reports the model version alongside every
 * detection surface, so a verdict is never separable from the model that
 * produced it.
 */
public interface ScreeningEngine {

    /**
     * Detects spans in one text value, ordered by begin offset. Offsets are
     * Java string indices into the given text; {@code end} is exclusive.
     *
     * @param text the text to screen
     * @return the detections, possibly empty, never null
     */
    List<Detection> detect(String text);

    /** The version of the model producing detections, reported as evidence. */
    String modelVersion();

    /**
     * One detected span: the entity type, its character range, and the
     * model's confidence. The span's text is deliberately NOT carried: the
     * detection travels into evidence, and evidence must never contain the
     * value it screened.
     *
     * @param type the detected entity type (for instance {@code person})
     * @param begin inclusive start offset in the screened text
     * @param end exclusive end offset in the screened text
     * @param confidence the model's confidence in [0, 1]
     */
    record Detection(String type, int begin, int end, double confidence) {

        public Detection {
            if (type == null || type.isEmpty()) {
                throw new IllegalArgumentException("detection type must be present");
            }
            if (begin < 0 || end <= begin) {
                throw new IllegalArgumentException(
                        "detection range must be non-empty and non-negative");
            }
        }
    }
}
