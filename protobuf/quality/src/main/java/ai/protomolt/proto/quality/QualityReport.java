package ai.protomolt.proto.quality;

import java.util.List;
import java.util.Map;

/**
 * A message's quality, as measured against the dimensions its schema declares.
 *
 * @param composite  the weighted average of the scored dimensions, in {@code [0, 1]}; 0 when
 *                   nothing scored
 * @param dimensions score per dimension id, insertion-ordered as declared
 * @param failed     ids of dimensions whose expressions failed on this message — measurement
 *                   gaps, not zeros, so they weigh nothing in the composite
 */
public record QualityReport(double composite, Map<String, Double> dimensions,
                            List<String> failed) {

    private static final QualityReport NONE = new QualityReport(0, Map.of(), List.of());

    /** The report for a type that declares no quality dimensions. */
    public static QualityReport none() {
        return NONE;
    }

    /** Whether the type declared anything to measure. */
    public boolean scored() {
        return !dimensions.isEmpty() || !failed.isEmpty();
    }
}
