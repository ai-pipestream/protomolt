package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.List;
import java.util.Objects;

/**
 * Thrown when a {@link ReleaseBaselineReport} contains unwaived incompatible changes on
 * stability-enforced surfaces.
 */
@ApiStability(StabilityTier.STABLE)
public final class IncompatibleSurfaceException extends RuntimeException {

    private final List<SurfaceChange> violations;

    public IncompatibleSurfaceException(List<SurfaceChange> violations) {
        super(formatMessage(Objects.requireNonNull(violations, "violations")));
        this.violations = List.copyOf(violations);
    }

    /**
     * The unwaived breaking changes that caused the gate failure.
     *
     * @return immutable list of violations
     */
    public List<SurfaceChange> violations() {
        return violations;
    }

    private static String formatMessage(List<SurfaceChange> violations) {
        StringBuilder sb = new StringBuilder();
        sb.append("Release-baseline compatibility check failed with ")
                .append(violations.size())
                .append(violations.size() == 1 ? " unwaived violation:" : " unwaived violations:");
        for (SurfaceChange violation : violations) {
            sb.append("\n  - [")
                    .append(violation.surface().id())
                    .append("] ")
                    .append(violation.ruleId())
                    .append(" at ")
                    .append(violation.path())
                    .append(" (stability=")
                    .append(violation.stability())
                    .append("): ")
                    .append(violation.message());
        }
        return sb.toString();
    }
}
