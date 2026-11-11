package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.List;
import java.util.Objects;

/**
 * Thrown when a {@link ReleaseBaselineReport} contains unwaived incompatible changes on
 * stability-enforced surfaces or unused compatibility waivers.
 */
@ApiStability(StabilityTier.STABLE)
public final class IncompatibleSurfaceException extends RuntimeException {

    private final List<SurfaceChange> violations;
    private final List<CompatibilityWaiver> unusedWaivers;

    /**
     * Creates an exception for unwaived breaking surface changes.
     *
     * @param violations unwaived breaking surface changes
     */
    public IncompatibleSurfaceException(List<SurfaceChange> violations) {
        this(violations, List.of());
    }

    /**
     * Creates an exception for unwaived breaking surface changes and/or unused waivers.
     *
     * @param violations    unwaived breaking surface changes
     * @param unusedWaivers declared waivers that did not match any breaking change
     */
    public IncompatibleSurfaceException(
            List<SurfaceChange> violations,
            List<CompatibilityWaiver> unusedWaivers) {
        super(formatMessage(
                Objects.requireNonNull(violations, "violations"),
                Objects.requireNonNull(unusedWaivers, "unusedWaivers")));
        this.violations = List.copyOf(violations);
        this.unusedWaivers = List.copyOf(unusedWaivers);
    }

    /**
     * The unwaived breaking changes that caused the gate failure.
     *
     * @return immutable list of violations
     */
    public List<SurfaceChange> violations() {
        return violations;
    }

    /**
     * Declared waivers that did not match any breaking change and caused the gate failure.
     *
     * @return immutable list of unused waivers
     */
    public List<CompatibilityWaiver> unusedWaivers() {
        return unusedWaivers;
    }

    private static String formatMessage(
            List<SurfaceChange> violations,
            List<CompatibilityWaiver> unusedWaivers) {
        StringBuilder sb = new StringBuilder("Release-baseline compatibility check failed:");
        if (!violations.isEmpty()) {
            sb.append("\n  Unwaived violations (").append(violations.size()).append("):");
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
        }
        if (!unusedWaivers.isEmpty()) {
            sb.append("\n  Unused waivers (").append(unusedWaivers.size()).append("):");
            for (CompatibilityWaiver waiver : unusedWaivers) {
                sb.append("\n  - ")
                        .append(waiver.id())
                        .append(" [")
                        .append(waiver.surface().id())
                        .append("] ")
                        .append(waiver.ruleId())
                        .append(" at ")
                        .append(waiver.targetPath());
            }
        }
        return sb.toString();
    }
}
