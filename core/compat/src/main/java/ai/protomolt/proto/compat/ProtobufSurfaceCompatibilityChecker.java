package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Evaluates protobuf schema compatibility between a baseline {@link ProtobufSurfaceSnapshot}
 * and a current {@link ProtobufSurfaceSnapshot}, combining {@link SchemaDiff} and
 * {@link CompatibilityChecker} with element-level {@link StabilityTier} resolution.
 */
@ApiStability(StabilityTier.STABLE)
public final class ProtobufSurfaceCompatibilityChecker {

    /** Rule ID emitted when a protobuf declaration's stability tier is downgraded. */
    public static final String PROTO_STABILITY_DOWNGRADED = "PROTO_STABILITY_DOWNGRADED";

    private static final CompatibilityChecker DEFAULT_CHECKER = CompatibilityChecker.builder()
            .includeJsonRules(true)
            .includeSourceRules(true)
            .build();

    private ProtobufSurfaceCompatibilityChecker() {
    }

    /**
     * Compares {@code current} against {@code baseline} under {@link CompatibilityMode#FULL}
     * with wire, proto3 JSON, and source impacts enabled.
     *
     * @param baseline the baseline protobuf snapshot
     * @param current  the current protobuf snapshot
     * @return immutable list of detected surface changes
     */
    public static List<SurfaceChange> compare(
            ProtobufSurfaceSnapshot baseline,
            ProtobufSurfaceSnapshot current) {
        return compare(baseline, current, DEFAULT_CHECKER, CompatibilityMode.FULL);
    }

    /**
     * Compares {@code current} against {@code baseline} using the supplied
     * {@link CompatibilityChecker} and {@link CompatibilityMode}.
     *
     * @param baseline the baseline protobuf snapshot
     * @param current  the current protobuf snapshot
     * @param checker  the compatibility policy checker
     * @param mode     the compatibility direction mode
     * @return immutable list of detected surface changes
     */
    public static List<SurfaceChange> compare(
            ProtobufSurfaceSnapshot baseline,
            ProtobufSurfaceSnapshot current,
            CompatibilityChecker checker,
            CompatibilityMode mode) {
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(checker, "checker");
        Objects.requireNonNull(mode, "mode");

        CompatibilityResult diffResult = checker.check(
                baseline.descriptorSet(), current.descriptorSet(), mode);
        Set<SchemaChange> violations = new HashSet<>(diffResult.violations());
        List<SurfaceChange> changes = new ArrayList<>();

        for (SchemaChange change : diffResult.changes()) {
            boolean isAddition = isAdditionRule(change.ruleId());
            StabilityTier tier = isAddition
                    ? current.stabilityForPath(change.path())
                    : baseline.stabilityForPath(change.path());
            boolean breaking = violations.contains(change);
            changes.add(new SurfaceChange(
                    CompatibilitySurface.PROTOBUF,
                    change.ruleId(),
                    change.path(),
                    tier,
                    change.before(),
                    change.after(),
                    change.message(),
                    breaking));
        }

        for (Map.Entry<String, StabilityTier> entry : baseline.elementStability().entrySet()) {
            String path = entry.getKey();
            StabilityTier oldTier = entry.getValue();
            StabilityTier newTier = current.elementStability().get(path);
            if (newTier != null && oldTier.isDowngradeTo(newTier)) {
                changes.add(new SurfaceChange(
                        CompatibilitySurface.PROTOBUF,
                        PROTO_STABILITY_DOWNGRADED,
                        path,
                        oldTier,
                        oldTier.name(),
                        newTier.name(),
                        "Protobuf declaration " + path + " downgraded stability from "
                                + oldTier + " to " + newTier,
                        true));
            }
        }

        return List.copyOf(changes);
    }

    private static boolean isAdditionRule(String ruleId) {
        return ChangeRules.MESSAGE_ADDED.equals(ruleId)
                || ChangeRules.ENUM_ADDED.equals(ruleId)
                || ChangeRules.FIELD_ADDED.equals(ruleId)
                || ChangeRules.ONEOF_ADDED.equals(ruleId)
                || ChangeRules.ENUM_VALUE_ADDED.equals(ruleId)
                || ChangeRules.SERVICE_ADDED.equals(ruleId)
                || ChangeRules.METHOD_ADDED.equals(ruleId);
    }
}
