package ai.protomolt.proto.compat;

import ai.protomolt.proto.compat.ActionEnvelopeSnapshot.ActionEnvelopeEntry;
import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Evaluates compatibility between a baseline {@link ActionEnvelopeSnapshot} and a current
 * {@link ActionEnvelopeSnapshot}.
 */
@ApiStability(StabilityTier.STABLE)
public final class ActionEnvelopeCompatibilityChecker {

    public static final String ACTION_ADDED = "ACTION_ADDED";
    public static final String ACTION_REMOVED = "ACTION_REMOVED";
    public static final String ACTION_SCOPE_CHANGED = "ACTION_SCOPE_CHANGED";
    public static final String ACTION_STREAMING_CHANGED = "ACTION_STREAMING_CHANGED";
    public static final String ACTION_REQUEST_TYPE_CHANGED = "ACTION_REQUEST_TYPE_CHANGED";
    public static final String ACTION_RESPONSE_TYPE_CHANGED = "ACTION_RESPONSE_TYPE_CHANGED";
    public static final String ACTION_REQUIRED_INPUT_ADDED = "ACTION_REQUIRED_INPUT_ADDED";
    public static final String ACTION_INPUT_FIELD_ADDED = "ACTION_INPUT_FIELD_ADDED";
    public static final String ACTION_INPUT_FIELD_REMOVED = "ACTION_INPUT_FIELD_REMOVED";
    public static final String ACTION_INPUT_FIELD_TYPE_CHANGED = "ACTION_INPUT_FIELD_TYPE_CHANGED";
    public static final String ACTION_OUTPUT_FIELD_ADDED = "ACTION_OUTPUT_FIELD_ADDED";
    public static final String ACTION_OUTPUT_FIELD_REMOVED = "ACTION_OUTPUT_FIELD_REMOVED";
    public static final String ACTION_OUTPUT_FIELD_TYPE_CHANGED = "ACTION_OUTPUT_FIELD_TYPE_CHANGED";
    public static final String ACTION_STABILITY_DOWNGRADED = "ACTION_STABILITY_DOWNGRADED";

    private ActionEnvelopeCompatibilityChecker() {
    }

    /**
     * Compares {@code current} against {@code baseline} and returns all detected
     * {@link SurfaceChange}s in deterministic order.
     *
     * @param baseline the baseline action envelope snapshot
     * @param current  the current action envelope snapshot
     * @return immutable list of detected changes
     */
    public static List<SurfaceChange> compare(
            ActionEnvelopeSnapshot baseline,
            ActionEnvelopeSnapshot current) {
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(current, "current");

        Map<String, ActionEnvelopeEntry> oldActions = baseline.actionsByName();
        Map<String, ActionEnvelopeEntry> newActions = current.actionsByName();
        List<SurfaceChange> changes = new ArrayList<>();

        for (ActionEnvelopeEntry oldAction : baseline.actions()) {
            String name = oldAction.name();
            ActionEnvelopeEntry newAction = newActions.get(name);
            if (newAction == null) {
                changes.add(change(
                        ACTION_REMOVED,
                        name,
                        oldAction.stability(),
                        oldAction.requestTypeName() + " -> " + oldAction.responseTypeName(),
                        "",
                        "Action verb removed from catalog: " + name,
                        true));
                continue;
            }
            compareAction(oldAction, newAction, changes);
        }

        for (ActionEnvelopeEntry newAction : current.actions()) {
            if (!oldActions.containsKey(newAction.name())) {
                changes.add(change(
                        ACTION_ADDED,
                        newAction.name(),
                        newAction.stability(),
                        "",
                        newAction.requestTypeName() + " -> " + newAction.responseTypeName(),
                        "Action verb added to catalog: " + newAction.name(),
                        false));
            }
        }

        return List.copyOf(changes);
    }

    private static void compareAction(
            ActionEnvelopeEntry oldAction,
            ActionEnvelopeEntry newAction,
            List<SurfaceChange> changes) {
        String name = oldAction.name();
        StabilityTier tier = oldAction.stability();

        if (!oldAction.requiredScope().equals(newAction.requiredScope())) {
            changes.add(change(
                    ACTION_SCOPE_CHANGED,
                    name + "#scope",
                    tier,
                    oldAction.requiredScope(),
                    newAction.requiredScope(),
                    "Action '" + name + "' changed requiredScope from '"
                            + oldAction.requiredScope() + "' to '" + newAction.requiredScope() + "'",
                    true));
        }

        if (oldAction.streaming() != newAction.streaming()) {
            changes.add(change(
                    ACTION_STREAMING_CHANGED,
                    name + "#streaming",
                    tier,
                    oldAction.streaming() ? "streaming" : "unary",
                    newAction.streaming() ? "streaming" : "unary",
                    "Action '" + name + "' changed streaming mode",
                    true));
        }

        if (!oldAction.requestTypeName().equals(newAction.requestTypeName())) {
            changes.add(change(
                    ACTION_REQUEST_TYPE_CHANGED,
                    name + "#requestType",
                    tier,
                    oldAction.requestTypeName(),
                    newAction.requestTypeName(),
                    "Action '" + name + "' changed request message type from "
                            + oldAction.requestTypeName() + " to " + newAction.requestTypeName(),
                    true));
        }

        if (!oldAction.responseTypeName().equals(newAction.responseTypeName())) {
            changes.add(change(
                    ACTION_RESPONSE_TYPE_CHANGED,
                    name + "#responseType",
                    tier,
                    oldAction.responseTypeName(),
                    newAction.responseTypeName(),
                    "Action '" + name + "' changed response message type from "
                            + oldAction.responseTypeName() + " to " + newAction.responseTypeName(),
                    true));
        }

        Set<String> oldRequired = new LinkedHashSet<>(oldAction.requiredInputFields());
        for (String reqField : newAction.requiredInputFields()) {
            if (!oldRequired.contains(reqField)) {
                changes.add(change(
                        ACTION_REQUIRED_INPUT_ADDED,
                        name + ".input." + reqField,
                        tier,
                        "optional/absent",
                        "required",
                        "Action '" + name + "' added required input property '" + reqField + "'",
                        true));
            }
        }

        compareFieldMap(
                name + ".input.",
                tier,
                newAction.stability(),
                oldAction.inputFieldTypes(),
                newAction.inputFieldTypes(),
                ACTION_INPUT_FIELD_ADDED,
                ACTION_INPUT_FIELD_REMOVED,
                ACTION_INPUT_FIELD_TYPE_CHANGED,
                "input",
                changes);

        compareFieldMap(
                name + ".output.",
                tier,
                newAction.stability(),
                oldAction.outputFieldTypes(),
                newAction.outputFieldTypes(),
                ACTION_OUTPUT_FIELD_ADDED,
                ACTION_OUTPUT_FIELD_REMOVED,
                ACTION_OUTPUT_FIELD_TYPE_CHANGED,
                "output",
                changes);

        if (tier.isDowngradeTo(newAction.stability())) {
            changes.add(change(
                    ACTION_STABILITY_DOWNGRADED,
                    name,
                    tier,
                    tier.name(),
                    newAction.stability().name(),
                    "Action '" + name + "' downgraded stability from "
                            + tier + " to " + newAction.stability(),
                    true));
        }
    }

    private static void compareFieldMap(
            String pathPrefix,
            StabilityTier oldTier,
            StabilityTier newTier,
            Map<String, String> oldFields,
            Map<String, String> newFields,
            String addedRule,
            String removedRule,
            String typeChangedRule,
            String directionLabel,
            List<SurfaceChange> changes) {
        for (Map.Entry<String, String> entry : oldFields.entrySet()) {
            String fieldName = entry.getKey();
            String oldType = entry.getValue();
            String newType = newFields.get(fieldName);
            String path = pathPrefix + fieldName;
            if (newType == null) {
                changes.add(change(
                        removedRule,
                        path,
                        oldTier,
                        oldType,
                        "",
                        "Action " + directionLabel + " field removed: " + path,
                        true));
            } else if (!oldType.equals(newType)) {
                changes.add(change(
                        typeChangedRule,
                        path,
                        oldTier,
                        oldType,
                        newType,
                        "Action " + directionLabel + " field type changed on " + path
                                + ": " + oldType + " -> " + newType,
                        true));
            }
        }
        for (Map.Entry<String, String> entry : newFields.entrySet()) {
            String fieldName = entry.getKey();
            if (!oldFields.containsKey(fieldName)) {
                String path = pathPrefix + fieldName;
                changes.add(change(
                        addedRule,
                        path,
                        newTier,
                        "",
                        entry.getValue(),
                        "Action " + directionLabel + " field added: " + path,
                        false));
            }
        }
    }

    private static SurfaceChange change(
            String ruleId,
            String path,
            StabilityTier stability,
            String before,
            String after,
            String message,
            boolean breaking) {
        return new SurfaceChange(
                CompatibilitySurface.ACTION_ENVELOPE,
                ruleId,
                path,
                stability,
                before,
                after,
                message,
                breaking);
    }
}
