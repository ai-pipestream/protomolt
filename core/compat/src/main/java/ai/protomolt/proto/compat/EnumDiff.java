package ai.protomolt.proto.compat;

import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Enums and their values. Values are matched by number, with a separate name-keyed pass so that a
 * renumbered value reads as a renumber rather than a removal plus an addition — and so that a
 * dropped alias name under {@code allow_alias}, which the by-number pass cannot see, is still
 * reported.
 */
final class EnumDiff {

    private EnumDiff() {
    }

    static void diffAll(DescriptorIndex oldIndex, DescriptorIndex newIndex,
                        List<SchemaChange> changes) {
        for (Map.Entry<String, EnumDescriptorProto> entry : oldIndex.enums.entrySet()) {
            String fqn = entry.getKey();
            EnumDescriptorProto newEnum = newIndex.enums.get(fqn);
            if (newEnum == null) {
                changes.add(new SchemaChange(ChangeRules.ENUM_REMOVED, fqn,
                        "enum " + fqn, "",
                        "Enum " + fqn + " was removed.", DiffImpacts.ALL));
            } else {
                diffEnum(fqn, entry.getValue(), newEnum, changes);
            }
        }
        for (String fqn : newIndex.enums.keySet()) {
            if (!oldIndex.enums.containsKey(fqn)) {
                changes.add(new SchemaChange(ChangeRules.ENUM_ADDED, fqn,
                        "", "enum " + fqn,
                        "Enum " + fqn + " was added.", DiffImpacts.INFO));
            }
        }
    }

    private static void diffEnum(String fqn, EnumDescriptorProto oldEnum,
                                 EnumDescriptorProto newEnum, List<SchemaChange> changes) {
        Map<Integer, EnumValueDescriptorProto> oldByNumber =
                DescriptorIndex.valuesByNumber(oldEnum);
        Map<Integer, EnumValueDescriptorProto> newByNumber =
                DescriptorIndex.valuesByNumber(newEnum);
        Map<String, EnumValueDescriptorProto> oldByName = DescriptorIndex.valuesByName(oldEnum);
        Map<String, EnumValueDescriptorProto> newByName = DescriptorIndex.valuesByName(newEnum);

        for (EnumValueDescriptorProto oldValue : oldByName.values()) {
            EnumValueDescriptorProto newValue = newByName.get(oldValue.getName());
            if (newValue != null && newValue.getNumber() != oldValue.getNumber()) {
                changes.add(new SchemaChange(ChangeRules.ENUM_VALUE_NUMBER_CHANGED,
                        fqn + "." + oldValue.getName(),
                        oldValue.getName() + " = " + oldValue.getNumber(),
                        newValue.getName() + " = " + newValue.getNumber(),
                        "Enum value " + fqn + "." + oldValue.getName() + " changed number from "
                                + oldValue.getNumber() + " to " + newValue.getNumber() + ".",
                        DiffImpacts.ALL));
            }
        }
        Set<String> reportedGoneNames = new HashSet<>();
        for (EnumValueDescriptorProto oldValue : oldByNumber.values()) {
            EnumValueDescriptorProto newValue = newByNumber.get(oldValue.getNumber());
            if (newValue == null) {
                if (!newByName.containsKey(oldValue.getName())) { // renumber already reported
                    reportedGoneNames.add(oldValue.getName());
                    changes.add(valueRemoved(fqn, oldValue));
                }
            } else if (!oldValue.getName().equals(newValue.getName())
                    && !newByName.containsKey(oldValue.getName())
                    && !oldByName.containsKey(newValue.getName())) {
                reportedGoneNames.add(oldValue.getName());
                changes.add(new SchemaChange(ChangeRules.ENUM_VALUE_NAME_CHANGED,
                        fqn + "." + newValue.getName(),
                        oldValue.getName() + " = " + oldValue.getNumber(),
                        newValue.getName() + " = " + newValue.getNumber(),
                        "Enum value number " + oldValue.getNumber() + " in " + fqn
                                + " was renamed from " + oldValue.getName() + " to "
                                + newValue.getName() + "; JSON payloads carry the name.",
                        DiffImpacts.JSON_BOTH_SOURCE));
            }
        }
        // Under allow_alias several names share one number, and the by-number pass sees only
        // the first declaration; a dropped alias name would otherwise go unreported even
        // though JSON payloads carrying it no longer parse.
        for (EnumValueDescriptorProto oldValue : oldByName.values()) {
            if (newByName.containsKey(oldValue.getName())
                    || reportedGoneNames.contains(oldValue.getName())) {
                continue;
            }
            changes.add(valueRemoved(fqn, oldValue));
        }
        for (EnumValueDescriptorProto newValue : newByNumber.values()) {
            if (!oldByNumber.containsKey(newValue.getNumber())
                    && !oldByName.containsKey(newValue.getName())) {
                changes.add(new SchemaChange(ChangeRules.ENUM_VALUE_ADDED,
                        fqn + "." + newValue.getName(),
                        "", newValue.getName() + " = " + newValue.getNumber(),
                        "Enum value " + fqn + "." + newValue.getName() + " was added.",
                        DiffImpacts.INFO));
            }
        }
    }

    private static SchemaChange valueRemoved(String fqn, EnumValueDescriptorProto oldValue) {
        return new SchemaChange(ChangeRules.ENUM_VALUE_REMOVED,
                fqn + "." + oldValue.getName(),
                oldValue.getName() + " = " + oldValue.getNumber(), "",
                "Enum value " + fqn + "." + oldValue.getName() + " was removed; the "
                        + "number still parses (open enum) but the JSON name does not.",
                Set.of(Impact.JSON_BACKWARD, Impact.SOURCE));
    }
}
