package ai.protomolt.proto.compat;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Messages and their fields. Messages are matched by fully-qualified name across the whole set;
 * within a message, fields are matched by number, which is the wire identity — so a rename is one
 * change against the number rather than a removal plus an addition.
 *
 * <p>Synthetic map-entry messages are never diffed here; they are reached through the map field
 * that names them, in {@link FieldTypeDiff}.</p>
 */
final class MessageDiff {

    private MessageDiff() {
    }

    static void diffAll(DescriptorIndex oldIndex, DescriptorIndex newIndex,
                        List<SchemaChange> changes) {
        for (Map.Entry<String, MessageInfo> entry : oldIndex.messages.entrySet()) {
            String fqn = entry.getKey();
            MessageInfo oldMsg = entry.getValue();
            if (oldMsg.mapEntry()) {
                continue; // diffed at the map field's site
            }
            MessageInfo newMsg = newIndex.messages.get(fqn);
            if (newMsg == null || newMsg.mapEntry()) {
                changes.add(new SchemaChange(ChangeRules.MESSAGE_REMOVED, fqn,
                        "message " + fqn, "",
                        "Message " + fqn + " was removed.", DiffImpacts.ALL));
                continue;
            }
            diffMessage(fqn, oldMsg, newMsg, oldIndex, newIndex, changes);
        }
        for (Map.Entry<String, MessageInfo> entry : newIndex.messages.entrySet()) {
            if (!entry.getValue().mapEntry() && !oldIndex.messages.containsKey(entry.getKey())) {
                changes.add(new SchemaChange(ChangeRules.MESSAGE_ADDED, entry.getKey(),
                        "", "message " + entry.getKey(),
                        "Message " + entry.getKey() + " was added.", DiffImpacts.INFO));
            }
        }
    }

    private static void diffMessage(String fqn, MessageInfo oldMsg, MessageInfo newMsg,
                                    DescriptorIndex oldIndex, DescriptorIndex newIndex,
                                    List<SchemaChange> changes) {
        Map<Integer, FieldDescriptorProto> oldFields =
                DescriptorIndex.fieldsByNumber(oldMsg.proto());
        Map<Integer, FieldDescriptorProto> newFields =
                DescriptorIndex.fieldsByNumber(newMsg.proto());

        for (FieldDescriptorProto oldField : oldMsg.proto().getFieldList()) {
            FieldDescriptorProto newField = newFields.get(oldField.getNumber());
            if (newField == null) {
                diffRemovedField(fqn, oldField, newMsg.proto(), oldIndex, changes);
            } else {
                diffField(fqn, oldField, newField, oldMsg, newMsg, oldIndex, newIndex, changes);
            }
        }
        for (FieldDescriptorProto newField : newMsg.proto().getFieldList()) {
            if (!oldFields.containsKey(newField.getNumber())) {
                diffAddedField(fqn, newField, oldMsg.proto(), newMsg, newIndex, changes);
            }
        }
        OneofDiff.diffDeclarations(fqn, oldMsg.proto(), newMsg.proto(), changes);
        ReservedRanges.diff(fqn, oldMsg.proto(), newMsg.proto(), changes);
    }

    private static void diffRemovedField(String fqn, FieldDescriptorProto oldField,
                                         DescriptorProto newMsg, DescriptorIndex oldIndex,
                                         List<SchemaChange> changes) {
        String path = fqn + "." + oldField.getName();
        changes.add(new SchemaChange(ChangeRules.FIELD_REMOVED, path,
                DescriptorSnippets.snippet(oldField, oldIndex), "",
                "Field " + path + " (number " + oldField.getNumber() + ") was removed; old "
                        + "binary payloads still parse as unknown fields, but strict proto3 JSON "
                        + "parsers on the new schema reject the old field name.",
                Set.of(Impact.JSON_BACKWARD, Impact.SOURCE)));
        boolean numberReserved = ReservedRanges.isReservedNumber(newMsg, oldField.getNumber());
        boolean nameReserved = newMsg.getReservedNameList().contains(oldField.getName());
        if (!numberReserved && !nameReserved) {
            changes.add(new SchemaChange(ChangeRules.FIELD_REMOVED_NOT_RESERVED, path,
                    DescriptorSnippets.snippet(oldField, oldIndex), "",
                    "Removed field " + path + " left number " + oldField.getNumber()
                            + " and name \"" + oldField.getName() + "\" unreserved; a future "
                            + "reuse would corrupt old payloads.",
                    DiffImpacts.INFO));
        }
    }

    private static void diffAddedField(String fqn, FieldDescriptorProto newField,
                                       DescriptorProto oldMsg, MessageInfo newMsgInfo,
                                       DescriptorIndex newIndex, List<SchemaChange> changes) {
        String path = fqn + "." + newField.getName();
        if (ReservedRanges.isReservedNumber(oldMsg, newField.getNumber())) {
            changes.add(new SchemaChange(ChangeRules.RESERVED_NUMBER_REUSED, path,
                    ReservedRanges.snippet(oldMsg, newField.getNumber()),
                    DescriptorSnippets.snippet(newField, newIndex),
                    "Field " + path + " reuses number " + newField.getNumber()
                            + ", which the old schema reserved.",
                    Set.of(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD,
                            Impact.JSON_BACKWARD, Impact.JSON_FORWARD)));
        }
        if (oldMsg.getReservedNameList().contains(newField.getName())) {
            changes.add(new SchemaChange(ChangeRules.RESERVED_NAME_REUSED, path,
                    "reserved \"" + newField.getName() + "\"",
                    DescriptorSnippets.snippet(newField, newIndex),
                    "Field " + path + " reuses name \"" + newField.getName()
                            + "\", which the old schema reserved.",
                    DiffImpacts.JSON_BOTH));
        }
        if (!newMsgInfo.proto3() && newField.getLabel() == Label.LABEL_REQUIRED) {
            changes.add(new SchemaChange(ChangeRules.FIELD_REQUIRED_ADDED, path,
                    "", DescriptorSnippets.snippet(newField, newIndex),
                    "Required field " + path + " was added; old payloads lack it, so the new "
                            + "schema cannot read them.",
                    Set.of(Impact.WIRE_BACKWARD, Impact.SOURCE)));
        } else {
            changes.add(new SchemaChange(ChangeRules.FIELD_ADDED, path,
                    "", DescriptorSnippets.snippet(newField, newIndex),
                    "Field " + path + " (number " + newField.getNumber() + ") was added.",
                    DiffImpacts.INFO));
        }
    }

    private static void diffField(String fqn, FieldDescriptorProto oldField,
                                  FieldDescriptorProto newField, MessageInfo oldMsg,
                                  MessageInfo newMsg, DescriptorIndex oldIndex,
                                  DescriptorIndex newIndex, List<SchemaChange> changes) {
        String path = fqn + "." + newField.getName();
        if (!oldField.getName().equals(newField.getName())) {
            changes.add(new SchemaChange(ChangeRules.FIELD_NAME_CHANGED, path,
                    oldField.getName() + " = " + oldField.getNumber(),
                    newField.getName() + " = " + newField.getNumber(),
                    "Field number " + newField.getNumber() + " in " + fqn + " was renamed from "
                            + oldField.getName() + " to " + newField.getName()
                            + "; the wire format is unaffected but the JSON payload key changes.",
                    DiffImpacts.JSON_BOTH_SOURCE));
        } else if (!DescriptorSnippets.effectiveJsonName(oldField)
                .equals(DescriptorSnippets.effectiveJsonName(newField))) {
            changes.add(new SchemaChange(ChangeRules.FIELD_JSON_NAME_CHANGED, path,
                    "json_name = \"" + DescriptorSnippets.effectiveJsonName(oldField) + "\"",
                    "json_name = \"" + DescriptorSnippets.effectiveJsonName(newField) + "\"",
                    "Field " + path + " changed its JSON name from \""
                            + DescriptorSnippets.effectiveJsonName(oldField) + "\" to \""
                            + DescriptorSnippets.effectiveJsonName(newField) + "\".",
                    DiffImpacts.JSON_BOTH));
        }
        FieldTypeDiff.diff(path, oldField, newField, oldIndex, newIndex, changes);
        diffFieldLabel(path, oldField, newField, oldMsg, newMsg, oldIndex, newIndex, changes);
        OneofDiff.diffMembership(path, oldField, newField, oldMsg.proto(), newMsg.proto(), changes);
    }

    /** Cardinality and presence: repeated vs singular, required vs optional, implicit vs explicit. */
    private static void diffFieldLabel(String path, FieldDescriptorProto oldField,
                                       FieldDescriptorProto newField, MessageInfo oldMsg,
                                       MessageInfo newMsg, DescriptorIndex oldIndex,
                                       DescriptorIndex newIndex, List<SchemaChange> changes) {
        boolean oldRepeated = oldField.getLabel() == Label.LABEL_REPEATED;
        boolean newRepeated = newField.getLabel() == Label.LABEL_REPEATED;
        String before = DescriptorSnippets.snippet(oldField, oldIndex);
        String after = DescriptorSnippets.snippet(newField, newIndex);
        if (oldRepeated != newRepeated) {
            changes.add(new SchemaChange(ChangeRules.FIELD_LABEL_CHANGED, path, before, after,
                    "Field " + path + " changed between repeated and singular.",
                    DiffImpacts.ALL));
            return;
        }
        boolean oldRequired = oldField.getLabel() == Label.LABEL_REQUIRED;
        boolean newRequired = newField.getLabel() == Label.LABEL_REQUIRED;
        if (oldRequired && !newRequired) {
            changes.add(new SchemaChange(ChangeRules.FIELD_LABEL_CHANGED, path, before, after,
                    "Field " + path + " changed from required to optional; new writers may omit "
                            + "it, which old readers reject.",
                    Set.of(Impact.WIRE_FORWARD)));
            return;
        }
        if (!oldRequired && newRequired) {
            changes.add(new SchemaChange(ChangeRules.FIELD_LABEL_CHANGED, path, before, after,
                    "Field " + path + " changed from optional to required; old payloads may omit "
                            + "it, which the new schema rejects.",
                    Set.of(Impact.WIRE_BACKWARD)));
            return;
        }
        if (oldMsg.proto3() && newMsg.proto3()
                && oldField.getProto3Optional() != newField.getProto3Optional()) {
            changes.add(new SchemaChange(ChangeRules.FIELD_PRESENCE_CHANGED, path, before, after,
                    "Field " + path + " changed between implicit and explicit presence; the wire "
                            + "and JSON formats are unaffected but generated accessors change.",
                    Set.of(Impact.SOURCE)));
        }
    }
}
