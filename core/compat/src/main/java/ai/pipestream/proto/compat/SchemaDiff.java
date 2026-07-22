package ai.pipestream.proto.compat;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The diff engine: compares two {@link FileDescriptorSet}s and reports every difference as a
 * {@link SchemaChange} tagged with the {@link Impact}s it carries. Policy — which changes are
 * acceptable under which compatibility mode — lives in {@link CompatibilityChecker}; this class
 * only observes.
 *
 * <p>Matching is by identity that survives refactoring: messages, enums and services are matched
 * by fully-qualified name across the <em>whole</em> set, so moving a type between files (with an
 * unchanged shape) produces no changes. Within a message, fields are matched by number — the
 * wire identity. Within an enum, values are matched by number, with a separate name-keyed pass
 * to distinguish a renumbered value from a removal plus an addition. Service methods are matched
 * by name within their service.</p>
 *
 * <p>Synthetic map-entry messages ({@code options.map_entry}) are never diffed as messages;
 * their key/value fields are diffed at the map field's site with paths like
 * {@code example.Doc.attrs (map value)}. Synthetic proto3-optional oneofs are likewise invisible
 * to the oneof rules.</p>
 *
 * <p>The engine diffs exactly the files it is given: if the old set contains a file (say a
 * well-known import) that the new set lacks, its types are reported as removed. Callers should
 * hand both sides the same dependency closure — the {@link CompatibilityChecker} overloads do.</p>
 */
public final class SchemaDiff {

    private static final Set<Impact> INFO = Set.of();
    private static final Set<Impact> ALL = Set.of(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD,
            Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
    private static final Set<Impact> WIRE_BOTH_SOURCE =
            Set.of(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD, Impact.SOURCE);
    private static final Set<Impact> JSON_BOTH_SOURCE =
            Set.of(Impact.JSON_BACKWARD, Impact.JSON_FORWARD, Impact.SOURCE);
    private static final Set<Impact> JSON_BOTH =
            Set.of(Impact.JSON_BACKWARD, Impact.JSON_FORWARD);

    private SchemaDiff() {
    }

    /**
     * Diffs {@code oldSet} against {@code newSet} and returns every change, informational ones
     * included, in a stable order (types first, then fields, oneofs, reserved declarations,
     * enums, services).
     */
    public static List<SchemaChange> diff(FileDescriptorSet oldSet, FileDescriptorSet newSet) {
        Index oldIndex = Index.of(oldSet);
        Index newIndex = Index.of(newSet);
        List<SchemaChange> changes = new ArrayList<>();
        diffMessages(oldIndex, newIndex, changes);
        diffEnums(oldIndex, newIndex, changes);
        diffServices(oldIndex, newIndex, changes);
        return List.copyOf(changes);
    }

    // ------------------------------------------------------------------ messages

    private static void diffMessages(Index oldIndex, Index newIndex, List<SchemaChange> changes) {
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
                        "Message " + fqn + " was removed.", ALL));
                continue;
            }
            diffMessage(fqn, oldMsg, newMsg, oldIndex, newIndex, changes);
        }
        for (Map.Entry<String, MessageInfo> entry : newIndex.messages.entrySet()) {
            if (!entry.getValue().mapEntry() && !oldIndex.messages.containsKey(entry.getKey())) {
                changes.add(new SchemaChange(ChangeRules.MESSAGE_ADDED, entry.getKey(),
                        "", "message " + entry.getKey(),
                        "Message " + entry.getKey() + " was added.", INFO));
            }
        }
    }

    private static void diffMessage(String fqn, MessageInfo oldMsg, MessageInfo newMsg,
                                    Index oldIndex, Index newIndex, List<SchemaChange> changes) {
        Map<Integer, FieldDescriptorProto> oldFields = fieldsByNumber(oldMsg.proto());
        Map<Integer, FieldDescriptorProto> newFields = fieldsByNumber(newMsg.proto());

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
        diffOneofs(fqn, oldMsg.proto(), newMsg.proto(), changes);
        diffReservedRanges(fqn, oldMsg.proto(), newMsg.proto(), changes);
    }

    private static void diffRemovedField(String fqn, FieldDescriptorProto oldField,
                                         DescriptorProto newMsg, Index oldIndex,
                                         List<SchemaChange> changes) {
        String path = fqn + "." + oldField.getName();
        changes.add(new SchemaChange(ChangeRules.FIELD_REMOVED, path,
                snippet(oldField, oldIndex), "",
                "Field " + path + " (number " + oldField.getNumber() + ") was removed; old "
                        + "binary payloads still parse as unknown fields, but strict proto3 JSON "
                        + "parsers on the new schema reject the old field name.",
                Set.of(Impact.JSON_BACKWARD, Impact.SOURCE)));
        boolean numberReserved = isReservedNumber(newMsg, oldField.getNumber());
        boolean nameReserved = newMsg.getReservedNameList().contains(oldField.getName());
        if (!numberReserved && !nameReserved) {
            changes.add(new SchemaChange(ChangeRules.FIELD_REMOVED_NOT_RESERVED, path,
                    snippet(oldField, oldIndex), "",
                    "Removed field " + path + " left number " + oldField.getNumber()
                            + " and name \"" + oldField.getName() + "\" unreserved; a future "
                            + "reuse would corrupt old payloads.",
                    INFO));
        }
    }

    private static void diffAddedField(String fqn, FieldDescriptorProto newField,
                                       DescriptorProto oldMsg, MessageInfo newMsgInfo,
                                       Index newIndex, List<SchemaChange> changes) {
        String path = fqn + "." + newField.getName();
        if (isReservedNumber(oldMsg, newField.getNumber())) {
            changes.add(new SchemaChange(ChangeRules.RESERVED_NUMBER_REUSED, path,
                    reservedRangeSnippet(oldMsg, newField.getNumber()), snippet(newField, newIndex),
                    "Field " + path + " reuses number " + newField.getNumber()
                            + ", which the old schema reserved.",
                    Set.of(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD,
                            Impact.JSON_BACKWARD, Impact.JSON_FORWARD)));
        }
        if (oldMsg.getReservedNameList().contains(newField.getName())) {
            changes.add(new SchemaChange(ChangeRules.RESERVED_NAME_REUSED, path,
                    "reserved \"" + newField.getName() + "\"", snippet(newField, newIndex),
                    "Field " + path + " reuses name \"" + newField.getName()
                            + "\", which the old schema reserved.",
                    JSON_BOTH));
        }
        if (!newMsgInfo.proto3() && newField.getLabel() == Label.LABEL_REQUIRED) {
            changes.add(new SchemaChange(ChangeRules.FIELD_REQUIRED_ADDED, path,
                    "", snippet(newField, newIndex),
                    "Required field " + path + " was added; old payloads lack it, so the new "
                            + "schema cannot read them.",
                    Set.of(Impact.WIRE_BACKWARD, Impact.SOURCE)));
        } else {
            changes.add(new SchemaChange(ChangeRules.FIELD_ADDED, path,
                    "", snippet(newField, newIndex),
                    "Field " + path + " (number " + newField.getNumber() + ") was added.",
                    INFO));
        }
    }

    private static void diffField(String fqn, FieldDescriptorProto oldField,
                                  FieldDescriptorProto newField, MessageInfo oldMsg,
                                  MessageInfo newMsg, Index oldIndex, Index newIndex,
                                  List<SchemaChange> changes) {
        String path = fqn + "." + newField.getName();
        if (!oldField.getName().equals(newField.getName())) {
            changes.add(new SchemaChange(ChangeRules.FIELD_NAME_CHANGED, path,
                    oldField.getName() + " = " + oldField.getNumber(),
                    newField.getName() + " = " + newField.getNumber(),
                    "Field number " + newField.getNumber() + " in " + fqn + " was renamed from "
                            + oldField.getName() + " to " + newField.getName()
                            + "; the wire format is unaffected but the JSON payload key changes.",
                    JSON_BOTH_SOURCE));
        } else if (!effectiveJsonName(oldField).equals(effectiveJsonName(newField))) {
            changes.add(new SchemaChange(ChangeRules.FIELD_JSON_NAME_CHANGED, path,
                    "json_name = \"" + effectiveJsonName(oldField) + "\"",
                    "json_name = \"" + effectiveJsonName(newField) + "\"",
                    "Field " + path + " changed its JSON name from \""
                            + effectiveJsonName(oldField) + "\" to \""
                            + effectiveJsonName(newField) + "\".",
                    JSON_BOTH));
        }
        diffFieldType(path, oldField, newField, oldIndex, newIndex, changes);
        diffFieldLabel(path, oldField, newField, oldMsg, newMsg, oldIndex, newIndex, changes);
        diffFieldOneof(path, oldField, newField, oldMsg.proto(), newMsg.proto(), changes);
    }

    // ------------------------------------------------------------------ field types

    private static void diffFieldType(String path, FieldDescriptorProto oldField,
                                      FieldDescriptorProto newField, Index oldIndex,
                                      Index newIndex, List<SchemaChange> changes) {
        MessageInfo oldEntry = mapEntryOf(oldField, oldIndex);
        MessageInfo newEntry = mapEntryOf(newField, newIndex);
        if (oldEntry != null && newEntry != null) {
            diffFieldType(path + " (map key)", keyField(oldEntry), keyField(newEntry),
                    oldIndex, newIndex, changes);
            diffFieldType(path + " (map value)", valueField(oldEntry), valueField(newEntry),
                    oldIndex, newIndex, changes);
            return;
        }
        if (oldEntry != null || newEntry != null) {
            changes.add(new SchemaChange(ChangeRules.FIELD_MAP_ENTRY_CHANGED, path,
                    snippet(oldField, oldIndex), snippet(newField, newIndex),
                    "Field " + path + " changed between a map and a repeated message; presence "
                            + "of last-key-wins map semantics and the JSON shape (object vs "
                            + "array) both change.",
                    ALL));
            return;
        }
        Type oldType = oldField.getType();
        Type newType = newField.getType();
        boolean oldMessage = oldType == Type.TYPE_MESSAGE || oldType == Type.TYPE_GROUP;
        boolean newMessage = newType == Type.TYPE_MESSAGE || newType == Type.TYPE_GROUP;
        if (oldMessage && newMessage) {
            String oldFqn = typeFqn(oldField);
            String newFqn = typeFqn(newField);
            if (!oldFqn.equals(newFqn)) {
                changes.add(new SchemaChange(ChangeRules.FIELD_MESSAGE_TYPE_CHANGED, path,
                        snippet(oldField, oldIndex), snippet(newField, newIndex),
                        "Field " + path + " changed message type from " + oldFqn + " to "
                                + newFqn + ".",
                        ALL));
            }
            return;
        }
        if (oldType == Type.TYPE_ENUM && newType == Type.TYPE_ENUM) {
            String oldFqn = typeFqn(oldField);
            String newFqn = typeFqn(newField);
            if (!oldFqn.equals(newFqn)) {
                changes.add(new SchemaChange(ChangeRules.FIELD_ENUM_TYPE_CHANGED, path,
                        snippet(oldField, oldIndex), snippet(newField, newIndex),
                        "Field " + path + " changed enum type from " + oldFqn + " to "
                                + newFqn + ".",
                        ALL));
            }
            return;
        }
        if (oldType == newType) {
            return;
        }
        String before = snippet(oldField, oldIndex);
        String after = snippet(newField, newIndex);
        if (oldType == Type.TYPE_BYTES && newType == Type.TYPE_STRING) {
            changes.add(new SchemaChange(ChangeRules.FIELD_TYPE_CHANGED, path, before, after,
                    "Field " + path + " changed from bytes to string; old payloads may contain "
                            + "non-UTF-8 bytes the new schema cannot read.",
                    Set.of(Impact.WIRE_BACKWARD, Impact.JSON_BACKWARD, Impact.JSON_FORWARD,
                            Impact.SOURCE)));
            return;
        }
        if (wireGroup(oldType) == wireGroup(newType)
                || (oldType == Type.TYPE_STRING && newType == Type.TYPE_BYTES)) {
            changes.add(new SchemaChange(ChangeRules.FIELD_TYPE_CHANGED_COMPATIBLE, path, before, after,
                    "Field " + path + " changed from " + typeName(oldField) + " to "
                            + typeName(newField) + "; the wire encoding is interchangeable but "
                            + "the JSON representation changes.",
                    JSON_BOTH_SOURCE));
            return;
        }
        changes.add(new SchemaChange(ChangeRules.FIELD_TYPE_CHANGED, path, before, after,
                "Field " + path + " changed from " + typeName(oldField) + " to "
                        + typeName(newField) + ", which are not wire-compatible.",
                ALL));
    }

    /**
     * Groups whose members encode identically on the wire. {@code TYPE_ENUM} sits in the varint
     * group: proto3 enums are open, so enum and integer payloads are interchangeable.
     */
    private enum WireGroup { VARINT, ZIGZAG, FIXED32, FIXED64, FLOAT, DOUBLE, STRING, BYTES, MESSAGE, GROUP }

    private static WireGroup wireGroup(Type type) {
        return switch (type) {
            case TYPE_INT32, TYPE_INT64, TYPE_UINT32, TYPE_UINT64, TYPE_BOOL, TYPE_ENUM -> WireGroup.VARINT;
            case TYPE_SINT32, TYPE_SINT64 -> WireGroup.ZIGZAG;
            case TYPE_FIXED32, TYPE_SFIXED32 -> WireGroup.FIXED32;
            case TYPE_FIXED64, TYPE_SFIXED64 -> WireGroup.FIXED64;
            case TYPE_FLOAT -> WireGroup.FLOAT;
            case TYPE_DOUBLE -> WireGroup.DOUBLE;
            case TYPE_STRING -> WireGroup.STRING;
            case TYPE_BYTES -> WireGroup.BYTES;
            case TYPE_MESSAGE -> WireGroup.MESSAGE;
            case TYPE_GROUP -> WireGroup.GROUP;
        };
    }

    // ------------------------------------------------------------------ labels and presence

    private static void diffFieldLabel(String path, FieldDescriptorProto oldField,
                                       FieldDescriptorProto newField, MessageInfo oldMsg,
                                       MessageInfo newMsg, Index oldIndex, Index newIndex,
                                       List<SchemaChange> changes) {
        boolean oldRepeated = oldField.getLabel() == Label.LABEL_REPEATED;
        boolean newRepeated = newField.getLabel() == Label.LABEL_REPEATED;
        String before = snippet(oldField, oldIndex);
        String after = snippet(newField, newIndex);
        if (oldRepeated != newRepeated) {
            changes.add(new SchemaChange(ChangeRules.FIELD_LABEL_CHANGED, path, before, after,
                    "Field " + path + " changed between repeated and singular.", ALL));
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

    // ------------------------------------------------------------------ oneofs

    private static void diffFieldOneof(String path, FieldDescriptorProto oldField,
                                       FieldDescriptorProto newField, DescriptorProto oldMsg,
                                       DescriptorProto newMsg, List<SchemaChange> changes) {
        String oldOneof = realOneofName(oldField, oldMsg);
        String newOneof = realOneofName(newField, newMsg);
        if (oldOneof == null && newOneof != null) {
            changes.add(new SchemaChange(ChangeRules.FIELD_MOVED_INTO_ONEOF, path,
                    oldField.getName(), "oneof " + newOneof + " { " + newField.getName() + " }",
                    "Field " + path + " moved into oneof " + newOneof
                            + "; setting a sibling now clears it.",
                    WIRE_BOTH_SOURCE));
        } else if (oldOneof != null && newOneof == null) {
            changes.add(new SchemaChange(ChangeRules.FIELD_MOVED_OUT_OF_ONEOF, path,
                    "oneof " + oldOneof + " { " + oldField.getName() + " }", newField.getName(),
                    "Field " + path + " moved out of oneof " + oldOneof
                            + "; its presence semantics change.",
                    WIRE_BOTH_SOURCE));
        } else if (oldOneof != null && !oldOneof.equals(newOneof)) {
            changes.add(new SchemaChange(ChangeRules.FIELD_MOVED_INTO_ONEOF, path,
                    "oneof " + oldOneof + " { " + oldField.getName() + " }",
                    "oneof " + newOneof + " { " + newField.getName() + " }",
                    "Field " + path + " moved from oneof " + oldOneof + " into oneof " + newOneof
                            + "; its sibling set changes.",
                    WIRE_BOTH_SOURCE));
        }
    }

    private static void diffOneofs(String fqn, DescriptorProto oldMsg, DescriptorProto newMsg,
                                   List<SchemaChange> changes) {
        Set<String> oldOneofs = realOneofNames(oldMsg);
        Set<String> newOneofs = realOneofNames(newMsg);
        for (String name : oldOneofs) {
            if (!newOneofs.contains(name)) {
                changes.add(new SchemaChange(ChangeRules.ONEOF_REMOVED, fqn + "." + name,
                        "oneof " + name, "",
                        "Oneof " + fqn + "." + name + " was removed.",
                        Set.of(Impact.SOURCE)));
            }
        }
        for (String name : newOneofs) {
            if (!oldOneofs.contains(name)) {
                // Members that already existed are flagged FIELD_MOVED_INTO_ONEOF by the field
                // diff; a brand-new oneof made only of new fields is purely additive.
                changes.add(new SchemaChange(ChangeRules.ONEOF_ADDED, fqn + "." + name,
                        "", "oneof " + name,
                        "Oneof " + fqn + "." + name + " was added.", INFO));
            }
        }
    }

    /** The containing oneof's name, or {@code null} for none or a synthetic proto3-optional oneof. */
    private static String realOneofName(FieldDescriptorProto field, DescriptorProto message) {
        if (!field.hasOneofIndex() || field.getProto3Optional()) {
            return null;
        }
        return message.getOneofDecl(field.getOneofIndex()).getName();
    }

    private static Set<String> realOneofNames(DescriptorProto message) {
        Set<String> synthetic = new HashSet<>();
        for (FieldDescriptorProto field : message.getFieldList()) {
            if (field.getProto3Optional() && field.hasOneofIndex()) {
                synthetic.add(message.getOneofDecl(field.getOneofIndex()).getName());
            }
        }
        Set<String> names = new HashSet<>();
        for (var oneof : message.getOneofDeclList()) {
            if (!synthetic.contains(oneof.getName())) {
                names.add(oneof.getName());
            }
        }
        return names;
    }

    // ------------------------------------------------------------------ reserved

    private static void diffReservedRanges(String fqn, DescriptorProto oldMsg,
                                           DescriptorProto newMsg, List<SchemaChange> changes) {
        for (DescriptorProto.ReservedRange range : oldMsg.getReservedRangeList()) {
            if (!isReservedRange(newMsg, range.getStart(), range.getEnd())) {
                changes.add(new SchemaChange(ChangeRules.RESERVED_RANGE_REMOVED, fqn,
                        reservedRangeText(range), "",
                        "Message " + fqn + " no longer reserves " + reservedRangeText(range)
                                + "; a future reuse would corrupt old payloads.",
                        INFO));
            }
        }
    }

    /**
     * Whether {@code [start, end)} is entirely covered by {@code message}'s reserved ranges.
     * The comparison is by interval: {@code reserved 2 to max} spans over half a billion
     * numbers, so walking a range one number at a time is not viable.
     */
    private static boolean isReservedRange(DescriptorProto message, int start, int end) {
        List<DescriptorProto.ReservedRange> ranges =
                new ArrayList<>(message.getReservedRangeList());
        ranges.sort(Comparator.comparingInt(DescriptorProto.ReservedRange::getStart));
        int covered = start;
        for (DescriptorProto.ReservedRange range : ranges) {
            if (covered >= end) {
                return true;
            }
            if (range.getStart() > covered) {
                return false; // a gap opens before this range begins
            }
            covered = Math.max(covered, range.getEnd());
        }
        return covered >= end;
    }

    private static boolean isReservedNumber(DescriptorProto message, int number) {
        for (DescriptorProto.ReservedRange range : message.getReservedRangeList()) {
            if (number >= range.getStart() && number < range.getEnd()) {
                return true;
            }
        }
        return false;
    }

    private static String reservedRangeSnippet(DescriptorProto message, int number) {
        for (DescriptorProto.ReservedRange range : message.getReservedRangeList()) {
            if (number >= range.getStart() && number < range.getEnd()) {
                return reservedRangeText(range);
            }
        }
        return "";
    }

    private static String reservedRangeText(DescriptorProto.ReservedRange range) {
        int lastInclusive = range.getEnd() - 1;
        return range.getStart() == lastInclusive
                ? "reserved " + range.getStart()
                : "reserved " + range.getStart() + " to " + lastInclusive;
    }

    // ------------------------------------------------------------------ enums

    private static void diffEnums(Index oldIndex, Index newIndex, List<SchemaChange> changes) {
        for (Map.Entry<String, EnumDescriptorProto> entry : oldIndex.enums.entrySet()) {
            String fqn = entry.getKey();
            EnumDescriptorProto newEnum = newIndex.enums.get(fqn);
            if (newEnum == null) {
                changes.add(new SchemaChange(ChangeRules.ENUM_REMOVED, fqn,
                        "enum " + fqn, "",
                        "Enum " + fqn + " was removed.", ALL));
            } else {
                diffEnum(fqn, entry.getValue(), newEnum, changes);
            }
        }
        for (String fqn : newIndex.enums.keySet()) {
            if (!oldIndex.enums.containsKey(fqn)) {
                changes.add(new SchemaChange(ChangeRules.ENUM_ADDED, fqn,
                        "", "enum " + fqn,
                        "Enum " + fqn + " was added.", INFO));
            }
        }
    }

    private static void diffEnum(String fqn, EnumDescriptorProto oldEnum,
                                 EnumDescriptorProto newEnum, List<SchemaChange> changes) {
        Map<Integer, EnumValueDescriptorProto> oldByNumber = valuesByNumber(oldEnum);
        Map<Integer, EnumValueDescriptorProto> newByNumber = valuesByNumber(newEnum);
        Map<String, EnumValueDescriptorProto> oldByName = valuesByName(oldEnum);
        Map<String, EnumValueDescriptorProto> newByName = valuesByName(newEnum);

        for (EnumValueDescriptorProto oldValue : oldByName.values()) {
            EnumValueDescriptorProto newValue = newByName.get(oldValue.getName());
            if (newValue != null && newValue.getNumber() != oldValue.getNumber()) {
                changes.add(new SchemaChange(ChangeRules.ENUM_VALUE_NUMBER_CHANGED,
                        fqn + "." + oldValue.getName(),
                        oldValue.getName() + " = " + oldValue.getNumber(),
                        newValue.getName() + " = " + newValue.getNumber(),
                        "Enum value " + fqn + "." + oldValue.getName() + " changed number from "
                                + oldValue.getNumber() + " to " + newValue.getNumber() + ".",
                        ALL));
            }
        }
        Set<String> reportedGoneNames = new HashSet<>();
        for (EnumValueDescriptorProto oldValue : oldByNumber.values()) {
            EnumValueDescriptorProto newValue = newByNumber.get(oldValue.getNumber());
            if (newValue == null) {
                if (!newByName.containsKey(oldValue.getName())) { // renumber already reported
                    reportedGoneNames.add(oldValue.getName());
                    changes.add(new SchemaChange(ChangeRules.ENUM_VALUE_REMOVED,
                            fqn + "." + oldValue.getName(),
                            oldValue.getName() + " = " + oldValue.getNumber(), "",
                            "Enum value " + fqn + "." + oldValue.getName() + " was removed; the "
                                    + "number still parses (open enum) but the JSON name does not.",
                            Set.of(Impact.JSON_BACKWARD, Impact.SOURCE)));
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
                        JSON_BOTH_SOURCE));
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
            changes.add(new SchemaChange(ChangeRules.ENUM_VALUE_REMOVED,
                    fqn + "." + oldValue.getName(),
                    oldValue.getName() + " = " + oldValue.getNumber(), "",
                    "Enum value " + fqn + "." + oldValue.getName() + " was removed; the "
                            + "number still parses (open enum) but the JSON name does not.",
                    Set.of(Impact.JSON_BACKWARD, Impact.SOURCE)));
        }
        for (EnumValueDescriptorProto newValue : newByNumber.values()) {
            if (!oldByNumber.containsKey(newValue.getNumber())
                    && !oldByName.containsKey(newValue.getName())) {
                changes.add(new SchemaChange(ChangeRules.ENUM_VALUE_ADDED,
                        fqn + "." + newValue.getName(),
                        "", newValue.getName() + " = " + newValue.getNumber(),
                        "Enum value " + fqn + "." + newValue.getName() + " was added.", INFO));
            }
        }
    }

    // ------------------------------------------------------------------ services

    private static void diffServices(Index oldIndex, Index newIndex, List<SchemaChange> changes) {
        for (Map.Entry<String, ServiceDescriptorProto> entry : oldIndex.services.entrySet()) {
            String fqn = entry.getKey();
            ServiceDescriptorProto newService = newIndex.services.get(fqn);
            if (newService == null) {
                changes.add(new SchemaChange(ChangeRules.SERVICE_REMOVED, fqn,
                        "service " + fqn, "",
                        "Service " + fqn + " was removed; old clients still call it.",
                        Set.of(Impact.WIRE_FORWARD, Impact.SOURCE)));
            } else {
                diffService(fqn, entry.getValue(), newService, changes);
            }
        }
        for (String fqn : newIndex.services.keySet()) {
            if (!oldIndex.services.containsKey(fqn)) {
                changes.add(new SchemaChange(ChangeRules.SERVICE_ADDED, fqn,
                        "", "service " + fqn,
                        "Service " + fqn + " was added.", INFO));
            }
        }
    }

    private static void diffService(String fqn, ServiceDescriptorProto oldService,
                                    ServiceDescriptorProto newService,
                                    List<SchemaChange> changes) {
        Map<String, MethodDescriptorProto> newMethods = new LinkedHashMap<>();
        for (MethodDescriptorProto method : newService.getMethodList()) {
            newMethods.put(method.getName(), method);
        }
        Set<String> oldNames = new HashSet<>();
        for (MethodDescriptorProto oldMethod : oldService.getMethodList()) {
            oldNames.add(oldMethod.getName());
            String path = fqn + "." + oldMethod.getName();
            MethodDescriptorProto newMethod = newMethods.get(oldMethod.getName());
            if (newMethod == null) {
                changes.add(new SchemaChange(ChangeRules.METHOD_REMOVED, path,
                        methodSnippet(oldMethod), "",
                        "Method " + path + " was removed; old clients still call it.",
                        Set.of(Impact.WIRE_FORWARD, Impact.SOURCE)));
                continue;
            }
            if (!stripDot(oldMethod.getInputType()).equals(stripDot(newMethod.getInputType()))) {
                changes.add(new SchemaChange(ChangeRules.METHOD_REQUEST_TYPE_CHANGED, path,
                        methodSnippet(oldMethod), methodSnippet(newMethod),
                        "Method " + path + " changed request type from "
                                + stripDot(oldMethod.getInputType()) + " to "
                                + stripDot(newMethod.getInputType()) + ".",
                        WIRE_BOTH_SOURCE));
            }
            if (!stripDot(oldMethod.getOutputType()).equals(stripDot(newMethod.getOutputType()))) {
                changes.add(new SchemaChange(ChangeRules.METHOD_RESPONSE_TYPE_CHANGED, path,
                        methodSnippet(oldMethod), methodSnippet(newMethod),
                        "Method " + path + " changed response type from "
                                + stripDot(oldMethod.getOutputType()) + " to "
                                + stripDot(newMethod.getOutputType()) + ".",
                        WIRE_BOTH_SOURCE));
            }
            if (oldMethod.getClientStreaming() != newMethod.getClientStreaming()
                    || oldMethod.getServerStreaming() != newMethod.getServerStreaming()) {
                changes.add(new SchemaChange(ChangeRules.METHOD_STREAMING_CHANGED, path,
                        methodSnippet(oldMethod), methodSnippet(newMethod),
                        "Method " + path + " changed its streaming shape.",
                        WIRE_BOTH_SOURCE));
            }
        }
        for (MethodDescriptorProto newMethod : newService.getMethodList()) {
            if (!oldNames.contains(newMethod.getName())) {
                changes.add(new SchemaChange(ChangeRules.METHOD_ADDED, fqn + "." + newMethod.getName(),
                        "", methodSnippet(newMethod),
                        "Method " + fqn + "." + newMethod.getName() + " was added.", INFO));
            }
        }
    }

    // ------------------------------------------------------------------ snippets and helpers

    private static Map<Integer, FieldDescriptorProto> fieldsByNumber(DescriptorProto message) {
        Map<Integer, FieldDescriptorProto> byNumber = new LinkedHashMap<>();
        for (FieldDescriptorProto field : message.getFieldList()) {
            byNumber.putIfAbsent(field.getNumber(), field);
        }
        return byNumber;
    }

    private static Map<Integer, EnumValueDescriptorProto> valuesByNumber(EnumDescriptorProto e) {
        Map<Integer, EnumValueDescriptorProto> byNumber = new LinkedHashMap<>();
        for (EnumValueDescriptorProto value : e.getValueList()) {
            byNumber.putIfAbsent(value.getNumber(), value); // aliases: first declaration wins
        }
        return byNumber;
    }

    private static Map<String, EnumValueDescriptorProto> valuesByName(EnumDescriptorProto e) {
        Map<String, EnumValueDescriptorProto> byName = new LinkedHashMap<>();
        for (EnumValueDescriptorProto value : e.getValueList()) {
            byName.putIfAbsent(value.getName(), value);
        }
        return byName;
    }

    private static MessageInfo mapEntryOf(FieldDescriptorProto field, Index index) {
        if (field.getType() != Type.TYPE_MESSAGE) {
            return null;
        }
        MessageInfo message = index.messages.get(typeFqn(field));
        return message != null && message.mapEntry() ? message : null;
    }

    private static FieldDescriptorProto keyField(MessageInfo entry) {
        return entryField(entry, 1);
    }

    private static FieldDescriptorProto valueField(MessageInfo entry) {
        return entryField(entry, 2);
    }

    private static FieldDescriptorProto entryField(MessageInfo entry, int number) {
        for (FieldDescriptorProto field : entry.proto().getFieldList()) {
            if (field.getNumber() == number) {
                return field;
            }
        }
        throw new IllegalStateException(
                "Map entry " + entry.proto().getName() + " lacks field " + number);
    }

    private static String typeFqn(FieldDescriptorProto field) {
        return stripDot(field.getTypeName());
    }

    private static String stripDot(String typeName) {
        return typeName.startsWith(".") ? typeName.substring(1) : typeName;
    }

    private static String typeName(FieldDescriptorProto field) {
        if (!field.getTypeName().isEmpty()) {
            return typeFqn(field);
        }
        // TYPE_INT32 -> int32
        return field.getType().name().substring("TYPE_".length()).toLowerCase(java.util.Locale.ROOT);
    }

    /** {@code repeated string tags = 3}-style declaration snippet, map-aware. */
    private static String snippet(FieldDescriptorProto field, Index index) {
        MessageInfo entry = mapEntryOf(field, index);
        if (entry != null) {
            return "map<" + typeName(keyField(entry)) + ", " + typeName(valueField(entry)) + "> "
                    + field.getName() + " = " + field.getNumber();
        }
        String label = switch (field.getLabel()) {
            case LABEL_REPEATED -> "repeated ";
            case LABEL_REQUIRED -> "required ";
            case LABEL_OPTIONAL -> field.getProto3Optional() ? "optional " : "";
        };
        return label + typeName(field) + " " + field.getName() + " = " + field.getNumber();
    }

    private static String methodSnippet(MethodDescriptorProto method) {
        return "rpc " + method.getName() + "("
                + (method.getClientStreaming() ? "stream " : "") + stripDot(method.getInputType())
                + ") returns ("
                + (method.getServerStreaming() ? "stream " : "") + stripDot(method.getOutputType())
                + ")";
    }

    /** The JSON payload key: explicit {@code json_name} if declared, else the derived camelCase. */
    private static String effectiveJsonName(FieldDescriptorProto field) {
        if (field.hasJsonName()) {
            return field.getJsonName();
        }
        StringBuilder json = new StringBuilder(field.getName().length());
        boolean upperNext = false;
        for (char c : field.getName().toCharArray()) {
            if (c == '_') {
                upperNext = true;
            } else if (upperNext) {
                json.append(Character.toUpperCase(c));
                upperNext = false;
            } else {
                json.append(c);
            }
        }
        return json.toString();
    }

    // ------------------------------------------------------------------ indexing

    private record MessageInfo(DescriptorProto proto, boolean proto3, boolean mapEntry) {
    }

    /** FQN-keyed view of one descriptor set; nested types are indexed at their full names. */
    private static final class Index {

        final Map<String, MessageInfo> messages = new LinkedHashMap<>();
        final Map<String, EnumDescriptorProto> enums = new LinkedHashMap<>();
        final Map<String, ServiceDescriptorProto> services = new LinkedHashMap<>();

        static Index of(FileDescriptorSet set) {
            Index index = new Index();
            for (FileDescriptorProto file : set.getFileList()) {
                String prefix = file.getPackage().isEmpty() ? "" : file.getPackage() + ".";
                boolean proto3 = "proto3".equals(file.getSyntax());
                for (DescriptorProto message : file.getMessageTypeList()) {
                    index.addMessage(prefix + message.getName(), message, proto3);
                }
                for (EnumDescriptorProto enumType : file.getEnumTypeList()) {
                    index.enums.putIfAbsent(prefix + enumType.getName(), enumType);
                }
                for (ServiceDescriptorProto service : file.getServiceList()) {
                    index.services.putIfAbsent(prefix + service.getName(), service);
                }
            }
            return index;
        }

        private void addMessage(String fqn, DescriptorProto message, boolean proto3) {
            messages.putIfAbsent(fqn,
                    new MessageInfo(message, proto3, message.getOptions().getMapEntry()));
            for (DescriptorProto nested : message.getNestedTypeList()) {
                addMessage(fqn + "." + nested.getName(), nested, proto3);
            }
            for (EnumDescriptorProto enumType : message.getEnumTypeList()) {
                enums.putIfAbsent(fqn + "." + enumType.getName(), enumType);
            }
        }
    }
}
