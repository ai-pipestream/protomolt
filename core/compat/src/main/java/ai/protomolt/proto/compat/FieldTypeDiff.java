package ai.protomolt.proto.compat;

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;

import java.util.List;
import java.util.Set;

/**
 * The type rules for one matched field: whether the old and new declarations encode the same way
 * on the wire, and what a difference costs.
 *
 * <p>A map field is diffed through its synthetic entry message rather than as a message type
 * change, so a map's key and value are each compared at paths like
 * {@code example.Doc.attrs (map value)}.</p>
 */
final class FieldTypeDiff {

    private FieldTypeDiff() {
    }

    static void diff(String path, FieldDescriptorProto oldField, FieldDescriptorProto newField,
                     DescriptorIndex oldIndex, DescriptorIndex newIndex,
                     List<SchemaChange> changes) {
        MessageInfo oldEntry = DescriptorIndex.mapEntryOf(oldField, oldIndex);
        MessageInfo newEntry = DescriptorIndex.mapEntryOf(newField, newIndex);
        if (oldEntry != null && newEntry != null) {
            diff(path + " (map key)", DescriptorIndex.keyField(oldEntry),
                    DescriptorIndex.keyField(newEntry), oldIndex, newIndex, changes);
            diff(path + " (map value)", DescriptorIndex.valueField(oldEntry),
                    DescriptorIndex.valueField(newEntry), oldIndex, newIndex, changes);
            return;
        }
        if (oldEntry != null || newEntry != null) {
            changes.add(new SchemaChange(ChangeRules.FIELD_MAP_ENTRY_CHANGED, path,
                    DescriptorSnippets.snippet(oldField, oldIndex),
                    DescriptorSnippets.snippet(newField, newIndex),
                    "Field " + path + " changed between a map and a repeated message; presence "
                            + "of last-key-wins map semantics and the JSON shape (object vs "
                            + "array) both change.",
                    DiffImpacts.ALL));
            return;
        }
        Type oldType = oldField.getType();
        Type newType = newField.getType();
        boolean oldMessage = oldType == Type.TYPE_MESSAGE || oldType == Type.TYPE_GROUP;
        boolean newMessage = newType == Type.TYPE_MESSAGE || newType == Type.TYPE_GROUP;
        if (oldMessage && newMessage) {
            String oldFqn = DescriptorSnippets.typeFqn(oldField);
            String newFqn = DescriptorSnippets.typeFqn(newField);
            if (!oldFqn.equals(newFqn)) {
                changes.add(new SchemaChange(ChangeRules.FIELD_MESSAGE_TYPE_CHANGED, path,
                        DescriptorSnippets.snippet(oldField, oldIndex),
                        DescriptorSnippets.snippet(newField, newIndex),
                        "Field " + path + " changed message type from " + oldFqn + " to "
                                + newFqn + ".",
                        DiffImpacts.ALL));
            }
            return;
        }
        if (oldType == Type.TYPE_ENUM && newType == Type.TYPE_ENUM) {
            String oldFqn = DescriptorSnippets.typeFqn(oldField);
            String newFqn = DescriptorSnippets.typeFqn(newField);
            if (!oldFqn.equals(newFqn)) {
                changes.add(new SchemaChange(ChangeRules.FIELD_ENUM_TYPE_CHANGED, path,
                        DescriptorSnippets.snippet(oldField, oldIndex),
                        DescriptorSnippets.snippet(newField, newIndex),
                        "Field " + path + " changed enum type from " + oldFqn + " to "
                                + newFqn + ".",
                        DiffImpacts.ALL));
            }
            return;
        }
        if (oldType == newType) {
            return;
        }
        String before = DescriptorSnippets.snippet(oldField, oldIndex);
        String after = DescriptorSnippets.snippet(newField, newIndex);
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
                    "Field " + path + " changed from " + DescriptorSnippets.typeName(oldField)
                            + " to " + DescriptorSnippets.typeName(newField)
                            + "; the wire encoding is interchangeable but "
                            + "the JSON representation changes.",
                    DiffImpacts.JSON_BOTH_SOURCE));
            return;
        }
        changes.add(new SchemaChange(ChangeRules.FIELD_TYPE_CHANGED, path, before, after,
                "Field " + path + " changed from " + DescriptorSnippets.typeName(oldField)
                        + " to " + DescriptorSnippets.typeName(newField)
                        + ", which are not wire-compatible.",
                DiffImpacts.ALL));
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
}
