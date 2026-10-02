package ai.protomolt.proto.compat;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Oneof membership, from both ends: the oneofs a message declares, and which oneof a given field
 * sits in. Synthetic proto3-optional oneofs are invisible to these rules — the compiler
 * manufactures one per {@code optional} field and it carries no author intent.
 */
final class OneofDiff {

    private OneofDiff() {
    }

    /** Oneofs the message gained or lost, by name. */
    static void diffDeclarations(String fqn, DescriptorProto oldMsg, DescriptorProto newMsg,
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
                        "Oneof " + fqn + "." + name + " was added.", DiffImpacts.INFO));
            }
        }
    }

    /** Whether one matched field moved into, out of, or between oneofs. */
    static void diffMembership(String path, FieldDescriptorProto oldField,
                               FieldDescriptorProto newField, DescriptorProto oldMsg,
                               DescriptorProto newMsg, List<SchemaChange> changes) {
        String oldOneof = realOneofName(oldField, oldMsg);
        String newOneof = realOneofName(newField, newMsg);
        if (oldOneof == null && newOneof != null) {
            changes.add(new SchemaChange(ChangeRules.FIELD_MOVED_INTO_ONEOF, path,
                    oldField.getName(), "oneof " + newOneof + " { " + newField.getName() + " }",
                    "Field " + path + " moved into oneof " + newOneof
                            + "; setting a sibling now clears it.",
                    DiffImpacts.WIRE_BOTH_SOURCE));
        } else if (oldOneof != null && newOneof == null) {
            changes.add(new SchemaChange(ChangeRules.FIELD_MOVED_OUT_OF_ONEOF, path,
                    "oneof " + oldOneof + " { " + oldField.getName() + " }", newField.getName(),
                    "Field " + path + " moved out of oneof " + oldOneof
                            + "; its presence semantics change.",
                    DiffImpacts.WIRE_BOTH_SOURCE));
        } else if (oldOneof != null && !oldOneof.equals(newOneof)) {
            changes.add(new SchemaChange(ChangeRules.FIELD_MOVED_INTO_ONEOF, path,
                    "oneof " + oldOneof + " { " + oldField.getName() + " }",
                    "oneof " + newOneof + " { " + newField.getName() + " }",
                    "Field " + path + " moved from oneof " + oldOneof + " into oneof " + newOneof
                            + "; its sibling set changes.",
                    DiffImpacts.WIRE_BOTH_SOURCE));
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
}
