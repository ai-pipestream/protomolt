package ai.protomolt.proto.compat;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FQN-keyed view of one descriptor set; nested types are indexed at their full names. Matching
 * across the whole set by fully-qualified name is what lets a type move between files without
 * registering as a change.
 *
 * <p>The map-entry accessors live here because recognising a synthetic map entry requires the
 * index: a field only names its entry message, and whether that message carries
 * {@code options.map_entry} is a property of the set, not of the field.</p>
 */
final class DescriptorIndex {

    final Map<String, MessageInfo> messages = new LinkedHashMap<>();
    final Map<String, EnumDescriptorProto> enums = new LinkedHashMap<>();
    final Map<String, ServiceDescriptorProto> services = new LinkedHashMap<>();

    static DescriptorIndex of(FileDescriptorSet set) {
        DescriptorIndex index = new DescriptorIndex();
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

    // ------------------------------------------------------------------ member lookup

    static Map<Integer, FieldDescriptorProto> fieldsByNumber(DescriptorProto message) {
        Map<Integer, FieldDescriptorProto> byNumber = new LinkedHashMap<>();
        for (FieldDescriptorProto field : message.getFieldList()) {
            byNumber.putIfAbsent(field.getNumber(), field);
        }
        return byNumber;
    }

    static Map<Integer, EnumValueDescriptorProto> valuesByNumber(EnumDescriptorProto e) {
        Map<Integer, EnumValueDescriptorProto> byNumber = new LinkedHashMap<>();
        for (EnumValueDescriptorProto value : e.getValueList()) {
            byNumber.putIfAbsent(value.getNumber(), value); // aliases: first declaration wins
        }
        return byNumber;
    }

    static Map<String, EnumValueDescriptorProto> valuesByName(EnumDescriptorProto e) {
        Map<String, EnumValueDescriptorProto> byName = new LinkedHashMap<>();
        for (EnumValueDescriptorProto value : e.getValueList()) {
            byName.putIfAbsent(value.getName(), value);
        }
        return byName;
    }

    // ------------------------------------------------------------------ map entries

    /** The synthetic entry message behind a map field, or {@code null} if the field is not a map. */
    static MessageInfo mapEntryOf(FieldDescriptorProto field, DescriptorIndex index) {
        if (field.getType() != Type.TYPE_MESSAGE) {
            return null;
        }
        MessageInfo message = index.messages.get(DescriptorSnippets.typeFqn(field));
        return message != null && message.mapEntry() ? message : null;
    }

    static FieldDescriptorProto keyField(MessageInfo entry) {
        return entryField(entry, 1);
    }

    static FieldDescriptorProto valueField(MessageInfo entry) {
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
}
