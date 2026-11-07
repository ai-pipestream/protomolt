package ai.protomolt.proto.helpers;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageDiffEdgeCasesTest {

    private static Descriptor person;

    @BeforeAll
    static void setUp() throws Exception {
        DescriptorProtos.DescriptorProto msg = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Person")
                .addField(field("name", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(field("age", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("tags").setNumber(3)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                        .build())
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("attrs").setNumber(4)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".edgecase.Person.AttrsEntry")
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                        .build())
                .addNestedType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("AttrsEntry")
                        .addField(field("key", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(field("value", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .setOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true))
                        .build())
                .build();
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("edge.proto").setPackage("edgecase").addMessageType(msg).build();
        person = FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Person");
    }

    private static DescriptorProtos.FieldDescriptorProto field(
            String name, int number, DescriptorProtos.FieldDescriptorProto.Type type) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
    }

    @Test
    void bothEmptyProduceEmptyDiff() {
        var left = DynamicMessage.newBuilder(person).build();
        var right = DynamicMessage.newBuilder(person).build();
        assertThat(MessageDiff.diff(left, right)).isEmpty();
    }

    @Test
    void oneSidedFieldIsReported() {
        var left = DynamicMessage.newBuilder(person)
                .setField(person.findFieldByName("name"), "Ada").build();
        var right = DynamicMessage.newBuilder(person).build();
        List<MessageDiff.FieldChange> changes = MessageDiff.diff(left, right);
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).path()).isEqualTo("name");
        assertThat(changes.get(0).left()).isEqualTo("Ada");
        assertThat(changes.get(0).right()).isNull();
    }

    @Test
    void repeatedOrderMatters() {
        var left = DynamicMessage.newBuilder(person)
                .addRepeatedField(person.findFieldByName("tags"), "a")
                .addRepeatedField(person.findFieldByName("tags"), "b").build();
        var right = DynamicMessage.newBuilder(person)
                .addRepeatedField(person.findFieldByName("tags"), "b")
                .addRepeatedField(person.findFieldByName("tags"), "a").build();
        assertThat(MessageDiff.diff(left, right)).hasSize(1);
    }

    @Test
    void mapOrderIsIrrelevant() {
        var attrsField = person.findFieldByName("attrs");
        Descriptor entry = person.findNestedTypeByName("AttrsEntry");
        var e1 = DynamicMessage.newBuilder(entry)
                .setField(entry.findFieldByName("key"), "k1")
                .setField(entry.findFieldByName("value"), "v1").build();
        var e2 = DynamicMessage.newBuilder(entry)
                .setField(entry.findFieldByName("key"), "k2")
                .setField(entry.findFieldByName("value"), "v2").build();
        var left = DynamicMessage.newBuilder(person)
                .addRepeatedField(attrsField, e1).addRepeatedField(attrsField, e2).build();
        var right = DynamicMessage.newBuilder(person)
                .addRepeatedField(attrsField, e2).addRepeatedField(attrsField, e1).build();
        assertThat(MessageDiff.diff(left, right)).isEmpty();
    }

    @Test
    void mapValueChangeIsReported() {
        var attrsField = person.findFieldByName("attrs");
        Descriptor entry = person.findNestedTypeByName("AttrsEntry");
        var e1 = DynamicMessage.newBuilder(entry)
                .setField(entry.findFieldByName("key"), "k1")
                .setField(entry.findFieldByName("value"), "v1").build();
        var e2 = DynamicMessage.newBuilder(entry)
                .setField(entry.findFieldByName("key"), "k1")
                .setField(entry.findFieldByName("value"), "v2").build();
        var left = DynamicMessage.newBuilder(person).addRepeatedField(attrsField, e1).build();
        var right = DynamicMessage.newBuilder(person).addRepeatedField(attrsField, e2).build();
        List<MessageDiff.FieldChange> changes = MessageDiff.diff(left, right);
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).path()).isEqualTo("attrs");
    }

    @Test
    void mismatchedTypesThrow() {
        Struct s = Struct.getDefaultInstance();
        var p = DynamicMessage.newBuilder(person).build();
        assertThrows(IllegalArgumentException.class, () -> MessageDiff.diff(s, p));
    }

    @Test
    void structWithSameContentHasNoDiff() {
        Struct left = Struct.newBuilder()
                .putFields("x", Value.newBuilder().setBoolValue(true).build()).build();
        Struct right = Struct.newBuilder()
                .putFields("x", Value.newBuilder().setBoolValue(true).build()).build();
        assertThat(MessageDiff.diff(left, right)).isEmpty();
    }
}
