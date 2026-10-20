package ai.protomolt.proto.workflow;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The typed edge's fan-out mechanics: the items path walk and its refusals, reading the
 * items through a nested holder, and collecting branch outputs in order.
 */
class EdgeFlowTest {

    private static final FileDescriptor FILE = file();
    private static final Descriptor ROOT = FILE.findMessageTypeByName("Root");
    private static final Descriptor HOLDER = FILE.findMessageTypeByName("Holder");
    private static final Descriptor ITEM = FILE.findMessageTypeByName("Item");

    @Test
    void aNestedItemsPathResolvesToTheRepeatedField() {
        assertThat(EdgeFlow.itemsField(ROOT, "holder.items"))
                .isEqualTo(HOLDER.findFieldByName("items"));
        assertThat(EdgeFlow.itemType(ROOT, "items")).isEqualTo(ITEM);
    }

    @Test
    void anUnknownSegmentNamesTheTypeItWasLookedUpOn() {
        assertThatThrownBy(() -> EdgeFlow.itemsField(ROOT, "holder.missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("items path 'holder.missing' does not resolve: "
                        + "edges.test.Holder has no field 'missing'");
    }

    @Test
    void anEmptyItemsPathFailsOnTheFieldLookup() {
        assertThatThrownBy(() -> EdgeFlow.itemsField(ROOT, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("items path '' does not resolve: edges.test.Root has no field ''");
    }

    @Test
    void anItemsPathEndingAtASingularFieldIsRefused() {
        assertThatThrownBy(() -> EdgeFlow.itemsField(ROOT, "holder.single"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("items path 'holder.single' ends at a non-repeated field");
    }

    @Test
    void anItemsPathEndingAtARepeatedScalarIsRefused() {
        assertThatThrownBy(() -> EdgeFlow.itemsField(ROOT, "holder.tags"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("items path 'holder.tags' ends at a non-message field; "
                        + "branches execute on messages");
    }

    @Test
    void anItemsPathThroughARepeatedOrScalarFieldIsRefused() {
        assertThatThrownBy(() -> EdgeFlow.itemsField(ROOT, "holders.items"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("items path 'holders.items' walks through 'holders', "
                        + "which is not a singular message field");
        assertThatThrownBy(() -> EdgeFlow.itemsField(ROOT, "note.items"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("items path 'note.items' walks through 'note', "
                        + "which is not a singular message field");
    }

    @Test
    void itemsAreReadThroughTheNestedHolderInFieldOrder() {
        Message first = item("first");
        Message second = item("second");
        FieldDescriptor items = HOLDER.findFieldByName("items");
        DynamicMessage holder = DynamicMessage.newBuilder(HOLDER)
                .addRepeatedField(items, first)
                .addRepeatedField(items, second)
                .build();
        DynamicMessage root = DynamicMessage.newBuilder(ROOT)
                .setField(ROOT.findFieldByName("holder"), holder)
                .build();

        assertThat(EdgeFlow.items(root, "holder.items")).containsExactly(first, second);
    }

    @Test
    void anUnsetHolderYieldsNoItems() {
        assertThat(EdgeFlow.items(DynamicMessage.getDefaultInstance(ROOT), "holder.items"))
                .isEmpty();
    }

    @Test
    void collectedOutputsKeepTheirBranchOrder() {
        Message first = item("first");
        Message second = item("second");

        DynamicMessage collected = EdgeFlow.collect(HOLDER, "items", List.of(second, first));

        assertThat(collected.getField(HOLDER.findFieldByName("items")))
                .isEqualTo(List.of(second, first));
    }

    @Test
    void aCollectTargetMustBeARepeatedFieldOfTheType() {
        assertThatThrownBy(() -> EdgeFlow.collect(HOLDER, "missing", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("collect target 'missing' is not a repeated field of edges.test.Holder");
        assertThatThrownBy(() -> EdgeFlow.collect(HOLDER, "single", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("collect target 'single' is not a repeated field of edges.test.Holder");
    }

    private static Message item(String name) {
        return DynamicMessage.newBuilder(ITEM).setField(ITEM.findFieldByName("name"), name).build();
    }

    private static FileDescriptor file() {
        FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                .setName("edges_test.proto")
                .setPackage("edges.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Item")
                        .addField(field("name", 1, Label.LABEL_OPTIONAL, Type.TYPE_STRING, null)))
                .addMessageType(DescriptorProto.newBuilder().setName("Holder")
                        .addField(field("items", 1, Label.LABEL_REPEATED, Type.TYPE_MESSAGE, "Item"))
                        .addField(field("tags", 2, Label.LABEL_REPEATED, Type.TYPE_STRING, null))
                        .addField(field("single", 3, Label.LABEL_OPTIONAL, Type.TYPE_MESSAGE, "Item")))
                .addMessageType(DescriptorProto.newBuilder().setName("Root")
                        .addField(field("holder", 1, Label.LABEL_OPTIONAL, Type.TYPE_MESSAGE, "Holder"))
                        .addField(field("holders", 2, Label.LABEL_REPEATED, Type.TYPE_MESSAGE, "Holder"))
                        .addField(field("note", 3, Label.LABEL_OPTIONAL, Type.TYPE_STRING, null))
                        .addField(field("items", 4, Label.LABEL_REPEATED, Type.TYPE_MESSAGE, "Item")))
                .build();
        try {
            return FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
        } catch (DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static FieldDescriptorProto field(String name, int number, Label label, Type type,
                                              String messageType) {
        FieldDescriptorProto.Builder field = FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setLabel(label).setType(type);
        if (messageType != null) {
            field.setTypeName(".edges.test." + messageType);
        }
        return field.build();
    }
}
