package ai.protomolt.proto.pipeline;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The unnest and fan-out path walk: a path resolves only through singular message fields
 * and ends at a repeated message field, and every refusal names the path and the segment.
 */
class StreamPathsTest {

    private static final FileDescriptor FILE = file();
    private static final Descriptor ROOT = FILE.findMessageTypeByName("Root");
    private static final Descriptor HOLDER = FILE.findMessageTypeByName("Holder");
    private static final Descriptor ITEM = FILE.findMessageTypeByName("Item");

    @Test
    void aTopLevelRepeatedMessageFieldResolves() {
        assertThat(StreamPaths.repeatedField(ROOT, "items"))
                .isEqualTo(ROOT.findFieldByName("items"));
        assertThat(StreamPaths.elementType(ROOT, "items")).isEqualTo(ITEM);
    }

    @Test
    void aNestedPathWalksSingularMessageFields() {
        assertThat(StreamPaths.repeatedField(ROOT, "holder.items"))
                .isEqualTo(HOLDER.findFieldByName("items"));
        assertThat(StreamPaths.elementType(ROOT, "holder.items")).isEqualTo(ITEM);
    }

    @Test
    void anUnknownSegmentNamesTheTypeItWasLookedUpOn() {
        assertThatThrownBy(() -> StreamPaths.repeatedField(ROOT, "holder.missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("path 'holder.missing' does not resolve: "
                        + "paths.test.Holder has no field 'missing'");
    }

    @Test
    void anEmptyPathFailsOnTheFieldLookup() {
        assertThatThrownBy(() -> StreamPaths.repeatedField(ROOT, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("path '' does not resolve: paths.test.Root has no field ''");
    }

    @Test
    void aPathEndingAtASingularFieldIsRefused() {
        assertThatThrownBy(() -> StreamPaths.repeatedField(ROOT, "holder.single"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("path 'holder.single' ends at a non-repeated field");
    }

    @Test
    void aPathEndingAtARepeatedScalarIsRefused() {
        assertThatThrownBy(() -> StreamPaths.repeatedField(ROOT, "holder.tags"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("path 'holder.tags' ends at a non-message field; streams carry messages");
    }

    @Test
    void aPathThroughARepeatedFieldIsRefused() {
        assertThatThrownBy(() -> StreamPaths.repeatedField(ROOT, "holders.items"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("path 'holders.items' walks through 'holders', "
                        + "which is not a singular message field");
    }

    @Test
    void aPathThroughAScalarFieldIsRefused() {
        assertThatThrownBy(() -> StreamPaths.repeatedField(ROOT, "note.items"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("path 'note.items' walks through 'note', "
                        + "which is not a singular message field");
    }

    private static FileDescriptor file() {
        FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                .setName("paths_test.proto")
                .setPackage("paths.test")
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
            field.setTypeName(".paths.test." + messageType);
        }
        return field.build();
    }
}
