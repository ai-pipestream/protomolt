package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Covers {@link ProtoFieldMapper#getValue(com.google.protobuf.MessageOrBuilder, String, boolean)}. */
class IncludeDefaultsTest {

    private final ProtoFieldMapper mapper = new ProtoFieldMapperImpl(new DescriptorRegistry());

    @Test
    void implicitPresenceDefaultsSurfaceOnlyWhenRequested() throws Exception {
        Descriptor descriptor = docDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();

        // default behaviour unchanged: fields at default read as null
        assertThat(mapper.getValue(message, "archived")).isNull();
        assertThat(mapper.getValue(message, "count")).isNull();
        assertThat(mapper.getValue(message, "title")).isNull();
        assertThat(mapper.getValue(message, "archived", false)).isNull();

        // includeDefaults surfaces the proto3 implicit-presence defaults
        assertThat(mapper.getValue(message, "archived", true)).isEqualTo(false);
        assertThat(mapper.getValue(message, "count", true)).isEqualTo(0);
        assertThat(mapper.getValue(message, "title", true)).isEqualTo("");
    }

    @Test
    void explicitPresenceFieldsStayNullWhenUnset() throws Exception {
        Descriptor descriptor = docDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();

        // optional (explicit presence) fields are genuinely absent, not defaulted
        assertThat(mapper.getValue(message, "maybe_flag", true)).isNull();
    }

    @Test
    void setValuesAreReturnedEitherWay() throws Exception {
        Descriptor descriptor = docDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("archived"), true)
                .setField(descriptor.findFieldByName("count"), 3)
                .build();

        assertThat(mapper.getValue(message, "archived", false)).isEqualTo(true);
        assertThat(mapper.getValue(message, "archived", true)).isEqualTo(true);
        assertThat(mapper.getValue(message, "count", true)).isEqualTo(3);
    }

    private static Descriptor docDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("defaults_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("archived")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("count")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("title")
                                .setNumber(3)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("maybe_flag")
                                .setNumber(4)
                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setProto3Optional(true)
                                .setOneofIndex(0))
                        .addOneofDecl(com.google.protobuf.DescriptorProtos.OneofDescriptorProto.newBuilder()
                                .setName("_maybe_flag")))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Doc");
    }
}
