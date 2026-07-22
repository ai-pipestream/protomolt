package ai.pipestream.proto.mapper;

import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;

/** Small dynamic schema shared by mapper tests; no application message stubs required. */
final class TestDescriptors {
    static final FileDescriptor FILE;
    static final Descriptor DOCUMENT;
    static final Descriptor INFO;

    static {
        try {
            var info = DescriptorProtos.DescriptorProto.newBuilder().setName("Info")
                    .addField(field("version", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                    .addField(field("enabled", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)).build();
            var document = DescriptorProtos.DescriptorProto.newBuilder().setName("Document")
                    .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                    .addField(field("title", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                    .addField(field("body", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                    .addField(field("language", 4, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                    .addField(field("score", 5, DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE))
                    .addField(field("tags", 6, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
                    .addField(field("metadata", 7, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName(".google.protobuf.Struct"))
                    .addField(field("payload", 8, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName(".google.protobuf.Any"))
                    .addField(field("info", 9, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName(".test.Info"))
                    .addField(field("counts", 10, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)).build();
            FILE = FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                    .setName("test_document.proto").setPackage("test")
                    .addDependency(Struct.getDescriptor().getFile().getFullName())
                    .addDependency(Any.getDescriptor().getFile().getFullName())
                    .addMessageType(info).addMessageType(document).build(),
                    new FileDescriptor[]{Struct.getDescriptor().getFile(), Any.getDescriptor().getFile()});
            DOCUMENT = FILE.findMessageTypeByName("Document");
            INFO = FILE.findMessageTypeByName("Info");
        } catch (Exception exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    static DynamicMessage.Builder document() {
        return DynamicMessage.newBuilder(DOCUMENT);
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder field(
            String name, int number, DescriptorProtos.FieldDescriptorProto.Type type) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type);
    }
}
