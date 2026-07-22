package ai.pipestream.proto.cel;

import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;

final class CelFixtures {
    static final FileDescriptor FILE;
    static final Descriptor DOCUMENT;
    static final Descriptor INFO;
    static {
        try {
            var info = DescriptorProtos.DescriptorProto.newBuilder().setName("Info")
                    .addField(field("version", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)).build();
            var document = DescriptorProtos.DescriptorProto.newBuilder().setName("Document")
                    .addField(field("title", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                    .addField(field("body", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                    .addField(field("language", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                    .addField(field("score", 4, DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE))
                    .addField(field("tier", 5, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                    .addField(field("metadata", 6, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".google.protobuf.Struct"))
                    .addField(field("info", 7, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".celtest.Info")).build();
            FILE = FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                    .setName("cel_fixtures.proto").setPackage("celtest")
                    .addDependency(Struct.getDescriptor().getFile().getFullName()).addDependency(Any.getDescriptor().getFile().getFullName())
                    .addMessageType(info).addMessageType(document).build(),
                    new FileDescriptor[]{Struct.getDescriptor().getFile(), Any.getDescriptor().getFile()});
            DOCUMENT = FILE.findMessageTypeByName("Document"); INFO = FILE.findMessageTypeByName("Info");
        } catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }
    static DynamicMessage.Builder doc(String title) {
        return DynamicMessage.newBuilder(DOCUMENT).setField(DOCUMENT.findFieldByName("title"), title);
    }
    static DescriptorProtos.FieldDescriptorProto.Builder field(String n, int i, DescriptorProtos.FieldDescriptorProto.Type t) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder().setName(n).setNumber(i).setType(t);
    }
}
