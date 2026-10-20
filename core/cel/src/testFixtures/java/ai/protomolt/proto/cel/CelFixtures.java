package ai.protomolt.proto.cel;

import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;

/**
 * A small dynamic {@code celtest} schema shared by the CEL and CEL-mapper tests: a
 * {@code Document} with scalar, {@link Struct} and nested {@code Info} fields.
 */
public final class CelFixtures {
    /** The {@code cel_fixtures.proto} file holding both message types. */
    public static final FileDescriptor FILE;
    /** The {@code celtest.Document} message type. */
    public static final Descriptor DOCUMENT;
    /** The {@code celtest.Info} message type nested in {@code Document.info}. */
    public static final Descriptor INFO;
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

    private CelFixtures() {
    }

    /**
     * Starts a {@code Document} with only its title set.
     *
     * @param title the title field value
     * @return a mutable document builder
     */
    public static DynamicMessage.Builder doc(String title) {
        return DynamicMessage.newBuilder(DOCUMENT).setField(DOCUMENT.findFieldByName("title"), title);
    }
    private static DescriptorProtos.FieldDescriptorProto.Builder field(String n, int i, DescriptorProtos.FieldDescriptorProto.Type t) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder().setName(n).setNumber(i).setType(t);
    }
}
