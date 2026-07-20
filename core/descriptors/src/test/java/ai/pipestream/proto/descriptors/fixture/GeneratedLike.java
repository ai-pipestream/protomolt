package ai.pipestream.proto.descriptors.fixture;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;

/**
 * Stands in for a protobuf-generated class in the shape {@code ClasspathDescriptorLoader} looks
 * for: a {@link Message} subtype whose fully qualified Java name equals the proto type name, with
 * a static {@code getDescriptor()}. Abstract because only the type and that static method are
 * exercised; no instance is ever created.
 */
public abstract class GeneratedLike implements Message {

    private static final FileDescriptor FILE = buildFile();

    private GeneratedLike() {
    }

    public static Descriptor getDescriptor() {
        return FILE.getMessageTypes().get(0);
    }

    /** A nested type, whose binary class name uses {@code $} where the proto name uses a dot. */
    public abstract static class Inner implements Message {

        private Inner() {
        }

        public static Descriptor getDescriptor() {
            return GeneratedLike.getDescriptor().getNestedTypes().get(0);
        }
    }

    private static FileDescriptor buildFile() {
        FileDescriptorProto proto = FileDescriptorProto.newBuilder()
            .setName("fixture/generated_like.proto")
            .setSyntax("proto3")
            .setPackage("ai.pipestream.proto.descriptors.fixture")
            .addMessageType(DescriptorProto.newBuilder()
                .setName("GeneratedLike")
                .addField(FieldDescriptorProto.newBuilder()
                    .setName("id")
                    .setNumber(1)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addNestedType(DescriptorProto.newBuilder().setName("Inner")))
            .build();
        try {
            return FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
        } catch (DescriptorValidationException e) {
            throw new IllegalStateException("Failed to build fixture descriptor", e);
        }
    }
}
