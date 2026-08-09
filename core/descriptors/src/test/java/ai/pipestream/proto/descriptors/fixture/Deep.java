package ai.pipestream.proto.descriptors.fixture;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;

/**
 * Companion to {@link GeneratedLike} with two levels of nesting, so the candidate-class-name
 * walk in {@code ClasspathDescriptorLoader} is exercised past a single {@code $} split: the
 * proto type {@code ...fixture.Deep.Mid.Leaf} lives in the binary class
 * {@code ...fixture.Deep$Mid$Leaf}. Abstract because only the type and the static
 * {@code getDescriptor()} methods are exercised; no instance is ever created.
 */
public abstract class Deep implements Message {

    private static final FileDescriptor FILE = buildFile();

    private Deep() {
    }

    public static Descriptor getDescriptor() {
        return FILE.getMessageTypes().get(0);
    }

    public abstract static class Mid implements Message {

        private Mid() {
        }

        public static Descriptor getDescriptor() {
            return Deep.getDescriptor().getNestedTypes().get(0);
        }

        public abstract static class Leaf implements Message {

            private Leaf() {
            }

            public static Descriptor getDescriptor() {
                return Mid.getDescriptor().getNestedTypes().get(0);
            }
        }
    }

    private static FileDescriptor buildFile() {
        FileDescriptorProto proto = FileDescriptorProto.newBuilder()
            .setName("fixture/deep.proto")
            .setSyntax("proto3")
            .setPackage("ai.pipestream.proto.descriptors.fixture")
            .addMessageType(DescriptorProto.newBuilder()
                .setName("Deep")
                .addNestedType(DescriptorProto.newBuilder()
                    .setName("Mid")
                    .addNestedType(DescriptorProto.newBuilder().setName("Leaf"))))
            .build();
        try {
            return FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
        } catch (DescriptorValidationException e) {
            throw new IllegalStateException("Failed to build fixture descriptor", e);
        }
    }
}
