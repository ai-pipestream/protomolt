package ai.protomolt.proto.descriptors;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageWireBudgetTest {
    private static final Descriptors.Descriptor TREE = tree();

    @Test void depthLimitIsDistinctFromMalformedPayloadAndExactBoundaryPasses() {
        var leaf = DynamicMessage.newBuilder(TREE).setField(TREE.findFieldByNumber(1), "leaf").build();
        var bytes = DynamicMessage.newBuilder(TREE).setField(TREE.findFieldByNumber(2), leaf).build().toByteString();
        assertThatThrownBy(() -> new MessageWireBudget(10, 0, () -> {}).check(bytes, TREE))
                .isInstanceOf(MessageWireBudget.LimitExceededException.class);
        assertThatCode(() -> new MessageWireBudget(2, 1, () -> {}).check(bytes, TREE)).doesNotThrowAnyException();
        // The child advertises three bytes but only carries its first string tag.
        var truncated = ByteString.copyFrom(new byte[] {0x12, 0x03, 0x0a});
        assertThatThrownBy(() -> new MessageWireBudget(10, 3, () -> {}).check(truncated, TREE))
                .isInstanceOf(InvalidProtocolBufferException.class)
                .isNotInstanceOf(MessageWireBudget.LimitExceededException.class);
    }

    @Test void packedValuesAndAggregateFragmentsConsumeTheSameBudget() throws Exception {
        var bytes = DynamicMessage.newBuilder(TREE)
                .addRepeatedField(TREE.findFieldByNumber(3), 1)
                .addRepeatedField(TREE.findFieldByNumber(3), 2)
                .addRepeatedField(TREE.findFieldByNumber(3), 3).build().toByteString();
        // One packed field occurrence plus three values.
        assertThatThrownBy(() -> new MessageWireBudget(3, 1, () -> {}).check(bytes, TREE))
                .isInstanceOf(MessageWireBudget.LimitExceededException.class);
        var shared = new MessageWireBudget(8, 1, () -> {});
        shared.check(bytes, TREE);
        shared.check(bytes, TREE);
        assertThatThrownBy(() -> shared.check(bytes, TREE))
                .isInstanceOf(MessageWireBudget.LimitExceededException.class);
        assertThatCode(() -> new MessageWireBudget(4, 1, () -> {}).check(bytes, TREE)).doesNotThrowAnyException();
    }

    @Test void malformedWireAndCancellationKeepTheirOwnFailureIdentity() {
        assertThatThrownBy(() -> new MessageWireBudget(10, 1, () -> {})
                .check(ByteString.copyFrom(new byte[] {0x0f}), TREE))
                .isInstanceOf(InvalidProtocolBufferException.class);
        var stop = new CancellationException("caller stopped");
        assertThatThrownBy(() -> new MessageWireBudget(10, 1, () -> { throw stop; })
                .check(ByteString.EMPTY, TREE)).isSameAs(stop);
    }

    private static Descriptors.Descriptor tree() {
        var node = DescriptorProto.newBuilder().setName("Tree")
                .addField(FieldDescriptorProto.newBuilder().setName("label").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("child").setNumber(2)
                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".wire.Tree"))
                .addField(FieldDescriptorProto.newBuilder().setName("values").setNumber(3)
                        .setType(FieldDescriptorProto.Type.TYPE_INT32)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .setOptions(FieldOptions.newBuilder().setPacked(true)));
        try {
            return Descriptors.FileDescriptor.buildFrom(FileDescriptorProto.newBuilder()
                    .setName("wire-tree.proto").setPackage("wire").setSyntax("proto3")
                    .addMessageType(node).build(), new Descriptors.FileDescriptor[0]).findMessageTypeByName("Tree");
        } catch (Descriptors.DescriptorValidationException failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }
}
