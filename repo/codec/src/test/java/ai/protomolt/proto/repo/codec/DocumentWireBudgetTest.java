package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.descriptors.MessageWireBudget;

import ai.protomolt.proto.repo.v1.Document;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.WireFormat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.*;

/** Real protobuf wire/parser comparisons; no storage or schema-registry behavior is simulated. */
class DocumentWireBudgetTest {
    @FunctionalInterface interface Writer { void write(CodedOutputStream output) throws IOException; }
    private static ByteString wire(Writer writer) throws IOException {
        var bytes = new ByteArrayOutputStream();
        var output = CodedOutputStream.newInstance(bytes);
        writer.write(output); output.flush();
        return ByteString.copyFrom(bytes.toByteArray());
    }
    private static Descriptors.Descriptor scalar(DescriptorProtos.FieldDescriptorProto.Type type) throws Exception {
        return Descriptors.FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("wire-budget.proto").setSyntax("proto2")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Values")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("values").setNumber(1)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED).setType(type)
                                .setOptions(DescriptorProtos.FieldOptions.newBuilder().setPacked(false))))
                .build(), new Descriptors.FileDescriptor[0]).getMessageTypes().getFirst();
    }

    @ParameterizedTest
    @EnumSource(value = DescriptorProtos.FieldDescriptorProto.Type.class, names = {"TYPE_INT32", "TYPE_FIXED32", "TYPE_FIXED64"})
    void countsPackedAndUnpackedRegardlessOfDeclaredPacking(DescriptorProtos.FieldDescriptorProto.Type type) throws Exception {
        var descriptor = scalar(type);
        var packedBody = wire(out -> {
            for (int i = 0; i < 3; i++) {
                switch (type) {
                    case TYPE_FIXED32 -> out.writeFixed32NoTag(i);
                    case TYPE_FIXED64 -> out.writeFixed64NoTag(i);
                    default -> out.writeInt32NoTag(i);
                }
            }
        });
        var packed = wire(out -> out.writeBytes(1, packedBody));
        var unpacked = wire(out -> {
            for (int i = 0; i < 3; i++) {
                switch (type) {
                    case TYPE_FIXED32 -> out.writeFixed32(1, i);
                    case TYPE_FIXED64 -> out.writeFixed64(1, i);
                    default -> out.writeInt32(1, i);
                }
            }
        });
        assertThat(DynamicMessage.parseFrom(descriptor, packed)).isEqualTo(DynamicMessage.parseFrom(descriptor, unpacked));
        new MessageWireBudget(4, 10, () -> {}).check(packed, descriptor); // one envelope plus three values
        assertThatThrownBy(() -> new MessageWireBudget(3, 10, () -> {}).check(packed, descriptor))
                .hasMessageContaining("value count");
        new MessageWireBudget(3, 10, () -> {}).check(unpacked, descriptor);
        assertThatThrownBy(() -> new MessageWireBudget(2, 10, () -> {}).check(unpacked, descriptor))
                .hasMessageContaining("value count");
    }

    @Test void unknownGroupsAndDuplicateSingularValuesShareTheBudget() throws Exception {
        var bytes = wire(out -> {
            out.writeString(1, "first"); out.writeString(1, "last");
            out.writeTag(1000, WireFormat.WIRETYPE_START_GROUP);
            out.writeUInt32(1, 1); out.writeUInt32(1, 2);
            out.writeTag(1000, WireFormat.WIRETYPE_END_GROUP);
        });
        assertThat(Document.parseFrom(bytes).getDocId()).isEqualTo("last");
        new MessageWireBudget(5, 1, () -> {}).check(bytes, Document.getDescriptor());
        assertThatThrownBy(() -> new MessageWireBudget(4, 1, () -> {}).check(bytes, Document.getDescriptor()))
                .hasMessageContaining("value count");
        assertThatThrownBy(() -> new MessageWireBudget(5, 0, () -> {}).check(bytes, Document.getDescriptor()))
                .hasMessageContaining("depth");
    }

    @Test void wrongWireKnownFieldsAndOpaqueUnknownBytesMatchParserBehavior() throws Exception {
        var descriptor = scalar(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32);
        var bytes = wire(out -> { out.writeString(2, "opaque"); out.writeUInt32(1, 9); });
        assertThat(DynamicMessage.parseFrom(descriptor, bytes).getUnknownFields().asMap()).hasSize(2);
        new MessageWireBudget(2, 1, () -> {}).check(bytes, descriptor);
    }

    @Test void malformedPackedFieldsAndGroupsCannotPassPreflight() throws Exception {
        var fixed = scalar(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32);
        var misaligned = wire(out -> out.writeBytes(1, ByteString.copyFrom(new byte[] {1, 2, 3})));
        assertThatThrownBy(() -> new MessageWireBudget(100, 10, () -> {}).check(misaligned, fixed))
                .isInstanceOf(InvalidProtocolBufferException.class);
        var integer = scalar(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
        // Unterminated varint must not consume a following outer field.
        var truncated = wire(out -> { out.writeBytes(1, ByteString.copyFrom(new byte[] {(byte) 0x80})); out.writeInt32(2, 1); });
        assertThatThrownBy(() -> new MessageWireBudget(100, 10, () -> {}).check(truncated, integer))
                .isInstanceOf(InvalidProtocolBufferException.class);
        for (var bytes : new byte[][] {{11}, {11, 20}, {12}, {10, 5, 1}}) {
            assertThatThrownBy(() -> new MessageWireBudget(100, 10, () -> {}).check(ByteString.copyFrom(bytes), Document.getDescriptor()))
                    .isInstanceOf(InvalidProtocolBufferException.class);
        }
    }

    @Test void cancellationIsCheckedInsidePackedVarints() throws Exception {
        var descriptor = scalar(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
        var packed = wire(out -> out.writeBytes(1, ByteString.copyFrom(new byte[100])));
        var checks = new java.util.concurrent.atomic.AtomicInteger();
        var cancelled = new java.util.concurrent.CancellationException("stop preflight");
        assertThatThrownBy(() -> new MessageWireBudget(1000, 10, () -> {
            if (checks.incrementAndGet() == 20) throw cancelled;
        }).check(packed, descriptor)).isSameAs(cancelled);
    }

    @Test void nestedUnknownGroupsCannotHideValuesBelowTheDepthLimit() throws Exception {
        var bytes = wire(out -> {
            out.writeTag(1000, WireFormat.WIRETYPE_START_GROUP);
            out.writeTag(1, WireFormat.WIRETYPE_START_GROUP);
            for (int i = 0; i < 3; i++) out.writeUInt32(2, i);
            out.writeTag(1, WireFormat.WIRETYPE_END_GROUP);
            out.writeTag(1000, WireFormat.WIRETYPE_END_GROUP);
        });
        Document.parseFrom(bytes);
        new MessageWireBudget(5, 4, () -> {}).check(bytes, Document.getDescriptor());
        assertThatThrownBy(() -> new MessageWireBudget(4, 4, () -> {}).check(bytes, Document.getDescriptor()))
                .hasMessageContaining("value count");
    }

    @Test void invalidLengthsInsideKnownMessagesAreRejected() throws Exception {
        int bag = Document.getDescriptor().findFieldByName("blob_bag").getNumber();
        for (var body : new byte[][] {{10, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 15}, {10, 5, 1}}) {
            var bytes = wire(out -> out.writeBytes(bag, ByteString.copyFrom(body)));
            assertThatThrownBy(() -> Document.parseFrom(bytes)).isInstanceOf(InvalidProtocolBufferException.class);
            assertThatThrownBy(() -> new MessageWireBudget(100, 10, () -> {}).check(bytes, Document.getDescriptor()))
                    .isInstanceOf(InvalidProtocolBufferException.class);
        }
    }
}
