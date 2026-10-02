package ai.protomolt.proto.actions;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogContractCauseTest {
    @Test
    void jsonParsingRetainsTheProtobufFailure() {
        var input = TestFixtures.obj("{\"unknown\":true}");
        assertCause("invalid-input", () -> CatalogContract.read(input, Empty.getDescriptor(), "probe"));
        assertCause("invalid-input", () -> CatalogContract.toRequest(input, Empty.getDescriptor(), "probe"));
        assertCause("internal-error", () -> CatalogContract.toResponse(input, Empty.getDescriptor(), "probe"));
    }

    @Test
    void renderingAnUnresolvedAnyRetainsTheProtobufFailure() {
        var unresolved = Any.pack(Empty.getDefaultInstance());
        assertCause("internal-error", () -> CatalogContract.toReply(unresolved, "probe"));
        assertCause("internal-error", () -> CatalogContract.toEnvelope(unresolved, "probe"));
    }

    @Test
    void generatedMessageConversionRetainsInvalidUtf8Failure() throws Exception {
        var file = DescriptorProtos.FileDescriptorProto.newBuilder().setName("incompatible.proto")
                .setPackage("google.protobuf").setSyntax("proto3")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("StringValue")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("value").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES))).build();
        var descriptor = Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("StringValue");
        var incompatible = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("value"), ByteString.copyFrom(new byte[] {(byte) 0xff})).build();
        assertCause("internal-error", () -> CatalogContract.as(incompatible, StringValue.getDefaultInstance(), "probe"));
    }

    private static void assertCause(String code, org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(ActionException.class, failure -> {
            assertThat(failure.code()).isEqualTo(code);
            assertThat(failure.getCause()).isInstanceOf(InvalidProtocolBufferException.class);
        });
    }
}
