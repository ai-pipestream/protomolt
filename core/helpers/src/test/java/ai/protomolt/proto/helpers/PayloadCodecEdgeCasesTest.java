package ai.protomolt.proto.helpers;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PayloadCodecEdgeCasesTest {

    @Test void emptyStructRoundTrip() throws Exception {
        DescriptorRegistry registry = DescriptorRegistry.create();
        registry.register(Struct.getDescriptor());
        PayloadCodec codec = new PayloadCodec(registry);
        Struct empty = Struct.getDefaultInstance();
        Any any = codec.pack(empty);
        assertEquals(empty, codec.unpack(any));
        assertEquals(empty, codec.unpackSafe(any));
    }

    @Test void unpackUnknownTypeThrows() {
        PayloadCodec codec = new PayloadCodec();
        Any any = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/unknown.Package")
                .setValue(com.google.protobuf.ByteString.EMPTY)
                .build();
        assertThrows(InvalidProtocolBufferException.class, () -> codec.unpack(any));
    }

    @Test void unpackSafeReturnsNullForEmptyAny() {
        assertNull(new PayloadCodec().unpackSafe(Any.getDefaultInstance()));
    }

    @Test void unpackToStructRoundTrip() throws Exception {
        DescriptorRegistry registry = DescriptorRegistry.create();
        registry.register(Struct.getDescriptor());
        PayloadCodec codec = new PayloadCodec(registry);
        Struct payload = Struct.newBuilder()
                .putFields("a", Value.newBuilder().setNumberValue(1).build())
                .build();
        Any any = codec.pack(payload);
        Struct asStruct = codec.unpackToStruct(any);
        assertNotNull(asStruct);
        assertEquals(1, asStruct.getFieldsCount());
        assertTrue(asStruct.containsFields("fields"));
    }

    @Test void toStructOfEmptyMessageIsEmpty() {
        PayloadCodec codec = new PayloadCodec();
        assertTrue(codec.toStruct(Struct.getDefaultInstance()).getFieldsMap().isEmpty());
    }

    @Test void accessorsAreNonNull() {
        PayloadCodec codec = new PayloadCodec();
        assertNotNull(codec.anyHandler());
        assertNotNull(codec.typeConverter());
    }
}
