package ai.protomolt.proto.helpers;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Any;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PayloadCodecTest {
    @Test void packsUnpacksAndConvertsStruct() throws Exception {
        var registry = DescriptorRegistry.create();
        PayloadCodec codec = new PayloadCodec(registry);
        Struct payload = Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("value").build()).build();
        registry.register(Struct.getDescriptor());
        Any any = codec.pack(payload);
        assertEquals(payload, codec.unpack(any));
        assertNotNull(codec.toStruct(payload));
        assertEquals(payload, codec.unpackSafe(any));
    }
    @Test void unpackSafeReturnsNullForUnknownPayload() {
        assertNull(new PayloadCodec().unpackSafe(Any.newBuilder().setTypeUrl("type.googleapis.com/nope.Type").build()));
    }
}
