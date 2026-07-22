package ai.pipestream.proto.json;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufJsonTranscoderTest {

    @Test
    void roundTripsTypedStruct() {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder();
        Struct original = Struct.newBuilder()
                .putFields("name", Value.newBuilder().setStringValue("pipestream").build())
                .putFields("count", Value.newBuilder().setNumberValue(3).build())
                .build();

        String json = transcoder.toJson(original);
        Struct parsed = transcoder.fromJson(json, Struct.class);

        assertThat(parsed.getFieldsMap().get("name").getStringValue()).isEqualTo("pipestream");
        assertThat(parsed.getFieldsMap().get("count").getNumberValue()).isEqualTo(3.0);
    }

    @Test
    void parsesDynamicMessageViaDescriptorRegistry() {
        DescriptorRegistry registry = DescriptorRegistry.create();
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(registry);

        DynamicMessage message = transcoder.fromJsonDynamic(
                "{\"fields\":{\"hello\":{\"stringValue\":\"world\"}}}",
                "google.protobuf.Struct");

        assertThat(message.getDescriptorForType().getFullName()).isEqualTo("google.protobuf.Struct");
        assertThat(transcoder.toJson(message)).contains("hello").contains("world");
    }

    @Test
    void rejectsMalformedJson() {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder();
        assertThatThrownBy(() -> transcoder.fromJson("{not-json", Struct.class))
                .isInstanceOf(MalformedProtobufJsonException.class)
                .extracting(ex -> ((MalformedProtobufJsonException) ex).getJson())
                .isEqualTo("{not-json");
    }

    @Test
    void resolvesAnyTypesRegisteredAfterConstruction() {
        DescriptorRegistry registry = DescriptorRegistry.create();
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(registry);

        // StringValue is not registered at construction time; the transcoder must pick it up.
        registry.register(StringValue.getDescriptor());
        Any packed = Any.pack(StringValue.of("late"));

        String json = transcoder.toJson(packed);
        assertThat(json).contains("google.protobuf.StringValue").contains("late");

        Any parsed = transcoder.fromJson(json, Any.class);
        assertThat(parsed.getTypeUrl()).endsWith("google.protobuf.StringValue");
    }

    @Test
    void dynamicRequiresRegistry() {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder();
        assertThatThrownBy(() -> transcoder.fromJsonDynamic("{}", "google.protobuf.Struct"))
                .isInstanceOf(ProtobufJsonException.class)
                .hasMessageContaining("DescriptorRegistry");
    }
}
