package ai.protomolt.proto.http.json;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
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

    @Test
    void resolvesAnyPayloadFromTheDynamicResponseDescriptorClosureWithoutRegisteringIt() throws Exception {
        DescriptorRegistry registry = DescriptorRegistry.create();
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(registry);
        int registeredWellKnownTypes = registry.size();

        DescriptorProtos.DescriptorProto payloadProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Evidence")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("label").setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        DescriptorProtos.DescriptorProto responseProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("CorrectionResponse")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("evidence").setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".google.protobuf.Any"))
                .build();
        Descriptors.FileDescriptor responseFile = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("closure/correction.proto")
                        .setPackage("closure.test")
                        .setSyntax("proto3")
                        .addDependency(Any.getDescriptor().getFile().getName())
                        .addMessageType(payloadProto)
                        .addMessageType(responseProto)
                        .build(),
                new Descriptors.FileDescriptor[]{Any.getDescriptor().getFile()});
        Descriptors.Descriptor evidenceType = responseFile.findMessageTypeByName("Evidence");
        Descriptors.Descriptor responseType = responseFile.findMessageTypeByName("CorrectionResponse");
        DynamicMessage evidence = DynamicMessage.newBuilder(evidenceType)
                .setField(evidenceType.findFieldByName("label"), "closure-only")
                .build();
        Any packed = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/closure.test.Evidence")
                .setValue(evidence.toByteString())
                .build();
        DynamicMessage original = DynamicMessage.newBuilder(responseType)
                .setField(responseType.findFieldByName("evidence"), packed)
                .build();

        String json = transcoder.toJson(original);
        assertThat(json).contains("closure.test.Evidence").contains("closure-only");
        DynamicMessage parsed = transcoder.fromJsonDynamic(json, responseType);
        Descriptors.FieldDescriptor evidenceField = responseType.findFieldByName("evidence");
        DynamicMessage parsedAny = (DynamicMessage) parsed.getField(evidenceField);
        String typeUrl = (String) parsedAny.getField(
                parsedAny.getDescriptorForType().findFieldByName("type_url"));
        com.google.protobuf.ByteString payloadBytes = (com.google.protobuf.ByteString) parsedAny.getField(
                parsedAny.getDescriptorForType().findFieldByName("value"));
        DynamicMessage parsedEvidence = DynamicMessage.parseFrom(evidenceType, payloadBytes);
        assertThat(typeUrl).endsWith("closure.test.Evidence");
        assertThat(parsedEvidence.getField(evidenceType.findFieldByName("label")))
                .isEqualTo("closure-only");
        assertThat(registry.size()).as("scoped Any support must not mutate the global registry")
                .isEqualTo(registeredWellKnownTypes);
        assertThat(registry.findDescriptorByFullName("closure.test.Evidence")).isNull();
    }
}
