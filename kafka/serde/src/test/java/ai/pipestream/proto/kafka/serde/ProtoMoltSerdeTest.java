package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The serde's whole point: the rules the schema already declares are enforced by the serializer,
 * so a producer cannot write data that violates its own contract by forgetting to call anything.
 */
class ProtoMoltSerdeTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.orders.v1;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Ignored { string filler = 1; }
            message Order {
              string id = 1 [(ai.pipestream.proto.validate.v1.field).string.min_len = 3];
              int32 quantity = 2 [(ai.pipestream.proto.validate.v1.field).int32.gte = 1];
            }
            """;

    private static String descriptorSetBase64;
    private static Descriptor orderType;

    @BeforeAll
    static void compile() throws Exception {
        String validateProto = new String(ProtoMoltSerdeTest.class.getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/validate/v1/validate.proto")
                .readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/validate/v1/validate.proto", validateProto, "test")
                .add("serde/orders/v1/order.proto", PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        FileDescriptor file = compiled.descriptorFor("serde/orders/v1/order.proto").orElseThrow();
        orderType = file.findMessageTypeByName("Order");
    }

    private static Map<String, Object> config(Map<String, Object> extra) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.orders.v1.Order");
        config.putAll(extra);
        return config;
    }

    private static Message order(String id, int quantity) {
        return DynamicMessage.newBuilder(orderType)
                .setField(orderType.findFieldByName("id"), id)
                .setField(orderType.findFieldByName("quantity"), quantity)
                .build();
    }

    @Test
    void roundTripsAMessage() {
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config(Map.of(ProtoMoltSerdeConfig.SCHEMA_ID, 17)), false);
            deserializer.configure(config(Map.of()), false);

            byte[] bytes = serializer.serialize("orders", order("A-100", 3));

            assertThat(ConfluentWireFormat.schemaId(bytes)).isEqualTo(17);
            // Order is the second message in its file, which is the case an unsigned reader breaks on.
            assertThat(ConfluentWireFormat.messageIndex(bytes)).containsExactly(1);

            // The serde links its own descriptors, so read the result through its type rather
            // than the test's: two FileDescriptor instances of the same schema are not equal.
            Message back = deserializer.deserialize("orders", bytes);
            Descriptor type = back.getDescriptorForType();
            assertThat(type.getFullName()).isEqualTo("serde.orders.v1.Order");
            assertThat(back.getField(type.findFieldByName("id"))).isEqualTo("A-100");
            assertThat(back.getField(type.findFieldByName("quantity"))).isEqualTo(3);
        }
    }

    /** The whole pitch: invalid data never reaches the topic, and nobody had to remember to check. */
    @Test
    void refusesToWriteAMessageThatViolatesItsOwnSchema() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            assertThatThrownBy(() -> serializer.serialize("orders", order("A", 3)))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("violates the schema's declared rules")
                    .hasMessageContaining("id");
        }
    }

    /** Every violation at once, not one per redeploy. */
    @Test
    void reportsEveryViolationNotJustTheFirst() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            assertThatThrownBy(() -> serializer.serialize("orders", order("A", 0)))
                    .hasMessageContaining("id")
                    .hasMessageContaining("quantity");
        }
    }

    @Test
    void writesWithoutValidationWhenAskedTo() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of(
                    ProtoMoltSerdeConfig.VALIDATE_ON_WRITE, false)), false);
            assertThat(serializer.serialize("orders", order("A", 0))).isNotEmpty();
        }
    }

    /** A consumer can catch a producer that never went through this serde. */
    @Test
    void validatesOnReadWhenAskedTo() {
        try (var lax = new ProtoMoltProtobufSerializer();
             var strict = new ProtoMoltProtobufDeserializer()) {
            lax.configure(config(Map.of(ProtoMoltSerdeConfig.VALIDATE_ON_WRITE, false)), false);
            strict.configure(config(Map.of(ProtoMoltSerdeConfig.VALIDATE_ON_READ, true)), false);

            byte[] smuggled = lax.serialize("orders", order("A", 0));

            assertThatThrownBy(() -> strict.deserialize("orders", smuggled))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("violates the schema's declared rules");
        }
    }

    /** Reading is off by default, so a consumer does not start rejecting history on upgrade. */
    @Test
    void doesNotValidateOnReadByDefault() {
        try (var lax = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            lax.configure(config(Map.of(ProtoMoltSerdeConfig.VALIDATE_ON_WRITE, false)), false);
            deserializer.configure(config(Map.of()), false);
            assertThat(deserializer.deserialize("orders", lax.serialize("orders", order("A", 0))))
                    .isNotNull();
        }
    }

    /** A topic carrying a different type is a misconfiguration, not bytes to guess at. */
    @Test
    void refusesAFrameThatPointsAtAnotherMessage() {
        try (var deserializer = new ProtoMoltProtobufDeserializer()) {
            deserializer.configure(config(Map.of()), false);
            byte[] wrongType = ConfluentWireFormat.frame(1, java.util.List.of(0), new byte[]{});
            assertThatThrownBy(() -> deserializer.deserialize("orders", wrongType))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("does not expect");
        }
    }

    @Test
    void passesTombstonesThrough() {
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config(Map.of()), false);
            deserializer.configure(config(Map.of()), false);
            assertThat(serializer.serialize("orders", null)).isNull();
            assertThat(deserializer.deserialize("orders", null)).isNull();
        }
    }

    @Test
    void rejectsAMessageOfTheWrongType() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            Descriptor other = orderType.getFile().findMessageTypeByName("Ignored");
            assertThatThrownBy(() -> serializer.serialize("orders",
                    DynamicMessage.newBuilder(other).build()))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("was handed a");
        }
    }

    @Test
    void requiresExactlyOneDescriptorSource() {
        Map<String, Object> neither = new HashMap<>();
        neither.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.orders.v1.Order");
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            assertThatThrownBy(() -> serializer.configure(neither, false))
                    .hasMessageContaining("Exactly one of");
        }
        Map<String, Object> both = config(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_RESOURCE, "somewhere.desc"));
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            assertThatThrownBy(() -> serializer.configure(both, false))
                    .hasMessageContaining("Exactly one of");
        }
    }
}
