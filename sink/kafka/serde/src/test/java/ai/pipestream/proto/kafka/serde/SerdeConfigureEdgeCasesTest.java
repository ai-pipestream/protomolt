package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Configure-time and parse-time failures that are not config-schema errors: a pinned type the
 * descriptor set does not declare is a loud misconfiguration on both halves of the serde, and a
 * frame whose payload is not the message it claims to be surfaces as a SerializationException
 * naming the type and the topic.
 */
class SerdeConfigureEdgeCasesTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.cfg.v1;
            message Event { string id = 1; }
            """;

    private static String descriptorSetBase64;
    private static Descriptor eventType;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("serde/cfg/v1/event.proto", PROTO, "test").build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        eventType = compiled.descriptorFor("serde/cfg/v1/event.proto").orElseThrow()
                .findMessageTypeByName("Event");
    }

    private static Map<String, Object> config(Map<String, Object> extra) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.cfg.v1.Event");
        config.putAll(extra);
        return config;
    }

    @Test
    void aPinnedTypeOutsideTheDescriptorSetFailsTheSerializerAtConfigure() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            assertThatThrownBy(() -> serializer.configure(
                    config(Map.of(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.cfg.v1.Missing")),
                    false))
                    .isInstanceOf(KafkaException.class)
                    .hasMessageContaining("'serde.cfg.v1.Missing'")
                    .hasMessageContaining("not in the configured descriptor set");
        }
    }

    @Test
    void aPinnedTypeOutsideTheDescriptorSetFailsTheDeserializerAtConfigure() {
        try (var deserializer = new ProtoMoltProtobufDeserializer()) {
            assertThatThrownBy(() -> deserializer.configure(
                    config(Map.of(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.cfg.v1.Missing")),
                    false))
                    .isInstanceOf(KafkaException.class)
                    .hasMessageContaining("'serde.cfg.v1.Missing'")
                    .hasMessageContaining("not in the configured descriptor set");
        }
    }

    /**
     * A well-formed frame wrapping bytes that are not the message: the frame checks pass, the
     * parse fails, and the consumer hears a SerializationException naming the type it could not
     * parse and the topic the record rode in on.
     */
    @Test
    void aPayloadThatIsNotTheMessageFailsTheRecord() {
        try (var deserializer = new ProtoMoltProtobufDeserializer()) {
            deserializer.configure(config(Map.of()), false);
            // A length-delimited field claiming 127 bytes with only one present.
            byte[] framed = ConfluentWireFormat.frame(1,
                    ConfluentWireFormat.indexPath(eventType), new byte[]{0x0A, 0x7F, 0x01});
            assertThatThrownBy(() -> deserializer.deserialize("events", framed))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("events")
                    .hasMessageContaining("is not a valid serde.cfg.v1.Event");
        }
    }
}
