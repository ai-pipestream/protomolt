package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The metrics SPI, through the serde itself: listeners found by ServiceLoader hear about every
 * record, rejection, refusal and registry fallback — and a listener that throws (the registered
 * {@link RecordingMetricsListener.Hostile}) is contained without costing a record or silencing
 * the well-behaved listener beside it.
 */
class SerdeMetricsTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.spi.v1;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Event {
              string id = 1 [(ai.pipestream.proto.validate.v1.field).string.min_len = 2];
            }
            """;

    private static String descriptorSetBase64;
    private static Descriptor eventType;

    @BeforeAll
    static void compile() throws Exception {
        String validateProto = new String(SerdeMetricsTest.class.getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/validate/v1/validate.proto")
                .readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/validate/v1/validate.proto", validateProto, "test")
                .add("serde/spi/v1/event.proto", PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        eventType = compiled.descriptorFor("serde/spi/v1/event.proto").orElseThrow()
                .findMessageTypeByName("Event");
    }

    @BeforeEach
    void reset() {
        RecordingMetricsListener.reset();
    }

    private static Map<String, Object> config(Map<String, Object> extra) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.spi.v1.Event");
        config.putAll(extra);
        return config;
    }

    private static Message event(String id) {
        return DynamicMessage.newBuilder(eventType)
                .setField(eventType.findFieldByName("id"), id)
                .build();
    }

    @Test
    void reportsRecordsBothDirections() {
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config(Map.of()), false);
            deserializer.configure(config(Map.of()), false);

            deserializer.deserialize("events", serializer.serialize("events", event("ok")));

            assertThat(RecordingMetricsListener.EVENTS)
                    .contains("serialized events serde.spi.v1.Event",
                            "deserialized events serde.spi.v1.Event");
        }
    }

    /** The hostile listener throws on every serialize — the record must still be written. */
    @Test
    void containsAThrowingListener() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            assertThat(serializer.serialize("events", event("ok"))).isNotEmpty();
            assertThat(RecordingMetricsListener.EVENTS)
                    .contains("serialized events serde.spi.v1.Event");
        }
    }

    @Test
    void reportsRejectionsWithTheViolatedRules() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            assertThatThrownBy(() -> serializer.serialize("events", event("x")))
                    .isInstanceOf(SerializationException.class);
            assertThat(RecordingMetricsListener.EVENTS)
                    .contains("rejected write events serde.spi.v1.Event [string.min_len]");
        }
    }

    @Test
    void reportsTypeRefusals() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            Message wrong = DynamicMessage.newBuilder(
                    com.google.protobuf.Empty.getDescriptor()).build();
            assertThatThrownBy(() -> serializer.serialize("events", wrong))
                    .isInstanceOf(SerializationException.class);
            assertThat(RecordingMetricsListener.EVENTS)
                    .contains("refused events " + SerdeMetricsListener.REASON_WRONG_TYPE);
        }
    }

    @Test
    void reportsRegistryFallbacks() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            // Nothing listens here; the lookup fails and the packaged set carries the record.
            serializer.configure(config(Map.of(
                    ProtoMoltSerdeConfig.REGISTRY_URL, "http://127.0.0.1:34998")), false);
            assertThat(serializer.serialize("events", event("ok"))).isNotEmpty();
            assertThat(RecordingMetricsListener.EVENTS).contains("fallback");
        }
    }
}
