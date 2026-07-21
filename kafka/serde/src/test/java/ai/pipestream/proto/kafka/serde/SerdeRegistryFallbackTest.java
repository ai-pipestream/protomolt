package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A registry that cannot answer must not stop a producer whose schema has not changed. These
 * point the serde at a port nothing is listening on, which is the honest version of an outage:
 * every lookup fails, and the packaged descriptor set has to carry the traffic on its own.
 */
class SerdeRegistryFallbackTest {

    // Nothing listens here. Dev-box convention keeps ad-hoc ports in the 3xxxx range.
    private static final String DEAD_REGISTRY = "http://127.0.0.1:34999";

    private static final String PROTO = """
            syntax = "proto3";
            package serde.fallback.v1;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Event {
              string id = 1 [(ai.pipestream.proto.validate.v1.field).string.min_len = 2];
            }
            """;

    private static String descriptorSetBase64;
    private static Descriptor eventType;

    @BeforeAll
    static void compile() throws Exception {
        String validateProto = new String(SerdeRegistryFallbackTest.class.getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/validate/v1/validate.proto")
                .readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/validate/v1/validate.proto", validateProto, "test")
                .add("serde/fallback/v1/event.proto", PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        eventType = compiled.descriptorFor("serde/fallback/v1/event.proto").orElseThrow()
                .findMessageTypeByName("Event");
    }

    private static Map<String, Object> config(Map<String, Object> extra) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.fallback.v1.Event");
        config.put(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL, DEAD_REGISTRY);
        config.putAll(extra);
        return config;
    }

    private static Message event(String id) {
        return DynamicMessage.newBuilder(eventType)
                .setField(eventType.findFieldByName("id"), id)
                .build();
    }

    /** The point: the registry is down and the producer keeps working. */
    @Test
    void producesWhenTheRegistryIsUnreachable() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of(ProtoMoltSerdeConfig.USE_SCHEMA_ID, 5)), false);
            byte[] bytes = serializer.serialize("events", event("ok"));
            assertThat(bytes).isNotEmpty();
            // The configured id stands in for the one the registry could not supply.
            assertThat(ConfluentWireFormat.schemaId(bytes)).isEqualTo(5);
        }
    }

    @Test
    void consumesWhenTheRegistryIsUnreachable() {
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config(Map.of()), false);
            deserializer.configure(config(Map.of()), false);
            Message back = deserializer.deserialize("events", serializer.serialize("events", event("ok")));
            assertThat(back.getField(back.getDescriptorForType().findFieldByName("id")))
                    .isEqualTo("ok");
        }
    }

    /** Falling back supplies a schema; it does not suspend the contract that schema declares. */
    @Test
    void stillValidatesWhileFallingBack() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            assertThatThrownBy(() -> serializer.serialize("events", event("x")))
                    .hasMessageContaining("violates the schema's declared rules");
        }
    }

    /** An outage must cost one lookup per topic, not one per record. */
    @Test
    void doesNotRetryTheRegistryOnEveryRecord() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            long start = System.nanoTime();
            for (int i = 0; i < 50; i++) {
                serializer.serialize("events", event("ok"));
            }
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            // 50 connection refusals would not land inside this budget.
            assertThat(elapsedMillis).isLessThan(2_000);
        }
    }

    @Test
    void runsWithNoRegistryConfiguredAtAll() {
        Map<String, Object> noRegistry = config(Map.of());
        noRegistry.remove(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL);
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(noRegistry, false);
            assertThat(serializer.serialize("events", event("ok"))).isNotEmpty();
        }
    }
}
