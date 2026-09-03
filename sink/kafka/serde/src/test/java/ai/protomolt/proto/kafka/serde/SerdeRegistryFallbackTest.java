package ai.protomolt.proto.kafka.serde;

import org.apache.kafka.common.errors.SerializationException;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
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
 * What a registry outage does, on each side of the serde. These point at a port nothing is
 * listening on, which is the honest version of an outage: every lookup fails.
 *
 * <p>The two sides answer differently on purpose. A frame is bytes on a topic and outlives
 * the outage that produced it, so a write with no id to stamp is refused rather than
 * guessed at. A read has nothing durable at stake and a configured type to fall back on, so
 * it keeps going.
 *
 * <p>Note what is <em>not</em> tested here, because it is not this file's job: a producer
 * whose subject resolved before the outage is unaffected either way, since the id is cached
 * for the serde's lifetime. That is the case people usually mean by "the registry went
 * down", and {@code SchemaIdsTest} covers it. What is left here is the cold start, where
 * nothing was ever resolved.
 */
class SerdeRegistryFallbackTest {

    // Nothing listens here. Dev-box convention keeps ad-hoc ports in the 3xxxx range.
    private static final String DEAD_REGISTRY = "http://127.0.0.1:34999";

    private static final String PROTO = """
            syntax = "proto3";
            package serde.fallback.v1;
            import "ai/protomolt/proto/validate/v1/validate.proto";
            message Event {
              string id = 1 [(ai.protomolt.proto.validate.v1.field).string.min_len = 2];
            }
            """;

    private static String descriptorSetBase64;
    private static Descriptor eventType;

    @BeforeAll
    static void compile() throws Exception {
        String validateProto = new String(SerdeRegistryFallbackTest.class.getClassLoader()
                .getResourceAsStream("ai/protomolt/proto/validate/v1/validate.proto")
                .readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/protomolt/proto/validate/v1/validate.proto", validateProto, "test")
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

    /**
     * A cold-start producer is refused rather than stamping its configured id. That id names
     * whatever the number happens to mean in the registry later, or nothing at all, and it
     * would be baked into bytes that outlast the outage. Refusing is a retry; stamping is a
     * topic nobody can read back.
     */
    @Test
    void refusesToProduceWhenTheRegistryCannotSupplyAnId() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of(ProtoMoltSerdeConfig.USE_SCHEMA_ID, 5)), false);
            assertThatThrownBy(() -> serializer.serialize("events", event("ok")))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("could not supply an id")
                    .hasMessageContaining("events-value");
        }
    }

    /**
     * The read side still carries on: the consumer has a configured type, the frame's index
     * path matches it, and nothing durable is at stake in decoding.
     */
    @Test
    void consumesWhenTheRegistryIsUnreachable() {
        Map<String, Object> noRegistry = config(Map.of());
        noRegistry.remove(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL);
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            // Written by a producer that never needed the registry, read by a consumer whose
            // registry is down.
            serializer.configure(noRegistry, false);
            deserializer.configure(config(Map.of()), false);
            Message back = deserializer.deserialize("events",
                    serializer.serialize("events", event("ok")));
            assertThat(back.getField(back.getDescriptorForType().findFieldByName("id")))
                    .isEqualTo("ok");
        }
    }

    /** The declared rules are judged before the frame is built, so a refusal cannot mask them. */
    @Test
    void stillValidatesWhenTheRegistryIsDown() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            assertThatThrownBy(() -> serializer.serialize("events", event("x")))
                    .hasMessageContaining("violates the schema's declared rules");
        }
    }

    /** An outage must cost one lookup per topic, not one per record, refusals included. */
    @Test
    void doesNotRetryTheRegistryOnEveryRecord() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of()), false);
            long start = System.nanoTime();
            for (int i = 0; i < 50; i++) {
                assertThatThrownBy(() -> serializer.serialize("events", event("ok")))
                        .isInstanceOf(SerializationException.class);
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
