package ai.pipestream.proto.kafka.serde.micrometer;

import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End to end through the real discovery path: this module's ServiceLoader registration is on the
 * test classpath, so a plain serde — no metrics configuration anywhere — must land counters in
 * the global registry. That is the module's whole contract: put the jar on the classpath and the
 * validation the serde already does becomes a data-quality feed.
 */
class MicrometerSerdeMetricsTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.metrics.v1;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Event {
              string id = 1 [(ai.pipestream.proto.validate.v1.field).string.min_len = 2];
            }
            """;

    private static String descriptorSetBase64;
    private static Descriptor eventType;
    private static SimpleMeterRegistry registry;

    @BeforeAll
    static void setUp() throws Exception {
        String validateProto = new String(MicrometerSerdeMetricsTest.class.getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/validate/v1/validate.proto")
                .readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/validate/v1/validate.proto", validateProto, "test")
                .add("serde/metrics/v1/event.proto", PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        eventType = compiled.descriptorFor("serde/metrics/v1/event.proto").orElseThrow()
                .findMessageTypeByName("Event");
        registry = new SimpleMeterRegistry();
        Metrics.addRegistry(registry);
    }

    @AfterAll
    static void tearDown() {
        Metrics.removeRegistry(registry);
        registry.close();
    }

    private static Map<String, Object> config() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.metrics.v1.Event");
        return config;
    }

    private static Message event(String id) {
        return DynamicMessage.newBuilder(eventType)
                .setField(eventType.findFieldByName("id"), id)
                .build();
    }

    @Test
    void countsRecordsAndViolationsWithNoConfigurationAtAll() {
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config(), false);
            deserializer.configure(config(), false);

            deserializer.deserialize("events", serializer.serialize("events", event("ok")));
            assertThatThrownBy(() -> serializer.serialize("events", event("x")))
                    .isInstanceOf(SerializationException.class);

            assertThat(registry.counter("protomolt.serde.records",
                    "direction", "write", "topic", "events", "type", "serde.metrics.v1.Event")
                    .count()).isEqualTo(1.0);
            assertThat(registry.counter("protomolt.serde.records",
                    "direction", "read", "topic", "events", "type", "serde.metrics.v1.Event")
                    .count()).isEqualTo(1.0);
            assertThat(registry.counter("protomolt.serde.rejections",
                    "direction", "write", "topic", "events", "type", "serde.metrics.v1.Event")
                    .count()).isEqualTo(1.0);
            assertThat(registry.counter("protomolt.serde.violations",
                    "topic", "events", "type", "serde.metrics.v1.Event",
                    "rule", "string.min_len").count()).isEqualTo(1.0);
        }
    }

    @Test
    void countsEveryEventKindDirectly() {
        SimpleMeterRegistry local = new SimpleMeterRegistry();
        MicrometerSerdeMetrics metrics = new MicrometerSerdeMetrics(local);

        metrics.onSerialized("t", "a.B");
        metrics.onDeserialized("t", "a.B");
        metrics.onValidationRejected("t", "a.B", false, List.of("int32.gte", "string.min_len"));
        metrics.onTypeRefused("t", MicrometerSerdeMetrics.REASON_WRONG_TYPE);
        metrics.onRegistryFallback();

        assertThat(local.counter("protomolt.serde.records",
                "direction", "write", "topic", "t", "type", "a.B").count()).isEqualTo(1.0);
        assertThat(local.counter("protomolt.serde.records",
                "direction", "read", "topic", "t", "type", "a.B").count()).isEqualTo(1.0);
        assertThat(local.counter("protomolt.serde.rejections",
                "direction", "read", "topic", "t", "type", "a.B").count()).isEqualTo(1.0);
        assertThat(local.counter("protomolt.serde.violations",
                "topic", "t", "type", "a.B", "rule", "int32.gte").count()).isEqualTo(1.0);
        assertThat(local.counter("protomolt.serde.refusals",
                "topic", "t", "reason", "wrong-type").count()).isEqualTo(1.0);
        assertThat(local.counter("protomolt.serde.registry.fallbacks").count()).isEqualTo(1.0);
    }
}
