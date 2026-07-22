package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The serde as a transformer. Write-side rules normalize before the contract judges — a rule
 * that fills a required field is the proof, because the unmapped message would be refused.
 * Read-side rules reshape records written before a schema moved, which is the migration-rules
 * use case. Rules mix text and CEL forms in one ordered list, and a rule that cannot apply
 * fails the record with a mapping error rather than being skipped.
 */
class SerdeMapperTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.map.v1;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Event {
              string id = 1 [(ai.pipestream.proto.validate.v1.field).string.min_len = 2];
              string legacy_name = 2;
              string display_name = 3;
              string scratch = 4;
              int32 size = 5;
            }
            """;

    private static String descriptorSetBase64;
    private static Descriptor eventType;

    @BeforeAll
    static void compile() throws Exception {
        String validateProto = new String(SerdeMapperTest.class.getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/validate/v1/validate.proto")
                .readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/validate/v1/validate.proto", validateProto, "test")
                .add("serde/map/v1/event.proto", PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        eventType = compiled.descriptorFor("serde/map/v1/event.proto").orElseThrow()
                .findMessageTypeByName("Event");
    }

    private static Map<String, Object> config(Map<String, Object> extra) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.map.v1.Event");
        config.putAll(extra);
        return config;
    }

    private static Message event(Map<String, Object> fields) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(eventType);
        fields.forEach((name, value) ->
                builder.setField(eventType.findFieldByName(name), value));
        return builder.build();
    }

    private static Object field(Message message, String name) {
        return message.getField(message.getDescriptorForType().findFieldByName(name));
    }

    /** The rule fills the id the validator requires: mapping demonstrably runs first. */
    @Test
    void writeRulesNormalizeBeforeValidation() {
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config(Map.of(ProtoMoltSerdeConfig.MAP_ON_WRITE,
                    List.of("id = legacy_name", "-scratch"))), false);
            deserializer.configure(config(Map.of()), false);

            Message written = deserializer.deserialize("events", serializer.serialize("events",
                    event(Map.of("legacy_name", "L-1", "scratch", "temp"))));

            assertThat(field(written, "id")).isEqualTo("L-1");
            assertThat(field(written, "scratch")).isEqualTo("");
        }
    }

    /** The migration-rules case: old records reshaped by the consumer, no producer upgrade. */
    @Test
    void readRulesReshapeOldRecords() {
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config(Map.of()), false);
            deserializer.configure(config(Map.of(ProtoMoltSerdeConfig.MAP_ON_READ,
                    List.of("display_name = legacy_name"))), false);

            Message read = deserializer.deserialize("events", serializer.serialize("events",
                    event(Map.of("id", "A-1", "legacy_name", "Old Name"))));

            assertThat(field(read, "display_name")).isEqualTo("Old Name");
        }
    }

    /** CEL rules: a computed value, gated by a filter, mixed into the same ordered list. */
    @Test
    void celRulesComputeAndFilter() {
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config(Map.of(ProtoMoltSerdeConfig.MAP_ON_WRITE, List.of(
                    "display_name := input.legacy_name + ' (legacy)' if input.display_name == ''",
                    "size := input.legacy_name.size()"))), false);
            deserializer.configure(config(Map.of()), false);

            Message mapped = deserializer.deserialize("events", serializer.serialize("events",
                    event(Map.of("id", "A-1", "legacy_name", "Old"))));
            assertThat(field(mapped, "display_name")).isEqualTo("Old (legacy)");
            assertThat(field(mapped, "size")).isEqualTo(3);

            // The filter holds: an existing display_name is left alone.
            Message kept = deserializer.deserialize("events", serializer.serialize("events",
                    event(Map.of("id", "A-2", "legacy_name", "Old", "display_name", "Kept"))));
            assertThat(field(kept, "display_name")).isEqualTo("Kept");
        }
    }

    @Test
    void malformedRulesFailAtConfigureTime() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            assertThatThrownBy(() -> serializer.configure(config(Map.of(
                    ProtoMoltSerdeConfig.MAP_ON_WRITE, List.of("just some words"))), false))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("not a mapping rule");
        }
    }

    /** A rule that cannot apply fails the record loudly; silent skipping hides real breakage. */
    @Test
    void aRuleNamingAMissingFieldFailsTheRecord() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(Map.of(ProtoMoltSerdeConfig.MAP_ON_WRITE,
                    List.of("no_such_field = legacy_name"))), false);
            assertThatThrownBy(() -> serializer.serialize("events",
                    event(Map.of("id", "A-1"))))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("mapping rule could not be applied");
        }
    }
}
