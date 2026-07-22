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
 * Quality through the serde: the schema declares its dimensions, the serializer measures every
 * record and reports through the metrics listeners, and the optional floor turns the
 * measurement into a gate. Types declaring nothing are untouched — no events, no cost.
 */
class SerdeQualityTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.quality.v1;
            import "ai/pipestream/proto/quality/v1/quality.proto";
            message Article {
              option (ai.pipestream.proto.quality.v1.quality) = {
                dimension: { id: "titled" cel: "this.title != ''" }
                dimension: { id: "sized" weight: 3.0
                             cel: "clamp(double(this.body.size()) / 10.0, 0.0, 1.0)" }
              };
              string title = 1;
              string body = 2;
            }
            message Plain { string anything = 1; }
            """;

    private static String descriptorSetBase64;
    private static Descriptor articleType;
    private static Descriptor plainType;

    @BeforeAll
    static void compile() throws Exception {
        String qualityProto = new String(SerdeQualityTest.class.getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/quality/v1/quality.proto")
                .readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/quality/v1/quality.proto", qualityProto, "test")
                .add("serde/quality/v1/article.proto", PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        var file = compiled.descriptorFor("serde/quality/v1/article.proto").orElseThrow();
        articleType = file.findMessageTypeByName("Article");
        plainType = file.findMessageTypeByName("Plain");
    }

    @BeforeEach
    void reset() {
        RecordingMetricsListener.reset();
    }

    private static Map<String, Object> config(String type, Map<String, Object> extra) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, type);
        config.putAll(extra);
        return config;
    }

    private static Message article(String title, String body) {
        return DynamicMessage.newBuilder(articleType)
                .setField(articleType.findFieldByName("title"), title)
                .setField(articleType.findFieldByName("body"), body)
                .build();
    }

    @Test
    void measuresAndReportsOnWriteByDefault() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config("serde.quality.v1.Article", Map.of()), false);
            // titled = 1.0, sized = 5/10 = 0.5 at weight 3 -> composite (1 + 1.5) / 4 = 0.625
            assertThat(serializer.serialize("articles", article("t", "12345"))).isNotEmpty();
            assertThat(RecordingMetricsListener.EVENTS)
                    .anyMatch(e -> e.startsWith("quality articles serde.quality.v1.Article 0.63"));
        }
    }

    @Test
    void measuresOnReadWhenAskedTo() {
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config("serde.quality.v1.Article", Map.of()), false);
            deserializer.configure(config("serde.quality.v1.Article",
                    Map.of(ProtoMoltSerdeConfig.QUALITY_ON_READ, true)), false);

            byte[] bytes = serializer.serialize("articles", article("t", "12345"));
            RecordingMetricsListener.reset();
            deserializer.deserialize("articles", bytes);

            assertThat(RecordingMetricsListener.EVENTS)
                    .anyMatch(e -> e.startsWith("quality articles serde.quality.v1.Article 0.63"));
        }
    }

    /** The floor: quality becomes admission criteria only when the deployment says so. */
    @Test
    void rejectsWritesBelowTheConfiguredFloor() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config("serde.quality.v1.Article",
                    Map.of(ProtoMoltSerdeConfig.QUALITY_MIN, 0.5)), false);

            // Untitled and nearly empty: composite (0 + 3 * 0.1) / 4 = 0.075.
            assertThatThrownBy(() -> serializer.serialize("articles", article("", "x")))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("quality dimensions")
                    .hasMessageContaining("below the configured floor");
            assertThat(RecordingMetricsListener.EVENTS)
                    .anyMatch(e -> e.startsWith("quality-rejected articles"));

            // The same producer still writes good records.
            assertThat(serializer.serialize("articles", article("t", "1234567890")))
                    .isNotEmpty();
        }
    }

    @Test
    void neverRejectsOnRead() {
        try (var lax = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            lax.configure(config("serde.quality.v1.Article", Map.of(
                    ProtoMoltSerdeConfig.QUALITY_ON_WRITE, false)), false);
            deserializer.configure(config("serde.quality.v1.Article", Map.of(
                    ProtoMoltSerdeConfig.QUALITY_ON_READ, true,
                    ProtoMoltSerdeConfig.QUALITY_MIN, 0.99)), false);

            byte[] poor = lax.serialize("articles", article("", "x"));
            assertThat(deserializer.deserialize("articles", poor)).isNotNull();
        }
    }

    @Test
    void unannotatedTypesCostNothingAndReportNothing() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config("serde.quality.v1.Plain",
                    Map.of(ProtoMoltSerdeConfig.QUALITY_MIN, 0.99)), false);
            assertThat(serializer.serialize("plain", DynamicMessage.newBuilder(plainType)
                    .setField(plainType.findFieldByName("anything"), "x")
                    .build())).isNotEmpty();
            assertThat(RecordingMetricsListener.EVENTS)
                    .noneMatch(e -> e.startsWith("quality"));
        }
    }
}
