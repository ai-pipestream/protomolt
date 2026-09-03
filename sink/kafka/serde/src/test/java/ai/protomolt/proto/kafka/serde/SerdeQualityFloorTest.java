package ai.protomolt.proto.kafka.serde;

import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
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
 * The quality floor's boundary: only a composite <em>below</em> the floor is refused, so a
 * record scoring exactly the floor is written. The schema scores {@code titled} at weight 1 and
 * {@code sized} at weight 3, so a titled record with a five-character body composes exactly
 * 0.625 — (1.0 + 3 x 0.5) / 4 — which is the boundary these tests straddle.
 */
class SerdeQualityFloorTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.floor.v1;
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
            """;

    private static String descriptorSetBase64;
    private static Descriptor articleType;

    @BeforeAll
    static void compile() throws Exception {
        String qualityProto = new String(SerdeQualityFloorTest.class.getClassLoader()
                .getResourceAsStream("ai/protomolt/proto/quality/v1/quality.proto")
                .readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/protomolt/proto/quality/v1/quality.proto", qualityProto, "test")
                .add("serde/floor/v1/article.proto", PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        articleType = compiled.descriptorFor("serde/floor/v1/article.proto").orElseThrow()
                .findMessageTypeByName("Article");
    }

    private static Map<String, Object> config(double qualityMin) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, descriptorSetBase64);
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, "serde.floor.v1.Article");
        config.put(ProtoMoltSerdeConfig.QUALITY_MIN, qualityMin);
        return config;
    }

    private static Message article() {
        return DynamicMessage.newBuilder(articleType)
                .setField(articleType.findFieldByName("title"), "t")
                .setField(articleType.findFieldByName("body"), "12345")
                .build();
    }

    @Test
    void aCompositeExactlyAtTheFloorIsWritten() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(0.625), false);
            assertThat(serializer.serialize("articles", article())).isNotEmpty();
        }
    }

    @Test
    void aCompositeJustBelowTheFloorIsRefused() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(0.626), false);
            assertThatThrownBy(() -> serializer.serialize("articles", article()))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("0.625")
                    .hasMessageContaining("0.626");
        }
    }
}
