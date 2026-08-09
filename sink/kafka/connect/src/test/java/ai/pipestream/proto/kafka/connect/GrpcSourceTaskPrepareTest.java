package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link GrpcSourceTask#prepare}'s validation matrix — everything that must fail at connector
 * start rather than first poll: the method's streaming shape, the request template, the
 * resume-token field path (each segment singular, intermediates messages, the leaf a string),
 * and both CEL expressions compiling against the output type.
 */
class GrpcSourceTaskPrepareTest {

    private static final String PROTO = """
            syntax = "proto3";
            package prep.test;
            message Position { string cursor = 1; }
            message Subscribe {
              string shard = 1;
              Position position = 2;
              repeated string tags = 3;
              int64 limit = 4;
            }
            message Tick { int64 seq = 1; string cursor = 2; }
            service Feed {
              rpc Watch(Subscribe) returns (stream Tick);
              rpc One(Subscribe) returns (Tick);
              rpc Bidi(stream Subscribe) returns (stream Tick);
            }
            """;

    private static String descriptorSetBase64;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("prep/test/feed.proto", PROTO, "test").build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
    }

    private static Map<String, String> config() {
        Map<String, String> props = new HashMap<>();
        props.put(GrpcSourceConfig.TARGET, "unused");
        props.put(GrpcSourceConfig.METHOD, "prep.test.Feed/Watch");
        props.put(GrpcSourceConfig.DESCRIPTOR_SET, descriptorSetBase64);
        props.put(GrpcSourceConfig.TOPIC, "ticks");
        return props;
    }

    private static GrpcSourceTask.Prepared prepare(Map<String, String> overrides) {
        Map<String, String> props = config();
        props.putAll(overrides);
        return GrpcSourceTask.prepare(new GrpcSourceConfig(props));
    }

    @Test
    void aNestedTokenPathResolvesToItsFieldChain() {
        GrpcSourceTask.Prepared prepared = prepare(
                Map.of(GrpcSourceConfig.RESUME_TOKEN_FIELD, "position.cursor"));
        assertThat(prepared.tokenPath()).hasSize(2);
        assertThat(prepared.tokenPath().get(0).getName()).isEqualTo("position");
        assertThat(prepared.tokenPath().get(1).getName()).isEqualTo("cursor");
        assertThat(prepared.evaluator()).isNull();   // no CEL configured, no evaluator built
    }

    @Test
    void theRequestTemplateParsesAndDefaultsToEmpty() {
        GrpcSourceTask.Prepared blank = prepare(Map.of());
        assertThat(blank.requestTemplate().getField(
                blank.requestTemplate().getDescriptorForType().findFieldByName("shard")))
                .isEqualTo("");

        GrpcSourceTask.Prepared filled = prepare(
                Map.of(GrpcSourceConfig.REQUEST_JSON, "{\"shard\": \"a\"}"));
        assertThat(filled.requestTemplate().getField(
                filled.requestTemplate().getDescriptorForType().findFieldByName("shard")))
                .isEqualTo("a");
    }

    @Test
    void anUnparseableRequestTemplateNamesTheConfigKey() {
        assertThatThrownBy(() -> prepare(Map.of(GrpcSourceConfig.REQUEST_JSON, "{not json")))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining(GrpcSourceConfig.REQUEST_JSON)
                .hasMessageContaining("prep.test.Subscribe");
    }

    @Test
    void unaryAndBidiStreamingMethodsAreRejected() {
        assertThatThrownBy(() -> prepare(Map.of(GrpcSourceConfig.METHOD, "prep.test.Feed/One")))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("not server-streaming");
        assertThatThrownBy(() -> prepare(Map.of(GrpcSourceConfig.METHOD, "prep.test.Feed/Bidi")))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("not server-streaming");
    }

    @Test
    void anUnknownTokenPathSegmentIsNamed() {
        assertThatThrownBy(() -> prepare(
                Map.of(GrpcSourceConfig.RESUME_TOKEN_FIELD, "position.nope")))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining(GrpcSourceConfig.RESUME_TOKEN_FIELD)
                .hasMessageContaining("no singular field 'nope'")
                .hasMessageContaining("prep.test.Position");
    }

    @Test
    void aRepeatedTokenPathSegmentIsRejected() {
        assertThatThrownBy(() -> prepare(
                Map.of(GrpcSourceConfig.RESUME_TOKEN_FIELD, "tags")))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("no singular field 'tags'")
                .hasMessageContaining("prep.test.Subscribe");
    }

    @Test
    void aNonMessageIntermediateCannotBeDescendedInto() {
        assertThatThrownBy(() -> prepare(
                Map.of(GrpcSourceConfig.RESUME_TOKEN_FIELD, "shard.cursor")))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("field 'shard' on prep.test.Subscribe is not a message");
    }

    @Test
    void aNonStringTokenLeafIsRejected() {
        assertThatThrownBy(() -> prepare(
                Map.of(GrpcSourceConfig.RESUME_TOKEN_FIELD, "limit")))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining(GrpcSourceConfig.RESUME_TOKEN_FIELD)
                .hasMessageContaining("must be a string")
                .hasMessageContaining("int64");
    }

    @Test
    void keyCelTypeChecksAgainstTheOutputType() {
        assertThatThrownBy(() -> prepare(Map.of(GrpcSourceConfig.KEY_CEL, "input.no_such_field")))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining(GrpcSourceConfig.KEY_CEL)
                .hasMessageContaining("does not compile");
    }

    @Test
    void validCelExpressionsPrepareAnEvaluator() {
        GrpcSourceTask.Prepared prepared = prepare(Map.of(
                GrpcSourceConfig.RESUME_TOKEN_CEL, "input.cursor",
                GrpcSourceConfig.KEY_CEL, "input.cursor"));
        assertThat(prepared.evaluator()).isNotNull();
    }
}
