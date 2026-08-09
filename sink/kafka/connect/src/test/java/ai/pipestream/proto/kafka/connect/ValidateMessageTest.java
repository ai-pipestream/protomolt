package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@link ValidateMessage} knobs {@link TransformsTest} does not exercise: the violations
 * header's configurable name, the case-insensitivity of {@code on.invalid}, and the enum's
 * rejection of unknown actions. The header payload also pins the {@code ruleId} field — it is
 * what a DLQ consumer groups violations by.
 */
class ValidateMessageTest {

    private static final String PROTO = """
            syntax = "proto3";
            package validate.test;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Event {
              string id = 1 [(ai.pipestream.proto.validate.v1.field) = {
                string: {min_len: 3}
              }];
            }
            """;

    private static Descriptor eventType;
    private static String descriptorSetBase64;

    private ValidateMessage<SinkRecord> transform;

    @BeforeAll
    static void compile() throws Exception {
        String validateProto;
        try (InputStream in = ValidateMessageTest.class.getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/validate/v1/validate.proto")) {
            validateProto = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/validate/v1/validate.proto", validateProto, "test")
                .add("validate/test/event.proto", PROTO, "test")
                .build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        FileDescriptor file = compiled.descriptorFor("validate/test/event.proto").orElseThrow();
        eventType = file.findMessageTypeByName("Event");
    }

    @AfterEach
    void close() {
        if (transform != null) {
            transform.close();
            transform = null;
        }
    }

    private ValidateMessage<SinkRecord> validate(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put(ValueCodec.DESCRIPTOR_SET, descriptorSetBase64);
        props.put(ValueCodec.MESSAGE_TYPE, "validate.test.Event");
        props.putAll(overrides);
        ValidateMessage<SinkRecord> smt = new ValidateMessage<>();
        smt.configure(props);
        transform = smt;
        return smt;
    }

    private static SinkRecord invalidRecord() {
        DynamicMessage event = DynamicMessage.newBuilder(eventType)
                .setField(eventType.findFieldByName("id"), "x")   // min_len: 3
                .build();
        return new SinkRecord("events", 0, null, null, null, event.toByteArray(), 0);
    }

    @Test
    void theViolationsHeaderNameIsConfigurable() {
        ValidateMessage<SinkRecord> smt = validate(Map.of(
                ValidateMessage.ON_INVALID, "header",
                ValidateMessage.HEADER_NAME, "x.validation"));
        SinkRecord out = smt.apply(invalidRecord());
        assertThat(out.headers().lastWithName("x.validation")).isNotNull();
        assertThat(out.headers().lastWithName("protomolt.violations")).isNull();
        String violations = (String) out.headers().lastWithName("x.validation").value();
        assertThat(violations)
                .contains("\"field\":\"id\"")
                .contains("\"ruleId\"")
                .contains("min_len");
    }

    @Test
    void onInvalidIsCaseInsensitive() {
        ValidateMessage<SinkRecord> smt = validate(Map.of(ValidateMessage.ON_INVALID, "DROP"));
        assertThat(smt.apply(invalidRecord())).isNull();
    }

    @Test
    void anUnknownOnInvalidIsRejectedAtConfigure() {
        assertThatThrownBy(() -> validate(Map.of(ValidateMessage.ON_INVALID, "explode")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ValidateMessage.ON_INVALID);
    }
}
