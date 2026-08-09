package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link MapMessage} rule shapes {@link TransformsTest} does not cover: clearing a field,
 * appending to a repeated field, the text-rules-before-CEL-rules ordering (CEL expressions see
 * the message the text rules left behind), and a rule that parses but cannot execute failing
 * the record as a {@link DataException}.
 */
class MapMessageTest {

    private static final String PROTO = """
            syntax = "proto3";
            package map.test;
            message Event {
              string id = 1;
              int64 seq = 2;
              string note = 3;
              string category = 4;
              repeated string tags = 5;
            }
            """;

    private static Descriptor eventType;
    private static String descriptorSetBase64;

    private MapMessage<SinkRecord> transform;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("map/test/event.proto", PROTO, "test").build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        FileDescriptor file = compiled.descriptorFor("map/test/event.proto").orElseThrow();
        eventType = file.findMessageTypeByName("Event");
    }

    @AfterEach
    void close() {
        if (transform != null) {
            transform.close();
            transform = null;
        }
    }

    private MapMessage<SinkRecord> map(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put(ValueCodec.DESCRIPTOR_SET, descriptorSetBase64);
        props.put(ValueCodec.MESSAGE_TYPE, "map.test.Event");
        props.putAll(overrides);
        MapMessage<SinkRecord> smt = new MapMessage<>();
        smt.configure(props);
        transform = smt;
        return smt;
    }

    private static DynamicMessage event(String id, String note, List<String> tags) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(eventType)
                .setField(eventType.findFieldByName("id"), id)
                .setField(eventType.findFieldByName("note"), note);
        for (String tag : tags) {
            builder.addRepeatedField(eventType.findFieldByName("tags"), tag);
        }
        return builder.build();
    }

    private static SinkRecord record(DynamicMessage event) {
        return new SinkRecord("events", 0, null, null, null, event.toByteArray(), 0);
    }

    private static DynamicMessage decode(SinkRecord record) throws Exception {
        return DynamicMessage.parseFrom(eventType, (byte[]) record.value());
    }

    private static String field(DynamicMessage message, String name) {
        return (String) message.getField(eventType.findFieldByName(name));
    }

    @Test
    void aClearRuleRemovesTheField() throws Exception {
        MapMessage<SinkRecord> smt = map(Map.of(MapMessage.RULES, "-note"));
        DynamicMessage out = decode(smt.apply(record(event("abc", "secret note", List.of()))));
        assertThat(field(out, "note")).isEmpty();
        assertThat(field(out, "id")).isEqualTo("abc");
    }

    @Test
    void anAppendRuleAppendsToARepeatedField() throws Exception {
        MapMessage<SinkRecord> smt = map(Map.of(MapMessage.RULES, "tags += id"));
        DynamicMessage out = decode(smt.apply(record(event("abc", "", List.of("existing")))));
        assertThat(out.getField(eventType.findFieldByName("tags")))
                .isEqualTo(List.of("existing", "abc"));
    }

    /**
     * Text rules run first and CEL rules see the progressive message: here the filter reads
     * {@code note}, which only holds the id because the text rule already ran.
     */
    @Test
    void celRulesSeeTheMessageTextRulesLeftBehind() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(MapMessage.RULES, "note = id");
        props.put(MapMessage.CEL_RULES_JSON, """
                [{"filter": "input.note == 'abc'", "selector": "'matched'", "target": "category"}]
                """);
        MapMessage<SinkRecord> smt = map(props);

        DynamicMessage hit = decode(smt.apply(record(event("abc", "", List.of()))));
        assertThat(field(hit, "category")).isEqualTo("matched");
        DynamicMessage miss = decode(smt.apply(record(event("xyz", "", List.of()))));
        assertThat(field(miss, "category")).isEmpty();
    }

    @Test
    void aRuleThatCannotExecuteFailsTheRecord() {
        MapMessage<SinkRecord> smt = map(Map.of(MapMessage.RULES, "note = no_such_field"));
        assertThatThrownBy(() -> smt.apply(record(event("abc", "", List.of()))))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Mapping failed")
                .hasMessageContaining("map.test.Event");
    }
}
