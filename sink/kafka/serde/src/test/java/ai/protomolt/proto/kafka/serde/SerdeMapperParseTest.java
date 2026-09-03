package ai.protomolt.proto.kafka.serde;

import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The mapper's rule parsing and application, exercised directly rather than through the serde.
 * The cases worth pinning: an empty rule list costs nothing (no mapper at all), rules see each
 * other's work in order, and the CEL filter separator is the <em>last</em> {@code " if "} in the
 * entry — which is what lets a CEL string literal contain the words " if ".
 */
class SerdeMapperParseTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.mapunit.v1;
            message Event {
              string id = 1;
              string legacy_name = 2;
              string display_name = 3;
              string scratch = 4;
              repeated string tags = 5;
            }
            """;

    private static Descriptor eventType;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("serde/mapunit/v1/event.proto", PROTO, "test").build());
        eventType = compiled.descriptorFor("serde/mapunit/v1/event.proto").orElseThrow()
                .findMessageTypeByName("Event");
    }

    private static DynamicMessage.Builder event() {
        return DynamicMessage.newBuilder(eventType);
    }

    private static Message.Builder set(Message.Builder builder, String field, Object value) {
        return builder.setField(eventType.findFieldByName(field), value);
    }

    private static Object field(Message message, String name) {
        return message.getField(message.getDescriptorForType().findFieldByName(name));
    }

    /** No rules means no mapper: the record path pays nothing. */
    @Test
    void noRulesParsesToNoMapper() {
        assertThat(SerdeMapper.parse(null, "test.key")).isNull();
        assertThat(SerdeMapper.parse(List.of(), "test.key")).isNull();
        // Blank entries are skipped; a list of only blanks is no mapper either.
        assertThat(SerdeMapper.parse(List.of("", "   "), "test.key")).isNull();
    }

    /** All three text rule forms in one list: assign, append to a repeated field, and clear. */
    @Test
    void appliesTextRulesInOrder() throws Exception {
        SerdeMapper mapper = SerdeMapper.parse(
                List.of("display_name = legacy_name", "tags += id", "-scratch"), "test.key");

        Message mapped = mapper.apply(set(set(set(event(),
                "id", "A-1"), "legacy_name", "Old"), "scratch", "temp").build());

        assertThat(field(mapped, "display_name")).isEqualTo("Old");
        assertThat(field(mapped, "tags")).isEqualTo(List.of("A-1"));
        assertThat(field(mapped, "scratch")).isEqualTo("");
    }

    /** Each rule sees the previous rules' work: reversing the list would change the outcome. */
    @Test
    void rulesSeeEachOthersWork() throws Exception {
        SerdeMapper mapper = SerdeMapper.parse(
                List.of("display_name = legacy_name", "id = display_name"), "test.key");

        Message mapped = mapper.apply(set(set(event(),
                "id", "A-1"), "legacy_name", "Old").build());

        assertThat(field(mapped, "id")).isEqualTo("Old");
    }

    /**
     * The filter separator is the last {@code " if "} in the entry, so a CEL string literal may
     * contain the words themselves: the selector below is
     * {@code input.legacy_name + ' if ' + input.id} and the filter {@code input.scratch == ''}.
     * A first-occurrence split would hand the compiler a selector ending in an open quote.
     */
    @Test
    void celFiltersSplitOnTheLastIfMarker() throws Exception {
        SerdeMapper mapper = SerdeMapper.parse(List.of(
                "display_name := input.legacy_name + ' if ' + input.id if input.scratch == ''"),
                "test.key");

        Message filtered = mapper.apply(set(set(event(),
                "id", "A-1"), "legacy_name", "Old").build());
        assertThat(field(filtered, "display_name")).isEqualTo("Old if A-1");

        // The filter holds: a record carrying scratch data keeps its display name.
        Message kept = mapper.apply(set(set(set(event(),
                "id", "A-2"), "legacy_name", "Old"), "scratch", "temp").build());
        assertThat(field(kept, "display_name")).isEqualTo("");
    }

    @Test
    void entriesThatAreNotRulesFailAtParseTime() {
        assertThatThrownBy(() -> SerdeMapper.parse(List.of("just some words"), "my.config.key"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("my.config.key")
                .hasMessageContaining("not a mapping rule")
                .hasMessageContaining("just some words");
    }

    /** A CEL expression that does not compile fails the record, not the configure. */
    @Test
    void anUncompilableCelRuleFailsTheFirstRecord() {
        SerdeMapper mapper = SerdeMapper.parse(
                List.of("display_name := input.nope ++"), "test.key");
        Message message = set(event(), "id", "A-1").build();
        assertThatThrownBy(() -> mapper.apply(message))
                .isInstanceOf(ai.protomolt.proto.cel.CelEvaluationException.class);
    }
}
