package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.HttpHeaderRule;
import ai.pipestream.proto.validate.model.MessageConstraints;
import ai.pipestream.proto.validate.model.StringConstraints;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import ai.pipestream.proto.validate.testdata.PatternGauntlet;
import ai.pipestream.proto.validate.testdata.Person;
import ai.pipestream.proto.validate.testdata.StringGauntlet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * String rules beyond the core gauntlet: the byte-length family (len_bytes/min_bytes/max_bytes),
 * pattern through the dialect, the format {@code _empty} companion rule, and the HTTP-header
 * well-known-regex rules attached through a custom source.
 */
class StringRulesExtendedTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static void assertViolation(ProtoValidator validator, Message message,
            String path, String ruleId) {
        assertThat(validator.validate(message).violations())
                .as("expected %s at %s", ruleId, path)
                .anyMatch(v -> v.path().equals(path) && v.ruleId().equals(ruleId));
    }

    /** A rule source contributing a fixed constraint set to one named field of any message. */
    private record FieldSource(String fieldName, FieldConstraints constraints)
            implements ValidationRuleSource {
        @Override
        public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
            return field.getName().equals(fieldName) ? Optional.of(constraints) : Optional.empty();
        }

        @Override
        public Optional<MessageConstraints> messageConstraints(Descriptor message) {
            return Optional.empty();
        }
    }

    /** Rules on {@code Person.name} with the dialect out of the chain. */
    private static ProtoValidator forName(StringConstraints string) {
        return ProtoValidator.create(List.of(new FieldSource("name",
                FieldConstraints.builder().string(string).build())));
    }

    @Test
    void patternThroughTheDialect() {
        assertThat(VALIDATOR.validate(PatternGauntlet.newBuilder().setLower("abc").build()).valid())
                .isTrue();
        assertViolation(VALIDATOR, PatternGauntlet.newBuilder().setLower("ABC").build(),
                "lower", "string.pattern");
        assertViolation(VALIDATOR, PatternGauntlet.newBuilder().setLower("ab1").build(),
                "lower", "string.pattern");
        // Unset is skipped (explicit presence).
        assertThat(VALIDATOR.validate(PatternGauntlet.getDefaultInstance()).valid()).isTrue();
    }

    @Test
    void byteLengthRulesCountUtf8BytesNotCodePoints() {
        // "🎉" is one code point but four UTF-8 bytes; the two length families must disagree.
        Person.Builder emoji = Person.newBuilder().setName("🎉");

        assertViolation(forName(StringConstraints.builder().minLen(2).build()),
                emoji.build(), "name", "string.min_len");
        assertThat(forName(StringConstraints.builder().minBytes(4).build())
                .validate(emoji.build()).valid()).isTrue();
        assertViolation(forName(StringConstraints.builder().lenBytes(5).build()),
                emoji.build(), "name", "string.len_bytes");
        assertViolation(forName(StringConstraints.builder().maxBytes(3).build()),
                emoji.build(), "name", "string.max_bytes");
        assertThat(forName(StringConstraints.builder().lenBytes(4).build())
                .validate(emoji.build()).valid()).isTrue();
    }

    @Test
    void emptyValueReportsTheFormatEmptyCompanionRule() {
        // id is optional: setting it to "" keeps it present, and an empty value reports
        // string.uuid_empty rather than string.uuid (matching protovalidate).
        assertViolation(VALIDATOR, StringGauntlet.newBuilder().setId("").build(),
                "id", "string.uuid_empty");
        assertViolation(VALIDATOR, StringGauntlet.newBuilder().setHost("").build(),
                "host", "string.hostname_empty");
    }

    @Test
    void httpHeaderNameStrict() {
        ProtoValidator validator = forName(StringConstraints.builder()
                .httpHeader(HttpHeaderRule.NAME_STRICT).build());

        assertThat(validator.validate(Person.newBuilder().setName("X-Custom-Header").build())
                .valid()).isTrue();
        // Pseudo-header form with a leading colon.
        assertThat(validator.validate(Person.newBuilder().setName(":authority").build())
                .valid()).isTrue();
        assertViolation(validator, Person.newBuilder().setName("bad header").build(),
                "name", "string.well_known_regex.header_name");
        // An empty name reports the companion _empty rule.
        assertViolation(validator, Person.newBuilder().setName("").build(),
                "name", "string.well_known_regex.header_name_empty");
    }

    @Test
    void httpHeaderValueStrictPermitsEmptyAndTab() {
        ProtoValidator validator = forName(StringConstraints.builder()
                .httpHeader(HttpHeaderRule.VALUE_STRICT).build());

        assertThat(validator.validate(Person.newBuilder().setName("").build()).valid()).isTrue();
        assertThat(validator.validate(Person.newBuilder().setName("a\tb").build()).valid())
                .isTrue();
        assertViolation(validator, Person.newBuilder().setName("a\nb").build(),
                "name", "string.well_known_regex.header_value");
    }

    @Test
    void httpHeaderLooseRejectsOnlyNulLfCr() {
        ProtoValidator names = forName(StringConstraints.builder()
                .httpHeader(HttpHeaderRule.NAME_LOOSE).build());
        // Spaces are fine in the loose form.
        assertThat(names.validate(Person.newBuilder().setName("any thing").build()).valid())
                .isTrue();
        assertViolation(names, Person.newBuilder().setName("a\rb").build(),
                "name", "string.well_known_regex.header_name");

        ProtoValidator values = forName(StringConstraints.builder()
                .httpHeader(HttpHeaderRule.VALUE_LOOSE).build());
        assertViolation(values, Person.newBuilder().setName("a\0b").build(),
                "name", "string.well_known_regex.header_value");
    }
}
