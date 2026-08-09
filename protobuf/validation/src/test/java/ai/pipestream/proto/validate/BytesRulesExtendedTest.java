package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.model.BytesConstraints;
import ai.pipestream.proto.validate.model.BytesFormat;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.MessageConstraints;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import ai.pipestream.proto.validate.testdata.MiscGauntlet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Bytes rules the Pipestream dialect does not express — const, pattern, in/not_in and the
 * packed-length formats — attached through a custom {@link ValidationRuleSource} on
 * {@code MiscGauntlet.payload} (the custom chain replaces the dialect, so its rules don't run).
 */
class BytesRulesExtendedTest {

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

    private static ProtoValidator forPayload(BytesConstraints bytes) {
        return ProtoValidator.create(List.of(new FieldSource("payload",
                FieldConstraints.builder().bytes(bytes).build())));
    }

    private static ByteString bytes(int... values) {
        byte[] raw = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            raw[i] = (byte) values[i];
        }
        return ByteString.copyFrom(raw);
    }

    private static MiscGauntlet payload(ByteString value) {
        return MiscGauntlet.newBuilder().setPayload(value).build();
    }

    @Test
    void bytesConst() {
        ProtoValidator validator = forPayload(
                BytesConstraints.builder().constant(bytes(1, 2)).build());

        assertThat(validator.validate(payload(bytes(1, 2))).valid()).isTrue();
        assertThat(validator.validate(payload(bytes(1, 3))).violations())
                .anyMatch(v -> v.path().equals("payload") && v.ruleId().equals("bytes.const"));
    }

    @Test
    void bytesPatternMatchesTheUtf8Decode() {
        ProtoValidator validator = forPayload(
                BytesConstraints.builder().pattern("^[0-9]+$").build());

        assertThat(validator.validate(payload(ByteString.copyFromUtf8("123"))).valid()).isTrue();
        assertThat(validator.validate(payload(ByteString.copyFromUtf8("12a"))).violations())
                .anyMatch(v -> v.path().equals("payload") && v.ruleId().equals("bytes.pattern"));
    }

    @Test
    void bytesPatternOnNonUtf8IsAnEvaluationError() {
        // protovalidate decodes the bytes as UTF-8 to apply the pattern; undecodable bytes are a
        // runtime failure (RuleEvaluationException), not a validation violation.
        ProtoValidator validator = forPayload(
                BytesConstraints.builder().pattern("^[0-9]+$").build());

        assertThatThrownBy(() -> validator.validate(payload(bytes(0xff, 0xfe))))
                .isInstanceOf(RuleEvaluationException.class)
                .satisfies(e -> assertThat(((RuleEvaluationException) e).ruleId())
                        .isEqualTo("bytes.pattern"));
    }

    @Test
    void bytesInAndNotIn() {
        ProtoValidator inValidator = forPayload(
                BytesConstraints.builder().in(List.of(bytes(1), bytes(2))).build());
        assertThat(inValidator.validate(payload(bytes(1))).valid()).isTrue();
        assertThat(inValidator.validate(payload(bytes(3))).violations())
                .anyMatch(v -> v.path().equals("payload") && v.ruleId().equals("bytes.in"));

        ProtoValidator notInValidator = forPayload(
                BytesConstraints.builder().notIn(List.of(bytes(9))).build());
        assertThat(notInValidator.validate(payload(bytes(1))).valid()).isTrue();
        assertThat(notInValidator.validate(payload(bytes(9))).violations())
                .anyMatch(v -> v.path().equals("payload") && v.ruleId().equals("bytes.not_in"));
    }

    @Test
    void bytesFormatsCheckThePackedLength() {
        ProtoValidator validator = forPayload(
                BytesConstraints.builder().format(BytesFormat.IP).build());

        assertThat(validator.validate(payload(bytes(127, 0, 0, 1))).valid()).isTrue();
        assertThat(validator.validate(payload(bytes(1, 2, 3))).violations())
                .anyMatch(v -> v.path().equals("payload") && v.ruleId().equals("bytes.ip"));
    }

    @Test
    void emptyBytesReportTheCompanionEmptyRule() {
        // payload is optional: setting it to empty keeps it present, and an empty value is
        // reported under the <id>_empty companion rule rather than the format rule itself.
        ProtoValidator validator = forPayload(
                BytesConstraints.builder().format(BytesFormat.UUID).build());

        assertThat(validator.validate(payload(ByteString.EMPTY)).violations())
                .anyMatch(v -> v.path().equals("payload") && v.ruleId().equals("bytes.uuid_empty"));
        assertThat(validator.validate(payload(bytes(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                15, 16))).valid()).isTrue();
    }

    @Test
    void unsetBytesAreSkipped() {
        ProtoValidator validator = forPayload(
                BytesConstraints.builder().constant(bytes(1)).pattern("x")
                        .in(List.of(bytes(1))).format(BytesFormat.IPV4).build());

        assertThat(validator.validate(MiscGauntlet.getDefaultInstance()).valid()).isTrue();
    }
}
