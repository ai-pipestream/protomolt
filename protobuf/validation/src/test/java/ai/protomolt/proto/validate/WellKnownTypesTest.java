package ai.protomolt.proto.validate;

import ai.protomolt.proto.validate.model.AnyConstraints;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.FieldMaskConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import ai.protomolt.proto.validate.testdata.Envelope;
import ai.protomolt.proto.validate.testdata.Item;
import ai.protomolt.proto.validate.testdata.Person;
import ai.protomolt.proto.validate.testdata.WrapperGauntlet;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Well-known type rules: google.protobuf.Any type-URL allow-lists, FieldMask path coverage
 * (a mask path is covered by a rule entry that equals it or is a dot-prefix of it), and the
 * wrapper types applying scalar rules to their wrapped value.
 */
class WellKnownTypesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static void assertValid(Message message) {
        assertThat(VALIDATOR.validate(message).valid())
                .as("expected no violations, got %s", VALIDATOR.validate(message).violations())
                .isTrue();
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

    private static ProtoValidator forField(String fieldName, FieldConstraints constraints) {
        return ProtoValidator.create(List.of(new FieldSource(fieldName, constraints)));
    }

    // ---- google.protobuf.Any ----

    @Test
    void anyInAllowListsTypeUrls() {
        ProtoValidator validator = forField("payload", FieldConstraints.builder()
                .any(new AnyConstraints(List.of("type.googleapis.com/ai.pipestream.proto.validate.testdata.v1.Item"),
                        List.of()))
                .build());

        assertThat(validator.validate(Envelope.newBuilder()
                        .setPayload(Any.pack(Item.newBuilder().setName("x").build())).build())
                .valid()).isTrue();
        assertThat(validator.validate(Envelope.newBuilder()
                        .setPayload(Any.pack(Person.getDefaultInstance())).build()).violations())
                .anyMatch(v -> v.path().equals("payload") && v.ruleId().equals("any.in"));
    }

    @Test
    void anyNotInForbidsTypeUrls() {
        ProtoValidator validator = forField("payload", FieldConstraints.builder()
                .any(new AnyConstraints(List.of(),
                        List.of("type.googleapis.com/ai.pipestream.proto.validate.testdata.v1.Item")))
                .build());

        assertThat(validator.validate(Envelope.newBuilder()
                        .setPayload(Any.pack(Item.newBuilder().setName("x").build())).build())
                .violations())
                .anyMatch(v -> v.path().equals("payload") && v.ruleId().equals("any.not_in"));
        assertThat(validator.validate(Envelope.newBuilder()
                        .setPayload(Any.pack(Person.getDefaultInstance())).build()).violations())
                .noneMatch(v -> v.ruleId().equals("any.not_in"));
        // An unset Any is absent (message presence) and never checked.
        assertThat(validator.validate(Envelope.getDefaultInstance()).valid()).isTrue();
    }

    // ---- google.protobuf.FieldMask ----

    @Test
    void fieldMaskConstComparesTheCommaJoinedForm() {
        ProtoValidator validator = forField("mask", FieldConstraints.builder()
                .fieldMask(new FieldMaskConstraints(Optional.of("a,b.c"), List.of(), List.of()))
                .build());

        assertThat(validator.validate(Envelope.newBuilder()
                        .setMask(FieldMask.newBuilder().addPaths("a").addPaths("b.c")).build())
                .valid()).isTrue();
        assertThat(validator.validate(Envelope.newBuilder()
                        .setMask(FieldMask.newBuilder().addPaths("a").addPaths("b")).build())
                .violations())
                .anyMatch(v -> v.path().equals("mask") && v.ruleId().equals("field_mask.const"));
    }

    @Test
    void fieldMaskInCoversNestedPathsByPrefix() {
        ProtoValidator validator = forField("mask", FieldConstraints.builder()
                .fieldMask(new FieldMaskConstraints(Optional.empty(), List.of("a"), List.of()))
                .build());

        // "a" itself and the nested "a.b" are both covered by the entry "a".
        assertThat(validator.validate(Envelope.newBuilder()
                        .setMask(FieldMask.newBuilder().addPaths("a").addPaths("a.b")).build())
                .valid()).isTrue();
        // "ab" is not nested under "a" — prefix coverage is on dot boundaries.
        assertThat(validator.validate(Envelope.newBuilder()
                        .setMask(FieldMask.newBuilder().addPaths("ab")).build()).violations())
                .anyMatch(v -> v.path().equals("mask") && v.ruleId().equals("field_mask.in"));
    }

    @Test
    void fieldMaskNotInRejectsCoveredPaths() {
        ProtoValidator validator = forField("mask", FieldConstraints.builder()
                .fieldMask(new FieldMaskConstraints(Optional.empty(), List.of(), List.of("a")))
                .build());

        assertThat(validator.validate(Envelope.newBuilder()
                        .setMask(FieldMask.newBuilder().addPaths("a.b")).build()).violations())
                .anyMatch(v -> v.path().equals("mask") && v.ruleId().equals("field_mask.not_in"));
        assertThat(validator.validate(Envelope.newBuilder()
                        .setMask(FieldMask.newBuilder().addPaths("b")).build()).valid())
                .isTrue();
    }

    // ---- wrapper types ----

    @Test
    void wrapperFieldsApplyScalarRulesToTheWrappedValue() {
        assertValid(WrapperGauntlet.getDefaultInstance());
        assertValid(WrapperGauntlet.newBuilder()
                .setName(StringValue.of("ab")).setCount(Int32Value.of(1)).build());

        assertThat(VALIDATOR.validate(WrapperGauntlet.newBuilder()
                        .setName(StringValue.of("a")).build()).violations())
                .anyMatch(v -> v.path().equals("name") && v.ruleId().equals("string.min_len"));
        // A wrapped zero is present (message presence) and fails gte: 1.
        assertThat(VALIDATOR.validate(WrapperGauntlet.newBuilder()
                        .setCount(Int32Value.of(0)).build()).violations())
                .anyMatch(v -> v.path().equals("count") && v.ruleId().equals("int32.gte"));
    }
}
