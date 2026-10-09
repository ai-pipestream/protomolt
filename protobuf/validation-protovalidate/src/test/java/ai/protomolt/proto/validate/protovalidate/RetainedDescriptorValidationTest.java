package ai.protomolt.proto.validate.protovalidate;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.RuleCompilationException;
import ai.protomolt.proto.validate.protovalidate.testdata.AnnotatedUser;
import build.buf.validate.ValidateProto;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetainedDescriptorValidationTest {
    @Test
    void retainedAnnotationsEnforceBoundsAndCrossFieldRules() {
        Descriptor type = retained(UnaryOperator.identity());
        var validator = ProtoValidator.forMessageType(type, List.of(new ProtovalidateRuleSource()));
        var valid = DynamicMessage.newBuilder(type).setField(type.findFieldByName("name"), "Alice").build();
        assertThat(validator.validate(valid).valid()).isTrue();
        assertThat(validator.validate(valid.toBuilder().setField(type.findFieldByName("age"), 151).build())
                .violations()).anyMatch(v -> v.path().equals("age") && v.ruleId().equals("uint32.lte"));
        var paid = type.findFieldByName("plan").getEnumType().findValueByNumber(2);
        assertThat(validator.validate(valid.toBuilder().setField(type.findFieldByName("plan"), paid).build())
                .violations()).anyMatch(v -> v.ruleId().equals("user.paid_needs_email"));
    }

    @Test
    void rejectsWrongWireTypeForFieldAnnotation() {
        Descriptor type = retained(message -> {
            var field = message.getFieldBuilder(0);
            field.setOptions(FieldOptions.newBuilder().setUnknownFields(wrongWire(ValidateProto.field.getNumber())));
            return message;
        });
        assertThatThrownBy(() -> new ProtovalidateRuleSource().fieldConstraints(type.findFieldByName("name")))
                .isInstanceOf(RuleCompilationException.class);
    }

    @Test
    void rejectsWrongWireTypeForMessageAnnotation() {
        Descriptor type = retained(message -> message.setOptions(MessageOptions.newBuilder()
                .setUnknownFields(wrongWire(ValidateProto.message.getNumber()))));
        assertThatThrownBy(() -> new ProtovalidateRuleSource().messageConstraints(type))
                .isInstanceOf(RuleCompilationException.class);
    }

    @Test
    void rejectsWrongWireTypeForOneofAnnotation() {
        Descriptor type = retained(message -> {
            // Turn synthetic optional oneofs into real oneofs before appending another.
            // Protobuf requires synthetic oneofs to follow all real oneofs.
            for (var field : message.getFieldBuilderList()) field.clearProto3Optional();
            int index = message.getOneofDeclCount();
            message.addOneofDecl(OneofDescriptorProto.newBuilder().setName("choice").setOptions(
                    OneofOptions.newBuilder().setUnknownFields(wrongWire(ValidateProto.oneof.getNumber()))));
            message.addField(FieldDescriptorProto.newBuilder().setName("choice_value").setNumber(100)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING).setOneofIndex(index));
            return message;
        });
        assertThatThrownBy(() -> new ProtovalidateRuleSource().messageConstraints(type))
                .isInstanceOf(RuleCompilationException.class);
    }

    @Test
    void rejectsValidFieldAnnotationMixedWithWrongWireOccurrence() {
        Descriptor type = retained(message -> {
            var field = message.getFieldBuilder(0);
            field.setOptions(field.getOptions().toBuilder()
                    .mergeUnknownFields(wrongWire(ValidateProto.field.getNumber())));
            return message;
        });
        assertThatThrownBy(() -> ProtoValidator.forMessageType(type, List.of(new ProtovalidateRuleSource()))
                .validate(DynamicMessage.getDefaultInstance(type)))
                .isInstanceOf(RuleCompilationException.class);
    }

    @Test
    void rejectsWrongWirePredefinedRuleDeclaration() {
        var original = ai.protomolt.proto.validate.protovalidate.testdata.PredefinedUser.getDescriptor();
        var set = DescriptorFingerprints.closure(original).toBuilder();
        boolean changed = false;
        for (var file : set.getFileBuilderList()) {
            for (var extension : file.getExtensionBuilderList()) {
                if (extension.getOptions().hasExtension(ValidateProto.predefined)) {
                    extension.setOptions(FieldOptions.newBuilder()
                            .setUnknownFields(wrongWire(ValidateProto.predefined.getNumber())));
                    changed = true;
                }
            }
        }
        assertThat(changed).isTrue();
        var files = ClosedDescriptorSet.load(set.build().toByteString(),
                new ClosedDescriptorSet.Limits(4_000_000, 100, 1000, 100));
        var type = files.stream().filter(f -> f.getName().equals(original.getFile().getName()))
                .findFirst().orElseThrow().findMessageTypeByName(original.getName());
        assertThatThrownBy(() -> ProtoValidator.forMessageType(type, List.of(new ProtovalidateRuleSource()))
                .validate(DynamicMessage.getDefaultInstance(type)))
                .isInstanceOf(RuleCompilationException.class);
    }

    @Test
    void rejectsUnknownIgnoreEnumBeforeValidatingAbsentField() {
        for (int value : new int[]{2, -1, 999}) {
            assertInvalidRules("name", unknownIgnore(value),
                    "ignore");
        }
    }

    @Test
    void rejectsUnknownRegexEnumBeforeValidatingAbsentField() {
        for (int value : new int[]{-1, 999}) {
            assertInvalidRules("name", build.buf.validate.FieldRules.newBuilder().setString(
                    unknownRegex(value)).build(),
                    "well_known_regex");
        }
    }

    @Test
    void rejectsUnknownEnumsInsideEmptyCollections() {
        var invalidIgnore = unknownIgnore(2);
        assertInvalidRules("tags", build.buf.validate.FieldRules.newBuilder().setRepeated(
                build.buf.validate.RepeatedRules.newBuilder().setItems(invalidIgnore)).build(), "ignore");
        assertInvalidRules("limits", build.buf.validate.FieldRules.newBuilder().setMap(
                build.buf.validate.MapRules.newBuilder().setKeys(invalidIgnore)).build(), "ignore");
        assertInvalidRules("limits", build.buf.validate.FieldRules.newBuilder().setMap(
                build.buf.validate.MapRules.newBuilder().setValues(invalidIgnore)).build(), "ignore");
        var invalidRegex = build.buf.validate.FieldRules.newBuilder().setString(
                unknownRegex(999));
        assertInvalidRules("tags", build.buf.validate.FieldRules.newBuilder().setRepeated(
                build.buf.validate.RepeatedRules.newBuilder().setItems(invalidRegex)).build(), "well_known_regex");
    }

    @Test
    void rejectsUnknownTopLevelAndNestedRuleNumbers() {
        assertInvalidRules("name", build.buf.validate.FieldRules.newBuilder()
                .setUnknownFields(wrongWire(999)).build(), "999");
        assertInvalidRules("name", build.buf.validate.FieldRules.newBuilder().setString(
                build.buf.validate.StringRules.newBuilder().setUnknownFields(wrongWire(999))).build(), "999");
        assertInvalidRules("name", build.buf.validate.FieldRules.newBuilder()
                .setIgnore(build.buf.validate.Ignore.IGNORE_ALWAYS).setString(
                        build.buf.validate.StringRules.newBuilder().setUnknownFields(wrongWire(999))).build(), "999");
    }

    @Test
    void rejectsWrongWireKnownNestedRule() {
        var unknown = UnknownFieldSet.newBuilder().addField(build.buf.validate.StringRules.MIN_LEN_FIELD_NUMBER,
                UnknownFieldSet.Field.newBuilder().addLengthDelimited(com.google.protobuf.ByteString.EMPTY).build()).build();
        assertInvalidRules("name", build.buf.validate.FieldRules.newBuilder().setString(
                build.buf.validate.StringRules.newBuilder().setUnknownFields(unknown)).build(), "min_len");
    }

    @Test
    void rejectsUnknownMessageAndCelRuleFields() {
        for (var rules : List.of(
                build.buf.validate.MessageRules.newBuilder().setUnknownFields(wrongWire(999)).build(),
                build.buf.validate.MessageRules.newBuilder().addCel(build.buf.validate.Rule.newBuilder()
                        .setId("unknown").setExpression("true").setUnknownFields(wrongWire(999))).build())) {
            Descriptor type = retained(message -> message.setOptions(MessageOptions.newBuilder()
                    .setExtension(ValidateProto.message, rules)));
            assertThatThrownBy(() -> ProtoValidator.forMessageType(type, List.of(new ProtovalidateRuleSource()))
                    .validate(DynamicMessage.getDefaultInstance(type)))
                    .isInstanceOf(RuleCompilationException.class).hasMessageContaining("999");
        }
    }

    @Test
    void rejectsIncorrectlyEncodedDeclaredPredefinedValue() {
        var original = ai.protomolt.proto.validate.protovalidate.testdata.PredefinedUser.getDescriptor();
        var unknown = UnknownFieldSet.newBuilder().addField(1101, UnknownFieldSet.Field.newBuilder()
                .addLengthDelimited(com.google.protobuf.ByteString.copyFromUtf8("wrong bool wire type")).build()).build();
        var rules = build.buf.validate.FieldRules.newBuilder().setString(
                build.buf.validate.StringRules.newBuilder().setUnknownFields(unknown)).build();
        var type = retained(original, message -> {
            message.getFieldBuilder(0).setOptions(FieldOptions.newBuilder().setExtension(ValidateProto.field, rules));
            return message;
        });
        assertThatThrownBy(() -> ProtoValidator.forMessageType(type, List.of(new ProtovalidateRuleSource()))
                .validate(DynamicMessage.getDefaultInstance(type)))
                .isInstanceOf(RuleCompilationException.class).hasMessageContaining("predefined rule");
    }

    private static build.buf.validate.FieldRules unknownIgnore(int value) {
        return build.buf.validate.FieldRules.newBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(build.buf.validate.FieldRules.IGNORE_FIELD_NUMBER,
                        UnknownFieldSet.Field.newBuilder().addVarint(value).build()).build()).build();
    }

    private static build.buf.validate.StringRules unknownRegex(int value) {
        return build.buf.validate.StringRules.newBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(build.buf.validate.StringRules.WELL_KNOWN_REGEX_FIELD_NUMBER,
                        UnknownFieldSet.Field.newBuilder().addVarint(value).build()).build()).build();
    }

    private static void assertInvalidRules(String fieldName, build.buf.validate.FieldRules rules, String error) {
        Descriptor type = retained(message -> {
            var field = message.getFieldBuilderList().stream().filter(f -> f.getName().equals(fieldName))
                    .findFirst().orElseThrow();
            field.setOptions(FieldOptions.newBuilder().setExtension(ValidateProto.field, rules));
            return message;
        });
        assertThatThrownBy(() -> ProtoValidator.forMessageType(type, List.of(new ProtovalidateRuleSource()))
                .validate(DynamicMessage.getDefaultInstance(type)))
                .isInstanceOf(RuleCompilationException.class).hasMessageContaining(error);
    }

    private static UnknownFieldSet wrongWire(int number) {
        return UnknownFieldSet.newBuilder().addField(number,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    }

    private static Descriptor retained(UnaryOperator<DescriptorProto.Builder> change) {
        return retained(AnnotatedUser.getDescriptor(), change);
    }

    private static Descriptor retained(Descriptor original, UnaryOperator<DescriptorProto.Builder> change) {
        var set = DescriptorFingerprints.closure(original).toBuilder();
        for (int i = 0; i < set.getFileCount(); i++) {
            if (!set.getFile(i).getName().equals(original.getFile().getName())) continue;
            var file = set.getFileBuilder(i);
            for (int j = 0; j < file.getMessageTypeCount(); j++) {
                if (file.getMessageType(j).getName().equals(original.getName())) {
                    file.setMessageType(j, change.apply(file.getMessageType(j).toBuilder()));
                }
            }
        }
        return ClosedDescriptorSet.load(set.build().toByteString(),
                        new ClosedDescriptorSet.Limits(4_000_000, 100, 1000, 100)).stream()
                .filter(f -> f.getName().equals(original.getFile().getName())).findFirst().orElseThrow()
                .findMessageTypeByName(original.getName());
    }
}
