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

    private static UnknownFieldSet wrongWire(int number) {
        return UnknownFieldSet.newBuilder().addField(number,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    }

    private static Descriptor retained(UnaryOperator<DescriptorProto.Builder> change) {
        var original = AnnotatedUser.getDescriptor();
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
