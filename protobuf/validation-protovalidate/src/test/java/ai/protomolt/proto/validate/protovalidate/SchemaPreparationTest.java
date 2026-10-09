package ai.protomolt.proto.validate.protovalidate;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.RuleCompilationException;
import ai.protomolt.proto.validate.protovalidate.testdata.AnnotatedUser;
import build.buf.validate.FieldRules;
import build.buf.validate.ValidateProto;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaPreparationTest {
    @Test
    void catchesInvalidChildRulesForUnsetSingularRepeatedAndMapValues() {
        for (String shape : List.of("singular", "repeated", "map")) {
            Descriptor root = schema(shape, true);
            ProtoValidator validator = validator();
            // No child exists in the candidate, so normal value validation has no child to visit.
            assertThat(validator.validate(DynamicMessage.getDefaultInstance(root)).valid()).isTrue();
            assertThatThrownBy(() -> validator.prepareSchema(root, 10, 20, () -> {}))
                    .isInstanceOf(RuleCompilationException.class).hasMessageContaining("999");
        }
    }

    @Test
    void recursiveSchemaTerminatesAndCountsEachTypeOnce() {
        Descriptor root = schema("recursive", false);
        validator().prepareSchema(root, 2, 3, () -> {});
        assertThatThrownBy(() -> validator().prepareSchema(root, 1, 3, () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("message count");
        assertThatThrownBy(() -> validator().prepareSchema(root, 2, 2, () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("field count");
    }

    @Test
    void mapEntryAndValueBothConsumeGraphBudget() {
        Descriptor root = schema("map", false);
        validator().prepareSchema(root, 3, 4, () -> {});
        assertThatThrownBy(() -> validator().prepareSchema(root, 2, 4, () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("message count");
        assertThatThrownBy(() -> validator().prepareSchema(root, 3, 3, () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("field count");
    }

    @Test
    void discoversBoundsBeforeAttemptingRuleCompilation() {
        Descriptor root = schema("recursive", true);
        assertThatThrownBy(() -> validator().prepareSchema(root, 2, 2, () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("field count");
    }

    @Test
    void propagatesCancellationAndAllowsFreshPreparation() {
        Descriptor root = schema("map", false);
        ProtoValidator validator = validator();
        AtomicInteger visits = new AtomicInteger();
        CancellationException cancelled = new CancellationException("stop preparation");
        assertThatThrownBy(() -> validator.prepareSchema(root, 3, 4, () -> {
            if (visits.incrementAndGet() == 3) throw cancelled;
        })).isSameAs(cancelled);
        validator.prepareSchema(root, 3, 4, () -> {});
        assertThatThrownBy(() -> validator.prepareSchema(root, 0, 4, () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> validator.prepareSchema(root, 3, 0, () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ProtoValidator validator() {
        return ProtoValidator.create(List.of(new ProtovalidateRuleSource()));
    }

    private static Descriptor schema(String shape, boolean invalid) {
        var name = FieldDescriptorProto.newBuilder().setName("name").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING);
        if (invalid) {
            var rules = FieldRules.newBuilder().setUnknownFields(UnknownFieldSet.newBuilder().addField(999,
                    UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
            name.setOptions(FieldOptions.newBuilder().setExtension(ValidateProto.field, rules));
        }
        var child = DescriptorProto.newBuilder().setName("Child").addField(name);
        if (shape.equals("recursive")) {
            child.addField(messageField("next", 2, ".prepare.Child"));
        }
        var parent = DescriptorProto.newBuilder().setName("Parent");
        var field = messageField("child", 1, ".prepare.Child");
        if (shape.equals("map")) {
            var entry = DescriptorProto.newBuilder().setName("ChildEntry")
                    .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                    .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1)
                            .setType(FieldDescriptorProto.Type.TYPE_STRING))
                    .addField(messageField("value", 2, ".prepare.Child"));
            parent.addNestedType(entry);
            field.setTypeName(".prepare.Parent.ChildEntry");
        }
        if (shape.equals("map") || shape.equals("repeated")) {
            field.setLabel(FieldDescriptorProto.Label.LABEL_REPEATED);
        }
        parent.addField(field);
        var file = FileDescriptorProto.newBuilder().setName("prepare.proto").setPackage("prepare").setSyntax("proto3")
                .addDependency(ValidateProto.getDescriptor().getName()).addMessageType(child).addMessageType(parent);
        var set = DescriptorFingerprints.closure(AnnotatedUser.getDescriptor()).toBuilder().addFile(file).build();
        return ClosedDescriptorSet.load(set.toByteString(), new ClosedDescriptorSet.Limits(4_000_000, 100, 1000, 100))
                .stream().filter(f -> f.getName().equals("prepare.proto")).findFirst().orElseThrow()
                .findMessageTypeByName("Parent");
    }

    private static FieldDescriptorProto.Builder messageField(String name, int number, String type) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(type);
    }
}
