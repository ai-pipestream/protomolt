package ai.pipestream.proto.validate;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The later-agreed Tier-1 formats wired through the dialect: a
 * {@code slug: true} declaration refuses anything but the agreed contract
 * (lowercase a-z0-9, interior single '.', '_' or '-', alphanumeric ends),
 * {@code region_code: true} refuses anything outside the JDK's ISO
 * 3166 table, and {@code path_safe_name: true} carries the wider
 * reference-name family (aliases holding service FQNs), each with its
 * stable rule id.
 */
class SlugAndRegionRulesTest {

    private static final Descriptors.Descriptor DOC = buildDoc();
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static Descriptors.Descriptor buildDoc() {
        DescriptorProtos.FieldOptions slugRule = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field, FieldRules.newBuilder()
                        .setString(StringRules.newBuilder().setSlug(true)).build())
                .build();
        DescriptorProtos.FieldOptions referenceRule = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field, FieldRules.newBuilder()
                        .setIgnoreIfZero(true)
                        .setString(StringRules.newBuilder().setPathSafeName(true)).build())
                .build();
        DescriptorProtos.FieldOptions regionRule = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field, FieldRules.newBuilder()
                        .setIgnoreIfZero(true)
                        .setString(StringRules.newBuilder().setRegionCode(true)).build())
                .build();
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto
                .newBuilder()
                .setName("slug_region_test.proto")
                .setSyntax("proto3")
                .setPackage("test.formats")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("name").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type
                                        .TYPE_STRING)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label
                                        .LABEL_OPTIONAL)
                                .setOptions(slugRule))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("region").setNumber(2)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type
                                        .TYPE_STRING)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label
                                        .LABEL_OPTIONAL)
                                .setOptions(regionRule))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("reference").setNumber(3)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type
                                        .TYPE_STRING)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label
                                        .LABEL_OPTIONAL)
                                .setOptions(referenceRule)))
                .build();
        try {
            return Descriptors.FileDescriptor
                    .buildFrom(file, new Descriptors.FileDescriptor[0])
                    .findMessageTypeByName("Doc");
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> ruleIds(String name, String region) {
        return ruleIds(name, region, "");
    }

    private static List<String> ruleIds(String name, String region, String reference) {
        return VALIDATOR.validate(DynamicMessage.newBuilder(DOC)
                        .setField(DOC.findFieldByName("name"), name)
                        .setField(DOC.findFieldByName("region"), region)
                        .setField(DOC.findFieldByName("reference"), reference)
                        .build())
                .violations().stream()
                .map(ValidationResult.Violation::ruleId)
                .toList();
    }

    @Test
    void theAgreedSlugContractPassesAndRefusesByName() {
        assertThat(ruleIds("parse-routing", "US")).isEmpty();
        assertThat(ruleIds("a.b_c-d", "")).isEmpty();
        assertThat(ruleIds("Bad--Slug", "US")).containsExactly("string.slug");
        assertThat(ruleIds("-leading", "US")).containsExactly("string.slug");
        // An empty implicit-presence scalar reports the companion empty rule.
        assertThat(ruleIds("", "US")).containsExactly("string.slug_empty");
    }

    @Test
    void regionCodesRefuseOutsideTheJdkTable() {
        assertThat(ruleIds("ok", "XZ")).containsExactly("string.region_code");
        assertThat(ruleIds("ok", "us")).containsExactly("string.region_code");
        // ignore_if_zero: absent-or-valid, the declared form of ^$|... .
        assertThat(ruleIds("ok", "")).isEmpty();
    }

    @Test
    void pathSafeNamesCarryTheWiderReferenceFamily() {
        // The compiler's FQN aliases and the hyphenated sentinel both pass...
        assertThat(ruleIds("ok", "", "pipeline.test.Worker")).isEmpty();
        assertThat(ruleIds("ok", "", "structured-generation")).isEmpty();
        // ...where the slug contract would refuse them.
        assertThat(ruleIds("pipeline.test.Worker", "", "")).containsExactly("string.slug");
        // Traversal shapes still refuse by name.
        assertThat(ruleIds("ok", "", "../escape"))
                .containsExactly("string.path_safe_name");
    }
}
