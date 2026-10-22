package ai.protomolt.proto.validate;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The {@code link_relation: true} declaration wired through the dialect: a Dublin Core term, an
 * IANA registered name, or an absolute extension-relation URI passes; anything else refuses
 * under the stable {@code string.link_relation} rule id, and the empty value reports the
 * {@code _empty} companion unless the field declares {@code ignore_if_zero}.
 */
class LinkRelationRuleTest {

    private static final Descriptors.Descriptor DOC = buildDoc();
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static Descriptors.Descriptor buildDoc() {
        DescriptorProtos.FieldOptions required = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field, FieldRules.newBuilder()
                        .setString(StringRules.newBuilder().setLinkRelation(true)).build())
                .build();
        DescriptorProtos.FieldOptions optional = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field, FieldRules.newBuilder()
                        .setIgnoreIfZero(true)
                        .setString(StringRules.newBuilder().setLinkRelation(true)).build())
                .build();
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto
                .newBuilder()
                .setName("link_relation_test.proto")
                .setSyntax("proto3")
                .setPackage("test.formats")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Link")
                        .addField(stringField("relation", 1, required))
                        .addField(stringField("inverse", 2, optional)))
                .build();
        try {
            return Descriptors.FileDescriptor
                    .buildFrom(file, new Descriptors.FileDescriptor[0])
                    .findMessageTypeByName("Link");
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder stringField(
            String name, int number, DescriptorProtos.FieldOptions options) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setOptions(options);
    }

    private static List<String> ruleIds(String relation, String inverse) {
        return VALIDATOR.validate(DynamicMessage.newBuilder(DOC)
                        .setField(DOC.findFieldByName("relation"), relation)
                        .setField(DOC.findFieldByName("inverse"), inverse)
                        .build())
                .violations().stream()
                .map(ValidationResult.Violation::ruleId)
                .toList();
    }

    @Test
    void dublinCoreTermsPass() {
        assertThat(ruleIds("isPartOf", "hasPart")).isEmpty();
        assertThat(ruleIds("references", "isReferencedBy")).isEmpty();
        assertThat(ruleIds("conformsTo", "")).isEmpty();
    }

    @Test
    void ianaRegisteredNamesPass() {
        assertThat(ruleIds("canonical", "")).isEmpty();
        assertThat(ruleIds("predecessor-version", "successor-version")).isEmpty();
        assertThat(ruleIds("openid2.local_id", "")).isEmpty();
    }

    @Test
    void extensionRelationUrisPass() {
        assertThat(ruleIds("https://example.com/rel/parent", "")).isEmpty();
        assertThat(ruleIds("urn:example:rel:sibling", "")).isEmpty();
    }

    @Test
    void unknownTokensAndRelativeReferencesRefuseByName() {
        assertThat(ruleIds("parent", "")).containsExactly("string.link_relation");
        assertThat(ruleIds("Next", "")).containsExactly("string.link_relation");
        assertThat(ruleIds("is part of", "")).containsExactly("string.link_relation");
        assertThat(ruleIds("rel/parent", "")).containsExactly("string.link_relation");
        assertThat(ruleIds("../parent", "")).containsExactly("string.link_relation");
        assertThat(ruleIds("isPartOf", "IsPartOf")).containsExactly("string.link_relation");
    }

    @Test
    void theEmptyValueReportsTheCompanionRuleUnlessIgnoredAtZero() {
        assertThat(ruleIds("", "")).containsExactly("string.link_relation_empty");
        // ignore_if_zero on the second field: absent-or-valid.
        assertThat(ruleIds("next", "")).isEmpty();
    }
}
