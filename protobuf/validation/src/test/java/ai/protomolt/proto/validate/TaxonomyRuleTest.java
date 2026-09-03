package ai.protomolt.proto.validate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.validate.spi.TaxonomyCatalog;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The taxonomy membership rule: a TreePath field bound to a taxonomy by name
 * checks its rendered path against the taxonomy mounted on the validator. The
 * binding is schema truth; the taxonomy's content is mount configuration;
 * enforcement is fail-closed with the mounted version as evidence. The
 * descriptors are built dynamically because this module cannot depend on the
 * generated types — the validator resolves TreePath by full name alone.
 */
class TaxonomyRuleTest {

    private static final Descriptors.FileDescriptor FILE = buildFile();
    private static final Descriptors.Descriptor TREE_PATH = FILE.findMessageTypeByName("TreePath");
    private static final Descriptors.Descriptor DOC = FILE.findMessageTypeByName("Doc");
    private static final Descriptors.Descriptor MISDECLARED =
            FILE.findMessageTypeByName("Misdeclared");
    private static final Descriptors.Descriptor NESTED_MISDECLARED =
            FILE.findMessageTypeByName("NestedMisdeclared");

    private static final TaxonomyCatalog CATALOG = name -> "products".equals(name)
            ? Optional.of(TaxonomyCatalog.Mounted.of("products", "v7", List.of(
                    List.of("electronics", "computers", "laptops"),
                    List.of("media", "books"))))
            : Optional.empty();

    private static final ProtoValidator VALIDATOR = ProtoValidator.create(CATALOG);

    private static Descriptors.FileDescriptor buildFile() {
        DescriptorProtos.FieldOptions taxonomyRule = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field,
                        FieldRules.newBuilder().setTaxonomy("products").build())
                .build();
        DescriptorProtos.FieldOptions structuralRules = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field, FieldRules.newBuilder()
                        .setRepeated(RepeatedRules.newBuilder()
                                .setMinItems(1)
                                .setItems(FieldRules.newBuilder()
                                        .setString(StringRules.newBuilder()
                                                .setMinLen(1)
                                                .setNotContains("/"))))
                        .build())
                .build();
        DescriptorProtos.FieldOptions nestedTaxonomyRule = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field, FieldRules.newBuilder()
                        .setRepeated(RepeatedRules.newBuilder()
                                .setItems(FieldRules.newBuilder().setTaxonomy("products")))
                        .build())
                .build();
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("taxonomy_rule_test.proto")
                .setSyntax("proto3")
                .setPackage("ai.pipestream.proto.types.v1")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("TreePath")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("segments").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                                .setOptions(structuralRules)))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("category").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.proto.types.v1.TreePath")
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setOptions(taxonomyRule))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("categories").setNumber(2)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.proto.types.v1.TreePath")
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                                .setOptions(taxonomyRule)))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Misdeclared")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("tag").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setOptions(taxonomyRule)))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("NestedMisdeclared")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("paths").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.proto.types.v1.TreePath")
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                                .setOptions(nestedTaxonomyRule)))
                .build();
        try {
            return Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0]);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static DynamicMessage treePath(String... segments) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(TREE_PATH);
        Descriptors.FieldDescriptor field = TREE_PATH.findFieldByName("segments");
        for (String segment : segments) {
            builder.addRepeatedField(field, segment);
        }
        return builder.build();
    }

    private static DynamicMessage doc(DynamicMessage category) {
        return DynamicMessage.newBuilder(DOC)
                .setField(DOC.findFieldByName("category"), category)
                .build();
    }

    @Test
    void entryPathsAndTheirAncestorsAreMembers() {
        assertThat(VALIDATOR.validate(doc(treePath("media", "books"))).valid()).isTrue();
        assertThat(VALIDATOR.validate(doc(treePath("electronics"))).valid()).isTrue();
        assertThat(VALIDATOR.validate(doc(treePath("electronics", "computers"))).valid())
                .isTrue();
    }

    @Test
    void aNonMemberPathIsRefusedWithTheMountedVersionAsEvidence() {
        ValidationResult result = VALIDATOR.validate(doc(treePath("media", "movies")));
        assertThat(result.violations()).singleElement().satisfies(violation -> {
            assertThat(violation.path()).isEqualTo("category");
            assertThat(violation.ruleId()).isEqualTo("taxonomy.member");
            assertThat(violation.message())
                    .contains("media/movies").contains("products").contains("v7");
        });
    }

    @Test
    void aPathDeeperThanAnyEntryIsNotAMember() {
        ValidationResult result = VALIDATOR.validate(
                doc(treePath("electronics", "computers", "laptops", "gaming")));
        assertThat(result.violations())
                .extracting(ValidationResult.Violation::ruleId)
                .containsExactly("taxonomy.member");
    }

    @Test
    void anUnmountedTaxonomyRefusesFailClosed() {
        // The default validator mounts nothing: declared membership that cannot
        // be checked refuses, never passes.
        ValidationResult result = ProtoValidator.create()
                .validate(doc(treePath("media", "books")));
        assertThat(result.violations()).singleElement().satisfies(violation -> {
            assertThat(violation.ruleId()).isEqualTo("taxonomy.unmounted");
            assertThat(violation.message()).contains("products");
        });
    }

    @Test
    void aRepeatedTreePathFieldChecksEveryElement() {
        Descriptors.FieldDescriptor categories = DOC.findFieldByName("categories");
        DynamicMessage message = DynamicMessage.newBuilder(DOC)
                .addRepeatedField(categories, treePath("media", "books"))
                .addRepeatedField(categories, treePath("media", "movies"))
                .build();
        ValidationResult result = VALIDATOR.validate(message);
        assertThat(result.violations()).singleElement().satisfies(violation -> {
            assertThat(violation.path()).isEqualTo("categories[1]");
            assertThat(violation.ruleId()).isEqualTo("taxonomy.member");
        });
    }

    @Test
    void anEmptyPathIsAStructuralRefusalNotAMembershipOne() {
        ValidationResult result = VALIDATOR.validate(doc(treePath()));
        assertThat(result.violations())
                .extracting(ValidationResult.Violation::ruleId)
                .containsExactly("repeated.min_items");
    }

    @Test
    void taxonomyOnANonTreePathFieldFailsRuleCompilation() {
        assertThatThrownBy(() -> VALIDATOR.validate(DynamicMessage.getDefaultInstance(MISDECLARED)))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("Misdeclared.tag")
                .hasMessageContaining("TreePath");
    }

    @Test
    void taxonomyOnRepeatedItemsFailsRuleCompilation() {
        assertThatThrownBy(() ->
                VALIDATOR.validate(DynamicMessage.getDefaultInstance(NESTED_MISDECLARED)))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("TreePath field itself");
    }
}
