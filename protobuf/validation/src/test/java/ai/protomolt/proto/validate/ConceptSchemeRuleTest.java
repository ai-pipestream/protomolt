package ai.protomolt.proto.validate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import ai.protomolt.proto.validate.spi.ConceptSchemeCatalog;
import ai.protomolt.proto.validate.spi.PostalCodeCatalog;
import ai.protomolt.proto.validate.spi.TaxonomyCatalog;
import ai.protomolt.proto.validate.spi.ValidationRuleSources;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The concept-scheme membership rule: a ConceptRef field bound to schemes by id checks
 * each value's scheme against the binding and its concept against the scheme mounted on
 * the validator. The binding is schema truth, the scheme content is mount configuration,
 * and enforcement is fail-closed with the mounted version as evidence. The descriptors are
 * built dynamically because this module cannot depend on the generated types: the
 * validator resolves ConceptRef by full name alone.
 */
class ConceptSchemeRuleTest {

    private static final Descriptors.FileDescriptor FILE = buildFile();
    private static final Descriptors.Descriptor CONCEPT_REF = FILE.findMessageTypeByName("ConceptRef");
    private static final Descriptors.Descriptor DOC = FILE.findMessageTypeByName("Doc");

    // "subject" is mounted at v3 with a deprecated member; "region" is bound but unmounted.
    private static final ConceptSchemeCatalog CATALOG = schemeId -> "subject".equals(schemeId)
            ? Optional.of(new ConceptSchemeCatalog.Mounted(
                    "subject", "v3", Set.of("physics", "chemistry", "alchemy")))
            : Optional.empty();

    private static final ProtoValidator VALIDATOR = ProtoValidator.create(
            ValidationRuleSources.defaults(), TaxonomyCatalog.empty(), PostalCodeCatalog.empty(),
            CATALOG);

    private static DescriptorProtos.FieldOptions rules(FieldRules rules) {
        return DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field, rules)
                .build();
    }

    private static FieldDescriptorProto.Builder field(
            String name, int number, FieldDescriptorProto.Type type, String typeName,
            FieldDescriptorProto.Label label, DescriptorProtos.FieldOptions options) {
        FieldDescriptorProto.Builder field = FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type).setLabel(label);
        if (typeName != null) {
            field.setTypeName(typeName);
        }
        if (options != null) {
            field.setOptions(options);
        }
        return field;
    }

    private static Descriptors.FileDescriptor buildFile() {
        DescriptorProtos.FieldOptions bound = rules(FieldRules.newBuilder()
                .addConceptSchemes("subject").addConceptSchemes("region").build());
        DescriptorProtos.FieldOptions required = rules(FieldRules.newBuilder()
                .setRequired(true).build());
        String ref = ".ai.protomolt.proto.types.v1.ConceptRef";
        FieldDescriptorProto.Type message = FieldDescriptorProto.Type.TYPE_MESSAGE;
        FieldDescriptorProto.Type string = FieldDescriptorProto.Type.TYPE_STRING;
        FieldDescriptorProto.Label optional = FieldDescriptorProto.Label.LABEL_OPTIONAL;
        FieldDescriptorProto.Label repeated = FieldDescriptorProto.Label.LABEL_REPEATED;
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("concept_scheme_rule_test.proto")
                .setSyntax("proto3")
                .setPackage("ai.protomolt.proto.types.v1")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("ConceptRef")
                        .addField(field("scheme_id", 1, string, null, optional, required))
                        .addField(field("concept_id", 2, string, null, optional, required)))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(field("subject", 1, message, ref, optional, bound))
                        .addField(field("tags", 2, message, ref, repeated, bound)))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Section")
                        .addField(field("tags", 1, message, ref, repeated, bound)))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Report")
                        .addField(field("sections", 1, message,
                                ".ai.protomolt.proto.types.v1.Section", repeated, null)))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Wrapper")
                        .addField(field("section", 1, message,
                                ".ai.protomolt.proto.types.v1.Section", optional, null)))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Keyed")
                        .addNestedType(DescriptorProtos.DescriptorProto.newBuilder()
                                .setName("SectionsEntry")
                                .setOptions(DescriptorProtos.MessageOptions.newBuilder()
                                        .setMapEntry(true))
                                .addField(field("key", 1, string, null, optional, null))
                                .addField(field("value", 2, message,
                                        ".ai.protomolt.proto.types.v1.Wrapper", optional, null)))
                        .addField(field("sections", 1, message,
                                ".ai.protomolt.proto.types.v1.Keyed.SectionsEntry", repeated,
                                null)))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("OnString")
                        .addField(field("tag", 1, string, null, optional, bound)))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("OnItems")
                        .addField(field("tags", 1, message, ref, repeated,
                                rules(FieldRules.newBuilder()
                                        .setRepeated(RepeatedRules.newBuilder()
                                                .setItems(FieldRules.newBuilder()
                                                        .addConceptSchemes("subject")))
                                        .build()))))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("BadId")
                        .addField(field("tag", 1, message, ref, optional,
                                rules(FieldRules.newBuilder()
                                        .addConceptSchemes("Not A Slug").build()))))
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("DuplicateId")
                        .addField(field("tag", 1, message, ref, optional,
                                rules(FieldRules.newBuilder()
                                        .addConceptSchemes("subject")
                                        .addConceptSchemes("subject").build()))))
                .build();
        try {
            return Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0]);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static DynamicMessage ref(String schemeId, String conceptId) {
        return DynamicMessage.newBuilder(CONCEPT_REF)
                .setField(CONCEPT_REF.findFieldByName("scheme_id"), schemeId)
                .setField(CONCEPT_REF.findFieldByName("concept_id"), conceptId)
                .build();
    }

    private static DynamicMessage doc(DynamicMessage subject) {
        return DynamicMessage.newBuilder(DOC)
                .setField(DOC.findFieldByName("subject"), subject)
                .build();
    }

    private static DynamicMessage empty(String type) {
        return DynamicMessage.getDefaultInstance(FILE.findMessageTypeByName(type));
    }

    @Test
    void aMemberOfAMountedBoundSchemePasses() {
        assertThat(VALIDATOR.validate(doc(ref("subject", "physics"))).valid()).isTrue();
    }

    @Test
    void aDeprecatedConceptIsStillAMember() {
        // Membership is the mounted id set, deprecated ids included, so a document tagged
        // before a retirement keeps validating when its CORE is carried forward.
        assertThat(VALIDATOR.validate(doc(ref("subject", "alchemy"))).valid()).isTrue();
    }

    @Test
    void anUnknownConceptIsRefusedNamingTheSchemeAndMountedVersion() {
        ValidationResult result = VALIDATOR.validate(doc(ref("subject", "biology")));
        assertThat(result.violations()).singleElement().satisfies(violation -> {
            assertThat(violation.path()).isEqualTo("subject");
            assertThat(violation.ruleId()).isEqualTo("concept.member");
            assertThat(violation.message())
                    .contains("biology").contains("\"subject\"").contains("version v3");
        });
    }

    @Test
    void aBoundSchemeThatIsNotMountedRefusesFailClosed() {
        ValidationResult result = VALIDATOR.validate(doc(ref("region", "emea")));
        assertThat(result.violations()).singleElement().satisfies(violation -> {
            assertThat(violation.ruleId()).isEqualTo("concept.unmounted");
            assertThat(violation.message()).contains("region");
        });
    }

    @Test
    void theDefaultValidatorMountsNothingAndRefuses() {
        ValidationResult result = ProtoValidator.create().validate(doc(ref("subject", "physics")));
        assertThat(result.violations())
                .extracting(ValidationResult.Violation::ruleId)
                .containsExactly("concept.unmounted");
    }

    @Test
    void aSchemeOutsideTheBindingIsRefusedEvenWhenMounted() {
        ConceptSchemeCatalog everything = schemeId -> Optional.of(
                new ConceptSchemeCatalog.Mounted(schemeId, "v1", Set.of("physics")));
        ProtoValidator validator = ProtoValidator.create(ValidationRuleSources.defaults(),
                TaxonomyCatalog.empty(), PostalCodeCatalog.empty(), everything);
        ValidationResult result = validator.validate(doc(ref("department", "physics")));
        assertThat(result.violations()).singleElement().satisfies(violation -> {
            assertThat(violation.ruleId()).isEqualTo("concept.scheme");
            assertThat(violation.message()).contains("department").contains("[subject, region]");
        });
    }

    @Test
    void aRepeatedConceptRefFieldChecksEveryElement() {
        Descriptors.FieldDescriptor tags = DOC.findFieldByName("tags");
        DynamicMessage message = DynamicMessage.newBuilder(DOC)
                .addRepeatedField(tags, ref("subject", "physics"))
                .addRepeatedField(tags, ref("subject", "biology"))
                .addRepeatedField(tags, ref("region", "emea"))
                .build();
        ValidationResult result = VALIDATOR.validate(message);
        assertThat(result.violations())
                .extracting(ValidationResult.Violation::path, ValidationResult.Violation::ruleId)
                .containsExactly(
                        tuple("tags[1]", "concept.member"),
                        tuple("tags[2]", "concept.unmounted"));
    }

    @Test
    void anEmptyConceptIdIsAStructuralRefusalNotAMembershipOne() {
        ValidationResult result = VALIDATOR.validate(doc(ref("subject", "")));
        assertThat(result.violations())
                .extracting(ValidationResult.Violation::path, ValidationResult.Violation::ruleId)
                .containsExactly(tuple("subject.concept_id", "required"));
    }

    @Test
    void underARepeatedAncestorTheViolationPathCarriesConcreteIndices() {
        Descriptors.Descriptor report = FILE.findMessageTypeByName("Report");
        Descriptors.Descriptor section = FILE.findMessageTypeByName("Section");
        DynamicMessage first = DynamicMessage.newBuilder(section)
                .addRepeatedField(section.findFieldByName("tags"), ref("subject", "physics"))
                .build();
        DynamicMessage second = DynamicMessage.newBuilder(section)
                .addRepeatedField(section.findFieldByName("tags"), ref("subject", "physics"))
                .addRepeatedField(section.findFieldByName("tags"), ref("subject", "biology"))
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(report)
                .addRepeatedField(report.findFieldByName("sections"), first)
                .addRepeatedField(report.findFieldByName("sections"), second)
                .build();
        assertThat(VALIDATOR.validate(message).violations())
                .extracting(ValidationResult.Violation::path, ValidationResult.Violation::ruleId)
                .containsExactly(tuple("sections[1].tags[1]", "concept.member"));
    }

    @Test
    void aBoundConceptRefAnywhereUnderAMapValueFailsSchemaPreparation() {
        // Two levels down (map value Wrapper, then Section.tags): occurrences under a map
        // have no field-path identity, so the binding is a schema error.
        Descriptors.Descriptor keyed = FILE.findMessageTypeByName("Keyed");
        assertThatThrownBy(() -> VALIDATOR.prepareSchema(keyed, 10, 20, () -> {}))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("Section.tags")
                .hasMessageContaining("map field ai.protomolt.proto.types.v1.Keyed.sections");
    }

    @Test
    void theWalkRejectsABoundConceptRefReachedUnderAMapValue() {
        // Without preparation, the binding is refused when a tag is reached under a map;
        // an empty map reaches nothing, and the same Section outside a map validates.
        Descriptors.Descriptor keyed = FILE.findMessageTypeByName("Keyed");
        Descriptors.Descriptor entry = keyed.findFieldByName("sections").getMessageType();
        Descriptors.Descriptor wrapper = FILE.findMessageTypeByName("Wrapper");
        Descriptors.Descriptor section = FILE.findMessageTypeByName("Section");
        DynamicMessage tagged = DynamicMessage.newBuilder(section)
                .addRepeatedField(section.findFieldByName("tags"), ref("subject", "physics"))
                .build();
        DynamicMessage wrapped = DynamicMessage.newBuilder(wrapper)
                .setField(wrapper.findFieldByName("section"), tagged)
                .build();
        assertThat(VALIDATOR.validate(wrapped).valid()).isTrue();
        assertThat(VALIDATOR.validate(empty("Keyed")).valid()).isTrue();
        DynamicMessage message = DynamicMessage.newBuilder(keyed)
                .addRepeatedField(keyed.findFieldByName("sections"), DynamicMessage.newBuilder(entry)
                        .setField(entry.findFieldByName("key"), "intro")
                        .setField(entry.findFieldByName("value"), wrapped)
                        .build())
                .build();
        assertThatThrownBy(() -> VALIDATOR.validate(message))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("Section.tags")
                .hasMessageContaining("the map value at sections[\"intro\"].section.tags[0]");
    }

    @Test
    void conceptSchemesOnANonConceptRefFieldFailsRuleCompilation() {
        assertThatThrownBy(() -> VALIDATOR.validate(empty("OnString")))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("OnString.tag")
                .hasMessageContaining("ai.protomolt.proto.types.v1.ConceptRef");
    }

    @Test
    void conceptSchemesOnRepeatedItemsFailsRuleCompilation() {
        assertThatThrownBy(() -> VALIDATOR.validate(empty("OnItems")))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("ConceptRef field itself");
    }

    @Test
    void aBindingThatIsNotASlugFailsRuleCompilation() {
        assertThatThrownBy(() -> VALIDATOR.validate(empty("BadId")))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("Not A Slug");
    }

    @Test
    void aBindingNamingASchemeTwiceFailsRuleCompilation() {
        assertThatThrownBy(() -> VALIDATOR.validate(empty("DuplicateId")))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("more than once");
    }

    @Test
    void theRuleFieldCarriesItsOwnDeclaredElementRules() {
        // validate.proto annotates concept_schemes with its own field option; a FieldRules
        // message validated as data refuses a duplicate and a non-slug id.
        ValidationResult result = ProtoValidator.create().validate(FieldRules.newBuilder()
                .addAllConceptSchemes(List.of("subject", "subject", "Bad Id"))
                .build());
        assertThat(result.violations())
                .extracting(ValidationResult.Violation::path, ValidationResult.Violation::ruleId)
                .containsExactlyInAnyOrder(
                        tuple("concept_schemes", "repeated.unique"),
                        tuple("concept_schemes[2]", "string.slug"));
    }
}
