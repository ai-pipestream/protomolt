package ai.protomolt.proto.types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.ValidationResult.Violation;
import org.junit.jupiter.api.Test;

/**
 * A concept scheme's two gates: the field rules its types declare (enforced by the house
 * validator the config consumer mounts) and {@link ConceptSchemeRules}, the whole-document
 * graph and label checks. Each refusal case below differs from the passing scheme in one
 * place, so a check that silently stopped running turns its case green.
 */
class ConceptSchemeRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static LangLabel label(String language, String text) {
        return LangLabel.newBuilder().setLanguage(language).setText(text).build();
    }

    private static ConceptRef ref(String schemeId, String conceptId) {
        return ConceptRef.newBuilder().setSchemeId(schemeId).setConceptId(conceptId).build();
    }

    private static Concept.Builder concept(String id) {
        return Concept.newBuilder().setId(id).addPrefLabel(label("en", id));
    }

    private static ConceptScheme scheme(Concept.Builder... concepts) {
        ConceptScheme.Builder builder = ConceptScheme.newBuilder().addPrefLabel(label("en", "Subjects"));
        for (Concept.Builder concept : concepts) {
            builder.addConcepts(concept);
        }
        return builder.build();
    }

    private static ValidationResult check(ConceptScheme scheme) {
        return ConceptSchemeRules.check("subject", scheme);
    }

    /** A polyhierarchy with labels in two languages, a mapping, a related pair and a retirement. */
    private static ConceptScheme wellFormed() {
        return scheme(
                concept("science"),
                concept("physics").addBroader("science")
                        .addPrefLabel(label("fr", "physique"))
                        .addAltLabel(label("en", "natural philosophy"))
                        .addHiddenLabel(label("en", "phisics"))
                        .addDefinition(label("en", "The study of matter."))
                        .addExactMatch(ref("lcsh", "sh85101653"))
                        .setIri("http://example.org/subject/physics"),
                concept("chemistry").addBroader("science").addRelated("physics"),
                concept("biophysics").addBroader("physics").addBroader("chemistry"),
                concept("alchemy").setDeprecated(true).addReplacedBy("chemistry"));
    }

    @Test
    void aWellFormedSchemePassesBothGates() {
        assertThat(VALIDATOR.validate(wellFormed()).violations()).isEmpty();
        assertThat(check(wellFormed()).violations()).isEmpty();
    }

    @Test
    void declaredRulesRefuseAnEmptySchemeAndMalformedFields() {
        assertThat(VALIDATOR.validate(ConceptScheme.getDefaultInstance()).violations())
                .extracting(Violation::path, Violation::ruleId)
                .containsExactly(tuple("concepts", "repeated.min_items"));
        ConceptScheme malformed = scheme(Concept.newBuilder()
                .setId("has space")
                .addPrefLabel(label("not a tag!", "x"))
                .addBroader("a").addBroader("a")
                .setIri("not a uri"));
        assertThat(VALIDATOR.validate(malformed).violations())
                .extracting(Violation::path, Violation::ruleId)
                .containsExactlyInAnyOrder(
                        tuple("concepts[0].id", "string.path_safe_name"),
                        tuple("concepts[0].pref_label[0].language", "string.language_tag"),
                        tuple("concepts[0].broader", "repeated.unique"),
                        tuple("concepts[0].iri", "string.uri"));
        assertThat(VALIDATOR.validate(scheme(Concept.newBuilder().setId("bare"))).violations())
                .extracting(Violation::path, Violation::ruleId)
                .containsExactly(tuple("concepts[0].pref_label", "repeated.min_items"));
    }

    @Test
    void conceptRefDeclaresSlugSchemeAndPathSafeConcept() {
        assertThat(VALIDATOR.validate(ref("Not Slug", "a/b")).violations())
                .extracting(Violation::path, Violation::ruleId)
                .containsExactlyInAnyOrder(
                        tuple("scheme_id", "string.slug"),
                        tuple("concept_id", "string.path_safe_name"));
        assertThat(VALIDATOR.validate(ConceptRef.getDefaultInstance()).violations())
                .extracting(Violation::ruleId)
                .containsExactly("required", "required");
    }

    @Test
    void conceptIdsAreUnique() {
        assertThat(check(scheme(concept("physics"), concept("physics"))).violations())
                .extracting(Violation::path, Violation::ruleId)
                .containsExactly(tuple("concepts[1].id", "concept_scheme.id_unique"));
    }

    @Test
    void s13OnePreferredLabelPerLanguageOnTheSchemeAndEachConcept() {
        ConceptScheme scheme = scheme(concept("physics")
                .addPrefLabel(label("EN", "Physics, again"))
                .addDefinition(label("en", "one"))
                .addDefinition(label("en", "two")))
                .toBuilder().addPrefLabel(label("en", "Topics")).build();
        assertThat(check(scheme).violations())
                .extracting(Violation::path, Violation::ruleId)
                .containsExactly(
                        tuple("pref_label[1]", "concept_scheme.pref_label_language"),
                        tuple("concepts[0].pref_label[1]", "concept_scheme.pref_label_language"),
                        tuple("concepts[0].definition[1]", "concept_scheme.definition_language"));
    }

    @Test
    void s14ALabelTextBelongsToOneLabelKindPerLanguage() {
        ConceptScheme scheme = scheme(concept("physics")
                .addAltLabel(label("en", "physics"))
                .addHiddenLabel(label("en", "Physics"))
                .addHiddenLabel(label("de", "physics")));
        // The same text in another case or another language is a different label.
        assertThat(check(scheme).violations()).singleElement().satisfies(violation -> {
            assertThat(violation.path()).isEqualTo("concepts[0].alt_label[0]");
            assertThat(violation.ruleId()).isEqualTo("concept_scheme.label_disjoint");
            assertThat(violation.message()).contains("pref_label");
        });
    }

    @Test
    void mappingsMustLeaveTheScheme() {
        ConceptScheme scheme = scheme(concept("physics"),
                concept("chemistry").addCloseMatch(ref("subject", "physics")));
        assertThat(check(scheme).violations())
                .extracting(Violation::path, Violation::ruleId)
                .containsExactly(tuple("concepts[1].close_match[0]", "concept_scheme.mapping_scheme"));
    }

    @Test
    void everyInSchemeReferenceResolves() {
        ConceptScheme scheme = scheme(concept("physics")
                .addBroader("science")
                .addRelated("chemistry")
                .setDeprecated(true).addReplacedBy("modern-physics"));
        assertThat(check(scheme).violations())
                .extracting(Violation::path, Violation::ruleId)
                .containsExactly(
                        tuple("concepts[0].broader[0]", "concept_scheme.reference"),
                        tuple("concepts[0].related[0]", "concept_scheme.reference"),
                        tuple("concepts[0].replaced_by[0]", "concept_scheme.reference"));
    }

    @Test
    void aBroaderCycleIsNamedInOrder() {
        ConceptScheme scheme = scheme(
                concept("a").addBroader("b"),
                concept("b").addBroader("c"),
                concept("c").addBroader("a"),
                concept("d").addBroader("a"));
        assertThat(check(scheme).violations()).singleElement().satisfies(violation -> {
            assertThat(violation.ruleId()).isEqualTo("concept_scheme.broader_cycle");
            assertThat(violation.message()).endsWith("a -> b -> c -> a");
        });
    }

    @Test
    void aConceptThatIsItsOwnParentIsACycle() {
        assertThat(check(scheme(concept("a").addBroader("a"))).violations())
                .extracting(Violation::message)
                .containsExactly("broader cycle: a -> a");
    }

    @Test
    void s27RelatedMustNotReachUpOrDownTheHierarchy() {
        ConceptScheme upward = scheme(
                concept("science"),
                concept("physics").addBroader("science"),
                concept("optics").addBroader("physics").addRelated("science"));
        assertThat(check(upward).violations()).singleElement().satisfies(violation -> {
            assertThat(violation.path()).isEqualTo("concepts[2].related[0]");
            assertThat(violation.ruleId()).isEqualTo("concept_scheme.related_hierarchy");
            assertThat(violation.message()).contains("an ancestor");
        });
        // Written on the ancestor's side, two levels down: the symmetric pair still counts.
        ConceptScheme downward = scheme(
                concept("science").addRelated("lasers"),
                concept("physics").addBroader("science"),
                concept("lasers").addBroader("physics"));
        assertThat(check(downward).violations()).singleElement().satisfies(violation -> {
            assertThat(violation.path()).isEqualTo("concepts[0].related[0]");
            assertThat(violation.message()).contains("a descendant");
        });
    }

    @Test
    void relatedSiblingsAndSelfRelation() {
        // Siblings and cousins may be related; a concept may not be related to itself.
        ConceptScheme scheme = scheme(
                concept("science"),
                concept("physics").addBroader("science").addRelated("chemistry"),
                concept("chemistry").addBroader("science").addRelated("chemistry"));
        assertThat(check(scheme).violations())
                .extracting(Violation::path, Violation::ruleId)
                .containsExactly(tuple("concepts[2].related[0]", "concept_scheme.related_self"));
    }

    @Test
    void replacedByIsOnlyForDeprecatedConcepts() {
        ConceptScheme scheme = scheme(concept("chemistry"),
                concept("alchemy").addReplacedBy("chemistry"));
        assertThat(check(scheme).violations())
                .extracting(Violation::path, Violation::ruleId)
                .containsExactly(tuple("concepts[1].replaced_by", "concept_scheme.replaced_by_live"));
    }

    @Test
    void aReplacedByChainMustNotEndAtARetiredConcept() {
        ConceptScheme deadEnd = scheme(
                concept("phlogiston").setDeprecated(true).addReplacedBy("caloric"),
                concept("caloric").setDeprecated(true),
                concept("alchemy").setDeprecated(true));
        assertThat(check(deadEnd).violations())
                .extracting(Violation::path, Violation::ruleId)
                .containsExactly(
                        tuple("concepts[0].replaced_by[0]", "concept_scheme.replaced_by_dead_end"));
        ConceptScheme live = scheme(
                concept("phlogiston").setDeprecated(true).addReplacedBy("caloric"),
                concept("caloric").setDeprecated(true).addReplacedBy("thermodynamics"),
                concept("thermodynamics"));
        assertThat(check(live).violations()).isEmpty();
    }

    @Test
    void aReplacedByCycleIsRefused() {
        ConceptScheme scheme = scheme(
                concept("a").setDeprecated(true).addReplacedBy("b"),
                concept("b").setDeprecated(true).addReplacedBy("a"));
        assertThat(check(scheme).violations()).singleElement().satisfies(violation -> {
            assertThat(violation.ruleId()).isEqualTo("concept_scheme.replaced_by_cycle");
            assertThat(violation.message()).isEqualTo("replaced_by cycle: a -> b -> a");
        });
    }

    @Test
    void aDeepHierarchyDoesNotExhaustTheStack() {
        ConceptScheme.Builder builder = ConceptScheme.newBuilder();
        builder.addConcepts(concept("c0"));
        for (int i = 1; i < 50_000; i++) {
            builder.addConcepts(concept("c" + i).addBroader("c" + (i - 1)));
        }
        builder.addConcepts(concept("loop").addBroader("c49999").addRelated("c0"));
        assertThat(check(builder.build()).violations())
                .extracting(Violation::ruleId)
                .containsExactly("concept_scheme.related_hierarchy");
    }
}
