package ai.pipestream.proto.types;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import org.junit.jupiter.api.Test;

/**
 * Taxonomy's own declared rules: the structural gate the config door and the
 * config consumer both mount, so a malformed taxonomy document can never
 * reach a mount. The document is just entries — its subject is the identity
 * and the config source's version is the version, so there is nothing else
 * to police.
 */
class TaxonomyRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static TreePath path(String... segments) {
        TreePath.Builder builder = TreePath.newBuilder();
        for (String segment : segments) {
            builder.addSegments(segment);
        }
        return builder.build();
    }

    @Test
    void aWellFormedTaxonomyPasses() {
        Taxonomy taxonomy = Taxonomy.newBuilder()
                .addEntries(path("electronics", "computers", "laptops"))
                .addEntries(path("media"))
                .build();
        assertThat(VALIDATOR.validate(taxonomy).valid()).isTrue();
    }

    @Test
    void aTaxonomyNeedsAtLeastOneEntry() {
        ValidationResult result = VALIDATOR.validate(Taxonomy.getDefaultInstance());
        assertThat(result.violations())
                .extracting(ValidationResult.Violation::ruleId)
                .containsExactly("repeated.min_items");
    }

    @Test
    void aDuplicateEntryIsRefused() {
        ValidationResult result = VALIDATOR.validate(Taxonomy.newBuilder()
                .addEntries(path("media"))
                .addEntries(path("media"))
                .build());
        assertThat(result.violations())
                .extracting(ValidationResult.Violation::ruleId)
                .containsExactly("repeated.unique");
    }

    @Test
    void entriesCarryTreePathsOwnRules() {
        ValidationResult result = VALIDATOR.validate(Taxonomy.newBuilder()
                .addEntries(path("electronics/audio"))
                .build());
        assertThat(result.violations()).singleElement().satisfies(violation -> {
            assertThat(violation.ruleId()).isEqualTo("string.not_contains");
            assertThat(violation.path()).isEqualTo("entries[0].segments[0]");
        });
    }
}
