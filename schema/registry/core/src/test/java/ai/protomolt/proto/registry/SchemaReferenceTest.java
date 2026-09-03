package ai.protomolt.proto.registry;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link SchemaReference} record validation: a reference with a blank name or subject, or a
 * non-positive version, could never resolve against the store, so it is refused at construction
 * rather than failing lookups later.
 */
class SchemaReferenceTest {

    @Test
    void aValidReferenceExposesItsComponents() {
        SchemaReference reference = new SchemaReference("common/v1/core.proto", "common/v1/core.proto", 3);

        assertThat(reference.name()).isEqualTo("common/v1/core.proto");
        assertThat(reference.subject()).isEqualTo("common/v1/core.proto");
        assertThat(reference.version()).isEqualTo(3);
    }

    @Test
    void nullNameAndSubjectAreRejected() {
        assertThatThrownBy(() -> new SchemaReference(null, "subject", 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("name");
        assertThatThrownBy(() -> new SchemaReference("name", null, 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("subject");
    }

    @Test
    void blankNameAndSubjectAreRejected() {
        assertThatThrownBy(() -> new SchemaReference("  ", "subject", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("reference name must not be blank");
        assertThatThrownBy(() -> new SchemaReference("name", "", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("reference subject must not be blank");
    }

    @Test
    void versionsBelowOneAreRejected() {
        assertThatThrownBy(() -> new SchemaReference("name", "subject", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("reference version must be >= 1: 0");
        assertThatThrownBy(() -> new SchemaReference("name", "subject", -2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("reference version must be >= 1: -2");
    }

    @Test
    void equalityIsByValueSoReferenceListsCompare() {
        assertThat(new SchemaReference("n", "s", 1))
                .isEqualTo(new SchemaReference("n", "s", 1))
                .isNotEqualTo(new SchemaReference("n", "s", 2))
                .isNotEqualTo(new SchemaReference("n", "other", 1))
                .isNotEqualTo(new SchemaReference("other", "s", 1));
    }
}
