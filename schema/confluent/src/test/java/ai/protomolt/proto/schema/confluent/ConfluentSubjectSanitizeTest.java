package ai.protomolt.proto.schema.confluent;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regex-free subject sanitizer, checked against the former
 * {@code replaceAll("[^A-Za-z0-9._-]", "_")}: one underscore per rejected code point.
 */
class ConfluentSubjectSanitizeTest {

    @Test
    void sanitizationMatchesTheFormerReplaceAll() {
        for (String subject : new String[]{
                "orders-value", "orders/value", "a b", "a:b", "", "...", "a.b_c-d",
                "émigré", "a\nb", "orders.v1.proto", "a\u00a0b"}) {
            assertThat(ConfluentSchemaRegistryLoader.sanitizeSubject(subject))
                    .as("sanitized '%s'", subject)
                    .isEqualTo(subject.replaceAll("[^A-Za-z0-9._-]", "_"));
        }
    }

    @Test
    void aSupplementaryCharacterFoldsToOneUnderscore() {
        String subject = "a\ud83d\ude00b";
        assertThat(ConfluentSchemaRegistryLoader.sanitizeSubject(subject)).isEqualTo("a_b");
        assertThat(ConfluentSchemaRegistryLoader.sanitizeSubject(subject))
                .isEqualTo(subject.replaceAll("[^A-Za-z0-9._-]", "_"));
    }
}
