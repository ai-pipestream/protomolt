package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

/** Occurrence field paths: which characters name a field, and which are refused. */
class RepositorySinkOccurrencesTest {

    @Test
    void identifierCharactersAreAsciiLettersDigitsAndUnderscoreOnly() {
        for (char c : "azAZ09_mM5".toCharArray()) {
            assertThat(RepositorySinkOccurrences.isIdentifierChar(c)).as("'%s'", c).isTrue();
        }
        for (char c : "{|~`@[/:.- é中".toCharArray()) {
            assertThat(RepositorySinkOccurrences.isIdentifierChar(c)).as("'%s'", c).isFalse();
        }
    }

    @Test
    void ordinaryFieldNamesWithLowercaseLettersAndDigitsParse() {
        assertThat(RepositorySinkOccurrences.parseOccurrenceFieldPath(
                        "structured_data.address2[3]"))
                .extracting(Object::toString)
                .hasSize(2)
                .first()
                .asString()
                .contains("structured_data");
    }

    @Test
    void namesWithCharactersPastTheLetterRangesAreRefused() {
        for (String path : new String[] {"{", "stép", "a.~b"}) {
            assertThatThrownBy(() -> RepositorySinkOccurrences.parseOccurrenceFieldPath(path))
                    .as(path)
                    .isInstanceOfSatisfying(StatusRuntimeException.class, refused ->
                            assertThat(refused.getStatus().getCode())
                                    .isEqualTo(Status.Code.INVALID_ARGUMENT));
        }
    }
}
