package ai.protomolt.proto.intake.service;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the zero-regex delimiter split, proven against the
 * {@code String.split} behavior it replaces (the oracle may use the
 * regex split; production code may not).
 */
class DelimitedValuesTest {

    @Test
    void matchesLiteralSplitOnRepresentativeValues() {
        for (char delimiter : new char[] {',', ';'}) {
            for (String value : new String[] {"", "a", "a,b", "a,b,", ",a", "a,,b", ",", ",,",
                    "a;b;c", " spaced , values "}) {
                if (delimiter == ';' && value.indexOf(',') >= 0 && value.indexOf(';') < 0) {
                    continue;
                }
                assertThat(DelimitedValues.segments(value, delimiter))
                        .as("'%s' on '%s'", value, delimiter)
                        .isEqualTo(List.of(value.split(String.valueOf(delimiter))));
            }
        }
    }
}
