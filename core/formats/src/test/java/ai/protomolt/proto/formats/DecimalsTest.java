package ai.protomolt.proto.formats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DecimalsTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "0",
            "19.99",
            "007",                          // leading zeros are the caller's business
            "12345678901234567890.5",       // longer than any binary numeric type
    })
    void decimalAccepts(String value) {
        assertThat(Decimals.isDecimal(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            ".",
            ".5",         // a digit is required on both sides of the dot
            "5.",
            "1.2.3",
            "-1",         // deliberately unsigned
            "+1",
            "1e5",        // and exponent-free
            "19,99",
            " 19",
    })
    void decimalRejects(String value) {
        assertThat(Decimals.isDecimal(value)).isFalse();
    }
}
