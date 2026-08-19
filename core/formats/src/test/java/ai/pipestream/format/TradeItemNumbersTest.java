package ai.pipestream.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TradeItemNumbersTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "40170725",           // GTIN-8
            "012345678905",       // GTIN-12 (UPC-A)
            "4006381333931",      // GTIN-13 (EAN-13)
            "00012345678905",     // GTIN-14
            "00000000",           // all zeros: check digit 0 is arithmetically valid
    })
    void gtinAccepts(String value) {
        assertThat(TradeItemNumbers.isGtin(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "1234",               // wrong length
            "123456789",          // 9 digits: no GTIN family
            "123456784",          // 9 digits with a VALID mod-10 check: length alone refuses
            "0001234567890",      // 15 minus one: 13 digits with a wrong check
            "00012345678906",     // valid shape, WRONG check digit
            "40170726",           // GTIN-8 with a wrong check digit
            "4006381333932",      // GTIN-13 with a wrong check digit
            "0001234567890a",     // letters never pass
            "0001234567890 ",     // trailing whitespace
    })
    void gtinRejects(String value) {
        assertThat(TradeItemNumbers.isGtin(value)).isFalse();
    }
}
