package ai.protomolt.proto.parse.playground;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for the {@code PARSE_PLAYGROUND_HTTP_PORT} refusals. */
class ParsePlaygroundMainTest {

    @Test
    void unsetOrBlankPortKeepsTheDefault() {
        assertThat(ParsePlaygroundMain.port(null)).isEqualTo(ParsePlaygroundMain.DEFAULT_HTTP_PORT);
        assertThat(ParsePlaygroundMain.port("")).isEqualTo(ParsePlaygroundMain.DEFAULT_HTTP_PORT);
        assertThat(ParsePlaygroundMain.port("   ")).isEqualTo(ParsePlaygroundMain.DEFAULT_HTTP_PORT);
    }

    @Test
    void validPortsParseIncludingBoundariesAndWhitespace() {
        assertThat(ParsePlaygroundMain.port("1")).isEqualTo(1);
        assertThat(ParsePlaygroundMain.port("8080")).isEqualTo(8080);
        assertThat(ParsePlaygroundMain.port("65535")).isEqualTo(65535);
        assertThat(ParsePlaygroundMain.port(" 9090 ")).isEqualTo(9090);
    }

    @Test
    void malformedAndOutOfRangePortsAreRefusedByName() {
        for (String bad : new String[] {"abc", "80a", "8_0", "0", "-1", "65536", "99999"}) {
            assertThatThrownBy(() -> ParsePlaygroundMain.port(bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("PARSE_PLAYGROUND_HTTP_PORT");
        }
    }
}
