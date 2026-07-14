package ai.pipestream.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link HostAndPort}: hostname or IPv4 host, or bracketed IPv6 literal; optional (or required)
 * {@code :port} with the port a no-leading-zero decimal in 0-65535.
 */
class HostAndPortTest {

    // ---------------------------------------------------------------- with port

    @ParameterizedTest
    @ValueSource(strings = {
            "example.com:8080",
            "localhost:1",
            "a.b.c:65535",
            "sub.example.com:443",
            "xn--e1afmkfd.example:443",
            "192.168.0.1:443",
            "0.0.0.0:0",
            "255.255.255.255:65535",
            "[::1]:80",
            "[::]:0",
            "[2001:db8::1]:8080",
            "[1:2:3:4:5:6:7:8]:65535",
            "[::ffff:192.0.2.1]:53",
            "example.com:0",              // port 0 is a valid port number
    })
    void acceptsWithPort(String value) {
        assertThat(HostAndPort.isHostAndPort(value, true)).isTrue();
        assertThat(HostAndPort.isHostAndPort(value, false)).isTrue();
    }

    // ---------------------------------------------------------------- without port

    @ParameterizedTest
    @ValueSource(strings = {
            "example.com",
            "localhost",
            "example.com.",
            "192.168.0.1",
            "[::1]",
            "[2001:db8::1]",
            "[fe80::1]",
    })
    void acceptsWithoutPortOnlyWhenPortOptional(String value) {
        assertThat(HostAndPort.isHostAndPort(value, false)).isTrue();
        assertThat(HostAndPort.isHostAndPort(value, true)).isFalse();
    }

    // ---------------------------------------------------------------- rejects either way

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            ":",
            ":80",                        // empty host
            "example.com:",               // empty port
            "example.com:65536",          // one above the max
            "example.com:99999",
            "example.com:100000",         // six digits
            "example.com:080",            // leading zero
            "example.com:00",
            "example.com:-1",
            "example.com:+1",
            "example.com:8a",
            "example.com:8 0",
            "example.com: 80",
            "example.com:8080:90",        // split happens at the last colon
            "-bad.com:80",
            "bad-.com:80",
            "example..com:80",
            "exa_mple.com:80",
            "256.1.1.1:80",               // not an IPv4 address, and numeric TLD kills hostname
            "1.2.3.4.5:80",
            "01.2.3.4:80",
            "::1:80",                     // raw IPv6 must be bracketed
            "::1",
            "2001:db8::1",
            "[::1]:",                     // bracket form with empty port
            "[::1]80",                    // missing ':' after bracket
            "[::1] :80",
            "[::1",                       // unclosed bracket
            "::1]",
            "[]:80",                      // empty brackets
            "[]",
            "[example.com]:80",           // brackets are only for IPv6 literals
            "[1.2.3.4]:80",
            "[::1]:65536",
            "[::1]:0080",
            "[::1]:8a",
            "[2001:db8::1]]:80",          // stray extra bracket
            "example.com:80\n",
    })
    void rejectsRegardlessOfPortRequirement(String value) {
        assertThat(HostAndPort.isHostAndPort(value, true)).isFalse();
        assertThat(HostAndPort.isHostAndPort(value, false)).isFalse();
    }

    @Test
    void zoneIdsInsideBracketsFollowTheAddressRules() {
        // IpAddresses.isIpv6 accepts raw zone ids, so the bracketed forms do too.
        assertThat(HostAndPort.isHostAndPort("[fe80::1%eth0]:80", true)).isTrue();
        assertThat(HostAndPort.isHostAndPort("[fe80::1%eth0]", false)).isTrue();
        assertThat(HostAndPort.isHostAndPort("[fe80::1%25eth0]:80", true)).isTrue(); // zone "25eth0"
        assertThat(HostAndPort.isHostAndPort("[fe80::1%]:80", true)).isFalse();      // empty zone
    }

    @Test
    void hostnameWithTrailingDotStillTakesAPort() {
        assertThat(HostAndPort.isHostAndPort("example.com.:8080", true)).isTrue();
    }

    // ---------------------------------------------------------------- isPort

    @ParameterizedTest
    @ValueSource(strings = {"0", "1", "9", "80", "443", "1023", "1024", "8080", "10000", "65534", "65535"})
    void portAccepts(String value) {
        assertThat(HostAndPort.isPort(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "", "65536", "65540", "70000", "99999", "100000", "655350",
            "00", "01", "007", "0080", "065535",
            "-1", "+1", " 80", "80 ", "8_0", "8.0", "8e1", "0x50", "٨0",
    })
    void portRejects(String value) {
        assertThat(HostAndPort.isPort(value)).isFalse();
    }

    @ParameterizedTest
    @CsvSource({
            "65535, true",
            "65536, false",
            "65539, false",
            "65529, true",
    })
    void portMaxBoundaryIsExact(String value, boolean expected) {
        assertThat(HostAndPort.isPort(value)).isEqualTo(expected);
    }
}
