package ai.pipestream.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link IpAddresses}: strict canonical IPv4 (RFC 3986 dec-octet, no leading zeros), RFC 4291
 * IPv6 (single {@code ::}, optional trailing embedded IPv4), permissive RFC 4007 zone ids on
 * addresses (any non-empty, NUL-free suffix after {@code %}), and CIDR prefixes with an optional
 * strict host-bits-zero mode.
 */
class IpAddressesTest {

    // ---------------------------------------------------------------- IPv4

    @ParameterizedTest
    @ValueSource(strings = {
            "0.0.0.0",
            "255.255.255.255",
            "1.2.3.4",
            "127.0.0.1",
            "10.0.0.1",
            "172.16.254.1",
            "192.168.1.1",
            "192.0.2.235",
            "100.200.250.255",
            "9.9.9.9",
            "0.0.0.255",
            "255.0.0.0",
            "1.10.100.200",
    })
    void ipv4Accepts(String value) {
        assertThat(IpAddresses.isIpv4(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "1",
            "1.2",
            "1.2.3",
            "1.2.3.4.5",
            "1.2.3.4.",              // trailing dot
            ".1.2.3.4",              // leading dot
            "1..2.3",
            "1..2.3.4",
            "256.1.1.1",
            "1.256.1.1",
            "1.1.256.1",
            "1.1.1.256",
            "999.1.1.1",
            "300.1.1.1",
            "01.2.3.4",              // leading zero
            "1.02.3.4",
            "1.2.03.4",
            "1.2.3.04",
            "00.0.0.0",
            "000.0.0.0",
            "0.0.0.00",
            "1234.1.1.1",            // more than 3 digits
            "-1.0.0.0",
            "+1.0.0.0",
            "1.2.3.a",
            "a.b.c.d",
            "0x1.2.3.4",             // hex octets are not dotted-decimal
            "1.2.3.4 ",
            " 1.2.3.4",
            "1.2.3.4\n",
            "1,2,3,4",
            "1.2.3.4/24",            // prefixes are a separate check
            "1.2.3.4:80",
            "١.2.3.4",               // non-ASCII digit
            "1.2.3.٤",
            "192.168.1",
            "192.168.1.1.1",
    })
    void ipv4Rejects(String value) {
        assertThat(IpAddresses.isIpv4(value)).isFalse();
    }

    // ---------------------------------------------------------------- IPv6

    @ParameterizedTest
    @ValueSource(strings = {
            "::",
            "::1",
            "1::",
            "0:0:0:0:0:0:0:0",
            "0:0:0:0:0:0:0:1",
            "1:2:3:4:5:6:7:8",
            "2001:db8::1",
            "2001:DB8::1",
            "2001:0db8:0000:0000:0000:0000:0000:0001",
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            "2001:db8:85a3::8a2e:370:7334",
            "fe80::1",
            "ff02::2",
            "::abcd",
            "abcd::",
            "1::8",
            "1:2::7:8",
            "1:2:3::6:7:8",
            "1:2:3:4:5:6:7::",       // :: compresses exactly one group — still legal
            "::2:3:4:5:6:7:8",       // ditto, on the left
            "a:b:c:d:e:f:0:1",
            "A:B:C:D:E:F:0:1",
            "AbCd:Ef01:2345:6789:aBcD:eF01:2345:6789",
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            // Embedded IPv4
            "::ffff:192.0.2.1",
            "::ffff:0:255.255.255.255",
            "::192.0.2.1",
            "::0.0.0.0",
            "64:ff9b::192.0.2.33",
            "1:2:3:4:5:6:1.2.3.4",   // full form: 6 hextets + IPv4
            "1::1.2.3.4",
            "2001:db8::192.0.2.1",
    })
    void ipv6Accepts(String value) {
        assertThat(IpAddresses.isIpv6(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            ":",
            ":::",
            "::1::",
            "1::2::3",               // two compressions
            "2001:db8::1::2",
            ":1:2:3:4:5:6:7",        // stray leading colon
            "1:2:3:4:5:6:7:",        // stray trailing colon
            ":1::2",
            "1::2:",
            "1:2:3:4:5:6:7",         // 7 groups, no compression
            "1:2:3:4:5:6:7:8:9",     // 9 groups
            "0:0:0:0:0:0:0:0:0",
            "1:2:3:4:5:6:7:8::",     // :: would compress zero groups
            "::1:2:3:4:5:6:7:8",
            "12345::",               // hextet longer than 4 digits
            "::00000",
            "1:2:3:4:5:6:7:12345",
            "g::1",
            "::g",
            "2001:db8::x",
            "1.2.3.4",               // plain IPv4 is not IPv6
            // Embedded IPv4 abuse
            "::ffff:999.1.1.1",
            "::ffff:1.2.3.256",
            "::ffff:1.2.3",
            "::ffff:1.2.3.4.5",
            "::ffff:01.2.3.4",       // embedded IPv4 keeps its no-leading-zero rule
            "1.2.3.4::",             // IPv4 may only appear at the very end
            "::1.2.3.4:5",           // hextet after the IPv4 tail
            "1:1.2.3.4:5:6:7:8",     // IPv4 not in final position
            "1:2:3:4:5:6:7:1.2.3.4", // 7 hextets + IPv4 = 18 bytes
            "1:2:3:4:5:6:1.2.3.4:7",
            // Zone-id abuse
            "fe80::1%",              // empty zone
            "%eth0",                 // zone with no address
            "::%",
            "fe80:%eth0:1",          // % splits mid-address, leaving an invalid address
            // Formatting
            " ::1",
            ":: 1",
            "::1 ",
            "[::1]",                 // brackets are URI/host syntax, not address syntax
            "٥::1",                  // non-ASCII "digit"
            "::١",
    })
    void ipv6Rejects(String value) {
        assertThat(IpAddresses.isIpv6(value)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "fe80::1%eth0",
            "fe80::1%0",
            "fe80::1%1",
            "fe80::1%en0.1",
            "::1%lo",
            "::%zone",
            "2001:db8::1%25eth0",    // '%25' here is a literal zone "25eth0", not pct-encoding
            "fe80::1%e!h0",          // the zone grammar is deliberately permissive: any non-NUL
            "fe80::1%%",
            "fe80::1% ",
    })
    void ipv6ZoneIdsAcceptAnyNonEmptyNulFreeSuffix(String value) {
        // RFC 4007 leaves the zone-id alphabet implementation-defined; this class documents
        // "any non-empty run of characters after % that contains no NUL".
        assertThat(IpAddresses.isIpv6(value)).isTrue();
    }

    @Test
    void ipv6ZoneIdWithNulIsRejected() {
        assertThat(IpAddresses.isIpv6("fe80::1%a\u0000b")).isFalse();
        assertThat(IpAddresses.isIpv6("fe80::1%\u0000")).isFalse();
    }

    // ---------------------------------------------------------------- isIp(value, version)

    @ParameterizedTest
    @CsvSource({
            "1.2.3.4,   4, true",
            "1.2.3.4,   6, false",
            "1.2.3.4,   0, true",
            "::1,       4, false",
            "::1,       6, true",
            "::1,       0, true",
            "not-an-ip, 0, false",
            "1.2.3.4,   5, false",
            "::1,       5, false",
            "1.2.3.4,   1, false",
            "1.2.3.4,  -1, false",
            "1.2.3.4,  46, false",
    })
    void isIpDispatchesOnVersion(String value, int version, boolean expected) {
        assertThat(IpAddresses.isIp(value, version)).isEqualTo(expected);
    }

    // ---------------------------------------------------------------- IPv4 prefixes

    @ParameterizedTest
    @ValueSource(strings = {
            "0.0.0.0/0",
            "0.0.0.0/32",
            "255.255.255.255/32",
            "192.168.0.0/24",
            "192.168.0.1/24",        // host bits set — fine when not strict
            "10.0.0.0/8",
            "172.16.0.0/12",
            "128.0.0.0/1",
            "1.2.3.4/32",
            "192.168.1.128/25",
            "203.0.113.255/0",
    })
    void ipv4PrefixAcceptsNonStrict(String value) {
        assertThat(IpAddresses.isIpPrefix(value, 4, false)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "0.0.0.0/0",
            "128.0.0.0/1",
            "10.0.0.0/8",
            "172.16.0.0/12",
            "192.168.0.0/16",
            "192.168.1.0/24",
            "192.168.1.128/25",
            "192.168.1.192/26",
            "192.168.0.1/32",        // /32 has no host bits at all
            "255.255.255.255/32",
    })
    void ipv4PrefixAcceptsStrict(String value) {
        assertThat(IpAddresses.isIpPrefix(value, 4, true)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "192.168.0.1/24",        // .1 below /24
            "10.0.0.1/8",
            "0.0.0.1/0",
            "128.0.0.0/0",           // the very first bit is a host bit under /0
            "192.168.1.64/25",       // bit 25 set
            "1.2.3.4/16",
    })
    void ipv4PrefixRejectsHostBitsWhenStrict(String value) {
        assertThat(IpAddresses.isIpPrefix(value, 4, true)).isFalse();
        // ... but the same strings are fine when strictness is off.
        assertThat(IpAddresses.isIpPrefix(value, 4, false)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "/24",
            "1.2.3.4",
            "1.2.3.4/",
            "1.2.3.4/33",
            "1.2.3.4/99",
            "1.2.3.4/999",
            "1.2.3.4/1000",
            "1.2.3.4/-1",
            "1.2.3.4/+1",
            "1.2.3.4/024",           // leading zero in prefix length
            "1.2.3.4/00",
            "1.2.3.4/24/24",         // two slashes
            "1.2.3.4//24",
            "1.2.3.4/24 ",
            "1.2.3.4 /24",
            "1.2.3.4/2a",
            "1.2.3.4/2.0",
            "256.0.0.0/8",           // bad address part
            "01.2.3.4/8",
            "1.2.3/24",
            "::1/128",               // wrong version
    })
    void ipv4PrefixRejects(String value) {
        assertThat(IpAddresses.isIpPrefix(value, 4, false)).isFalse();
        assertThat(IpAddresses.isIpPrefix(value, 4, true)).isFalse();
    }

    // ---------------------------------------------------------------- IPv6 prefixes

    @ParameterizedTest
    @ValueSource(strings = {
            "::/0",
            "::/128",
            "::1/128",
            "2001:db8::/32",
            "2001:db8::1/32",        // host bits set — fine when not strict
            "fe80::/10",
            "8000::/1",
            "ff00::/8",
            "2001:db8:85a3::8a2e:370:7334/128",
            "::ffff:192.0.2.0/120",
    })
    void ipv6PrefixAcceptsNonStrict(String value) {
        assertThat(IpAddresses.isIpPrefix(value, 6, false)).isTrue();
    }

    @ParameterizedTest
    @CsvSource({
            "::/0,               true",
            "8000::/1,           true",
            "fe80::/10,          true",
            "2001:db8::/32,      true",
            "::1/128,            true",
            "2001:db8::1/128,    true",
            "2001:db8::1/32,     false",
            "::1/0,              false",
            "8000::/0,           false",
            "fe80::1/64,         false",
            "2001:db8:8000::/33,  true",
            "2001:db8:4000::/33,  false",
    })
    void ipv6PrefixStrictMode(String value, boolean expected) {
        assertThat(IpAddresses.isIpPrefix(value, 6, true)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "::",
            "::/",
            "/64",
            "::/129",
            "::/1000",
            "::/-1",
            "::/00",
            "::/012",                // leading zero
            "2001:db8::/32/32",
            "2001:db8:://32",
            "fe80::1%eth0/64",       // zone ids are not part of prefix syntax
            "fe80::1%25eth0/64",
            "g::/32",
            "1.2.3.4/24",            // wrong version
            "::/128 ",
    })
    void ipv6PrefixRejects(String value) {
        assertThat(IpAddresses.isIpPrefix(value, 6, false)).isFalse();
        assertThat(IpAddresses.isIpPrefix(value, 6, true)).isFalse();
    }

    // ---------------------------------------------------------------- isIpPrefix version dispatch

    @ParameterizedTest
    @CsvSource({
            "192.168.0.0/24, 0, true",
            "2001:db8::/32,  0, true",
            "192.168.0.0/24, 6, false",
            "2001:db8::/32,  4, false",
            "192.168.0.0/24, 5, false",
            "2001:db8::/32, -1, false",
            "nonsense/24,    0, false",
    })
    void isIpPrefixDispatchesOnVersion(String value, int version, boolean expected) {
        assertThat(IpAddresses.isIpPrefix(value, version, false)).isEqualTo(expected);
    }

    @Test
    void prefixLengthBoundariesAreExact() {
        assertThat(IpAddresses.isIpPrefix("1.2.3.4/31", 4, false)).isTrue();
        assertThat(IpAddresses.isIpPrefix("1.2.3.4/32", 4, false)).isTrue();
        assertThat(IpAddresses.isIpPrefix("1.2.3.4/33", 4, false)).isFalse();
        assertThat(IpAddresses.isIpPrefix("::/127", 6, false)).isTrue();
        assertThat(IpAddresses.isIpPrefix("::/128", 6, false)).isTrue();
        assertThat(IpAddresses.isIpPrefix("::/129", 6, false)).isFalse();
    }

    @Test
    void strictBitBoundaryWithinAByte() {
        // /31 leaves exactly one host bit (the lowest). 0.0.0.1 sets it; 0.0.0.2 does not.
        assertThat(IpAddresses.isIpPrefix("0.0.0.2/31", 4, true)).isTrue();
        assertThat(IpAddresses.isIpPrefix("0.0.0.1/31", 4, true)).isFalse();
        assertThat(IpAddresses.isIpPrefix("0.0.0.1/31", 4, false)).isTrue();
    }
}
