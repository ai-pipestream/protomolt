package ai.protomolt.proto.formats;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Byte-level coverage of {@link IpAddresses#parseIpv4} and {@link IpAddresses#parseIpv6}.
 * The boolean validators only prove accept/reject; these tests pin the parsed bytes, which is
 * where the {@code ::} zero-gap placement (head before the gap, tail after it) and the embedded
 * IPv4 tail can be silently wrong while still "valid". Bytes are compared as unsigned ints.
 */
class IpAddressesParseTest {

    private static int[] unsigned(byte[] bytes) {
        int[] out = new int[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            out[i] = bytes[i] & 0xFF;
        }
        return out;
    }

    // ---------------------------------------------------------------- IPv4

    @Test
    void ipv4BytesAreInOrder() {
        assertThat(unsigned(IpAddresses.parseIpv4("192.168.0.1")))
                .containsExactly(192, 168, 0, 1);
        assertThat(unsigned(IpAddresses.parseIpv4("0.0.0.0")))
                .containsExactly(0, 0, 0, 0);
        assertThat(unsigned(IpAddresses.parseIpv4("255.255.255.255")))
                .containsExactly(255, 255, 255, 255);
        // Octets above 127 land in negative byte values: verify the bit pattern, not the sign.
        assertThat(unsigned(IpAddresses.parseIpv4("200.100.50.25")))
                .containsExactly(0xC8, 0x64, 0x32, 0x19);
    }

    @Test
    void ipv4InvalidParsesToNull() {
        assertThat(IpAddresses.parseIpv4("1.2.3")).isNull();
        assertThat(IpAddresses.parseIpv4("1.2.3.4.5")).isNull();
        assertThat(IpAddresses.parseIpv4("256.1.1.1")).isNull();
        assertThat(IpAddresses.parseIpv4("01.2.3.4")).isNull();
        assertThat(IpAddresses.parseIpv4("")).isNull();
    }

    // ---------------------------------------------------------------- IPv6 full form

    @Test
    void ipv6FullFormParsesBigEndian() {
        assertThat(unsigned(IpAddresses.parseIpv6("2001:0db8:0000:0000:0000:0000:0000:0001")))
                .containsExactly(
                        0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0x01);
        assertThat(unsigned(IpAddresses.parseIpv6("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")))
                .containsExactly(
                        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff);
    }

    @Test
    void ipv6HexIsCaseInsensitive() {
        assertThat(IpAddresses.parseIpv6("ABCD:ef01:0:0:0:0:0:1"))
                .isEqualTo(IpAddresses.parseIpv6("abcd:EF01:0:0:0:0:0:1"));
    }

    // ---------------------------------------------------------------- IPv6 zero-gap placement

    @Test
    void allZerosAddressIsSixteenZeroBytes() {
        assertThat(unsigned(IpAddresses.parseIpv6("::"))).containsExactly(new int[16]);
    }

    @Test
    void gapAfterTheHeadLeavesTheTailAtTheEnd() {
        // 2001:db8 up front, 0001 as the last hextet, everything between zeroed.
        assertThat(unsigned(IpAddresses.parseIpv6("2001:db8::1")))
                .containsExactly(
                        0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0x01);
    }

    @Test
    void gapBeforeTheTailKeepsTheHeadAtTheFront() {
        // 1::8 -> first hextet 0x0001, last hextet 0x0008.
        assertThat(unsigned(IpAddresses.parseIpv6("1::8")))
                .containsExactly(
                        0, 1, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 8);
    }

    @Test
    void singleGroupGapIsPlacedExactly() {
        // "1:2:3:4:5:6:7::" compresses one zero group between hextet 7 and the end.
        assertThat(unsigned(IpAddresses.parseIpv6("1:2:3:4:5:6:7::")))
                .containsExactly(
                        0, 1, 0, 2, 0, 3, 0, 4,
                        0, 5, 0, 6, 0, 7, 0, 0);
        // And on the left: "::2:3:4:5:6:7:8" starts with one zero group.
        assertThat(unsigned(IpAddresses.parseIpv6("::2:3:4:5:6:7:8")))
                .containsExactly(
                        0, 0, 0, 2, 0, 3, 0, 4,
                        0, 5, 0, 6, 0, 7, 0, 8);
    }

    @Test
    void middleGapSplitsHeadAndTail() {
        assertThat(unsigned(IpAddresses.parseIpv6("1:2:3::6:7:8")))
                .containsExactly(
                        0, 1, 0, 2, 0, 3, 0, 0,
                        0, 0, 0, 6, 0, 7, 0, 8);
    }

    // ---------------------------------------------------------------- embedded IPv4

    @Test
    void ipv4MappedAddressHasTheMappedPrefix() {
        assertThat(unsigned(IpAddresses.parseIpv6("::ffff:192.0.2.1")))
                .containsExactly(
                        0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0xff, 0xff, 192, 0, 2, 1);
    }

    @Test
    void embeddedIpv4InFullFormOccupiesTheLastFourBytes() {
        assertThat(unsigned(IpAddresses.parseIpv6("1:2:3:4:5:6:1.2.3.4")))
                .containsExactly(
                        0, 1, 0, 2, 0, 3, 0, 4,
                        0, 5, 0, 6, 1, 2, 3, 4);
    }

    @Test
    void embeddedIpv4AfterCompressionKeepsBothSides() {
        assertThat(unsigned(IpAddresses.parseIpv6("64:ff9b::192.0.2.33")))
                .containsExactly(
                        0, 0x64, 0xff, 0x9b, 0, 0, 0, 0,
                        0, 0, 0, 0, 192, 0, 2, 33);
    }

    @Test
    void ipv6InvalidParsesToNull() {
        assertThat(IpAddresses.parseIpv6("")).isNull();
        assertThat(IpAddresses.parseIpv6("1::2::3")).isNull();
        assertThat(IpAddresses.parseIpv6("1:2:3:4:5:6:7")).isNull();
        assertThat(IpAddresses.parseIpv6("1:2:3:4:5:6:7:8:9")).isNull();
        assertThat(IpAddresses.parseIpv6("12345::")).isNull();
        assertThat(IpAddresses.parseIpv6("::ffff:1.2.3")).isNull();
        assertThat(IpAddresses.parseIpv6("1:2:3:4:5:6:7:1.2.3.4")).isNull();
    }

    // ---------------------------------------------------------------- strict prefixes use the parsed bytes

    @Test
    void strictPrefixChecksHostBitsOfTheParsedAddress() {
        // ::ffff:192.0.2.1 has its lowest bit set; /127 strict must see that bit.
        assertThat(IpAddresses.isIpPrefix("::ffff:192.0.2.1/127", 6, true)).isFalse();
        assertThat(IpAddresses.isIpPrefix("::ffff:192.0.2.0/127", 6, true)).isTrue();
        // The mapped prefix sits above /16: any mapped address has host bits under /16.
        assertThat(IpAddresses.isIpPrefix("::ffff:192.0.2.1/16", 6, true)).isFalse();
        assertThat(IpAddresses.isIpPrefix("::/16", 6, true)).isTrue();
    }
}
