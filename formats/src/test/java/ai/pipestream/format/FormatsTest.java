package ai.pipestream.format;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FormatsTest {

    @Test
    void hostnames() {
        assertThat(Formats.isHostname("example.com")).isTrue();
        assertThat(Formats.isHostname("foo-bar.example.com")).isTrue();
        assertThat(Formats.isHostname("example.com.")).isTrue();      // trailing dot allowed
        assertThat(Formats.isHostname("a.b.c.d.e.f")).isTrue();
        assertThat(Formats.isHostname("xn--nxasmq6b.example")).isTrue();

        assertThat(Formats.isHostname("")).isFalse();
        assertThat(Formats.isHostname("-leading.com")).isFalse();
        assertThat(Formats.isHostname("trailing-.com")).isFalse();
        assertThat(Formats.isHostname("under_score.com")).isFalse();
        assertThat(Formats.isHostname("double..dot")).isFalse();
        assertThat(Formats.isHostname("1.2.3.4")).isFalse();          // all-numeric TLD
        assertThat(Formats.isHostname("a".repeat(64) + ".com")).isFalse(); // label > 63
    }

    @Test
    void emails() {
        assertThat(Formats.isEmail("user@example.com")).isTrue();
        assertThat(Formats.isEmail("first.last+tag@sub.example.co")).isTrue();
        assertThat(Formats.isEmail("a!#$%&'*+/=?^_`{|}~-@example.com")).isTrue();

        assertThat(Formats.isEmail("no-at-sign")).isFalse();
        assertThat(Formats.isEmail("two@@example.com")).isFalse();
        assertThat(Formats.isEmail("space in@example.com")).isFalse();
        assertThat(Formats.isEmail("trailingdot@example.com.")).isFalse();
    }

    @Test
    void ipv4() {
        assertThat(Formats.isIp("192.168.0.1", 4)).isTrue();
        assertThat(Formats.isIp("0.0.0.0", 4)).isTrue();
        assertThat(Formats.isIp("255.255.255.255", 4)).isTrue();

        assertThat(Formats.isIp("192.168.0.1", 6)).isFalse();
        assertThat(Formats.isIp("256.0.0.1", 4)).isFalse();
        assertThat(Formats.isIp("01.2.3.4", 4)).isFalse();           // leading zero
        assertThat(Formats.isIp("1.2.3", 4)).isFalse();
        assertThat(Formats.isIp("1.2.3.4.5", 4)).isFalse();
    }

    @Test
    void ipv6() {
        assertThat(Formats.isIp("::1", 6)).isTrue();
        assertThat(Formats.isIp("::", 6)).isTrue();
        assertThat(Formats.isIp("2001:db8::1", 6)).isTrue();
        assertThat(Formats.isIp("2001:0db8:0000:0000:0000:0000:0000:0001", 6)).isTrue();
        assertThat(Formats.isIp("::ffff:192.168.0.1", 6)).isTrue();  // embedded IPv4
        assertThat(Formats.isIp("fe80::1", 6)).isTrue();

        assertThat(Formats.isIp("2001:db8::1::2", 6)).isFalse();     // two "::"
        assertThat(Formats.isIp("2001:db8:0:0:0:0:0:0:1", 6)).isFalse(); // 9 groups
        assertThat(Formats.isIp("12345::", 6)).isFalse();            // hextet too long
        assertThat(Formats.isIp("::ffff:999.1.1.1", 6)).isFalse();   // bad embedded IPv4
        assertThat(Formats.isIp("1:2:3:4:5:6:7:8::", 6)).isFalse();  // "::" compresses nothing
    }

    @Test
    void ipPrefix() {
        assertThat(Formats.isIpPrefix("192.168.0.0/24", 4, false)).isTrue();
        assertThat(Formats.isIpPrefix("192.168.0.0/24", 4, true)).isTrue();   // host bits zero
        assertThat(Formats.isIpPrefix("192.168.0.1/24", 4, false)).isTrue();
        assertThat(Formats.isIpPrefix("192.168.0.1/24", 4, true)).isFalse();  // host bits set
        assertThat(Formats.isIpPrefix("2001:db8::/32", 6, true)).isTrue();
        assertThat(Formats.isIpPrefix("2001:db8::1/32", 6, true)).isFalse();

        assertThat(Formats.isIpPrefix("192.168.0.0/33", 4, false)).isFalse(); // bits > 32
        assertThat(Formats.isIpPrefix("192.168.0.0", 4, false)).isFalse();    // no prefix
    }

    @Test
    void ipVersionMustBeCheckedAsFullLong() {
        // 4294967300 == 2^32 + 4: a bare (int) cast would truncate it to 4.
        assertThat(Formats.isIp("1.2.3.4", 4294967300L)).isFalse();
        assertThat(Formats.isIp("::1", 4294967302L)).isFalse();
        assertThat(Formats.isIp("1.2.3.4", 5)).isFalse();
        assertThat(Formats.isIp("1.2.3.4", -4)).isFalse();

        assertThat(Formats.isIpPrefix("192.168.0.0/24", 4294967300L)).isFalse();
        assertThat(Formats.isIpPrefix("192.168.0.0/24", 4294967300L, true)).isFalse();
        assertThat(Formats.isIpWithPrefixLen("192.168.0.1/24", 4294967300L)).isFalse();

        // Sanity: the supported versions still work.
        assertThat(Formats.isIp("1.2.3.4", 4)).isTrue();
        assertThat(Formats.isIp("::1", 6)).isTrue();
        assertThat(Formats.isIp("1.2.3.4", 0)).isTrue();
        assertThat(Formats.isIpPrefix("192.168.0.0/24", 4)).isTrue();
        assertThat(Formats.isIpWithPrefixLen("192.168.0.1/24", 4)).isTrue();
    }

    @Test
    void ipv6ZoneIds() {
        // RFC 6874: ZoneID = 1*( unreserved / pct-encoded )
        assertThat(Formats.isUri("http://[fe80::1%25eth0]/")).isTrue();
        assertThat(Formats.isUri("http://[fe80::1%25en1.2_3~x-]/")).isTrue();
        assertThat(Formats.isUri("http://[fe80::1%25et%41h0]/")).isTrue();  // pct-encoded 'A'

        assertThat(Formats.isUri("http://[fe80::1%25e!0]/")).isFalse();     // sub-delim '!'
        assertThat(Formats.isUri("http://[fe80::1%25e$0]/")).isFalse();     // sub-delim '$'
        assertThat(Formats.isUri("http://[fe80::1%25]/")).isFalse();        // empty zone id
        assertThat(Formats.isUri("http://[fe80::1%25e%zzh]/")).isFalse();   // bad pct-encoding
    }

    @Test
    void uris() {
        assertThat(Formats.isUri("https://example.com/path?q=1#frag")).isTrue();
        assertThat(Formats.isUri("ftp://user@host:21/dir/file")).isTrue();
        assertThat(Formats.isUri("urn:oasis:names:specification:docbook:dtd:xml:4.1.2")).isTrue();
        assertThat(Formats.isUri("mailto:someone@example.com")).isTrue();
        assertThat(Formats.isUri("file:///etc/hosts")).isTrue();
        assertThat(Formats.isUri("http://[2001:db8::1]:8080/")).isTrue();
        assertThat(Formats.isUri("foo://info.example.com?token=a%20b")).isTrue();

        assertThat(Formats.isUri("/relative/path")).isFalse();       // no scheme
        assertThat(Formats.isUri("http://exa mple.com")).isFalse();  // space in host
        assertThat(Formats.isUri("http://example.com/%zz")).isFalse(); // bad pct-encoding
        assertThat(Formats.isUri("1http://example.com")).isFalse();  // scheme starts with digit
    }

    @Test
    void uriReferences() {
        assertThat(Formats.isUriRef("https://example.com")).isTrue();
        assertThat(Formats.isUriRef("//example.com/path")).isTrue(); // network-path reference
        assertThat(Formats.isUriRef("/absolute/path")).isTrue();
        assertThat(Formats.isUriRef("relative/path")).isTrue();
        assertThat(Formats.isUriRef("../up")).isTrue();
        assertThat(Formats.isUriRef("")).isTrue();                   // empty same-document ref
        assertThat(Formats.isUriRef("?query-only")).isTrue();
        assertThat(Formats.isUriRef("#fragment-only")).isTrue();

        assertThat(Formats.isUriRef("http://exa mple.com")).isFalse();
        assertThat(Formats.isUriRef("path/%2")).isFalse();           // truncated pct-encoding
    }

    @Test
    void hostAndPort() {
        assertThat(Formats.isHostAndPort("example.com:8080", true)).isTrue();
        assertThat(Formats.isHostAndPort("example.com", false)).isTrue();
        assertThat(Formats.isHostAndPort("192.168.0.1:443", true)).isTrue();
        assertThat(Formats.isHostAndPort("[::1]:80", true)).isTrue();
        assertThat(Formats.isHostAndPort("[2001:db8::1]", false)).isTrue();

        assertThat(Formats.isHostAndPort("example.com", true)).isFalse();     // port required
        assertThat(Formats.isHostAndPort("example.com:99999", true)).isFalse(); // port > 65535
        assertThat(Formats.isHostAndPort("example.com:0080", true)).isFalse(); // leading zero
        assertThat(Formats.isHostAndPort("", false)).isFalse();
    }

    @Test
    void singleArgumentIpAcceptsEitherVersion() {
        assertThat(Formats.isIp("192.168.0.1")).isTrue();
        assertThat(Formats.isIp("::1")).isTrue();
        assertThat(Formats.isIp("fe80::1%eth0")).isTrue();
        assertThat(Formats.isIp("not-an-ip")).isFalse();
        assertThat(Formats.isIp("")).isFalse();
    }

    @Test
    void singleArgumentIpPrefixAcceptsEitherVersion() {
        assertThat(Formats.isIpPrefix("192.168.0.0/24")).isTrue();
        assertThat(Formats.isIpPrefix("192.168.0.1/24")).isTrue();  // host bits allowed
        assertThat(Formats.isIpPrefix("2001:db8::/32")).isTrue();
        assertThat(Formats.isIpPrefix("2001:db8::1/32")).isTrue();
        assertThat(Formats.isIpPrefix("192.168.0.0")).isFalse();
        assertThat(Formats.isIpPrefix("2001:db8::")).isFalse();
    }

    @Test
    void ipWithPrefixLenIgnoresHostBits() {
        assertThat(Formats.isIpWithPrefixLen("192.168.1.1/24", 4)).isTrue();
        assertThat(Formats.isIpWithPrefixLen("192.168.1.1/24", 0)).isTrue();
        assertThat(Formats.isIpWithPrefixLen("2001:db8::1/32", 6)).isTrue();
        assertThat(Formats.isIpWithPrefixLen("192.168.1.1", 4)).isFalse();
        assertThat(Formats.isIpWithPrefixLen("192.168.1.1/33", 4)).isFalse();
    }

    @Test
    void address() {
        assertThat(Formats.isAddress("example.com")).isTrue();       // hostname
        assertThat(Formats.isAddress("192.168.0.1")).isTrue();       // IPv4
        assertThat(Formats.isAddress("::1")).isTrue();               // IPv6
        assertThat(Formats.isAddress("fe80::1%eth0")).isTrue();      // IPv6 with zone
        assertThat(Formats.isAddress("localhost")).isTrue();

        assertThat(Formats.isAddress("")).isFalse();
        assertThat(Formats.isAddress("1.2.3.4.5")).isFalse();        // neither hostname nor IP
        assertThat(Formats.isAddress("256.1.1.1")).isFalse();        // numeric TLD, bad octet
        assertThat(Formats.isAddress("-x.example.com")).isFalse();
        assertThat(Formats.isAddress("[::1]")).isFalse();            // brackets are not address syntax
        assertThat(Formats.isAddress("example.com:80")).isFalse();   // ports are not addresses
    }

    @Test
    void uuidFacades() {
        assertThat(Formats.isUuid("123e4567-e89b-12d3-a456-426614174000")).isTrue();
        assertThat(Formats.isUuid("123e4567e89b12d3a456426614174000")).isFalse();

        assertThat(Formats.isTuuid("123e4567e89b12d3a456426614174000")).isTrue();
        assertThat(Formats.isTuuid("123e4567-e89b-12d3-a456-426614174000")).isFalse();
    }

    @Test
    void ulidFacade() {
        assertThat(Formats.isUlid("01ARZ3NDEKTSV4RRFFQ69G5FAV")).isTrue();
        assertThat(Formats.isUlid("8ZZZZZZZZZZZZZZZZZZZZZZZZZ")).isFalse();
    }

    @Test
    void protobufNameFacades() {
        assertThat(Formats.isProtobufFqn("foo.bar.Baz")).isTrue();
        assertThat(Formats.isProtobufFqn(".foo.bar.Baz")).isFalse();

        assertThat(Formats.isProtobufDotFqn(".foo.bar.Baz")).isTrue();
        assertThat(Formats.isProtobufDotFqn("foo.bar.Baz")).isFalse();
    }
}
