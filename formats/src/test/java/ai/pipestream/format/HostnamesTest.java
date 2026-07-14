package ai.pipestream.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Hostnames} implements RFC 1034 label syntax with the RFC 1123 relaxation (labels may
 * start with a digit), a 253-character overall cap (measured on the input, including any trailing
 * dot), an optional single trailing dot, and the rule that the rightmost label must not be
 * entirely numeric.
 */
class HostnamesTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "example.com",
            "EXAMPLE.COM",
            "Example.Com",
            "localhost",
            "a",
            "a.b",
            "a.b.c.d.e.f",
            "foo-bar.example.com",
            "foo--bar.example.com",       // consecutive interior hyphens are legal
            "xn--nxasmq6b.example",       // punycode-shaped label
            "xn--bcher-kva.de",
            "example.com.",               // single trailing dot (root-qualified name)
            "localhost.",
            "9lives.com",                 // RFC 1123: labels may start with a digit
            "3com.com",
            "123.example.com",            // all-numeric label is fine when not rightmost
            "123.456.com",
            "1-1",                        // rightmost label with hyphen is not "all digits"
            "example.co1",
            "example.1a",
            "example.a1",
            "a1-b2.c3-d4",
            "sub.sub2.sub3.sub4.example.com",
    })
    void accepts(String value) {
        assertThat(Hostnames.isHostname(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            ".",
            "..",
            "example..com",              // empty label
            ".example.com",              // leading dot => empty first label
            "example.com..",             // only a single trailing dot is stripped
            "-leading.com",
            "trailing-.com",
            "example.-com",
            "example.com-",
            "-",
            "a-",
            "-a",
            "under_score.com",
            "_dmarc.example.com",        // underscore labels (DNS SRV convention) are not hostnames
            "example.com_",
            "1.2.3.4",                   // all-numeric rightmost label (looks like an IPv4 address)
            "example.123",
            "example.123.",              // trailing dot does not rescue a numeric TLD
            "999",
            "0",
            "exa mple.com",
            " example.com",
            "example.com ",
            "example.com\n",
            "exa\tmple.com",
            "bücher.de",                 // non-ASCII (IDN must be punycode-encoded first)
            "例え.jp",
            "example.com/path",
            "example.com:80",
            "*.example.com",             // wildcards are certificate syntax, not hostname syntax
            "!bang.com",
            "exam!ple.com",
            "host~name.com",
    })
    void rejects(String value) {
        assertThat(Hostnames.isHostname(value)).isFalse();
    }

    @Test
    void labelLengthBoundary() {
        String label63 = "a".repeat(63);
        String label64 = "a".repeat(64);
        assertThat(Hostnames.isHostname(label63)).isTrue();
        assertThat(Hostnames.isHostname(label63 + ".com")).isTrue();
        assertThat(Hostnames.isHostname("com." + label63)).isTrue();
        assertThat(Hostnames.isHostname(label64)).isFalse();
        assertThat(Hostnames.isHostname(label64 + ".com")).isFalse();
        assertThat(Hostnames.isHostname("com." + label64)).isFalse();
    }

    @Test
    void totalLengthBoundary() {
        String name253 = "a.".repeat(126) + "a";   // 253 chars
        assertThat(name253).hasSize(253);
        assertThat(Hostnames.isHostname(name253)).isTrue();

        String name254 = "a.".repeat(126) + "ab";  // 254 chars
        assertThat(name254).hasSize(254);
        assertThat(Hostnames.isHostname(name254)).isFalse();
    }

    @Test
    void totalLengthCountsTheTrailingDot() {
        // The 253-char cap is applied to the input as given, so a 253-char name plus a trailing
        // dot (254 total) is rejected, while a 252-char name with its dot (253 total) passes.
        String name252 = "a.".repeat(125) + "ab";  // 252 chars
        assertThat(Hostnames.isHostname(name252 + ".")).isTrue();

        String name253 = "a.".repeat(126) + "a";   // 253 chars
        assertThat(Hostnames.isHostname(name253 + ".")).isFalse();
    }

    @Test
    void hyphensInsidePunycodePositionsAreAllowed() {
        // RFC 5891 restricts hyphens in positions 3-4 to A-labels, but plain RFC 1034/1123
        // hostname syntax (which this class implements) has no such restriction.
        assertThat(Hostnames.isHostname("ab--cd.example")).isTrue();
        assertThat(Hostnames.isHostname("xn--.example")).isFalse(); // still no trailing hyphen
    }
}
