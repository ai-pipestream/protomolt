package ai.pipestream.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Rfc3986}: syntactic URI / URI-reference validation straight from the RFC 3986 ABNF,
 * with RFC 6874 percent-encoded zone ids and the extra rule that pct-decoded host and zone-id
 * octets must form valid UTF-8. No normalization, no scheme-specific logic.
 */
class Rfc3986Test {

    // ---------------------------------------------------------------- valid absolute URIs

    @ParameterizedTest
    @ValueSource(strings = {
            // RFC 3986 §1.1.2 examples
            "ftp://ftp.is.co.za/rfc/rfc1808.txt",
            "http://www.ietf.org/rfc/rfc2396.txt",
            "ldap://[2001:db8::7]/c=GB?objectClass?one",
            "mailto:John.Doe@example.com",
            "news:comp.infosystems.www.servers.unix",
            "tel:+1-816-555-1212",
            "telnet://192.0.2.16:80/",
            "urn:oasis:names:specification:docbook:dtd:xml:4.1.2",
            // scheme shapes
            "a:",                                    // shortest URI: scheme + empty hier-part
            "a:b",
            "z9+-.://host",                          // full scheme alphabet
            "scheme+ext-1.2://host",
            "HTTP://EXAMPLE.COM",                    // grammar is case-preserving, not case-limited
            // authority shapes
            "http://",                               // empty authority: reg-name is *( ... )
            "http://example.com",
            "http://example.com/",
            "http://example.com:80/path",
            "http://example.com:/path",              // empty port: port = *DIGIT
            "http://example.com:99999999/",          // no 65535 cap in the URI grammar
            "http://user@example.com",
            "http://user:pass@example.com/",
            "http://user:pa:ss@example.com/",        // ':' is fine anywhere in userinfo
            "http://@example.com",                   // empty userinfo
            "http://!$&'()*+,;=host/",               // sub-delims in reg-name
            "http://1.2.3.4/",
            "http://999.999.999.999/",               // overflowing IPv4 is still a valid reg-name
            "http://host_name/",                     // hmm: '_' is unreserved, reg-name allows it
            // IP literals
            "http://[::1]/",
            "http://[2001:db8::1]:8080/",
            "http://[::ffff:192.0.2.1]/",
            "http://[1:2:3:4:5:6:7:8]",
            "http://[v1.a]/",                        // IPvFuture
            "http://[V7.a:b]/",
            "http://[vF.!$&'()*+,;=:]/",
            "http://[fe80::1%25eth0]/",              // RFC 6874 pct-encoded zone id
            "http://[fe80::1%25en1.2_3~x-]/",
            "http://[fe80::1%25et%41h0]/",           // pct-encoded byte inside the zone id
            // paths
            "file:///etc/hosts",                     // empty authority + abempty path
            "a:///p",
            "http:////",                             // empty authority, path "//" of empty segments
            "http://example.com//double//slash",
            "http://example.com/a/../b",             // dot-segments are valid syntax
            "http://example.com/a/./b",
            "http:/rooted/path",                     // path-absolute, no authority
            "http:/a//b",
            "a:%41",                                 // path-rootless with pct-encoding
            "a:@",                                   // '@' is a pchar
            "a::b",                                  // ':' allowed in a rootless first segment
            "urn:",
            "http://example.com/~user/file.html",
            "http://example.com/a%2Fb",
            "http://example.com/%20",
            "http://example.com/%ff",                // path pct-octets need not be UTF-8
            "http://example.com/!$&'()*+,;=:@",      // every pchar special at once
            // query and fragment
            "http://example.com/?",                  // empty query
            "http://example.com/#",                  // empty fragment
            "http://example.com/?q=1#frag",
            "http://example.com/path?a=b&c=d,e",
            "http://example.com?q",                  // query straight after authority
            "http://example.com#f",
            "http://example.com/?a/b?c",             // '/' and '?' are legal in a query
            "http://example.com/#a/b?c:d@e",         // ... and in a fragment
            "http://example.com#frag?not-a-query",   // '#' splits first, '?' stays in the fragment
            "http:?q",                               // empty hier-part with a query
            "http:#f",                               // empty hier-part with a fragment
            "foo://info.example.com?token=a%20b",
    })
    void uriAccepts(String value) {
        assertThat(Rfc3986.isUri(value)).isTrue();
        // Every URI is also a URI-reference.
        assertThat(Rfc3986.isUriReference(value)).isTrue();
    }

    // ---------------------------------------------------------------- invalid as URI and as reference

    @ParameterizedTest
    @ValueSource(strings = {
            ":",                                     // empty scheme
            ":x",
            ":/",
            "http://exa mple.com",                   // raw space
            "http://example.com/a b",
            "http ://example.com",
            "http://example.com/\t",
            "http://example.com/\n",
            "http://example.com/%zz",                // bad pct hex
            "http://example.com/%1",                 // truncated pct triplet
            "http://example.com/%",
            "http://example.com/%G1",
            "http://example.com/%1G",
            "http://%zz/",
            "http://example.com/<>",                 // gen-delims and friends outside pchar
            "http://example.com/\"quote\"",
            "http://example.com/{brace}",
            "http://example.com/back\\slash",
            "http://example.com/^caret",
            "http://example.com/`tick`",
            "http://example.com/|pipe",
            "http://exämple.com/",                   // raw non-ASCII must be pct-encoded
            "http://example.com/päth",
            "http://example.com/?quäry",
            "http://example.com/#frägment",
            "http://host:80a/",                      // non-digit port
            "http://host:8_0/",
            "http://[::1/",                          // unclosed IPv6 bracket
            "http://[::1]x/",                        // junk after the bracket
            "http://[::1]8080/",
            "http://[gggg::1]/",
            "http://[1.2.3.4]/",                     // brackets hold IPv6/IPvFuture only
            "http://[example.com]/",
            "http://[]/",
            "http://[:::]/",
            "http://[v.a]/",                         // IPvFuture needs 1*HEXDIG before '.'
            "http://[v1.]/",                         // ... and a non-empty tail
            "http://[vg1.a]/",                       // 'g' is not HEXDIG
            "http://[v1.a/b]/",                      // '/' not allowed in the tail
            "http://[fe80::1%eth0]/",                // zone id must be pct-encoded (%25)
            "http://[fe80::1%25]/",                  // empty zone id
            "http://[fe80::1%25e!0]/",               // sub-delims are not ZoneID chars
            "http://[fe80::1%25e%zzh]/",
            "http://a@b@c/",                         // second '@' lands in the host
            "http://foo%c3.example/",                // host pct-octets must decode to UTF-8
            "http://%ff/",
            "http://foo%c3x%96.example/",
            "http://[fe80::1%25%ff]/",               // zone id octets must be UTF-8 too
            "#frag with space",
            "?query with space",
    })
    void rejectsAsUriAndAsReference(String value) {
        assertThat(Rfc3986.isUri(value)).isFalse();
        assertThat(Rfc3986.isUriReference(value)).isFalse();
    }

    // ---------------------------------------------------------------- relative references

    @ParameterizedTest
    @ValueSource(strings = {
            "",                                      // same-document reference
            "//example.com",                         // network-path reference
            "//example.com/path",
            "//user@example.com:8080/p?q#f",
            "//[::1]/p",
            "///path",                               // empty authority + absolute path
            "/",
            "/absolute/path",
            "/a/b/c",
            "relative/path",
            "relative",
            "./rel",
            "../up",
            "../../up/up",
            "g;x=1/./y",                             // RFC 3986 §5.4 reference sample
            ";x",                                    // ';' is a sub-delim: fine in a segment
            "%41",
            "./a:b",                                 // the RFC's own workaround for colon-first
            "seg/a:b",                               // ':' fine in a non-first segment
            "?query",
            "?",
            "#frag",
            "#",
            "?q#f",
            "@leading-at",                           // '@' is allowed in a noscheme segment
            "~tilde",
            "path/",                                 // trailing empty segment
            "path//x",
    })
    void uriReferenceAcceptsRelativeForms(String value) {
        assertThat(Rfc3986.isUriReference(value)).isTrue();
        // ... none of these carry a scheme, so they are not absolute URIs.
        assertThat(Rfc3986.isUri(value)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "1a:b",       // looks like a scheme but can't be one (digit first), and the
                          // colon-in-first-segment rule then kills it as a relative ref
            "a/b:c ",
            "rel ative",
            "%2",
            "path/%2",
            "path/%gg",
            "\\windows\\path",
            "<a>",
            "über",
            "//exa mple.com",
    })
    void uriReferenceRejects(String value) {
        assertThat(Rfc3986.isUriReference(value)).isFalse();
    }

    // ---------------------------------------------------------------- targeted structural cases

    @Test
    void schemeMustPrecedeAnySlash() {
        // The ':' in "/a:b" is inside a path segment, so there is no scheme.
        assertThat(Rfc3986.isUri("/a:b")).isFalse();
        assertThat(Rfc3986.isUriReference("/a:b")).isTrue(); // path-absolute allows ':' anywhere
        assertThat(Rfc3986.isUri("//host/a:b")).isFalse();
        assertThat(Rfc3986.isUriReference("//host/a:b")).isTrue();
    }

    @Test
    void firstColonDelimitsTheScheme() {
        // "a::b" parses as scheme "a" + rootless path ":b" — legal.
        assertThat(Rfc3986.isUri("a::b")).isTrue();
        // A scheme with an invalid char before the colon fails outright.
        assertThat(Rfc3986.isUri("a%41:b")).isFalse();
        assertThat(Rfc3986.isUri("-a:b")).isFalse();
        assertThat(Rfc3986.isUri("+a:b")).isFalse();
        assertThat(Rfc3986.isUri(".a:b")).isFalse();
        assertThat(Rfc3986.isUri("9a:b")).isFalse();
        // ... but those same chars are fine after the first char.
        assertThat(Rfc3986.isUri("a9:b")).isTrue();
        assertThat(Rfc3986.isUri("a-:b")).isTrue();
        assertThat(Rfc3986.isUri("a+:b")).isTrue();
        assertThat(Rfc3986.isUri("a.:b")).isTrue();
    }

    @Test
    void fragmentIsSplitBeforeQuery() {
        // '#' wins: everything after it is fragment, so a '?' there is fragment content.
        assertThat(Rfc3986.isUri("http://e/#f?x")).isTrue();
        // A '#' inside what would be the fragment terminates nothing else — a second '#'
        // means the fragment itself contains '#', which pchar forbids.
        assertThat(Rfc3986.isUri("http://e/#a#b")).isFalse();
        assertThat(Rfc3986.isUriReference("#a#b")).isFalse();
        // A second '?' is legal query content.
        assertThat(Rfc3986.isUri("http://e/?a?b")).isTrue();
    }

    @Test
    void hostUtf8RuleAppliesOnlyToTheHost() {
        // %c3%96 decodes to U+00D6 — valid UTF-8, accepted in a host.
        assertThat(Rfc3986.isUri("http://foo%c3%96.example/")).isTrue();
        // %c3 alone is a dangling UTF-8 lead byte — rejected in a host...
        assertThat(Rfc3986.isUri("http://foo%c3.example/")).isFalse();
        // ... but paths, queries and fragments only require well-formed pct-triplets.
        assertThat(Rfc3986.isUri("http://example.com/%c3")).isTrue();
        assertThat(Rfc3986.isUri("http://example.com/?x=%ff")).isTrue();
        assertThat(Rfc3986.isUri("http://example.com/#%ff")).isTrue();
        assertThat(Rfc3986.isUri("http://user%ff@example.com/")).isTrue(); // userinfo too
    }

    @Test
    void pathAbsoluteMayNotStartWithTwoSlashes() {
        // "a:" + "//x" is authority syntax; a *relative* "//x" is a network-path reference.
        // But a path-absolute first segment must be non-empty, so "/​/" only appears via
        // authority parsing. "http:/" + "/path" -> authority "" ; plain "/" + "/path" as a
        // reference -> authority "path"? No: "//path" is authority "path" with empty path.
        assertThat(Rfc3986.isUri("http://x")).isTrue();       // authority "x"
        assertThat(Rfc3986.isUriReference("//x")).isTrue();   // network-path reference
        assertThat(Rfc3986.isUriReference("/x")).isTrue();    // path-absolute
        // authority parsing rejects a host with a raw space either way
        assertThat(Rfc3986.isUriReference("// /")).isFalse();
    }

    @Test
    void emptyStringIsAReferenceButNotAUri() {
        assertThat(Rfc3986.isUri("")).isFalse();
        assertThat(Rfc3986.isUriReference("")).isTrue();
    }

    @Test
    void portDigitsAreUnbounded() {
        // RFC 3986 port = *DIGIT: no numeric range check at the URI layer.
        assertThat(Rfc3986.isUri("http://host:0/")).isTrue();
        assertThat(Rfc3986.isUri("http://host:00080/")).isTrue();
        assertThat(Rfc3986.isUri("http://host:65536/")).isTrue();
        assertThat(Rfc3986.isUri("http://[::1]:99999999/")).isTrue();
        assertThat(Rfc3986.isUri("http://[::1]:/")).isTrue(); // empty port after brackets
    }
}
