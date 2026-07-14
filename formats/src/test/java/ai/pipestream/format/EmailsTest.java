package ai.pipestream.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Emails} implements the WHATWG "valid e-mail address" production, not full RFC 5322:
 * no quoted local parts, no domain address literals, no comments/folding whitespace, and no
 * 64/255-octet length caps (the WHATWG grammar has none). The tests below pin exactly that.
 */
class EmailsTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "user@example.com",
            "USER@EXAMPLE.COM",
            "User.Name@Example.Com",
            "first.last@example.com",
            "first.last+tag@sub.example.co",
            "user+mailbox/department=shipping@example.com",
            "customer/department=shipping@example.com",
            "!def!xyz%abc@example.com",
            "a!#$%&'*+/=?^_`{|}~-@example.com",
            "!@example.com",
            "#@example.com",
            "$@example.com",
            "%@example.com",
            "&@example.com",
            "'@example.com",
            "*@example.com",
            "+@example.com",
            "/@example.com",
            "=@example.com",
            "?@example.com",
            "^@example.com",
            "_@example.com",
            "`@example.com",
            "{@example.com",
            "|@example.com",
            "}@example.com",
            "~@example.com",
            "-@example.com",
            "1234567890@example.com",
            "x@example.com",
            "user@example",                 // single-label domain is permitted by WHATWG
            "user@localhost",
            "user@a",
            "user@a-b.c-d",
            "user@sub.sub2.sub3.example.com",
            "user@xn--nxasmq6b.example",
            "user@123.example.com",
            "user@example.c-m",
            "user@example.co1",
    })
    void accepts(String value) {
        assertThat(Emails.isEmail(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            ".user@example.com",   // leading dot in local part (RFC 5321 would reject; WHATWG allows)
            "user.@example.com",   // trailing dot in local part
            "a..b@example.com",    // consecutive dots in local part
            ".@example.com",       // local part of a single dot
            "..@example.com",
            "user@1.2.3.4",        // WHATWG has no "TLD must not be numeric" rule
            "user@999.999.999.999",
    })
    void acceptsWhatwgLooseCorners(String value) {
        // These are rejected by RFC 5321 dot-atom rules but accepted by the WHATWG grammar,
        // which is the contract this class documents.
        assertThat(Emails.isEmail(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "no-at-sign",
            "@",
            "@example.com",                // empty local part
            "user@",                       // empty domain
            "two@@example.com",
            "a@b@c.com",
            "space in@example.com",
            "user@exam ple.com",
            "user name@example.com",
            "user@example .com",
            "\"quoted\"@example.com",      // quoted local parts are RFC 5322, not WHATWG
            "\"quoted string\"@example.com",
            "\"user@inside\"@example.com",
            "user@[192.168.1.1]",          // address literals are not part of the WHATWG grammar
            "user@[IPv6:::1]",
            "user@[IPv6:2001:db8::1]",
            "user(comment)@example.com",   // comments are RFC 5322 only
            "user@(comment)example.com",
            "user,name@example.com",       // ',' is not an atext char
            "user;name@example.com",
            "user:name@example.com",
            "user<name@example.com",
            "user>name@example.com",
            "user[name@example.com",
            "user]name@example.com",
            "user\\name@example.com",
            "user@exa_mple.com",           // underscore not allowed in domain labels
            "user@_example.com",
            "user@-example.com",           // leading hyphen in label
            "user@example-.com",           // trailing hyphen in label
            "user@example.-com",
            "user@example.com-",
            "user@.example.com",           // empty first label
            "user@example..com",           // empty middle label
            "user@example.com.",           // trailing dot means an empty last label here
            "user@.",
            "user@..",
            "user@example.com/path",
            "user@example.com:25",
            "user@exa+mple.com",           // local-part specials are not domain chars
            "user@exa!mple.com",
            "üser@example.com",            // non-ASCII local part
            "user@exämple.com",            // non-ASCII domain
            "user@例え.jp",
            "user\t@example.com",
            "user@example.com\n",
            "\nuser@example.com",
            " user@example.com",
            "user@example.com ",
    })
    void rejects(String value) {
        assertThat(Emails.isEmail(value)).isFalse();
    }

    @Test
    void domainLabelBoundaries() {
        String label63 = "a".repeat(63);
        String label64 = "a".repeat(64);
        assertThat(Emails.isEmail("user@" + label63)).isTrue();
        assertThat(Emails.isEmail("user@" + label63 + ".com")).isTrue();
        assertThat(Emails.isEmail("user@" + label64)).isFalse();
        assertThat(Emails.isEmail("user@" + label64 + ".com")).isFalse();
        assertThat(Emails.isEmail("user@com." + label64)).isFalse();
    }

    @Test
    void noLengthCapsBeyondLabels() {
        // The WHATWG grammar (unlike RFC 5321 §4.5.3.1) imposes no 64-octet local-part cap and
        // no 255-octet domain cap; only the 63-char label limit applies.
        assertThat(Emails.isEmail("a".repeat(64) + "@example.com")).isTrue();
        assertThat(Emails.isEmail("a".repeat(500) + "@example.com")).isTrue();
        String longDomain = "a.".repeat(200) + "com"; // 403 chars, every label valid
        assertThat(Emails.isEmail("user@" + longDomain)).isTrue();
    }

    @Test
    void embeddedNulIsRejected() {
        assertThat(Emails.isEmail("user\u0000@example.com")).isFalse();
        assertThat(Emails.isEmail("user@exam\u0000ple.com")).isFalse();
    }

}
