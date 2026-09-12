package ai.protomolt.proto.asset.characterize.signature;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Compiling and matching the published byte-pattern syntax. */
class SignatureExpressionTest {

    private static ByteWindows content(String latin1) {
        return ByteWindows.ofWhole(latin1.getBytes(StandardCharsets.ISO_8859_1));
    }

    private static ByteWindows content(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return ByteWindows.ofWhole(bytes);
    }

    private static SignatureExpression compile(String expression) {
        try {
            return SignatureExpression.compile(expression);
        } catch (SignatureSyntaxException wrong) {
            throw new AssertionError(wrong);
        }
    }

    @Test
    @DisplayName("hex values match, with or without separating spaces")
    void hexValues() {
        assertThat(compile("49492A00").matchAt(content(0x49, 0x49, 0x2A, 0x00), 0)).isEqualTo(4);
        assertThat(compile("49 49 2A 00").matchAt(content(0x49, 0x49, 0x2A, 0x00), 0))
                .isEqualTo(4);
        assertThat(compile("49492a00").matchAt(content(0x49, 0x49, 0x2A, 0x00), 0)).isEqualTo(4);
        assertThat(compile("49492A01").matchAt(content(0x49, 0x49, 0x2A, 0x00), 0))
                .isEqualTo(SignatureExpression.NO_MATCH);
    }

    @Test
    @DisplayName("a Latin-1 string matches its bytes")
    void strings() {
        SignatureExpression expression = compile("'ContentType=\"application/vnd'");
        assertThat(expression.matchAt(content("ContentType=\"application/vnd.x"), 0))
                .isEqualTo(28);
        assertThat(expression.matchAt(content("ContentType=\"application/xml\""), 0))
                .isEqualTo(SignatureExpression.NO_MATCH);
    }

    @Test
    @DisplayName("a hex value and a quoted character are the same thing")
    void quotedCharacterIsAValue() {
        assertThat(compile("30 '0'").matchAt(content("00"), 0)).isEqualTo(2);
        assertThat(compile("'PK' 03 04").matchAt(content(0x50, 0x4B, 0x03, 0x04), 0))
                .isEqualTo(4);
    }

    @Test
    @DisplayName("ranges accept either separator and either notation")
    void ranges() {
        assertThat(compile("[30:39]").matchAt(content("5"), 0)).isEqualTo(1);
        assertThat(compile("[30-39]").matchAt(content("5"), 0)).isEqualTo(1);
        assertThat(compile("['0'-'9']").matchAt(content("5"), 0)).isEqualTo(1);
        assertThat(compile("['0'-'9']").matchAt(content("x"), 0))
                .isEqualTo(SignatureExpression.NO_MATCH);
    }

    @Test
    @DisplayName("an inverted range matches outside itself")
    void invertedRange() {
        SignatureExpression expression = compile("[!61:7a]");
        assertThat(expression.matchAt(content("A"), 0)).isEqualTo(1);
        assertThat(expression.matchAt(content("a"), 0))
                .isEqualTo(SignatureExpression.NO_MATCH);
    }

    @Test
    @DisplayName("a bitmask matches the bytes sharing its bits")
    void bitmask() {
        SignatureExpression expression = compile("[&88]");
        assertThat(expression.matchAt(content(0x88), 0)).isEqualTo(1);
        assertThat(expression.matchAt(content(0xF8), 0)).isEqualTo(1);
        assertThat(expression.matchAt(content(0x08), 0))
                .isEqualTo(SignatureExpression.NO_MATCH);
        assertThat(compile("[!&01]").matchAt(content(0x02), 0)).isEqualTo(1);
        assertThat(compile("[!&01]").matchAt(content(0x03), 0))
                .isEqualTo(SignatureExpression.NO_MATCH);
    }

    @Test
    @DisplayName("a set unions values, ranges and characters")
    void sets() {
        SignatureExpression expression = compile("[00 C2 DE 'A'-'Z' 'aeiou']");
        assertThat(expression.matchAt(content(0xC2), 0)).isEqualTo(1);
        assertThat(expression.matchAt(content("Q"), 0)).isEqualTo(1);
        assertThat(expression.matchAt(content("e"), 0)).isEqualTo(1);
        assertThat(expression.matchAt(content("b"), 0))
                .isEqualTo(SignatureExpression.NO_MATCH);
        assertThat(compile("[!00 C2 DE]").matchAt(content(0x01), 0)).isEqualTo(1);
    }

    @Test
    @DisplayName("alternatives of unequal length each match on their own terms")
    void alternatives() {
        SignatureExpression expression = compile("(00 01 | B0 B1 B2 | 'end')");
        assertThat(expression.matchAt(content(0x00, 0x01), 0)).isEqualTo(2);
        assertThat(expression.matchAt(content(0xB0, 0xB1, 0xB2), 0)).isEqualTo(3);
        assertThat(expression.matchAt(content("end"), 0)).isEqualTo(3);
        assertThat(expression.matchAt(content(0xB0, 0xB1), 0))
                .isEqualTo(SignatureExpression.NO_MATCH);
        assertThat(expression.minLength()).isEqualTo(2);
        assertThat(expression.maxLength()).isEqualTo(3);
    }

    @Test
    @DisplayName("an alternative followed by more pattern backtracks to the arm that fits")
    void alternativesBacktrack() {
        // The two-byte arm matches first but leaves the rest unmatched, so
        // the match only succeeds if the longer arm is tried afterwards.
        SignatureExpression expression = compile("('AB' | 'ABC') 'Z'");
        assertThat(expression.matchAt(content("ABCZ"), 0)).isEqualTo(4);
        assertThat(expression.matchAt(content("ABZ"), 0)).isEqualTo(3);
    }

    @Test
    @DisplayName("wildcards cover one byte, a fixed stretch and a range")
    void wildcards() {
        assertThat(compile("01 02 ?? 04").matchAt(content(1, 2, 99, 4), 0)).isEqualTo(4);
        assertThat(compile("01 {2} 04").matchAt(content(1, 9, 9, 4), 0)).isEqualTo(4);
        assertThat(compile("01 {1-3} 04").matchAt(content(1, 9, 4), 0)).isEqualTo(3);
        assertThat(compile("01 {1-3} 04").matchAt(content(1, 9, 9, 9, 4), 0)).isEqualTo(5);
        assertThat(compile("01 {1-3} 04").matchAt(content(1, 9, 9, 9, 9, 4), 0))
                .isEqualTo(SignatureExpression.NO_MATCH);
    }

    @Test
    @DisplayName("a stretch with no limit compiles but reports itself unbounded")
    void unbounded() {
        SignatureExpression star = compile("30 31 * 43 2A");
        assertThat(star.bounded()).isFalse();
        assertThat(star.maxLength()).isEqualTo(SignatureExpression.UNBOUNDED);
        assertThat(star.evaluableAt(content("0123456"), 0)).isFalse();
        assertThat(compile("01 {8-*} FF").bounded()).isFalse();
        // A bounded pattern is evaluable exactly when its bytes are resident.
        SignatureExpression bounded = compile("'PK' 03 04");
        assertThat(bounded.bounded()).isTrue();
        assertThat(bounded.evaluableAt(content(0x50, 0x4B, 0x03, 0x04), 0)).isTrue();
        assertThat(bounded.evaluableAt(ByteWindows.ofHead(new byte[] {0x50, 0x4B}), 0))
                .isFalse();
    }

    @Test
    @DisplayName("bytes outside the windows do not match, so a blind spot never passes")
    void unobservableNeverMatches() {
        ByteWindows split = ByteWindows.of(
                "PK".getBytes(StandardCharsets.ISO_8859_1),
                "END".getBytes(StandardCharsets.ISO_8859_1), 100);
        assertThat(compile("'PK'").matchAt(split, 0)).isEqualTo(2);
        assertThat(compile("'PK' 03").matchAt(split, 0))
                .isEqualTo(SignatureExpression.NO_MATCH);
        assertThat(compile("'PK' 03").evaluableAt(split, 0)).isFalse();
        assertThat(compile("'END'").matchAt(split, 97)).isEqualTo(3);
    }

    @Test
    @DisplayName("searching a span finds the leftmost match")
    void findBetween() {
        ByteWindows bytes = content("....PK..PK..");
        assertThat(compile("'PK'").findBetween(bytes, 0, 11)).isEqualTo(4);
        assertThat(compile("'PK'").findBetween(bytes, 5, 11)).isEqualTo(8);
        assertThat(compile("'ZZ'").findBetween(bytes, 0, 11))
                .isEqualTo(SignatureExpression.NO_MATCH);
    }

    @Test
    @DisplayName("lengths are reported for fixed and variable patterns alike")
    void lengths() {
        assertThat(compile("49492A00").minLength()).isEqualTo(4);
        assertThat(compile("49492A00").maxLength()).isEqualTo(4);
        assertThat(compile("01 {2-6} FF").minLength()).isEqualTo(4);
        assertThat(compile("01 {2-6} FF").maxLength()).isEqualTo(8);
    }

    @Test
    @DisplayName("malformed expressions are refused with the position")
    void malformed() {
        assertThatThrownBy(() -> SignatureExpression.compile("ZZ"))
                .isInstanceOf(SignatureSyntaxException.class)
                .hasMessageContaining("hex digits");
        assertThatThrownBy(() -> SignatureExpression.compile("[30:39"))
                .isInstanceOf(SignatureSyntaxException.class)
                .hasMessageContaining("unterminated set");
        assertThatThrownBy(() -> SignatureExpression.compile("'open"))
                .isInstanceOf(SignatureSyntaxException.class)
                .hasMessageContaining("unterminated string");
        assertThatThrownBy(() -> SignatureExpression.compile("(00|01"))
                .isInstanceOf(SignatureSyntaxException.class)
                .hasMessageContaining("unterminated alternatives");
        assertThatThrownBy(() -> SignatureExpression.compile("01 {4-2} FF"))
                .isInstanceOf(SignatureSyntaxException.class)
                .hasMessageContaining("cannot end before it begins");
        assertThatThrownBy(() -> SignatureExpression.compile("[]"))
                .isInstanceOf(SignatureSyntaxException.class)
                .hasMessageContaining("empty set");
        assertThatThrownBy(() -> SignatureExpression.compile(""))
                .isInstanceOf(SignatureSyntaxException.class)
                .hasMessageContaining("empty");
        assertThatThrownBy(() -> SignatureExpression.compile("01)"))
                .isInstanceOf(SignatureSyntaxException.class)
                .hasMessageContaining("unexpected ')'");
    }

    @Test
    @DisplayName("the reported position points into the expression")
    void syntaxPosition() {
        assertThatThrownBy(() -> SignatureExpression.compile("4949 ZZ 00"))
                .isInstanceOf(SignatureSyntaxException.class)
                .satisfies(thrown -> {
                    SignatureSyntaxException syntax = (SignatureSyntaxException) thrown;
                    assertThat(syntax.position()).isEqualTo(5);
                    assertThat(syntax.expression()).isEqualTo("4949 ZZ 00");
                });
    }
}
