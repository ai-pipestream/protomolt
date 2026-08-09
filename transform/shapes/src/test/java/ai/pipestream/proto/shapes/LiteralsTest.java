package ai.pipestream.proto.shapes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The literal half of the text-rule dialect: {@code null}, booleans, double-quoted strings,
 * and numbers are values; everything else is a field path. The static checker and the
 * runtime mapper share this exact recognition, so the matrix is pinned down here.
 */
class LiteralsTest {

    @Test
    void recognizesTheLiteralForms() {
        assertThat(Literals.isLiteral("null")).isTrue();
        assertThat(Literals.isLiteral("true")).isTrue();
        assertThat(Literals.isLiteral("false")).isTrue();
        assertThat(Literals.isLiteral("\"\"")).isTrue();
        assertThat(Literals.isLiteral("\"hello\"")).isTrue();
        assertThat(Literals.isLiteral("0")).isTrue();
        assertThat(Literals.isLiteral("-7")).isTrue();
        assertThat(Literals.isLiteral("3.14")).isTrue();
        assertThat(Literals.isLiteral("-0.5")).isTrue();
    }

    @Test
    void rejectsPathsAndNearMisses() {
        assertThat(Literals.isLiteral("order.id")).isFalse();
        assertThat(Literals.isLiteral("order")).isFalse();
        // Single quotes are not the string syntax of this dialect.
        assertThat(Literals.isLiteral("'fixed'")).isFalse();
        // A lone quote is too short to be a quoted string.
        assertThat(Literals.isLiteral("\"")).isFalse();
        assertThat(Literals.isLiteral("\"unterminated")).isFalse();
        // Numbers are decimal digits with an optional fraction — no exponents or hex.
        assertThat(Literals.isLiteral("1.")).isFalse();
        assertThat(Literals.isLiteral(".5")).isFalse();
        assertThat(Literals.isLiteral("1e5")).isFalse();
        assertThat(Literals.isLiteral("0x10")).isFalse();
        // Near-keywords are paths, not booleans.
        assertThat(Literals.isLiteral("truee")).isFalse();
        assertThat(Literals.isLiteral("")).isFalse();
    }

    @Test
    void valuesFollowTheRecognizedForm() {
        assertThat(Literals.valueOf("null")).isNull();
        assertThat(Literals.valueOf("true")).isEqualTo(Boolean.TRUE);
        assertThat(Literals.valueOf("false")).isEqualTo(Boolean.FALSE);
        assertThat(Literals.valueOf("\"hi there\"")).isEqualTo("hi there");
        assertThat(Literals.valueOf("\"\"")).isEqualTo("");
        // Integral numbers are longs; anything with a fraction is a double.
        assertThat(Literals.valueOf("42")).isEqualTo(42L);
        assertThat(Literals.valueOf("-7")).isEqualTo(-7L);
        assertThat(Literals.valueOf("2.5")).isEqualTo(2.5);
        assertThat(Literals.valueOf("-0.5")).isEqualTo(-0.5);
    }
}
