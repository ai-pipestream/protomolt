package ai.pipestream.proto.formats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PostalMasksTest {

    @Test
    void digitsLettersAndLiteralsMatchPositionally() {
        assertThat(PostalMasks.matches("94105", "NNNNN")).isTrue();
        assertThat(PostalMasks.matches("94105-1234", "NNNNN-NNNN")).isTrue();
        assertThat(PostalMasks.matches("N1 9AA", "AN NAA")).isTrue();
        assertThat(PostalMasks.matches("EC1A 1BB", "AANA NAA")).isTrue();
    }

    @Test
    void everyPositionIsChecked() {
        assertThat(PostalMasks.matches("9410", "NNNNN")).isFalse();     // short
        assertThat(PostalMasks.matches("941056", "NNNNN")).isFalse();   // long
        assertThat(PostalMasks.matches("9410A", "NNNNN")).isFalse();    // letter for N
        assertThat(PostalMasks.matches("11 9AA", "AN NAA")).isFalse();  // digit for A
        assertThat(PostalMasks.matches("n1 9AA", "AN NAA")).isFalse();  // lowercase
        assertThat(PostalMasks.matches("94105 1234", "NNNNN-NNNN")).isFalse(); // literal
        assertThat(PostalMasks.matches("", "")).isFalse();              // empty mask
    }
}
