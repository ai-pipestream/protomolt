package ai.protomolt.proto.asset.characterize;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The bounded view identification is allowed to see. */
class ByteWindowsTest {

    private static byte[] ramp(int from, int length) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) (from + i);
        }
        return out;
    }

    @Test
    @DisplayName("content read whole resolves at every offset")
    void whole() {
        ByteWindows windows = ByteWindows.ofWhole(ramp(0, 10));
        assertThat(windows.sizeBytes()).isEqualTo(10);
        assertThat(windows.byteAt(0)).isEqualTo(0);
        assertThat(windows.byteAt(9)).isEqualTo(9);
        assertThat(windows.byteAt(10)).isEqualTo(ByteWindows.NOT_OBSERVED);
        assertThat(windows.observable(0, 10)).isTrue();
    }

    @Test
    @DisplayName("a leading window alone leaves the middle and the end unobservable")
    void headOnly() {
        ByteWindows windows = ByteWindows.ofHead(ramp(0, 4));
        assertThat(windows.sizeKnown()).isFalse();
        assertThat(windows.byteAt(3)).isEqualTo(3);
        assertThat(windows.byteAt(4)).isEqualTo(ByteWindows.NOT_OBSERVED);
        assertThat(windows.fromEnd(1)).isEqualTo(ByteWindows.NOT_OBSERVED);
    }

    @Test
    @DisplayName("the gap between the windows is not observable, and says so")
    void gap() {
        ByteWindows windows = ByteWindows.of(ramp(0, 4), ramp(96, 4), 100);
        assertThat(windows.byteAt(3)).isEqualTo(3);
        assertThat(windows.byteAt(4)).isEqualTo(ByteWindows.NOT_OBSERVED);
        assertThat(windows.byteAt(50)).isEqualTo(ByteWindows.NOT_OBSERVED);
        assertThat(windows.byteAt(96)).isEqualTo(96);
        assertThat(windows.byteAt(99)).isEqualTo(99);
        assertThat(windows.byteAt(100)).isEqualTo(ByteWindows.NOT_OBSERVED);
    }

    @Test
    @DisplayName("a span straddling the gap is not observable even though both ends are")
    void straddling() {
        ByteWindows windows = ByteWindows.of(ramp(0, 4), ramp(96, 4), 100);
        assertThat(windows.observable(0, 4)).isTrue();
        assertThat(windows.observable(96, 4)).isTrue();
        assertThat(windows.observable(2, 96)).isFalse();
        assertThat(windows.slice(2, 96)).isNull();
    }

    @Test
    @DisplayName("offsets from the end resolve into the absolute space")
    void fromEnd() {
        ByteWindows windows = ByteWindows.of(ramp(0, 4), ramp(96, 4), 100);
        assertThat(windows.fromEnd(1)).isEqualTo(99);
        assertThat(windows.fromEnd(4)).isEqualTo(96);
        assertThat(windows.byteAt(windows.fromEnd(1))).isEqualTo(99);
        assertThat(windows.fromEnd(0)).isEqualTo(ByteWindows.NOT_OBSERVED);
        assertThat(windows.fromEnd(101)).isEqualTo(ByteWindows.NOT_OBSERVED);
    }

    @Test
    @DisplayName("overlapping windows on small content agree")
    void overlap() {
        byte[] content = ramp(0, 6);
        ByteWindows windows = ByteWindows.of(content, content, 6);
        for (int i = 0; i < 6; i++) {
            assertThat(windows.byteAt(i)).isEqualTo(i);
        }
        assertThat(windows.observable(0, 6)).isTrue();
    }

    @Test
    @DisplayName("slicing and matching read out of whichever window holds the span")
    void sliceAndMatch() {
        ByteWindows windows = ByteWindows.of(ramp(0, 4), ramp(96, 4), 100);
        assertThat(windows.slice(96, 4)).isEqualTo(ramp(96, 4));
        assertThat(windows.matchesAt(96, ramp(96, 4))).isTrue();
        assertThat(windows.matchesAt(96, ramp(0, 4))).isFalse();
        assertThat(windows.matchesAt(50, ramp(0, 1))).isFalse();
    }

    @Test
    @DisplayName("empty content and no capture are both empty, not null")
    void empties() {
        assertThat(ByteWindows.EMPTY.isEmpty()).isTrue();
        assertThat(ByteWindows.ofHead(null).isEmpty()).isTrue();
        assertThat(ByteWindows.ofHead(new byte[0]).isEmpty()).isTrue();
        assertThat(ByteWindows.ofWhole(new byte[0]).isEmpty()).isTrue();
        assertThat(ByteWindows.ofWhole(new byte[0]).sizeBytes()).isZero();
    }

    @Test
    @DisplayName("a window longer than the content it windows is refused")
    void refusesImpossibleWindows() {
        assertThatThrownBy(() -> ByteWindows.of(ramp(0, 10), new byte[0], 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be longer");
        assertThatThrownBy(() -> ByteWindows.of(new byte[0], new byte[0], -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("negative");
    }

    @Test
    @DisplayName("negative and zero-length spans behave")
    void degenerate() {
        ByteWindows windows = ByteWindows.ofWhole(ramp(0, 10));
        assertThat(windows.byteAt(-1)).isEqualTo(ByteWindows.NOT_OBSERVED);
        assertThat(windows.observable(-1, 2)).isFalse();
        assertThat(windows.observable(0, -1)).isFalse();
        assertThat(windows.observable(4, 0)).isTrue();
        assertThat(windows.slice(4, 0)).isEqualTo(new byte[0]);
        assertThat(windows.matchesAt(0, null)).isFalse();
    }

    @Test
    @DisplayName("the defaults reach the structures they are sized for")
    void defaults() {
        // The ZIP end-of-central-directory record can sit 22 + 65,535 bytes
        // from the end; the trailing window has to reach past that.
        assertThat(ByteWindows.DEFAULT_TAIL_BYTES).isGreaterThan(22 + 0xFFFF);
        assertThat(ByteWindows.DEFAULT_HEAD_BYTES)
                .isGreaterThanOrEqualTo(Characterizer.PREFIX_BYTES);
        assertThat(Arrays.asList(ByteWindows.DEFAULT_HEAD_BYTES, ByteWindows.DEFAULT_TAIL_BYTES))
                .allSatisfy(size -> assertThat(size).isPositive());
    }
}
