package ai.protomolt.proto.sources;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoSourceTest {

    @Test
    void ofSetsTheUnspecifiedOrigin() {
        ProtoSource source = ProtoSource.of("a/b.proto", "syntax = \"proto3\";");
        assertThat(source.path()).isEqualTo("a/b.proto");
        assertThat(source.content()).isEqualTo("syntax = \"proto3\";");
        assertThat(source.origin()).isEqualTo("unspecified");
    }

    @Test
    void nullComponentsAreRejected() {
        assertThatNullPointerException()
                .isThrownBy(() -> new ProtoSource(null, "content", "origin"));
        assertThatNullPointerException()
                .isThrownBy(() -> new ProtoSource("a.proto", null, "origin"));
        assertThatNullPointerException()
                .isThrownBy(() -> new ProtoSource("a.proto", "content", null));
    }

    @Test
    void blankPathsAreRejected() {
        assertThatThrownBy(() -> new ProtoSource("", "content", "origin"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
        assertThatThrownBy(() -> new ProtoSource("   ", "content", "origin"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
    }

    @Test
    void emptyContentIsAllowed() {
        // Only the path is constrained; an empty file is a valid (if useless) source.
        assertThat(ProtoSource.of("empty.proto", "").content()).isEmpty();
    }

    @Test
    void recordEqualityIsByValue() {
        assertThat(new ProtoSource("a.proto", "x", "o1"))
                .isEqualTo(new ProtoSource("a.proto", "x", "o1"))
                .isNotEqualTo(new ProtoSource("a.proto", "x", "o2"))
                .isNotEqualTo(new ProtoSource("a.proto", "y", "o1"))
                .isNotEqualTo(new ProtoSource("b.proto", "x", "o1"));
    }
}
