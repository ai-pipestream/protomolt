package ai.protomolt.proto.asset.characterize.signature;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.asset.characterize.signature.SignatureSequence.Anchor;
import ai.protomolt.proto.asset.characterize.signature.SignatureSequence.Fragment;
import ai.protomolt.proto.asset.characterize.signature.SignatureSequence.Outcome;
import ai.protomolt.proto.asset.characterize.signature.SignatureSequence.Subsequence;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Placing a signature's pieces relative to an anchor. */
class SignatureSequenceTest {

    private static ByteWindows content(String latin1) {
        return ByteWindows.ofWhole(latin1.getBytes(StandardCharsets.ISO_8859_1));
    }

    private static SignatureExpression pattern(String expression) {
        try {
            return SignatureExpression.compile(expression);
        } catch (SignatureSyntaxException wrong) {
            throw new AssertionError(wrong);
        }
    }

    private static Subsequence piece(int minOffset, int maxOffset, String expression) {
        return new Subsequence(minOffset, maxOffset, pattern(expression), List.of(), List.of());
    }

    private static SignatureSequence leading(Subsequence... pieces) {
        return new SignatureSequence(Anchor.LEADING, List.of(pieces));
    }

    @Test
    @DisplayName("one piece at a fixed distance from the start")
    void anchoredAtTheStart() {
        SignatureSequence signature = leading(piece(0, 0, "'PK' 03 04"));
        assertThat(signature.matchIn(content("PKrest"))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("xPK"))).isEqualTo(Outcome.NOT_MATCHED);
    }

    @Test
    @DisplayName("a distance range searches the span it allows and no further")
    void distanceRange() {
        SignatureSequence signature = leading(piece(0, 8, "'MARK'"));
        assertThat(signature.matchIn(content("....MARK...."))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("........MARK"))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content(".........MARK"))).isEqualTo(Outcome.NOT_MATCHED);
    }

    @Test
    @DisplayName("later pieces are measured from the end of the piece before")
    void chainedPieces() {
        SignatureSequence signature = leading(
                piece(0, 0, "'HEAD'"),
                piece(2, 4, "'TAIL'"));
        assertThat(signature.matchIn(content("HEAD..TAIL"))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("HEAD....TAIL"))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("HEAD.TAIL"))).isEqualTo(Outcome.NOT_MATCHED);
        assertThat(signature.matchIn(content("HEAD.....TAIL"))).isEqualTo(Outcome.NOT_MATCHED);
    }

    @Test
    @DisplayName("a piece is measured from the end when the anchor trails")
    void anchoredAtTheEnd() {
        SignatureSequence signature = new SignatureSequence(Anchor.TRAILING,
                List.of(piece(0, 0, "'PAR1'")));
        assertThat(signature.matchIn(content("PAR1 middle PAR1"))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("PAR1 middle x"))).isEqualTo(Outcome.NOT_MATCHED);

        SignatureSequence backOffFour = new SignatureSequence(Anchor.TRAILING,
                List.of(piece(4, 4, "'STOP'")));
        assertThat(backOffFour.matchIn(content("junk STOPabcd"))).isEqualTo(Outcome.MATCHED);
        assertThat(backOffFour.matchIn(content("junk STOPabc"))).isEqualTo(Outcome.NOT_MATCHED);
    }

    @Test
    @DisplayName("an unanchored piece is searched for in the resident spans")
    void unanchored() {
        SignatureSequence signature = new SignatureSequence(Anchor.ANYWHERE,
                List.of(piece(0, 0, "'NEEDLE'")));
        assertThat(signature.matchIn(content("a long stretch with a NEEDLE inside")))
                .isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("a long stretch with no marker inside")))
                .isEqualTo(Outcome.NOT_MATCHED);
    }

    @Test
    @DisplayName("an unanchored search over unread content says so, without walking it")
    void unanchoredStaysResident() {
        // Most of a gigabyte was not captured, so the pattern could be in
        // there and absence cannot be concluded. The point is that the
        // answer arrives from the resident spans alone: no walk over a
        // billion offsets that carry no bytes.
        ByteWindows sparse = ByteWindows.of(
                "HEAD".getBytes(StandardCharsets.ISO_8859_1),
                "TAIL".getBytes(StandardCharsets.ISO_8859_1), 1_000_000_000L);
        SignatureSequence signature = new SignatureSequence(Anchor.ANYWHERE,
                List.of(piece(0, 0, "'NEEDLE'")));
        long started = System.nanoTime();
        Outcome outcome = signature.matchIn(sparse);
        long tookMillis = (System.nanoTime() - started) / 1_000_000;
        assertThat(outcome).isEqualTo(Outcome.NOT_EVALUABLE);
        assertThat(tookMillis).isLessThan(1_000);
    }

    @Test
    @DisplayName("an unanchored search over content read whole can conclude absence")
    void unanchoredOverWholeContent() {
        SignatureSequence signature = new SignatureSequence(Anchor.ANYWHERE,
                List.of(piece(0, 0, "'NEEDLE'")));
        assertThat(signature.matchIn(content("every byte of this is resident")))
                .isEqualTo(Outcome.NOT_MATCHED);
    }

    @Test
    @DisplayName("a fragment sits after its piece at the stated distance")
    void trailingFragment() {
        Subsequence withFragment = new Subsequence(0, 0, pattern("'HEAD'"),
                List.of(),
                List.of(new Fragment(1, 2, 4, pattern("'X'"))));
        SignatureSequence signature = leading(withFragment);
        assertThat(signature.matchIn(content("HEAD..X"))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("HEAD....X"))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("HEAD.X"))).isEqualTo(Outcome.NOT_MATCHED);
        assertThat(signature.matchIn(content("HEAD.....X"))).isEqualTo(Outcome.NOT_MATCHED);
    }

    @Test
    @DisplayName("a fragment sits before its piece at the stated distance")
    void leadingFragment() {
        Subsequence withFragment = new Subsequence(0, 8, pattern("'BODY'"),
                List.of(new Fragment(1, 1, 1, pattern("'Q'"))),
                List.of());
        SignatureSequence signature = leading(withFragment);
        assertThat(signature.matchIn(content("..Q.BODY"))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("..QBODY"))).isEqualTo(Outcome.NOT_MATCHED);
    }

    @Test
    @DisplayName("fragments at one position are alternatives of each other")
    void fragmentAlternatives() {
        Subsequence withFragment = new Subsequence(0, 0, pattern("'HEAD'"),
                List.of(),
                List.of(new Fragment(1, 0, 0, pattern("'A'")),
                        new Fragment(1, 0, 0, pattern("'B'"))));
        SignatureSequence signature = leading(withFragment);
        assertThat(signature.matchIn(content("HEADA"))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("HEADB"))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("HEADC"))).isEqualTo(Outcome.NOT_MATCHED);
    }

    @Test
    @DisplayName("fragments at increasing positions chain outward")
    void chainedFragments() {
        Subsequence withFragments = new Subsequence(0, 0, pattern("'HEAD'"),
                List.of(),
                List.of(new Fragment(1, 0, 0, pattern("'A'")),
                        new Fragment(2, 1, 1, pattern("'B'"))));
        SignatureSequence signature = leading(withFragments);
        assertThat(signature.matchIn(content("HEADA.B"))).isEqualTo(Outcome.MATCHED);
        assertThat(signature.matchIn(content("HEADAB"))).isEqualTo(Outcome.NOT_MATCHED);
    }

    @Test
    @DisplayName("reaching into the unread span is not evaluable, not a non-match")
    void blindSpot() {
        ByteWindows sparse = ByteWindows.of(
                "HEAD".getBytes(StandardCharsets.ISO_8859_1),
                "TAIL".getBytes(StandardCharsets.ISO_8859_1), 4096);
        SignatureSequence reachesTheMiddle = leading(piece(2000, 2000, "'MARK'"));
        assertThat(reachesTheMiddle.matchIn(sparse)).isEqualTo(Outcome.NOT_EVALUABLE);
    }

    @Test
    @DisplayName("running off the end of the content is a non-match, not a blind spot")
    void shortContent() {
        // The content genuinely stops; there is no unread span to blame.
        SignatureSequence signature = leading(piece(0, 0, "'A VERY LONG MARKER'"));
        assertThat(signature.matchIn(content("short"))).isEqualTo(Outcome.NOT_MATCHED);
    }

    @Test
    @DisplayName("a trailing anchor needs to know how long the content is")
    void trailingNeedsSize() {
        SignatureSequence signature = new SignatureSequence(Anchor.TRAILING,
                List.of(piece(0, 0, "'END'")));
        assertThat(signature.matchIn(ByteWindows.ofHead("END".getBytes(StandardCharsets.ISO_8859_1))))
                .isEqualTo(Outcome.NOT_EVALUABLE);
    }

    @Test
    @DisplayName("an unbounded pattern is not evaluated at all")
    void unboundedIsNotEvaluated() {
        SignatureSequence signature = leading(piece(0, 0, "'A' * 'B'"));
        assertThat(signature.bounded()).isFalse();
        assertThat(signature.matchIn(content("A....B"))).isEqualTo(Outcome.NOT_EVALUABLE);
    }

    @Test
    @DisplayName("a signature with no pieces is refused")
    void refusesEmpty() {
        assertThatThrownBy(() -> new SignatureSequence(Anchor.LEADING, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one subsequence");
        assertThatThrownBy(() -> new Subsequence(4, 2, pattern("00"), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative range");
    }
}
