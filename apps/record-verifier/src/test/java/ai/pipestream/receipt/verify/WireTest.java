package ai.pipestream.receipt.verify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.receipt.ConformanceCorpus;
import ai.pipestream.receipt.verify.Wire.MalformedException;
import ai.pipestream.receipt.verify.Wire.Notes;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * The reader reads bytes nobody vouched for. Every input here is hostile or malformed on
 * purpose: the contract is that such bytes leave by the declared exception, never by an
 * {@link Error} and never as a silently accepted value. The corpus cross-check covers
 * agreement on well-formed records; this covers what the corpus cannot reach.
 */
class WireTest {

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }

    private static Wire reader(byte[] data, Notes notes) {
        return new Wire(data, notes, "");
    }

    // --- truncation and overflow ------------------------------------------------

    @Test
    void aTruncatedVarintIsMalformed() {
        // 0x80 continues into a byte that is not there.
        Wire wire = reader(bytes(0x08, 0x80), new Notes());
        assertThatThrownBy(() -> {
            wire.readTag();
            wire.readVarint("value");
        }).isInstanceOf(MalformedException.class).hasMessageContaining("truncated");
    }

    @Test
    void aVarintLongerThanTenBytesIsMalformed() {
        byte[] eleven = new byte[12];
        eleven[0] = 0x08;
        Arrays.fill(eleven, 1, 12, (byte) 0x80);
        Wire wire = reader(eleven, new Notes());
        assertThatThrownBy(() -> {
            wire.readTag();
            wire.readVarint("value");
        }).isInstanceOf(MalformedException.class).hasMessageContaining("ten bytes");
    }

    @Test
    void fieldNumberZeroIsMalformed() {
        // Tag 0x00 decodes to field number 0, which the grammar does not allow.
        Wire wire = reader(bytes(0x00), new Notes());
        assertThatThrownBy(wire::readTag)
                .isInstanceOf(MalformedException.class)
                .hasMessageContaining("field number zero");
    }

    @Test
    void aLengthThatOverrunsTheBufferIsMalformed() {
        // Field 1, length-delimited, claims 40 bytes of a 3-byte buffer.
        Wire wire = reader(bytes(0x0A, 0x28, 0x00), new Notes());
        assertThatThrownBy(() -> {
            wire.readTag();
            wire.readLengthDelimited("payload");
        }).isInstanceOf(MalformedException.class).hasMessageContaining("overruns");
    }

    @Test
    void aLengthWithTheSignBitSetIsMalformed() {
        // A ten-byte varint setting bit 63: the length decodes negative.
        byte[] data = bytes(0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01);
        Wire wire = reader(data, new Notes());
        assertThatThrownBy(() -> {
            wire.readTag();
            wire.readLengthDelimited("payload");
        }).isInstanceOf(MalformedException.class).hasMessageContaining("overruns");
    }

    @Test
    void aTruncatedFixedWidthValueIsMalformed() {
        Wire fixed64 = reader(bytes(0x09, 0x01, 0x02), new Notes());
        assertThatThrownBy(() -> {
            fixed64.readTag();
            fixed64.skip(1, 1, "value");
        }).isInstanceOf(MalformedException.class).hasMessageContaining("truncated");

        Wire fixed32 = reader(bytes(0x0D, 0x01), new Notes());
        assertThatThrownBy(() -> {
            fixed32.readTag();
            fixed32.skip(5, 1, "value");
        }).isInstanceOf(MalformedException.class).hasMessageContaining("truncated");
    }

    @Test
    void anUnmatchedGroupEndIsMalformed() {
        Wire wire = reader(bytes(0x0C), new Notes());
        assertThatThrownBy(() -> {
            int tag = wire.readTag();
            wire.skip(tag & 7, tag >>> 3, "value");
        }).isInstanceOf(MalformedException.class).hasMessageContaining("unmatched group end");
    }

    @Test
    void wireTypesSixAndSevenAreMalformed() {
        for (int wireType : new int[] {6, 7}) {
            Wire wire = reader(bytes(0x08 | wireType), new Notes());
            assertThatThrownBy(() -> {
                wire.readTag();
                wire.skip(wireType, 1, "value");
            }).isInstanceOf(MalformedException.class)
                    .hasMessageContaining("wire type " + wireType);
        }
    }

    // --- the group-nesting bound ------------------------------------------------

    /**
     * A group opens a new nesting level, and nothing in the byte stream costs more than the
     * one byte {@code 0x0B} to open another. Without a bound, a short hostile input walks
     * the reader off the stack, and a {@link StackOverflowError} is not a refusal: it is an
     * unhandled crash in the one tool whose entire job is surviving bytes it was handed.
     */
    @Test
    void deeplyNestedGroupsRefuseInsteadOfExhaustingTheStack() {
        byte[] nested = new byte[200_000];
        Arrays.fill(nested, (byte) 0x0B); // field 1, start-group, forever

        assertThatThrownBy(() -> RecordWire.signed(nested, new Notes()))
                .isInstanceOf(MalformedException.class)
                .hasMessageContaining("nested too deeply");
    }

    @Test
    void theNestingBoundIsNotSoTightThatHonestGroupsBreak() {
        // Ten levels of properly closed groups: deep enough to prove the counter unwinds,
        // shallow enough that no real encoder would ever exceed it.
        int depth = 10;
        byte[] data = new byte[depth * 2];
        Arrays.fill(data, 0, depth, (byte) 0x0B);
        Arrays.fill(data, depth, depth * 2, (byte) 0x0C);
        Wire wire = reader(data, new Notes());
        assertThatCode(() -> {
            int tag = wire.readTag();
            wire.skip(tag & 7, tag >>> 3, "value");
        }).doesNotThrowAnyException();
    }

    @Test
    void theDepthCounterUnwindsSoSiblingGroupsAreNotCharged() {
        // The same group, opened and closed repeatedly at the top level. If the reader
        // counted opens instead of depth, this would refuse.
        int siblings = 5_000;
        byte[] data = new byte[siblings * 2];
        for (int i = 0; i < siblings; i++) {
            data[i * 2] = 0x0B;
            data[i * 2 + 1] = 0x0C;
        }
        Wire wire = reader(data, new Notes());
        assertThatCode(() -> {
            while (wire.hasMore()) {
                int tag = wire.readTag();
                wire.skip(tag & 7, tag >>> 3, "value");
            }
        }).doesNotThrowAnyException();
    }

    /**
     * The property that matters to a caller: hostile bytes come back as a verdict, not as a
     * thrown {@link Error} through the public entry point.
     */
    @Test
    void theVerifierRefusesHostileNestingRatherThanCrashing() {
        // Well inside MAX_RECORD_BYTES, so the size bound does not answer for the parser.
        byte[] nested = new byte[200_000];
        Arrays.fill(nested, (byte) 0x0B);
        byte[] trust = ConformanceCorpus.trust().toByteArray();

        ExternalVerifier.Result result = ExternalVerifier.verify(nested, trust);

        assertThat(result.verified()).isFalse();
        assertThat(result.refusal().id()).isEqualTo(ExternalVerifier.CHECK_CONTAINER_BOUNDS);
    }

    // --- UTF-8 ------------------------------------------------------------------

    @Test
    void invalidUtf8IsMalformed() {
        // Field 1, length 1, a lone 0xFF: not a UTF-8 sequence in any position.
        Wire wire = reader(bytes(0x0A, 0x01, 0xFF), new Notes());
        assertThatThrownBy(() -> {
            wire.readTag();
            wire.readLengthDelimited("name").utf8("name");
        }).isInstanceOf(MalformedException.class).hasMessageContaining("not valid UTF-8");
    }

    @Test
    void aTruncatedMultiByteSequenceIsMalformed() {
        // 0xE2 0x82 opens a three-byte sequence and stops one byte short.
        Wire wire = reader(bytes(0x0A, 0x02, 0xE2, 0x82), new Notes());
        assertThatThrownBy(() -> {
            wire.readTag();
            wire.readLengthDelimited("name").utf8("name");
        }).isInstanceOf(MalformedException.class).hasMessageContaining("not valid UTF-8");
    }

    @Test
    void anHonestReplacementCharacterSurvivesTheRoundTrip() {
        // U+FFFD is what invalid input decodes to, so a string that legitimately contains
        // it must not be mistaken for corruption.
        byte[] payload = "�".getBytes(StandardCharsets.UTF_8);
        byte[] data = bytes(0x0A, payload.length, payload[0], payload[1], payload[2]);
        Wire wire = reader(data, new Notes());
        assertThatCode(() -> {
            wire.readTag();
            assertThat(wire.readLengthDelimited("name").utf8("name")).isEqualTo("�");
        }).doesNotThrowAnyException();
    }

    @Test
    void astralPlaneTextDecodes() {
        byte[] payload = "📄".getBytes(StandardCharsets.UTF_8);
        byte[] data = new byte[payload.length + 2];
        data[0] = 0x0A;
        data[1] = (byte) payload.length;
        System.arraycopy(payload, 0, data, 2, payload.length);
        Wire wire = reader(data, new Notes());
        assertThatCode(() -> {
            wire.readTag();
            assertThat(wire.readLengthDelimited("name").utf8("name")).isEqualTo("📄");
        }).doesNotThrowAnyException();
    }

    // --- canonicality accounting ------------------------------------------------

    @Test
    void aNonMinimalVarintIsNoted() throws Exception {
        Notes notes = new Notes();
        Wire wire = reader(bytes(0x08, 0x81, 0x00), notes);
        wire.readTag();
        assertThat(wire.readVarint("version")).isEqualTo(1);
        assertThat(notes.nonCanonical).containsExactly("version uses a non-minimal varint");
    }

    @Test
    void aMinimalVarintIsNotNoted() throws Exception {
        Notes notes = new Notes();
        Wire wire = reader(bytes(0x08, 0x01), notes);
        wire.readTag();
        assertThat(wire.readVarint("version")).isEqualTo(1);
        assertThat(notes.nonCanonical).isEmpty();
    }

    @Test
    void aDescendingFieldNumberIsNoted() {
        Notes notes = new Notes();
        Wire wire = reader(new byte[0], notes);
        wire.ordered(3);
        wire.ordered(1);
        assertThat(notes.nonCanonical)
                .containsExactly("field 1 appears after field 3");
    }

    @Test
    void anAscendingRunIsNotNoted() {
        Notes notes = new Notes();
        Wire wire = reader(new byte[0], notes);
        wire.ordered(1);
        wire.ordered(2);
        wire.ordered(2);
        wire.ordered(7);
        assertThat(notes.nonCanonical).isEmpty();
    }

    @Test
    void notesCarryThePathOfTheSliceTheyCameFrom() throws Exception {
        Notes notes = new Notes();
        Wire wire = reader(bytes(0x0A, 0x03, 0x08, 0x81, 0x00), notes);
        wire.readTag();
        Wire slice = wire.readLengthDelimited("subject");
        slice.readTag();
        slice.readVarint("kind");
        assertThat(notes.nonCanonical)
                .containsExactly("subject: kind uses a non-minimal varint");
    }

    @Test
    void unknownFieldsAreRecordedByPath() throws Exception {
        Notes notes = new Notes();
        Wire wire = reader(bytes(0x0A, 0x02, 0x08, 0x01), notes);
        wire.readTag();
        Wire slice = wire.readLengthDelimited("subject");
        slice.noteUnknown(9);
        assertThat(notes.unknownFields).containsExactly("subject.9");
    }

    // --- slice discipline -------------------------------------------------------

    @Test
    void aSliceCannotReadPastItsOwnLimit() throws Exception {
        // Outer: field 1 length 1 holding 0x08, then a stray byte the slice must not see.
        Notes notes = new Notes();
        Wire wire = reader(bytes(0x0A, 0x01, 0x08, 0x7F), notes);
        wire.readTag();
        Wire slice = wire.readLengthDelimited("inner");
        assertThat(slice.bytes()).containsExactly(0x08);
        assertThat(slice.hasMore()).isFalse();
        assertThat(wire.hasMore()).isTrue();
    }

    @Test
    void requireExhaustedRefusesTrailingBytes() throws Exception {
        Wire wire = reader(bytes(0x0A, 0x01, 0x08, 0x7F), new Notes());
        wire.readTag();
        wire.readLengthDelimited("inner");
        assertThatThrownBy(wire::requireExhausted)
                .isInstanceOf(MalformedException.class)
                .hasMessageContaining("trailing bytes");
    }

    @Test
    void requireExhaustedAcceptsAFullyReadBuffer() throws Exception {
        Wire wire = reader(bytes(0x0A, 0x01, 0x08), new Notes());
        wire.readTag();
        wire.readLengthDelimited("inner").bytes();
        assertThatCode(wire::requireExhausted).doesNotThrowAnyException();
    }

    @Test
    void anEmptyBufferHasNothingToRead() {
        Wire wire = reader(new byte[0], new Notes());
        assertThat(wire.hasMore()).isFalse();
    }
}
