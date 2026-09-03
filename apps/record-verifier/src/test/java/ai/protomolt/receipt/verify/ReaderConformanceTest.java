package ai.protomolt.receipt.verify;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.receipt.verify.Wire.MalformedException;
import ai.protomolt.receipt.verify.Wire.Notes;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The hand-rolled reader is held to one direction of agreement with the standard parser:
 * <strong>it must never accept bytes the standard parser refuses.</strong> That is the
 * security direction. The external verifier exists to reach the runtime's verdict without
 * sharing the runtime's code, so a reader that reads something the runtime cannot is a
 * reader that can vouch for a record nobody else will accept.
 *
 * <p>The other direction is allowed to differ, and does. The standard parser tolerates a
 * tag varint wider than the 32 bits a tag can hold, silently keeping the low bits; this
 * reader refuses it. Being stricter costs nothing, because a record carrying such a tag is
 * refused by the runtime too, one check later, for the unknown field or the non-canonical
 * bytes the truncation implies. {@link #strictnessIsConfinedToOversizedTags()} pins the
 * only place the two part.
 *
 * <p>The corpus cross-check compares verdicts on records shaped like records. This compares
 * the readers themselves, on bytes no encoder would ever produce.
 */
class ReaderConformanceTest {

    /** Whether {@code protobuf-java} accepts the bytes as some message. */
    private static boolean standardAccepts(byte[] data) {
        try {
            UnknownFieldSet.parseFrom(data);
            return true;
        } catch (InvalidProtocolBufferException e) {
            return false;
        }
    }

    /** Whether the hand-rolled reader walks the same bytes to the end. */
    private static boolean readerAccepts(byte[] data) {
        try {
            Wire wire = new Wire(data, new Notes(), "");
            while (wire.hasMore()) {
                int tag = wire.readTag();
                wire.skip(tag & 7, tag >>> 3, "field");
            }
            return true;
        } catch (MalformedException e) {
            return false;
        }
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }

    /** Byte strings chosen to sit on the grammar's edges, each with the reason it is here. */
    static List<org.junit.jupiter.params.provider.Arguments> edgeCases() {
        List<org.junit.jupiter.params.provider.Arguments> cases = new ArrayList<>();
        cases.add(of("empty", new byte[0]));
        cases.add(of("varint field", bytes(0x08, 0x01)));
        cases.add(of("non-minimal varint", bytes(0x08, 0x81, 0x00)));
        cases.add(of("ten-byte varint", bytes(0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF, 0xFF, 0x01)));
        cases.add(of("eleven-byte varint", bytes(0x08, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80, 0x00)));
        cases.add(of("truncated varint", bytes(0x08, 0x80)));
        cases.add(of("field number zero", bytes(0x00, 0x01)));
        cases.add(of("empty length-delimited", bytes(0x0A, 0x00)));
        cases.add(of("length overruns", bytes(0x0A, 0x05, 0x01)));
        cases.add(of("length is negative", bytes(0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF, 0xFF, 0x01)));
        cases.add(of("fixed64", bytes(0x09, 1, 2, 3, 4, 5, 6, 7, 8)));
        cases.add(of("fixed64 truncated", bytes(0x09, 1, 2, 3)));
        cases.add(of("fixed32", bytes(0x0D, 1, 2, 3, 4)));
        cases.add(of("fixed32 truncated", bytes(0x0D, 1, 2)));
        cases.add(of("wire type 6", bytes(0x0E, 0x01)));
        cases.add(of("wire type 7", bytes(0x0F, 0x01)));
        cases.add(of("empty group", bytes(0x0B, 0x0C)));
        cases.add(of("group with a varint inside", bytes(0x0B, 0x08, 0x01, 0x0C)));
        cases.add(of("group closed by another field's end tag", bytes(0x0B, 0x14)));
        cases.add(of("group never closed", bytes(0x0B, 0x08, 0x01)));
        cases.add(of("bare end group", bytes(0x0C)));
        cases.add(of("nested groups", bytes(0x0B, 0x0B, 0x0C, 0x0C)));
        cases.add(of("nested groups crossed", bytes(0x0B, 0x13, 0x0C, 0x14)));
        cases.add(of("high field number", bytes(0xF8, 0xFF, 0xFF, 0xFF, 0x0F, 0x01)));
        cases.add(of("trailing byte after a complete field", bytes(0x08, 0x01, 0x80)));
        return cases;
    }

    private static org.junit.jupiter.params.provider.Arguments of(String name, byte[] data) {
        return org.junit.jupiter.params.provider.Arguments.of(name, data);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("edgeCases")
    void theReaderNeverOutreadsTheStandardParserOnEdgeCases(String name, byte[] data) {
        if (readerAccepts(data)) {
            assertThat(standardAccepts(data))
                    .as("%s: the reader accepted %s and the standard parser did not",
                            name, Arrays.toString(data))
                    .isTrue();
        }
    }

    /**
     * Seeded random bytes, which mostly fail both readers but occasionally stumble onto a
     * valid encoding. The value is in the stumbles: nothing here was chosen by someone who
     * already knew where the reader was weak.
     */
    @Test
    void theReaderNeverOutreadsTheStandardParserOnRandomBytes() {
        Random random = new Random(8979323846264338L);
        assertNeverMoreLenient(random, 20_000, () -> {
            byte[] data = new byte[random.nextInt(24)];
            random.nextBytes(data);
            return data;
        });
    }

    /**
     * Random bytes rarely nest, so this generator only emits tags, keeping the reader in
     * group and length-delimited territory where the two grammars are most likely to part.
     * This is the generator that catches an unbalanced group the reader would close.
     */
    @Test
    void theReaderNeverOutreadsTheStandardParserOnTagSoup() {
        int[] tags = {0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x13, 0x14, 0x00, 0x01, 0x02};
        Random random = new Random(2643383279502884L);
        assertNeverMoreLenient(random, 20_000, () -> {
            byte[] data = new byte[1 + random.nextInt(12)];
            for (int j = 0; j < data.length; j++) {
                data[j] = (byte) tags[random.nextInt(tags.length)];
            }
            return data;
        });
    }

    private static void assertNeverMoreLenient(Random random, int rounds,
                                               java.util.function.Supplier<byte[]> generator) {
        List<String> lenient = new ArrayList<>();
        int agreedAccepted = 0;
        for (int i = 0; i < rounds; i++) {
            byte[] data = generator.get();
            boolean standard = standardAccepts(data);
            boolean reader = readerAccepts(data);
            if (reader && !standard && lenient.size() < 20) {
                lenient.add(Arrays.toString(data));
            }
            if (reader && standard) {
                agreedAccepted++;
            }
        }
        assertThat(lenient).as("bytes the reader accepted and the standard parser refused")
                .isEmpty();
        // Guards the guard: a reader that refused everything would satisfy the invariant
        // above and be useless, so the run has to have read something.
        assertThat(agreedAccepted).as("inputs both readers accepted").isGreaterThan(100);
    }

    /**
     * The one deliberate asymmetry, pinned so it stays deliberate. A tag is a 32-bit value;
     * the standard parser reads its varint and keeps the low 32 bits, so a six-byte tag
     * parses there and refuses here. If some later change makes the reader tolerant of
     * these, this test fails and the choice gets made again on purpose.
     */
    @Test
    void strictnessIsConfinedToOversizedTags() {
        // A tag varint spanning six bytes: field number and wire type survive truncation,
        // the high bits do not.
        byte[] oversized = bytes(0x98, 0xB3, 0x9D, 0xF7, 0xF7, 0x40, 0x36);
        assertThat(standardAccepts(oversized)).isTrue();
        assertThat(readerAccepts(oversized)).isFalse();
    }
}
