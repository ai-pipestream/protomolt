package ai.protomolt.proto.asset.characterize.signature;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Running the published signature set over an asset's windows. */
class BinaryIdentificationTest {

    private static ByteWindows content(String latin1) {
        return ByteWindows.ofWhole(latin1.getBytes(StandardCharsets.ISO_8859_1));
    }

    private static List<String> formatIds(BinaryIdentification.Result result) {
        return result.hits().stream().map(BinaryIdentification.Hit::formatId).toList();
    }

    @Test
    @DisplayName("the bundled set loads with nothing skipped")
    void bundledSetLoads() {
        BinarySignatures signatures = BinarySignatures.bundled();
        assertThat(signatures.skipped()).isEmpty();
        assertThat(signatures.version()).isNotBlank();
        assertThat(signatures.signatures()).hasSizeGreaterThan(2000);
        assertThat(signatures.formats()).hasSizeGreaterThan(1500);
    }

    @Test
    @DisplayName("every format points at a signature that compiled")
    void formatsResolve() {
        BinarySignatures signatures = BinarySignatures.bundled();
        List<String> dangling = signatures.formats().stream()
                .flatMap(format -> format.signatureIds().stream())
                .filter(id -> !signatures.signatures().containsKey(id))
                .distinct()
                .toList();
        assertThat(dangling).isEmpty();
    }

    @Test
    @DisplayName("a PDF is identified, with the version-specific formats reported")
    void identifiesPdf() {
        BinaryIdentification.Result result = BinaryIdentification.identify(
                content("%PDF-1.4\nbody of the document\n%%EOF\n"));
        assertThat(result.identified()).isTrue();
        assertThat(formatIds(result)).contains("fmt/18");
        assertThat(result.evaluated()).isGreaterThan(1000);
    }

    @Test
    @DisplayName("a GIF needs both the leading magic and the trailer")
    void identifiesGif() {
        // The definition is a pair of byte sequences: the header at the
        // start and the terminator at the end. Only the pair identifies it.
        byte[] header = {'G', 'I', 'F', '8', '9', 'a', 0x01, 0x00, 0x01, 0x00};
        byte[] whole = new byte[header.length + 1];
        System.arraycopy(header, 0, whole, 0, header.length);
        whole[header.length] = 0x3B;
        assertThat(formatIds(BinaryIdentification.identify(ByteWindows.ofWhole(whole))))
                .contains("fmt/4");
        assertThat(formatIds(BinaryIdentification.identify(ByteWindows.ofWhole(header))))
                .doesNotContain("fmt/4");
    }

    @Test
    @DisplayName("an archive is identified even though the registry stops at the layout")
    void identifiesArchive() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream out = new ZipOutputStream(bytes)) {
            out.putNextEntry(new ZipEntry("a.txt"));
            out.write("hello".getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
        BinaryIdentification.Result result =
                BinaryIdentification.identify(ByteWindows.ofWhole(bytes.toByteArray()));
        assertThat(formatIds(result)).contains("x-fmt/263");
    }

    @Test
    @DisplayName("content matching nothing is reported as matching nothing")
    void noMatch() {
        BinaryIdentification.Result result = BinaryIdentification.identify(
                content("just some prose with no format signature in it at all"));
        assertThat(result.identified()).isFalse();
        assertThat(result.evaluated()).isGreaterThan(1000);
    }

    @Test
    @DisplayName("empty content runs nothing")
    void empty() {
        BinaryIdentification.Result result = BinaryIdentification.identify(ByteWindows.EMPTY);
        assertThat(result.identified()).isFalse();
        assertThat(result.evaluated()).isZero();
    }

    @Test
    @DisplayName("signatures anchored to the end are evaluated when a tail was captured")
    void trailingSignaturesNeedTheTail() {
        // With a leading window alone, anything anchored to the end cannot
        // be decided, and the count reports how many that was.
        byte[] whole = "%PDF-1.4\nbody\n%%EOF\n".getBytes(StandardCharsets.ISO_8859_1);
        BinaryIdentification.Result headOnly =
                BinaryIdentification.identify(ByteWindows.ofHead(whole));
        BinaryIdentification.Result both =
                BinaryIdentification.identify(ByteWindows.ofWhole(whole));
        assertThat(headOnly.notEvaluable()).isGreaterThan(both.notEvaluable());
    }

    @Test
    @DisplayName("cost follows the windows, not how much content sits behind them")
    void costFollowsTheWindows() {
        // The property worth pinning is not a stopwatch reading, which a
        // loaded machine can miss for reasons unrelated to this code. It is
        // that identifying a gigabyte and identifying a kilobyte do the
        // same amount of work when the captured windows are the same.
        byte[] head = new byte[16 * 1024];
        System.arraycopy("%PDF-1.7".getBytes(StandardCharsets.ISO_8859_1), 0, head, 0, 8);
        byte[] tail = "trailing bytes %%EOF".getBytes(StandardCharsets.ISO_8859_1);

        BinaryIdentification.Result small =
                BinaryIdentification.identify(ByteWindows.of(head, tail, 1 << 20));
        BinaryIdentification.Result huge =
                BinaryIdentification.identify(ByteWindows.of(head, tail, 1L << 40));

        assertThat(huge.evaluated()).isEqualTo(small.evaluated());
        assertThat(huge.notEvaluable()).isEqualTo(small.notEvaluable());
        assertThat(formatIds(huge)).isEqualTo(formatIds(small));
        assertThat(small.evaluated() + small.notEvaluable())
                .isEqualTo(BinarySignatures.bundled().signatures().size());
    }
}
