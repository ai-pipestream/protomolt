package ai.protomolt.proto.asset.characterize.container;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.asset.characterize.Characterizer;
import ai.protomolt.proto.asset.characterize.WindowCapture;
import ai.protomolt.proto.asset.v1.FormatFact;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The container readers against documents real applications wrote.
 *
 * <p>A hand-built fixture proves a reader agrees with the builder beside
 * it, and if the two share a misunderstanding of the layout they agree and
 * are both wrong. These files came out of Word, Excel and a scanning
 * application, so they settle that question the way a builder cannot.
 */
class RealDocumentsTest {

    private static byte[] document(String name) throws IOException {
        try (InputStream in = RealDocumentsTest.class.getResourceAsStream(name)) {
            assertThat(in).as("the fixture %s is on the test classpath", name).isNotNull();
            return in.readAllBytes();
        }
    }

    private static ByteWindows whole(String name) throws IOException {
        return ByteWindows.ofWhole(document(name));
    }

    /** The same content as the streaming upload path would capture it. */
    private static ByteWindows captured(String name) throws IOException {
        WindowCapture capture = new WindowCapture(new ByteArrayInputStream(document(name)));
        byte[] buffer = new byte[8192];
        while (capture.read(buffer, 0, buffer.length) >= 0) {
            // Drained the way the object store drains it.
        }
        return capture.windows();
    }

    private static List<String> formatIds(ContainerIdentification.Result result) {
        return result.hits().stream().map(ContainerIdentification.Hit::formatId).toList();
    }

    @Test
    @DisplayName("a Word 97 document identifies down to the version")
    void word97() throws IOException {
        // The generic rule needs only the WordDocument stream. The
        // version-specific one reads CompObj, which is short enough to live
        // in the nested filing system, so this is the case that proves the
        // small-member reader against a document Word actually wrote.
        ContainerIdentification.Result result = ContainerIdentification.identify(whole("word97.doc"));
        assertThat(result.kind()).isEqualTo(ContainerKind.OLE2);
        assertThat(result.complete()).isTrue();
        assertThat(formatIds(result)).contains("fmt/40", "fmt/609");

        Ole2Members members = Ole2Members.read(whole("word97.doc"));
        assertThat(members.paths()).contains("WordDocument", "CompObj", "1Table");
        assertThat(members.entry("CompObj").mini()).isTrue();
        assertThat(members.content(members.entry("CompObj"))).isNotEmpty();
    }

    @Test
    @DisplayName("an OOXML document and spreadsheet are told apart")
    void ooxml() throws IOException {
        assertThat(formatIds(ContainerIdentification.identify(whole("word_ooxml.docx"))))
                .contains("fmt/412");
        assertThat(formatIds(ContainerIdentification.identify(whole("Book1.xlsx"))))
                .contains("fmt/214");
        assertThat(Characterizer.identify(whole("word_ooxml.docx"), "word_ooxml.docx")
                .fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.WORD);
        assertThat(Characterizer.identify(whole("Book1.xlsx"), "Book1.xlsx")
                .fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.SPREADSHEET);
    }

    @Test
    @DisplayName("a word processor document named as a spreadsheet is not a spreadsheet")
    void mislabelled() throws IOException {
        // This one is a real mislabelled file, not a contrived one: a Word
        // document carrying an .xlsx name.
        FormatFact fact = Characterizer.identify(whole("docx-file-as-xls.xlsx"),
                "docx-file-as-xls.xlsx").fact();
        assertThat(fact.getFormatCase()).isEqualTo(FormatFact.FormatCase.WORD);
        // The name fits no word processor grammar, so it is left off rather
        // than recorded as though a producer had claimed it.
        assertThat(fact.getWord().getFilename()).isEmpty();
    }

    @Test
    @DisplayName("nested storages come back as paths")
    void nestedStorages() throws IOException {
        Ole2Members members = Ole2Members.read(whole("OmniPagePro18-Sample2.opd"));
        assertThat(members.complete()).isTrue();
        assertThat(members.paths()).contains("Document", "Document/Page1", "Document/Page1/Data");
        assertThat(formatIds(ContainerIdentification.identify(whole("OmniPagePro18-Sample2.opd"))))
                .contains("fmt/1373");
    }

    @Test
    @DisplayName("the identification a real upload would reach through its windows")
    void throughTheCaptureWindows() throws IOException {
        // Reading a whole document into memory is what a test does. The
        // write path sees the windows, so the fixtures are run through a
        // capture as well.
        assertThat(formatIds(ContainerIdentification.identify(captured("word97.doc"))))
                .contains("fmt/40");
        assertThat(formatIds(ContainerIdentification.identify(captured("word_ooxml.docx"))))
                .contains("fmt/412");
        assertThat(formatIds(ContainerIdentification.identify(captured("Book1.xlsx"))))
                .contains("fmt/214");
        assertThat(Characterizer.identify(captured("docx-file-as-xls.xlsx"),
                "docx-file-as-xls.xlsx").fact().getFormatCase())
                .isEqualTo(FormatFact.FormatCase.WORD);
    }

    @Test
    @DisplayName("a document larger than the windows reports what it could not examine")
    void beyondTheWindows() throws IOException {
        // At 245 KB this one does not fit the captured windows, and the
        // honest answer is a partial one rather than a confident absence.
        ByteWindows windows = captured("OmniPagePro18-Sample2.opd");
        assertThat(windows.sizeBytes()).isGreaterThan(
                (long) ByteWindows.DEFAULT_HEAD_BYTES + ByteWindows.DEFAULT_TAIL_BYTES);
        ContainerIdentification.Result result = ContainerIdentification.identify(windows);
        assertThat(result.kind()).isEqualTo(ContainerKind.OLE2);
        if (!result.complete()) {
            assertThat(result.detail()).isNotBlank();
        }
    }
}
