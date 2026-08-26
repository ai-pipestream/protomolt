package ai.pipestream.proto.asset.characterize;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.asset.characterize.ContentTypeSniffer.Sniff;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Pins the magic-byte table and, above all, the honesty contract: a sniffed
 * answer only when the bytes really said so, the declared type as fallback,
 * octet-stream as the last resort.
 */
class ContentTypeSnifferTest {

    private static Sniff sniff(byte[] head, String filename) {
        return ContentTypeSniffer.sniff(head, filename);
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }

    @Test
    void pdfMagic() {
        assertThat(sniff("%PDF-1.7 rest".getBytes(), "whatever.bin"))
                .isEqualTo(new Sniff("application/pdf", true));
    }

    @Test
    void zipDispatchesOnExtension() {
        byte[] zip = bytes('P', 'K', 0x03, 0x04, 0, 0, 0, 0);
        assertThat(sniff(zip, "report.docx").mimeType())
                .isEqualTo("application/vnd.openxmlformats-officedocument.wordprocessingml.document");
        assertThat(sniff(zip, "sheet.XLSX").mimeType())
                .isEqualTo("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        assertThat(sniff(zip, "deck.pptx").mimeType())
                .isEqualTo("application/vnd.openxmlformats-officedocument.presentationml.presentation");
        assertThat(sniff(zip, "book.epub").mimeType()).isEqualTo("application/epub+zip");
        assertThat(sniff(zip, "archive.zip")).isEqualTo(new Sniff("application/zip", true));
        assertThat(sniff(zip, "")).isEqualTo(new Sniff("application/zip", true));
    }

    @Test
    void legacyOfficeIsOleStorage() {
        assertThat(sniff(bytes(0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1), "old.doc"))
                .isEqualTo(new Sniff("application/x-ole-storage", true));
    }

    @Test
    void markupSplitsHtmlFromXml() {
        assertThat(sniff("<?xml version=\"1.0\"?><catalog/>".getBytes(), "c.xml"))
                .isEqualTo(new Sniff("application/xml", true));
        assertThat(sniff("<?xml version=\"1.0\"?><html><body/></html>".getBytes(), "p.html"))
                .isEqualTo(new Sniff("text/html", true));
        assertThat(sniff("  \n\t<!DOCTYPE html><HTML><head/>".getBytes(), "p.html"))
                .isEqualTo(new Sniff("text/html", true));
    }

    @Test
    void imageMagics() {
        assertThat(sniff(bytes(0x89, 'P', 'N', 'G', 0x0D, 0x0A), "x").mimeType())
                .isEqualTo("image/png");
        assertThat(sniff(bytes(0xFF, 0xD8, 0xFF, 0xE0), "x").mimeType()).isEqualTo("image/jpeg");
        assertThat(sniff("GIF89a....".getBytes(), "x").mimeType()).isEqualTo("image/gif");
        assertThat(sniff("RIFF....WEBPVP8 ".getBytes(StandardCharsets.ISO_8859_1), "x").mimeType())
                .isEqualTo("image/webp");
        assertThat(sniff(bytes('I', 'I', '*', 0, 8, 0), "x").mimeType()).isEqualTo("image/tiff");
        assertThat(sniff(bytes('M', 'M', 0, '*', 0, 8), "x").mimeType()).isEqualTo("image/tiff");
    }

    @Test
    void audioAndVideoMagics() {
        assertThat(sniff("RIFF....WAVEfmt ".getBytes(StandardCharsets.ISO_8859_1), "x").mimeType())
                .isEqualTo("audio/wav");
        assertThat(sniff("ID3tag".getBytes(StandardCharsets.ISO_8859_1), "x").mimeType())
                .isEqualTo("audio/mpeg");
        assertThat(sniff(bytes(0xFF, 0xFB, 0x90, 0x00), "x").mimeType()).isEqualTo("audio/mpeg");
        assertThat(sniff(bytes(0, 0, 0, 0x18, 'f', 't', 'y', 'p', 'm', 'p', '4', '2'), "x").mimeType())
                .isEqualTo("video/mp4");
    }

    @Test
    void gzipMagic() {
        assertThat(sniff(bytes(0x1F, 0x8B, 0x08, 0x00), "x.gz"))
                .isEqualTo(new Sniff("application/gzip", true));
    }

    @Test
    void mostlyPrintableUtf8IsTextPlain() {
        assertThat(sniff("plain ascii text with\nnewlines and tabs\t".getBytes(), "note.txt"))
                .isEqualTo(new Sniff("text/plain", true));
        assertThat(sniff("umlauts äöü und mehr".getBytes(StandardCharsets.UTF_8), ""))
                .isEqualTo(new Sniff("text/plain", true));
    }

    @Test
    void inconclusiveBytesFallBackToTheDeclaredType() {
        byte[] opaque = bytes(0x00, 0x01, 0x02, 0x03, 0x04);
        assertThat(sniff(opaque, "mystery.bin"))
                .isEqualTo(new Sniff(ContentTypeSniffer.OCTET_STREAM, false));
        assertThat(ContentTypeSniffer.resolve(opaque, "mystery.bin", "application/x-custom"))
                .isEqualTo(new Sniff("application/x-custom", false));
        // A sniffed answer is never overridden by the declaration.
        assertThat(ContentTypeSniffer.resolve("%PDF-1.4".getBytes(), "f.pdf", "text/plain"))
                .isEqualTo(new Sniff("application/pdf", true));
    }

    @Test
    void octetStreamIsTheLastResort() {
        assertThat(ContentTypeSniffer.resolve(new byte[0], "", ""))
                .isEqualTo(new Sniff(ContentTypeSniffer.OCTET_STREAM, false));
        assertThat(ContentTypeSniffer.resolve(bytes(0x00, 0x01), "", null))
                .isEqualTo(new Sniff(ContentTypeSniffer.OCTET_STREAM, false));
    }

    @Test
    void extensionOfLowercasesAndHandlesTheEdgeCases() {
        assertThat(ContentTypeSniffer.extensionOf("Report.DOCX")).isEqualTo("docx");
        assertThat(ContentTypeSniffer.extensionOf("noext")).isEmpty();
        assertThat(ContentTypeSniffer.extensionOf("trailing.")).isEmpty();
        assertThat(ContentTypeSniffer.extensionOf(null)).isEmpty();
        assertThat(ContentTypeSniffer.extensionOf("archive.tar.gz")).isEqualTo("gz");
    }
}
