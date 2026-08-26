package ai.pipestream.proto.asset.characterize;

import ai.pipestream.proto.asset.v1.FormatFact;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The identifier's verdicts on real signatures, and the property the whole
 * layer rests on: every fact identification produces is valid against the
 * contract's own rules — the identifier can never conclude something the
 * validator would refuse to store.
 */
class CharacterizerTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static byte[] tarHead(String memberName) {
        byte[] head = new byte[512];
        byte[] name = memberName.getBytes(StandardCharsets.ISO_8859_1);
        System.arraycopy(name, 0, head, 0, name.length);
        byte[] magic = "ustar".getBytes(StandardCharsets.ISO_8859_1);
        System.arraycopy(magic, 0, head, 257, magic.length);
        return head;
    }

    private static byte[] gzipBytes() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write("payload".getBytes(StandardCharsets.UTF_8));
        }
        return out.toByteArray();
    }

    @Test
    void tarMagicIdentifiesATarWithItsGrammarMatchingName() {
        Characterizer.Identification id = Characterizer.identify(tarHead("member"), "docs.tar");
        assertThat(id.identified()).isTrue();
        assertThat(id.fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.TAR);
        assertThat(id.fact().getTar().getFilename()).isEqualTo("docs.tar");
    }

    @Test
    void gzipBytesWithATarNameConcludeACompressedTar() throws Exception {
        Characterizer.Identification id = Characterizer.identify(gzipBytes(), "docs.tar.gz");
        assertThat(id.fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.TAR);
        assertThat(id.fact().getTar().getFilename()).isEqualTo("docs.tar.gz");
    }

    @Test
    void gzipBytesWithAPlainGzNameStayAGzipFile() throws Exception {
        Characterizer.Identification id = Characterizer.identify(gzipBytes(), "dump.sql.gz");
        assertThat(id.fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.GZIP);
        assertThat(id.fact().getGzip().getFilename()).isEqualTo("dump.sql.gz");
    }

    @Test
    void zipMagicDispatchesOnTheOoxmlExtension() {
        byte[] zip = {'P', 'K', 0x03, 0x04, 0, 0, 0, 0};
        assertThat(Characterizer.identify(zip, "report.docx").fact().getFormatCase())
                .isEqualTo(FormatFact.FormatCase.WORD);
        assertThat(Characterizer.identify(zip, "numbers.xlsx").fact().getFormatCase())
                .isEqualTo(FormatFact.FormatCase.SPREADSHEET);
        assertThat(Characterizer.identify(zip, "bundle.zip").fact().getFormatCase())
                .isEqualTo(FormatFact.FormatCase.ZIP);
    }

    @Test
    void aContradictingNameIsNotEndorsedByTheIdentifiedFact() {
        byte[] pdf = "%PDF-1.7 rest".getBytes(StandardCharsets.ISO_8859_1);
        Characterizer.Identification id = Characterizer.identify(pdf, "notes.txt");
        assertThat(id.fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.PDF);
        // Bytes prove the format; the fact does not carry a name that
        // contradicts it.
        assertThat(id.fact().getPdf().getFilename()).isEmpty();
    }

    @Test
    void aDelimitedNameConcludesNothingBecauseTheClaimNeedsParameters() {
        byte[] csv = "a,b,c\n1,2,3\n".getBytes(StandardCharsets.UTF_8);
        Characterizer.Identification id = Characterizer.identify(csv, "rows.csv");
        assertThat(id.identified()).isFalse();
        assertThat(id.evidence()).extracting(e -> e.getSignal())
                .contains("probe", "extension");
    }

    @Test
    void jsonBytesWithAJsonNameProbeAsAJsonDocument() {
        byte[] json = "  {\"a\": 1}".getBytes(StandardCharsets.UTF_8);
        Characterizer.Identification id = Characterizer.identify(json, "config.json");
        assertThat(id.fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.JSON);
    }

    @Test
    void unknownBinaryConcludesNothingAndSaysSo() {
        byte[] noise = {0x00, 0x01, 0x02, 0x03};
        Characterizer.Identification id = Characterizer.identify(noise, "blob.bin");
        assertThat(id.identified()).isFalse();
    }

    @Test
    void everyIdentifiedFactValidatesAgainstTheContractsOwnRules() throws Exception {
        List<Characterizer.Identification> verdicts = List.of(
                Characterizer.identify(tarHead("m"), "a.tar"),
                Characterizer.identify(tarHead("m"), "wrong.zip"),
                Characterizer.identify(gzipBytes(), "a.tgz"),
                Characterizer.identify(gzipBytes(), "a.gz"),
                Characterizer.identify(gzipBytes(), null),
                Characterizer.identify(new byte[] {'P', 'K', 0x03, 0x04}, "d.docx"),
                Characterizer.identify(new byte[] {'P', 'K', 0x03, 0x04}, "d.pptx"),
                Characterizer.identify(new byte[] {'P', 'K', 0x03, 0x04}, "weird.name"),
                Characterizer.identify("%PDF-1.4".getBytes(StandardCharsets.ISO_8859_1), "x.txt"),
                Characterizer.identify("# Title".getBytes(StandardCharsets.UTF_8), "readme.md"),
                Characterizer.identify("k: v".getBytes(StandardCharsets.UTF_8), "cfg.yaml"),
                Characterizer.identify("{\"x\":1}".getBytes(StandardCharsets.UTF_8), "x.json"),
                Characterizer.identify("plain words".getBytes(StandardCharsets.UTF_8), "note.txt"),
                Characterizer.identify("plain words".getBytes(StandardCharsets.UTF_8), "no-ext"),
                Characterizer.identify(new byte[] {(byte) 0x89, 'P', 'N', 'G'}, "pic.png"),
                Characterizer.identify(new byte[] {(byte) 0x89, 'P', 'N', 'G'}, "pic.tar"));
        for (Characterizer.Identification verdict : verdicts) {
            if (verdict.identified()) {
                ValidationResult result = VALIDATOR.validate(verdict.fact());
                assertThat(result.valid())
                        .as(verdict.fact() + " -> " + result.violations())
                        .isTrue();
            }
        }
    }
}
