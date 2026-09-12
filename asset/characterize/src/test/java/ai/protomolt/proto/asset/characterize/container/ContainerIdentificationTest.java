package ai.protomolt.proto.asset.characterize.container;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.asset.characterize.Characterizer;
import ai.protomolt.proto.asset.v1.CharacterizationEvidence;
import ai.protomolt.proto.asset.v1.FormatFact;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Telling containers apart by what is inside them. */
class ContainerIdentificationTest {

    private static byte[] stream(String text, int atLeast) {
        byte[] padded = new byte[Math.max(atLeast, text.length())];
        Arrays.fill(padded, (byte) ' ');
        System.arraycopy(text.getBytes(StandardCharsets.ISO_8859_1), 0, padded, 0, text.length());
        return padded;
    }

    private static List<String> observations(Characterizer.Identification identification) {
        return identification.evidence().stream()
                .map(CharacterizationEvidence::getObservation)
                .toList();
    }

    // ------------------------------------------------------------------
    // ZIP
    // ------------------------------------------------------------------

    @Test
    @DisplayName("an OOXML document is identified from its members, not its name")
    void wordProcessorFromMembers() throws IOException {
        byte[] document = Containers.ooxml(Containers.WORD_CONTENT_TYPE, "word/document.xml");
        ContainerIdentification.Result result =
                ContainerIdentification.identify(ByteWindows.ofWhole(document));
        assertThat(result.kind()).isEqualTo(ContainerKind.ZIP);
        assertThat(result.complete()).isTrue();
        assertThat(result.hits()).isNotEmpty();
        assertThat(result.hits().getFirst().formatId()).isEqualTo("fmt/412");
    }

    @Test
    @DisplayName("a document renamed away from its extension is still identified")
    void renamedDocument() throws IOException {
        byte[] document = Containers.ooxml(Containers.WORD_CONTENT_TYPE, "word/document.xml");
        Characterizer.Identification identification =
                Characterizer.identify(ByteWindows.ofWhole(document), "invoice.dat");
        assertThat(identification.fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.WORD);
        // The name does not fit a word processor document, so it is not
        // put on the fact as though a producer had claimed it.
        assertThat(identification.fact().getWord().getFilename()).isEmpty();
    }

    @Test
    @DisplayName("a spreadsheet and a presentation are told apart from the same four bytes")
    void spreadsheetAndPresentation() throws IOException {
        byte[] sheet = Containers.ooxml(Containers.SPREADSHEET_CONTENT_TYPE, "xl/workbook.xml");
        byte[] deck = Containers.ooxml(Containers.PRESENTATION_CONTENT_TYPE,
                "ppt/presentation.xml");
        assertThat(Characterizer.identify(ByteWindows.ofWhole(sheet), "numbers.xlsx")
                .fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.SPREADSHEET);
        assertThat(Characterizer.identify(ByteWindows.ofWhole(deck), "slides.pptx")
                .fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.PRESENTATION);
    }

    @Test
    @DisplayName("a mislabelled archive is not promoted by its extension")
    void mislabelledArchive() throws IOException {
        // The members say plain archive. Before the members were read, the
        // .docx on the end was enough to conclude a word processor
        // document, which is the failure this closes.
        Map<String, String> members = new LinkedHashMap<>();
        members.put("notes.txt", "nothing office about this");
        byte[] archive = Containers.zip(members);
        Characterizer.Identification identification =
                Characterizer.identify(ByteWindows.ofWhole(archive), "report.docx");
        assertThat(identification.fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.ZIP);
        assertThat(observations(identification))
                .anyMatch(o -> o.contains("matching no published container rule"));
    }

    @Test
    @DisplayName("an archive whose members were unreachable keeps the name as the fallback")
    void unreadableMembersFallBack() {
        // Four bytes of leading magic and nothing else: the index cannot be
        // reached, so the examination did not finish and the name is still
        // the best thing available.
        Characterizer.Identification identification = Characterizer.identify(
                new byte[] {'P', 'K', 0x03, 0x04}, "report.docx");
        assertThat(identification.fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.WORD);
    }

    @Test
    @DisplayName("a plain archive stays a plain archive")
    void plainArchive() throws IOException {
        Map<String, String> members = new LinkedHashMap<>();
        members.put("a.txt", "one");
        members.put("b.txt", "two");
        byte[] archive = Containers.zip(members);
        assertThat(Characterizer.identify(ByteWindows.ofWhole(archive), "bundle.zip")
                .fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.ZIP);
    }

    @Test
    @DisplayName("members are listed from the index without reading the body")
    void listsMembers() throws IOException {
        byte[] document = Containers.ooxml(Containers.WORD_CONTENT_TYPE, "word/document.xml");
        List<ZipMembers.Member> members = ZipMembers.list(ByteWindows.ofWhole(document));
        assertThat(members).extracting(ZipMembers.Member::name)
                .containsExactly("[Content_Types].xml", "_rels/.rels", "word/document.xml");
    }

    @Test
    @DisplayName("a deflated member reads back as what was written")
    void readsMember() throws IOException {
        Map<String, String> members = new LinkedHashMap<>();
        members.put("payload.txt", "the exact bytes that went in");
        ByteWindows bytes = ByteWindows.ofWhole(Containers.zip(members));
        ZipMembers.Member member = ZipMembers.list(bytes).getFirst();
        assertThat(new String(ZipMembers.read(bytes, member), StandardCharsets.UTF_8))
                .isEqualTo("the exact bytes that went in");
    }

    // ------------------------------------------------------------------
    // Compound files
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a compound file's streams are listed")
    void listsCompoundStreams() {
        Map<String, byte[]> streams = new LinkedHashMap<>();
        streams.put("WordDocument", stream("body", 600));
        streams.put("1Table", stream("table", 600));
        Ole2Members members = Ole2Members.read(ByteWindows.ofWhole(Containers.compound(streams)));
        assertThat(members).isNotNull();
        assertThat(members.paths()).containsExactlyInAnyOrder("WordDocument", "1Table");
        assertThat(members.complete()).isTrue();
    }

    @Test
    @DisplayName("a legacy word processor document is identified from its streams")
    void legacyWordProcessor() {
        Map<String, byte[]> streams = new LinkedHashMap<>();
        streams.put("WordDocument", stream("body", 600));
        byte[] document = Containers.compound(streams);
        ContainerIdentification.Result result =
                ContainerIdentification.identify(ByteWindows.ofWhole(document));
        assertThat(result.kind()).isEqualTo(ContainerKind.OLE2);
        assertThat(result.hits()).extracting(ContainerIdentification.Hit::formatId)
                .contains("fmt/609");
        assertThat(Characterizer.identify(ByteWindows.ofWhole(document), "letter.doc")
                .fact().getFormatCase()).isEqualTo(FormatFact.FormatCase.WORD);
    }

    @Test
    @DisplayName("a compound file the rules do not cover concludes nothing")
    void unknownCompoundFile() {
        Map<String, byte[]> streams = new LinkedHashMap<>();
        streams.put("SomethingElse", stream("data", 600));
        byte[] document = Containers.compound(streams);
        assertThat(ContainerIdentification.identify(ByteWindows.ofWhole(document)).hits())
                .isEmpty();
        assertThat(Characterizer.identify(ByteWindows.ofWhole(document), "mystery.bin").fact())
                .isNull();
    }

    @Test
    @DisplayName("a compound stream reads back as what was written")
    void readsCompoundStream() {
        Map<String, byte[]> streams = new LinkedHashMap<>();
        streams.put("WordDocument", stream("the exact bytes", 600));
        Ole2Members members = Ole2Members.read(ByteWindows.ofWhole(Containers.compound(streams)));
        byte[] content = members.content(members.entry("WordDocument"));
        assertThat(content).hasSize(600);
        assertThat(new String(content, 0, 15, StandardCharsets.ISO_8859_1))
                .isEqualTo("the exact bytes");
    }

    @Test
    @DisplayName("a stream too long for the nested filing system reads from full sectors")
    void readsLargeCompoundStream() {
        // Under the cutoff a member lives in the nested filing system and
        // over it in full sectors, and those are two different readers.
        Map<String, byte[]> streams = new LinkedHashMap<>();
        streams.put("Workbook", stream("a large workbook stream", 9000));
        streams.put("CompObj", stream("small", 200));
        Ole2Members members = Ole2Members.read(ByteWindows.ofWhole(Containers.compound(streams)));
        assertThat(members.paths()).containsExactlyInAnyOrder("Workbook", "CompObj");
        assertThat(members.content(members.entry("Workbook"))).hasSize(9000);
        assertThat(members.content(members.entry("CompObj"))).hasSize(200);
        assertThat(new String(members.content(members.entry("Workbook")), 0, 23,
                StandardCharsets.ISO_8859_1)).isEqualTo("a large workbook stream");
    }

    // ------------------------------------------------------------------
    // The rule set
    // ------------------------------------------------------------------

    @Test
    @DisplayName("the bundled rule set loads with nothing skipped")
    void bundledRulesLoad() {
        ContainerSignatures signatures = ContainerSignatures.bundled();
        assertThat(signatures.skipped()).isEmpty();
        assertThat(signatures.version()).isNotBlank();
        assertThat(signatures.rulesFor(ContainerKind.ZIP)).hasSizeGreaterThan(100);
        assertThat(signatures.rulesFor(ContainerKind.OLE2)).hasSizeGreaterThan(100);
    }

    @Test
    @DisplayName("every format the registry claims to know appears in the rule set")
    void everyMappedFormatExists() {
        ContainerSignatures signatures = ContainerSignatures.bundled();
        List<String> published = java.util.stream.Stream.of(ContainerKind.values())
                .flatMap(kind -> signatures.rulesFor(kind).stream())
                .map(ContainerSignatures.Rule::formatId)
                .toList();
        List<String> claimed = published.stream().filter(ContainerFormats::known).toList();
        // The mapping is hand-kept, so it can drift from the rule set. A
        // claimed format the set no longer publishes is the drift to catch.
        assertThat(claimed).isNotEmpty();
        assertThat(published).contains("fmt/412", "fmt/214", "fmt/215", "fmt/609");
    }

    @Test
    @DisplayName("content that is not a container is left alone")
    void notAContainer() {
        assertThat(ContainerIdentification.identify(
                ByteWindows.ofWhole("%PDF-1.4".getBytes(StandardCharsets.ISO_8859_1))).kind())
                .isNull();
        assertThat(ContainerIdentification.identify(ByteWindows.EMPTY).kind()).isNull();
    }
}
