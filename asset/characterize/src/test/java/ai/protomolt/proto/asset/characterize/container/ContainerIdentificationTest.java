package ai.protomolt.proto.asset.characterize.container;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.asset.characterize.Characterizer;
import ai.protomolt.proto.asset.characterize.Trailers;
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

    @Test
    @DisplayName("a stored member reads back without inflating")
    void storedMemberReadsBack() throws IOException {
        byte[] archive = Containers.zipStored("plain.txt", "no compression here");
        ByteWindows bytes = ByteWindows.ofWhole(archive);
        ZipMembers.Member member = ZipMembers.list(bytes).getFirst();
        assertThat(member.method()).isEqualTo(ZipMembers.STORED);
        assertThat(new String(ZipMembers.read(bytes, member), StandardCharsets.UTF_8))
                .isEqualTo("no compression here");
    }

    @Test
    @DisplayName("a trailing archive comment does not stop the index from being found")
    void archiveWithTrailingComment() throws IOException {
        Map<String, String> members = new LinkedHashMap<>();
        members.put("a.txt", "one");
        byte[] archive = Containers.zipWithComment(members, "a comment written after the directory");
        List<ZipMembers.Member> listed = ZipMembers.list(ByteWindows.ofWhole(archive));
        assertThat(listed).extracting(ZipMembers.Member::name).containsExactly("a.txt");
    }

    @Test
    @DisplayName("an archive with no members lists none, not null")
    void emptyArchive() throws IOException {
        byte[] archive = Containers.emptyZip();
        assertThat(ZipMembers.list(ByteWindows.ofWhole(archive))).isEmpty();
    }

    @Test
    @DisplayName("a member whose bytes lie outside the windows reads as unreadable, not absent")
    void memberOutsideWindows() throws IOException {
        Map<String, String> members = new LinkedHashMap<>();
        members.put("a.txt", "short");
        members.put("b.txt", "a body long enough to land past a narrow leading window");
        byte[] archive = Containers.zip(members);

        // The index is only ever read from the tail, so a window built from
        // exactly the central directory onward still finds it; a leading
        // window that stops before "b"'s local header leaves a genuine,
        // unread gap in between that swallows all of "b"'s bytes.
        ZipMembers.Member second = ZipMembers.list(ByteWindows.ofWhole(archive)).get(1);
        long directoryOffset = Trailers.zipDirectory(ByteWindows.ofWhole(archive)).directoryOffset();
        byte[] head = Arrays.copyOfRange(archive, 0, (int) second.headerOffset());
        byte[] tail = Arrays.copyOfRange(archive, (int) directoryOffset, archive.length);
        ByteWindows narrow = ByteWindows.of(head, tail, archive.length);

        List<ZipMembers.Member> listed = ZipMembers.list(narrow);
        assertThat(listed).extracting(ZipMembers.Member::name).containsExactly("a.txt", "b.txt");
        assertThat(ZipMembers.read(narrow, listed.get(1))).isNull();
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

    @Test
    @DisplayName("a compound file with 4096-byte sectors (major version 4) is read")
    void wideSectorCompoundFile() {
        Map<String, byte[]> streams = new LinkedHashMap<>();
        streams.put("BigStream", stream("content living in a 4096-byte sector", 5000));
        Ole2Members members =
                Ole2Members.read(ByteWindows.ofWhole(Containers.compoundWideSectors(streams)));
        assertThat(members).isNotNull();
        assertThat(members.complete()).isTrue();
        assertThat(members.paths()).containsExactly("BigStream");
        byte[] content = members.content(members.entry("BigStream"));
        assertThat(content).hasSize(5000);
        assertThat(new String(content, 0, 36, StandardCharsets.ISO_8859_1))
                .isEqualTo("content living in a 4096-byte sector");
    }

    @Test
    @DisplayName("a directory chain running outside the windows leaves the read incomplete")
    void directoryOutsideWindows() {
        Map<String, byte[]> streams = new LinkedHashMap<>();
        streams.put("WordDocument", stream("body", 600));
        byte[] document = Containers.compound(streams);
        // Only the header is resident; the directory sector it points at,
        // and everything past it, was never captured.
        byte[] headerOnly = Arrays.copyOf(document, 512);
        Ole2Members members = Ole2Members.read(ByteWindows.ofHead(headerOnly));
        assertThat(members).isNotNull();
        assertThat(members.complete()).isFalse();
        assertThat(members.entries()).isEmpty();
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
