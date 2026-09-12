package ai.protomolt.proto.asset.characterize;

import ai.protomolt.proto.asset.characterize.container.ContainerFormats;
import ai.protomolt.proto.asset.characterize.container.ContainerIdentification;
import ai.protomolt.proto.asset.characterize.container.ContainerKind;
import ai.protomolt.proto.asset.characterize.signature.BinaryIdentification;
import ai.protomolt.proto.asset.v1.AvroDataset;
import ai.protomolt.proto.asset.v1.CharacterizationEvidence;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.GzipFile;
import ai.protomolt.proto.asset.v1.HtmlDocument;
import ai.protomolt.proto.asset.v1.JsonDocument;
import ai.protomolt.proto.asset.v1.MarkdownDocument;
import ai.protomolt.proto.asset.v1.NdjsonDataset;
import ai.protomolt.proto.asset.v1.ParquetDataset;
import ai.protomolt.proto.asset.v1.PdfDocument;
import ai.protomolt.proto.asset.v1.PlainText;
import ai.protomolt.proto.asset.v1.PresentationDocument;
import ai.protomolt.proto.asset.v1.RasterImage;
import ai.protomolt.proto.asset.v1.SpreadsheetDocument;
import ai.protomolt.proto.asset.v1.TarArchive;
import ai.protomolt.proto.asset.v1.WordDocument;
import ai.protomolt.proto.asset.v1.XmlDocument;
import ai.protomolt.proto.asset.v1.YamlDocument;
import ai.protomolt.proto.asset.v1.ZipArchive;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Format identification: the seam every consumer of "what is this file"
 * calls. The identifier reads a bounded byte prefix and the filename,
 * concludes a {@link FormatFact} when the evidence supports one, and
 * records what it saw. Nothing is invented: when the evidence supports no
 * specific format — or supports one whose claim needs parameters only a
 * producer can state, like a delimited table's delimiter — the conclusion
 * stays empty and the evidence still tells the story.
 *
 * <p>An identified fact carries the filename only when the name matches
 * the identified format's own grammar ({@link FormatGrammars}); bytes can
 * prove a format without endorsing a name that contradicts it. Every fact
 * this class produces is valid against its message's own rules by
 * construction — a property the tests pin against the contract's actual
 * annotations.
 */
public final class Characterizer {

    /** How many prefix bytes identification wants (tar magic sits at 257). */
    public static final int PREFIX_BYTES = 512;

    /**
     * One identification: the conclusion (absent when the evidence supports
     * none) and what the identifier saw.
     *
     * @param fact the identified format, or null
     * @param evidence the recorded observations, possibly empty
     */
    public record Identification(FormatFact fact, List<CharacterizationEvidence> evidence) {
        /** Whether a format was concluded. */
        public boolean identified() {
            return fact != null;
        }
    }

    /** Extensions whose claims need producer-stated parameters (delimited tables). */
    private static final Set<String> PARAMETERIZED_TEXT_EXTENSIONS = Set.of("csv", "tsv", "psv");

    private Characterizer() {
    }

    /**
     * Identifies the format from a leading byte window alone. Nothing
     * anchored to the end of the content is observable this way.
     *
     * @param head the first bytes ({@link #PREFIX_BYTES} are plenty for the
     *        leading magics); may be empty
     * @param filename the asset's filename; may be blank or null
     * @return the identification — never null, possibly empty-handed
     */
    public static Identification identify(byte[] head, String filename) {
        return identify(ByteWindows.ofHead(head), filename);
    }

    /**
     * Identifies the format of some content.
     *
     * @param windows the captured windows over the content
     * @param filename the asset's filename; may be blank or null
     * @return the identification — never null, possibly empty-handed
     */
    public static Identification identify(ByteWindows windows, String filename) {
        List<CharacterizationEvidence> evidence = new ArrayList<>();
        byte[] head = windows.head();
        ContentTypeSniffer.Sniff sniff = ContentTypeSniffer.sniff(head, filename);
        String extension = ContentTypeSniffer.extensionOf(filename);
        if (sniff.sniffed()) {
            evidence.add(evidence("magic-bytes", "content sniffs as " + sniff.mimeType()));
        }
        // A container's leading bytes say only that it is a container. What
        // is inside decides, and the filename is the last thing to ask.
        ContainerIdentification.Result container = ContainerIdentification.identify(windows);
        FormatFact fact = containerFact(container, filename, evidence);
        if (fact == null && sniff.sniffed()) {
            fact = fromMediaType(containerMediaType(container, sniff.mimeType()),
                    windows, filename, extension, evidence);
        }
        if (!extension.isEmpty()) {
            evidence.add(evidence("extension", "filename extension is ." + extension));
        }
        if (fact == null) {
            publishedEvidence(windows, evidence);
        }
        return new Identification(fact, List.copyOf(evidence));
    }

    /**
     * The conclusion a container's members support, when the registry has
     * an entry for the format they identify.
     */
    private static FormatFact containerFact(ContainerIdentification.Result container,
                                            String filename,
                                            List<CharacterizationEvidence> evidence) {
        if (container.kind() == null) {
            return null;
        }
        for (ContainerIdentification.Hit hit : container.hits()) {
            FormatFact fact = ContainerFormats.factFor(hit.formatId(), filename);
            if (fact != null) {
                evidence.add(evidence("container",
                        "members identify " + hit.description() + " (" + hit.formatId() + ")"));
                return fact;
            }
        }
        if (!container.hits().isEmpty()) {
            ContainerIdentification.Hit hit = container.hits().getFirst();
            evidence.add(evidence("container", "members identify " + hit.description()
                    + " (" + hit.formatId() + "), for which the registry has no entry"));
        } else if (!container.complete()) {
            evidence.add(evidence("container", container.detail()));
        } else {
            evidence.add(evidence("container", "listed " + container.memberCount()
                    + " members, matching no published container rule"));
        }
        return null;
    }

    /**
     * What the content should be treated as when its members identified no
     * format the registry knows.
     *
     * <p>This is where a filename stops being evidence and starts being a
     * guess. The leading bytes of every OOXML document, OpenDocument file
     * and EPUB are the same, so a name is the only thing that separates
     * them, and a name can be wrong. Once the members have been listed and
     * matched against the published rules, the archive is an archive: the
     * name does not get to promote it into a format its members contradict.
     * The exception is an examination that could not finish, where the name
     * remains the best thing available.
     */
    private static String containerMediaType(ContainerIdentification.Result container,
                                             String sniffed) {
        if (container.kind() == null || !container.complete()) {
            return sniffed;
        }
        return container.kind() == ContainerKind.ZIP ? "application/zip" : sniffed;
    }

    private static FormatFact fromMediaType(String mediaType, ByteWindows windows,
                                            String filename, String extension,
                                            List<CharacterizationEvidence> evidence) {
        byte[] head = windows.head();
        return switch (mediaType) {
            case "application/x-tar" -> FormatFact.newBuilder().setTar(
                    TarArchive.newBuilder()
                            .setFilename(matching(filename, FormatGrammars.TAR))).build();
            case "application/zip" -> zip(windows, filename, evidence);
            case "application/gzip" -> gzipOrTar(filename, evidence);
            case "application/vnd.apache.parquet" -> parquet(windows, filename, evidence);
            case "application/avro" -> FormatFact.newBuilder().setAvro(
                    AvroDataset.newBuilder()
                            .setFilename(matching(filename, FormatGrammars.AVRO))).build();
            case "application/pdf" -> FormatFact.newBuilder().setPdf(
                    PdfDocument.newBuilder()
                            .setFilename(matching(filename, FormatGrammars.PDF))).build();
            case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" ->
                    FormatFact.newBuilder().setWord(WordDocument.newBuilder()
                            .setFilename(matching(filename, FormatGrammars.WORD))).build();
            case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ->
                    FormatFact.newBuilder().setSpreadsheet(SpreadsheetDocument.newBuilder()
                            .setFilename(matching(filename, FormatGrammars.SPREADSHEET))).build();
            case "application/vnd.openxmlformats-officedocument.presentationml.presentation" ->
                    FormatFact.newBuilder().setPresentation(PresentationDocument.newBuilder()
                            .setFilename(matching(filename, FormatGrammars.PRESENTATION))).build();
            case "application/xml" -> FormatFact.newBuilder().setXml(
                    XmlDocument.newBuilder()
                            .setFilename(matching(filename, FormatGrammars.XML))).build();
            case "text/html" -> FormatFact.newBuilder().setHtml(
                    HtmlDocument.newBuilder()
                            .setFilename(matching(filename, FormatGrammars.HTML))).build();
            case "text/plain" -> textual(head, filename, extension, evidence);
            default -> mediaType.startsWith("image/") ? image(mediaType, filename) : null;
        };
    }

    /**
     * Parquet writes its magic at both ends. The leading one identifies the
     * format; the trailing one is what makes the content readable, because
     * the schema and the row-group index live in the footer. A file with
     * only the header is still a Parquet asset, so the conclusion stands —
     * the damage is recorded as a finding, not hidden by refusing to name
     * the format.
     */
    private static FormatFact parquet(ByteWindows windows, String filename,
                                      List<CharacterizationEvidence> evidence) {
        if (!Trailers.parquetFooterObservable(windows)) {
            evidence.add(evidence("trailer", "the footer lies outside the captured window,"
                    + " so whether the content is sealed was not established"));
        } else if (Trailers.parquetSealed(windows)) {
            evidence.add(evidence("trailer", "footer magic seals the content"));
        } else {
            evidence.add(evidence("trailer", "header magic without footer magic:"
                    + " the content carries no readable schema or row-group index"));
        }
        return FormatFact.newBuilder().setParquet(ParquetDataset.newBuilder()
                .setFilename(matching(filename, FormatGrammars.PARQUET))).build();
    }

    /**
     * A ZIP's entry list lives in the central directory, which the format
     * writes at the end. Finding its record proves the archive was closed;
     * not finding one within reach means either a truncated archive or a
     * trailer beyond the captured window, and those are recorded as the
     * different observations they are.
     */
    private static FormatFact zip(ByteWindows windows, String filename,
                                  List<CharacterizationEvidence> evidence) {
        Trailers.EndOfCentralDirectory directory = Trailers.zipDirectory(windows);
        if (directory != null) {
            evidence.add(evidence("trailer", "central directory declares "
                    + directory.entryCount() + " entries"));
        } else if (!windows.sizeKnown()) {
            evidence.add(evidence("trailer", "no trailing window was captured,"
                    + " so the central directory was not looked for"));
        } else {
            evidence.add(evidence("trailer", "no central directory within reach of the"
                    + " end: the archive is truncated or its trailer lies outside the"
                    + " captured window"));
        }
        return FormatFact.newBuilder().setZip(ZipArchive.newBuilder()
                .setFilename(matching(filename, FormatGrammars.ZIP))).build();
    }

    /** gzip magic: a tar-grammar name means a compressed tar, not a gzip file. */
    private static FormatFact gzipOrTar(String filename,
                                        List<CharacterizationEvidence> evidence) {
        if (filename != null && FormatGrammars.TAR.matcher(filename).matches()) {
            evidence.add(evidence("probe", "gzip bytes carrying a tar-grammar name"));
            return FormatFact.newBuilder()
                    .setTar(TarArchive.newBuilder().setFilename(filename)).build();
        }
        return FormatFact.newBuilder().setGzip(GzipFile.newBuilder()
                .setFilename(matching(filename, FormatGrammars.GZIP))).build();
    }

    /** Text bytes: refine by extension, probe JSON, or conclude plain text. */
    private static FormatFact textual(byte[] head, String filename, String extension,
                                      List<CharacterizationEvidence> evidence) {
        if (PARAMETERIZED_TEXT_EXTENSIONS.contains(extension)) {
            // The bytes say text and the name suggests a delimited table,
            // but that claim needs a delimiter and header presence only a
            // producer can state. Concluding nothing is the honest verdict.
            evidence.add(evidence("probe", "." + extension
                    + " suggests a delimited table, whose claim needs a stated"
                    + " delimiter and header presence"));
            return null;
        }
        return switch (extension) {
            case "md", "markdown" -> FormatFact.newBuilder().setMarkdown(
                    MarkdownDocument.newBuilder().setFilename(filename)).build();
            case "yaml", "yml" -> FormatFact.newBuilder().setYaml(
                    YamlDocument.newBuilder().setFilename(filename)).build();
            case "ndjson", "jsonl" -> FormatFact.newBuilder().setNdjson(
                    NdjsonDataset.newBuilder().setFilename(filename)).build();
            case "json" -> jsonProbe(head, filename, evidence);
            default -> FormatFact.newBuilder().setText(PlainText.newBuilder()
                    .setFilename(matching(filename, FormatGrammars.TEXT))).build();
        };
    }

    /** A .json name plus a leading value delimiter concludes a JSON document. */
    private static FormatFact jsonProbe(byte[] head, String filename,
                                        List<CharacterizationEvidence> evidence) {
        String text = new String(head, StandardCharsets.UTF_8).stripLeading();
        if (text.startsWith("{") || text.startsWith("[")) {
            evidence.add(evidence("probe", "leading JSON value delimiter"));
            return FormatFact.newBuilder()
                    .setJson(JsonDocument.newBuilder().setFilename(filename)).build();
        }
        return FormatFact.newBuilder().setText(PlainText.newBuilder()).build();
    }

    private static FormatFact image(String mediaType, String filename) {
        return FormatFact.newBuilder().setImage(RasterImage.newBuilder()
                .setMediaType(mediaType)
                .setFilename(matching(filename, FormatGrammars.IMAGE))).build();
    }

    /** How many published formats an unclassified asset lists by name. */
    private static final int PUBLISHED_EVIDENCE_LIMIT = 4;

    /** The contract's limit on one observation. */
    private static final int OBSERVATION_LIMIT = 500;

    /**
     * What the wider published signature set makes of an asset the
     * registry could not place.
     *
     * <p>This runs only when nothing was concluded, which is both where it
     * is useful and where its cost is affordable. Placing an asset takes a
     * handful of table lookups; running a couple of thousand published
     * signatures takes long enough to be worth avoiding on the assets that
     * were already placed. On the assets that were not, a backlog entry
     * reading "the bytes carry the signature of a particular published
     * format" is the difference between something an operator can act on
     * and an opaque blob.
     *
     * <p>None of it becomes a conclusion. The registry names the formats
     * this platform can act on; a signature hit outside that list is an
     * observation about bytes and stays one.
     */
    private static void publishedEvidence(ByteWindows windows,
                                          List<CharacterizationEvidence> evidence) {
        BinaryIdentification.Result published = BinaryIdentification.identify(windows);
        if (published.hits().isEmpty()) {
            evidence.add(evidence("published-signature", "no published signature matches: "
                    + published.evaluated() + " were evaluated and " + published.notEvaluable()
                    + " reached outside the captured windows"));
            return;
        }
        int reported = 0;
        for (BinaryIdentification.Hit hit : published.hits()) {
            if (reported == PUBLISHED_EVIDENCE_LIMIT) {
                break;
            }
            String mediaType = hit.mediaType().isBlank() ? "" : ", " + hit.mediaType();
            evidence.add(evidence("published-signature", "the bytes carry the signature of "
                    + hit.label() + " (" + hit.formatId() + ")" + mediaType));
            reported++;
        }
        int remaining = published.hits().size() - reported;
        if (remaining > 0) {
            evidence.add(evidence("published-signature",
                    remaining + " further published formats also match"));
        }
    }

    /** The filename when it matches the grammar; "" (left unset) otherwise. */
    private static String matching(String filename, Pattern grammar) {
        return filename != null && grammar.matcher(filename).matches() ? filename : "";
    }

    private static CharacterizationEvidence evidence(String signal, String observation) {
        return CharacterizationEvidence.newBuilder()
                .setSignal(signal)
                .setObservation(observation.length() <= OBSERVATION_LIMIT
                        ? observation : observation.substring(0, OBSERVATION_LIMIT))
                .build();
    }
}
