package ai.pipestream.proto.asset.characterize;

import ai.pipestream.proto.asset.v1.AvroDataset;
import ai.pipestream.proto.asset.v1.CharacterizationEvidence;
import ai.pipestream.proto.asset.v1.FormatFact;
import ai.pipestream.proto.asset.v1.GzipFile;
import ai.pipestream.proto.asset.v1.HtmlDocument;
import ai.pipestream.proto.asset.v1.JsonDocument;
import ai.pipestream.proto.asset.v1.MarkdownDocument;
import ai.pipestream.proto.asset.v1.NdjsonDataset;
import ai.pipestream.proto.asset.v1.ParquetDataset;
import ai.pipestream.proto.asset.v1.PdfDocument;
import ai.pipestream.proto.asset.v1.PlainText;
import ai.pipestream.proto.asset.v1.PresentationDocument;
import ai.pipestream.proto.asset.v1.RasterImage;
import ai.pipestream.proto.asset.v1.SpreadsheetDocument;
import ai.pipestream.proto.asset.v1.TarArchive;
import ai.pipestream.proto.asset.v1.WordDocument;
import ai.pipestream.proto.asset.v1.XmlDocument;
import ai.pipestream.proto.asset.v1.YamlDocument;
import ai.pipestream.proto.asset.v1.ZipArchive;

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
     * Identifies the format of some bytes.
     *
     * @param head the first bytes ({@link #PREFIX_BYTES} are plenty); may be
     *        empty
     * @param filename the asset's filename; may be blank or null
     * @return the identification — never null, possibly empty-handed
     */
    public static Identification identify(byte[] head, String filename) {
        List<CharacterizationEvidence> evidence = new ArrayList<>();
        ContentTypeSniffer.Sniff sniff = ContentTypeSniffer.sniff(head, filename);
        String extension = ContentTypeSniffer.extensionOf(filename);
        if (sniff.sniffed()) {
            evidence.add(evidence("magic-bytes", "content sniffs as " + sniff.mimeType()));
        }
        FormatFact fact = sniff.sniffed()
                ? fromMediaType(sniff.mimeType(), head, filename, extension, evidence)
                : null;
        if (!extension.isEmpty()) {
            evidence.add(evidence("extension", "filename extension is ." + extension));
        }
        return new Identification(fact, List.copyOf(evidence));
    }

    private static FormatFact fromMediaType(String mediaType, byte[] head, String filename,
                                            String extension,
                                            List<CharacterizationEvidence> evidence) {
        return switch (mediaType) {
            case "application/x-tar" -> FormatFact.newBuilder().setTar(
                    TarArchive.newBuilder()
                            .setFilename(matching(filename, FormatGrammars.TAR))).build();
            case "application/zip" -> FormatFact.newBuilder().setZip(
                    ZipArchive.newBuilder()
                            .setFilename(matching(filename, FormatGrammars.ZIP))).build();
            case "application/gzip" -> gzipOrTar(filename, evidence);
            case "application/vnd.apache.parquet" -> FormatFact.newBuilder().setParquet(
                    ParquetDataset.newBuilder()
                            .setFilename(matching(filename, FormatGrammars.PARQUET))).build();
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

    /** The filename when it matches the grammar; "" (left unset) otherwise. */
    private static String matching(String filename, Pattern grammar) {
        return filename != null && grammar.matcher(filename).matches() ? filename : "";
    }

    private static CharacterizationEvidence evidence(String signal, String observation) {
        return CharacterizationEvidence.newBuilder()
                .setSignal(signal)
                .setObservation(observation)
                .build();
    }
}
