package ai.protomolt.proto.asset.characterize;

import java.util.regex.Pattern;

/**
 * The format name grammars, compiled once. Each pattern is the SAME
 * expression the contract declares on the corresponding format message's
 * {@code filename} field — a parity the tests enforce against the actual
 * descriptor annotations, so the identifier and the validator can never
 * drift apart on what a name means.
 */
public final class FormatGrammars {

    /** {@code TarArchive.filename}. */
    public static final Pattern TAR = Pattern.compile("(?i)^.+\\.(tar|tar\\.gz|tgz)$");
    /** {@code ZipArchive.filename}. */
    public static final Pattern ZIP = Pattern.compile("(?i)^.+\\.zip$");
    /** {@code GzipFile.filename}. */
    public static final Pattern GZIP = Pattern.compile("(?i)^.+\\.gz$");
    /** {@code ParquetDataset.filename}. */
    public static final Pattern PARQUET = Pattern.compile("(?i)^.+\\.parquet$");
    /** {@code AvroDataset.filename}. */
    public static final Pattern AVRO = Pattern.compile("(?i)^.+\\.avro$");
    /** {@code DelimitedTable.filename}. */
    public static final Pattern DELIMITED = Pattern.compile("(?i)^.+\\.(csv|tsv|psv)$");
    /** {@code NdjsonDataset.filename}. */
    public static final Pattern NDJSON = Pattern.compile("(?i)^.+\\.(ndjson|jsonl)$");
    /** {@code JsonDocument.filename}. */
    public static final Pattern JSON = Pattern.compile("(?i)^.+\\.json$");
    /** {@code XmlDocument.filename}. */
    public static final Pattern XML = Pattern.compile("(?i)^.+\\.xml$");
    /** {@code YamlDocument.filename}. */
    public static final Pattern YAML = Pattern.compile("(?i)^.+\\.(yaml|yml)$");
    /** {@code PdfDocument.filename}. */
    public static final Pattern PDF = Pattern.compile("(?i)^.+\\.pdf$");
    /** {@code WordDocument.filename}. */
    public static final Pattern WORD = Pattern.compile("(?i)^.+\\.docx?$");
    /** {@code SpreadsheetDocument.filename}. */
    public static final Pattern SPREADSHEET = Pattern.compile("(?i)^.+\\.(xlsx?|ods)$");
    /** {@code PresentationDocument.filename}. */
    public static final Pattern PRESENTATION = Pattern.compile("(?i)^.+\\.pptx?$");
    /** {@code MarkdownDocument.filename}. */
    public static final Pattern MARKDOWN = Pattern.compile("(?i)^.+\\.(md|markdown)$");
    /** {@code HtmlDocument.filename}. */
    public static final Pattern HTML = Pattern.compile("(?i)^.+\\.html?$");
    /** {@code PlainText.filename}. */
    public static final Pattern TEXT = Pattern.compile("(?i)^.+\\.(txt|text|log)$");
    /** {@code RasterImage.filename}. */
    public static final Pattern IMAGE =
            Pattern.compile("(?i)^.+\\.(png|jpe?g|gif|webp|tiff?|bmp)$");

    private FormatGrammars() {
    }
}
