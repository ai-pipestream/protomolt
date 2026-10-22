package ai.protomolt.proto.asset.characterize;

/**
 * The format name grammars. Each grammar carries the SAME expression the
 * contract declares on the corresponding format message's
 * {@code filename} field — a parity the tests enforce against the actual
 * descriptor annotations, so the identifier and the validator can never
 * drift apart on what a name means. The matching itself is a zero-regex
 * suffix walk in {@link FormatGrammar}.
 */
public final class FormatGrammars {

    /** {@code TarArchive.filename}. */
    public static final FormatGrammar TAR =
            FormatGrammar.of("(?i)^.+\\.(tar|tar\\.gz|tgz)$", "tar", "tar.gz", "tgz");
    /** {@code ZipArchive.filename}. */
    public static final FormatGrammar ZIP = FormatGrammar.of("(?i)^.+\\.zip$", "zip");
    /** {@code GzipFile.filename}. */
    public static final FormatGrammar GZIP = FormatGrammar.of("(?i)^.+\\.gz$", "gz");
    /** {@code ParquetDataset.filename}. */
    public static final FormatGrammar PARQUET =
            FormatGrammar.of("(?i)^.+\\.parquet$", "parquet");
    /** {@code AvroDataset.filename}. */
    public static final FormatGrammar AVRO = FormatGrammar.of("(?i)^.+\\.avro$", "avro");
    /** {@code DelimitedTable.filename}. */
    public static final FormatGrammar DELIMITED =
            FormatGrammar.of("(?i)^.+\\.(csv|tsv|psv)$", "csv", "tsv", "psv");
    /** {@code NdjsonDataset.filename}. */
    public static final FormatGrammar NDJSON =
            FormatGrammar.of("(?i)^.+\\.(ndjson|jsonl)$", "ndjson", "jsonl");
    /** {@code JsonDocument.filename}. */
    public static final FormatGrammar JSON = FormatGrammar.of("(?i)^.+\\.json$", "json");
    /** {@code XmlDocument.filename}. */
    public static final FormatGrammar XML = FormatGrammar.of("(?i)^.+\\.xml$", "xml");
    /** {@code YamlDocument.filename}. */
    public static final FormatGrammar YAML =
            FormatGrammar.of("(?i)^.+\\.(yaml|yml)$", "yaml", "yml");
    /** {@code PdfDocument.filename}. */
    public static final FormatGrammar PDF = FormatGrammar.of("(?i)^.+\\.pdf$", "pdf");
    /** {@code WordDocument.filename}. */
    public static final FormatGrammar WORD =
            FormatGrammar.of("(?i)^.+\\.docx?$", "doc", "docx");
    /** {@code SpreadsheetDocument.filename}. */
    public static final FormatGrammar SPREADSHEET =
            FormatGrammar.of("(?i)^.+\\.(xlsx?|ods)$", "xls", "xlsx", "ods");
    /** {@code PresentationDocument.filename}. */
    public static final FormatGrammar PRESENTATION =
            FormatGrammar.of("(?i)^.+\\.pptx?$", "ppt", "pptx");
    /** {@code MarkdownDocument.filename}. */
    public static final FormatGrammar MARKDOWN =
            FormatGrammar.of("(?i)^.+\\.(md|markdown)$", "md", "markdown");
    /** {@code HtmlDocument.filename}. */
    public static final FormatGrammar HTML =
            FormatGrammar.of("(?i)^.+\\.html?$", "htm", "html");
    /** {@code PlainText.filename}. */
    public static final FormatGrammar TEXT =
            FormatGrammar.of("(?i)^.+\\.(txt|text|log)$", "txt", "text", "log");
    /** {@code RasterImage.filename}. */
    public static final FormatGrammar IMAGE = FormatGrammar.of(
            "(?i)^.+\\.(png|jpe?g|gif|webp|tiff?|bmp)$",
            "png", "jpg", "jpeg", "gif", "webp", "tif", "tiff", "bmp");

    private FormatGrammars() {
    }
}
