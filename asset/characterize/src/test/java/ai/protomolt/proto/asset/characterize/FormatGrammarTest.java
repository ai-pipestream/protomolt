package ai.protomolt.proto.asset.characterize;

import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The zero-regex {@link FormatGrammar} walk against the expressions it
 * stands for, compiled here as the oracle (tests may use regex;
 * production code may not). Covers every declared grammar over names that
 * separate a suffix check from the actual expression: a dot with nothing
 * before it, case, multi-part extensions, and line terminators, which the
 * expression's {@code .} never matches.
 */
class FormatGrammarTest {

    private static final List<FormatGrammar> ALL = List.of(
            FormatGrammars.TAR, FormatGrammars.ZIP, FormatGrammars.GZIP, FormatGrammars.PARQUET,
            FormatGrammars.AVRO, FormatGrammars.DELIMITED, FormatGrammars.NDJSON, FormatGrammars.JSON,
            FormatGrammars.XML, FormatGrammars.YAML, FormatGrammars.PDF, FormatGrammars.WORD,
            FormatGrammars.SPREADSHEET, FormatGrammars.PRESENTATION, FormatGrammars.MARKDOWN,
            FormatGrammars.HTML, FormatGrammars.TEXT, FormatGrammars.IMAGE);

    private static final List<String> NAMES = List.of(
            "", "x", ".pdf", "..pdf", "x.pdf", "X.PDF", "report.PdF", ".tar.gz", "x.tar.gz",
            "x.TAR.GZ", "x.tar", "x.tgz", "xtgz", "xtar.gz", "a..zip", ".zip", "x.doc", "x.docx",
            "x.docxx", "x.xls", "x.xlsx", "x.ods", "x.ppt", "x.pptx", "x.htm", "x.html", "x.jpg",
            "x.jpeg", "x.jpeeg", "x.tif", "x.tiff", "x.md", "x.markdown", "x.txt", "x.text",
            "x.log", "x.csv", "x.tsv", "x.psv", "x.ndjson", "x.jsonl", "x.json", "x.xml",
            "x.yaml", "x.yml", "x.parquet", "x.avro", "x.gz", "x.png", "x.gif", "x.webp",
            "x.bmp", "archive.tar.gz.bak", "no-extension", "x.", "x.pdf ", "x.pdf\n",
            "a\nb.pdf", "x.p\ndf", "x.pdf\r", "line\u2028name.pdf", "x\u0085y.pdf");

    @Test
    void matchesTheDeclaredExpressionOnEveryName() {
        for (FormatGrammar grammar : ALL) {
            Pattern oracle = Pattern.compile(grammar.expression());
            for (String name : NAMES) {
                assertThat(grammar.matches(name))
                        .as("%s on '%s'", grammar.expression(), name)
                        .isEqualTo(oracle.matcher(name).matches());
            }
        }
    }

    @Test
    void dotOnlyAndNewlineNamesAreRefused() {
        assertThat(FormatGrammars.PDF.matches(".pdf")).isFalse();
        assertThat(FormatGrammars.PDF.matches("..pdf")).isTrue();
        assertThat(FormatGrammars.TAR.matches(".tar.gz")).isFalse();
        assertThat(FormatGrammars.TAR.matches("x.tar.gz")).isTrue();
        assertThat(FormatGrammars.PDF.matches("x.pdf\n")).isFalse();
        assertThat(FormatGrammars.PDF.matches(null)).isFalse();
    }
}
