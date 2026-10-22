package ai.protomolt.proto.emit.okf;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regex-free text helpers behind yaml rendering and frontmatter parsing, checked
 * against the regexes they replaced: the plain-scalar class
 * {@code matches("[A-Za-z0-9 ._:/-]*")} and the line walk over {@code split("\n")}.
 */
class OkfRendererTextTest {

    @Test
    void yamlQuotingMatchesTheFormerPattern() {
        for (String value : new String[]{
                "plain", "a b", "a.b_c-d", "a:b", "a/b", "", " ", " a", "a ",
                "key: value", "a\"b", "a\\b", "a\nb", "émigré", "a=b", "a,b", "a|b"}) {
            boolean plain = value.matches("[A-Za-z0-9 ._:/-]*") && !value.isBlank()
                    && !value.contains(": ") && !value.startsWith(" ") && !value.endsWith(" ");
            String oracle = plain ? value
                    : '"' + value.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
            assertThat(OkfRenderer.yaml(value))
                    .as("yaml of '%s'", value.replace("\n", "\\n"))
                    .isEqualTo(oracle);
        }
    }

    @Test
    void frontmatterParsingMatchesTheFormerSplit() {
        for (String doc : new String[]{
                "---\ntitle: One\n---\nbody", "---\ntitle: \"Qu\\\"oted\"\n---\n",
                "no frontmatter", "", "---\n---\ntitle: after block",
                "---\ntitle: trailing  \n---\n", "---\r\ntitle: crlf\r\n---\r\n",
                "---\ndescription: d\ntitle: t\n---\n", "---\ntitle: a\n\n\n"}) {
            assertThat(OkfRenderer.frontmatterField(doc, "title"))
                    .as("title of '%s'", doc.replace("\n", "\\n").replace("\r", "\\r"))
                    .isEqualTo(oracleField(doc, "title"));
        }
    }

    /** The former implementation, regex split kept here as the oracle. */
    private static String oracleField(String doc, String key) {
        boolean inFrontmatter = false;
        for (String line : doc.split("\n")) {
            if (line.equals("---")) {
                if (inFrontmatter) {
                    break;
                }
                inFrontmatter = true;
                continue;
            }
            if (inFrontmatter && line.startsWith(key + ": ")) {
                String value = line.substring(key.length() + 2).trim();
                if (value.startsWith("\"") && value.endsWith("\"") && value.length() > 1) {
                    value = value.substring(1, value.length() - 1)
                            .replace("\\\"", "\"").replace("\\\\", "\\");
                }
                return value;
            }
        }
        return "";
    }
}
