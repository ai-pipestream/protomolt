package ai.protomolt.proto.samples;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regex-free slug, checked against the two {@code replaceAll} calls it replaced
 * ({@code [^a-z0-9]+} to one dash, then {@code ^-+|-+$} removed).
 */
class CourtDocumentsSlugTest {

    @Test
    void slugMatchesTheFormerReplaceAllPair() {
        for (String caseName : new String[]{
                "United States v. Davila-Gonzalez",
                "ANSYS, Inc. v. Computational Dynamics North America",
                "  spaced  out  ", "---", "", "a", "A--B", "1:23-cv-456",
                "émigré v. naïveté", "smith\nv.\tjones", "..leading..",
                "mixed\u00a0spaces", "case \ud83d\ude00 name", "\ud83d\ude00"}) {
            String oracle = caseName.toLowerCase(Locale.ROOT)
                    .replaceAll("[^a-z0-9]+", "-")
                    .replaceAll("^-+|-+$", "");
            assertThat(CourtDocuments.slug(caseName))
                    .as("slug of '%s'", caseName)
                    .isEqualTo(oracle);
        }
    }

    @Test
    void aSupplementaryCharacterRunCollapsesLikeAnyOtherRun() {
        assertThat(CourtDocuments.slug("a\ud83d\ude00\ud83d\ude00b")).isEqualTo("a-b");
    }
}
