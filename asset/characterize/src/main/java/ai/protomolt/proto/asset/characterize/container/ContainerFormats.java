package ai.protomolt.proto.asset.characterize.container;

import ai.protomolt.proto.asset.characterize.FormatGrammars;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.PresentationDocument;
import ai.protomolt.proto.asset.v1.SpreadsheetDocument;
import ai.protomolt.proto.asset.v1.WordDocument;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Which published container formats the registry has a place for.
 *
 * <p>The rule set names a couple of thousand formats. The registry names
 * eighteen, on purpose: a format earns an entry by having something in the
 * platform that reads it, and a name for a format nothing can open is a
 * string, not an identification. So most rules produce evidence and a few
 * produce a conclusion, and this is the list of the few.
 *
 * <p>A format identifier appears here only when the registry entry it maps
 * to already admits that format's file names. The spreadsheet entry's
 * grammar covers {@code .ods}, so OpenDocument spreadsheets belong here;
 * nothing covers {@code .odt} or {@code .odp}, so OpenDocument text and
 * presentations stay evidence rather than being filed under a word
 * processor they are not.
 */
public final class ContainerFormats {

    /** Word processor documents, in both the OOXML and compound layouts. */
    private static final Set<String> WORD_PROCESSOR = Set.of(
            "fmt/412", "fmt/523", "fmt/597", "fmt/599", "fmt/1827",
            "fmt/39", "fmt/40", "fmt/609", "fmt/754", "fmt/755", "x-fmt/45");

    /** Spreadsheets, including the OpenDocument ones the grammar admits. */
    private static final Set<String> SPREADSHEET = Set.of(
            "fmt/214", "fmt/445", "fmt/595", "fmt/598", "fmt/627", "fmt/628", "fmt/1828",
            "fmt/59", "fmt/61", "x-fmt/17",
            "fmt/137", "fmt/294", "fmt/295", "fmt/1755", "fmt/2045");

    /** Presentations, in both layouts. */
    private static final Set<String> PRESENTATION = Set.of(
            "fmt/215", "fmt/487", "fmt/629", "fmt/630", "fmt/631", "fmt/632", "fmt/633",
            "fmt/636", "fmt/1829",
            "fmt/125", "fmt/126", "x-fmt/88");

    private ContainerFormats() {
    }

    /**
     * The registry entry a published format identifier maps to.
     *
     * @param formatId the registry's identifier for the format
     * @param filename the asset's filename, kept only when the entry's own
     *        grammar admits it; may be null or blank
     * @return the fact, or null when the registry has no entry for it
     */
    public static FormatFact factFor(String formatId, String filename) {
        if (formatId == null || formatId.isBlank()) {
            return null;
        }
        if (WORD_PROCESSOR.contains(formatId)) {
            return FormatFact.newBuilder().setWord(WordDocument.newBuilder()
                    .setFilename(matching(filename, FormatGrammars.WORD))).build();
        }
        if (SPREADSHEET.contains(formatId)) {
            return FormatFact.newBuilder().setSpreadsheet(SpreadsheetDocument.newBuilder()
                    .setFilename(matching(filename, FormatGrammars.SPREADSHEET))).build();
        }
        if (PRESENTATION.contains(formatId)) {
            return FormatFact.newBuilder().setPresentation(PresentationDocument.newBuilder()
                    .setFilename(matching(filename, FormatGrammars.PRESENTATION))).build();
        }
        return null;
    }

    /** Whether the registry has an entry for a published format identifier. */
    public static boolean known(String formatId) {
        return formatId != null
                && (WORD_PROCESSOR.contains(formatId)
                || SPREADSHEET.contains(formatId)
                || PRESENTATION.contains(formatId));
    }

    /** The entry's own grammar decides whether the name belongs on the fact. */
    private static String matching(String filename, Pattern grammar) {
        return filename != null && grammar.matcher(filename).matches() ? filename : "";
    }
}
