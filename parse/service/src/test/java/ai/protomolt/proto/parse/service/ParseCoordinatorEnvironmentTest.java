package ai.protomolt.proto.parse.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How the parse coordinator reads its parser source and routing rules from the
 * environment: exactly one source of each, and every refusal names the variable.
 */
class ParseCoordinatorEnvironmentTest {

    private static final String RULES = """
            [{"ruleId": "r-pdf", "when": "mime_type == 'application/pdf'", "parserName": "docling"}]
            """;

    @Test
    void theParserListIsTrimmedAndKeepsItsOrder() {
        assertThat(ParseCoordinatorMain.parsersFromEnvironment(" tika = localhost:1 ,, docling=inprocess:d ,"))
                .containsExactly(Map.entry("tika", "localhost:1"), Map.entry("docling", "inprocess:d"));
    }

    @Test
    void aMissingParserListIsRefusedWithItsFormat() {
        for (String missing : new String[] {null, "", "  "}) {
            assertThatThrownBy(() -> ParseCoordinatorMain.parsersFromEnvironment(missing))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(ParseCoordinatorConfig.ENV_PARSERS
                            + " is required (format: <parser_name>=<target>,...)");
        }
    }

    @Test
    void anEntryWithoutANameOrATargetIsRefused() {
        for (String entry : new String[] {"=localhost:1", "tika=", "tika"}) {
            assertThatThrownBy(() -> ParseCoordinatorMain.parsersFromEnvironment("docling=d," + entry))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(ParseCoordinatorConfig.ENV_PARSERS
                            + " entry is not <parser_name>=<target>: '" + entry + "'");
        }
    }

    @Test
    void aListOfOnlySeparatorsNamesNoParsers() {
        assertThatThrownBy(() -> ParseCoordinatorMain.parsersFromEnvironment(" , ,"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(ParseCoordinatorConfig.ENV_PARSERS + " names no parsers");
    }

    @Test
    void theInlineParserListBuildsTheRegistry() {
        try (ParserRegistry registry =
                     ParseCoordinatorMain.registryFromEnvironment(null, null, "tika=localhost:1")) {
            assertThat(registry.parserNames()).containsExactly("tika");
        }
    }

    @Test
    void bothParserSourcesAtOnceAreRefused(@TempDir Path profiles) {
        assertThatThrownBy(() -> ParseCoordinatorMain.registryFromEnvironment(
                profiles.toString(), "parsers", "tika=localhost:1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(ParseCoordinatorConfig.ENV_PROFILES + " and "
                        + ParseCoordinatorConfig.ENV_PARSERS
                        + " are both set; configure exactly one parser source");
    }

    @Test
    void neitherParserSourceFallsThroughToTheMissingListRefusal() {
        assertThatThrownBy(() -> ParseCoordinatorMain.registryFromEnvironment(" ", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(ParseCoordinatorConfig.ENV_PARSERS
                        + " is required (format: <parser_name>=<target>,...)");
    }

    @Test
    void profilesNeedAnEndpointName(@TempDir Path profiles) {
        for (String endpoint : new String[] {null, " "}) {
            assertThatThrownBy(() -> ParseCoordinatorMain.registryFromEnvironment(
                    profiles.toString(), endpoint, null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(ParseCoordinatorConfig.ENV_PROFILE_ENDPOINT + " is required with "
                            + ParseCoordinatorConfig.ENV_PROFILES);
        }
    }

    @Test
    void aProfileStoreThatCannotBeOpenedNamesItsPath(@TempDir Path dir) throws IOException {
        Path file = Files.writeString(dir.resolve("not-a-directory"), "x");

        assertThatThrownBy(() -> ParseCoordinatorMain.registryFromEnvironment(
                file.toString(), "parsers", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("could not open the service-profile store at " + file)
                .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void rulesComeFromInlineJson() throws IOException {
        assertThat(ParseCoordinatorMain.rulesFromEnvironment(RULES, null).rules())
                .singleElement()
                .satisfies(rule -> assertThat(rule.getRuleId()).isEqualTo("r-pdf"));
    }

    @Test
    void rulesComeFromAFile(@TempDir Path dir) throws IOException {
        Path file = Files.writeString(dir.resolve("rules.json"), RULES);

        assertThat(ParseCoordinatorMain.rulesFromEnvironment(" ", file.toString()).rules())
                .singleElement()
                .satisfies(rule -> assertThat(rule.getParserName()).isEqualTo("docling"));
    }

    @Test
    void aMissingRulesFileIsAnIoFailure(@TempDir Path dir) {
        assertThatThrownBy(() -> ParseCoordinatorMain.rulesFromEnvironment(
                null, dir.resolve("absent.json").toString()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void rulesNeedExactlyOneSource(@TempDir Path dir) {
        String expected = "exactly one of " + ParseCoordinatorConfig.ENV_RULES_JSON + " and "
                + ParseCoordinatorConfig.ENV_RULES_FILE + " must be set";
        assertThatThrownBy(() -> ParseCoordinatorMain.rulesFromEnvironment(null, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expected);
        assertThatThrownBy(() -> ParseCoordinatorMain.rulesFromEnvironment(
                RULES, dir.resolve("rules.json").toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expected);
    }
}
