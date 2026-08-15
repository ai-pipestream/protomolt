package ai.pipestream.proto.parse.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone entry point: environment-configured parsing coordinator over
 * Netty.
 *
 * <p>The routing rules come from {@code DOCUMENT_PLATFORM_PARSE_RULES_JSON}
 * (an inline proto3-JSON array) or {@code DOCUMENT_PLATFORM_PARSE_RULES_FILE}
 * (a file holding the same) — exactly one must be set. The parser fleet comes
 * from {@code DOCUMENT_PLATFORM_PARSE_PARSERS}, a comma-separated list of
 * {@code <parser_name>=<target>} entries. A coordinator with zero parsers can
 * parse nothing, so a missing fleet fails the boot loudly.
 */
public final class ParseCoordinatorMain {

    private static final Logger LOG = LoggerFactory.getLogger(ParseCoordinatorMain.class);

    private ParseCoordinatorMain() {
    }

    /**
     * Boots the coordinator from the environment and serves until terminated.
     *
     * @param args unused
     * @throws Exception when the boot fails
     */
    public static void main(String[] args) throws Exception {
        ParseCoordinatorConfig config = ParseCoordinatorConfig.fromEnvironment();
        RoutingRules rules = rulesFromEnvironment(
                System.getenv(ParseCoordinatorConfig.ENV_RULES_JSON),
                System.getenv(ParseCoordinatorConfig.ENV_RULES_FILE));
        ParserRegistry parsers = ParserRegistry.of(
                parsersFromEnvironment(System.getenv(ParseCoordinatorConfig.ENV_PARSERS)));
        ParseCoordinatorServices services = ParseCoordinatorServices.build(config, rules, parsers);
        services.startNetty(config.grpcPort());
        LOG.info(
                "parse-coordinator listening on gRPC port {} (repo target {}, {} rules, parsers {})",
                services.server().getPort(),
                config.repoTarget(),
                rules.rules().size(),
                parsers.parserNames());
        Runtime.getRuntime().addShutdownHook(new Thread(services::close, "parse-shutdown"));
        services.server().awaitTermination();
    }

    /**
     * Loads the routing rules from the inline JSON or the rules file —
     * exactly one of the two.
     *
     * @param inlineJson the {@code RULES_JSON} value, possibly null
     * @param rulesFile the {@code RULES_FILE} value, possibly null
     * @return the compiled rule set
     * @throws IOException when the rules file cannot be read
     */
    static RoutingRules rulesFromEnvironment(String inlineJson, String rulesFile)
            throws IOException {
        boolean hasJson = inlineJson != null && !inlineJson.isBlank();
        boolean hasFile = rulesFile != null && !rulesFile.isBlank();
        if (hasJson == hasFile) {
            throw new IllegalArgumentException("exactly one of "
                    + ParseCoordinatorConfig.ENV_RULES_JSON + " and "
                    + ParseCoordinatorConfig.ENV_RULES_FILE + " must be set");
        }
        return RoutingRules.fromJson(hasJson ? inlineJson : Files.readString(Path.of(rulesFile)));
    }

    /**
     * Parses the {@code DOCUMENT_PLATFORM_PARSE_PARSERS} format. Rejects a
     * missing or malformed value loudly — a coordinator with zero parsers
     * would only look healthy.
     *
     * @param spec the env value: {@code <name>=<target>,...}
     * @return parser name → target
     */
    static Map<String, String> parsersFromEnvironment(String spec) {
        if (spec == null || spec.isBlank()) {
            throw new IllegalArgumentException(ParseCoordinatorConfig.ENV_PARSERS
                    + " is required (format: <parser_name>=<target>,...)");
        }
        Map<String, String> targets = new LinkedHashMap<>();
        for (String entry : spec.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int eq = trimmed.indexOf('=');
            if (eq <= 0 || eq == trimmed.length() - 1) {
                throw new IllegalArgumentException(ParseCoordinatorConfig.ENV_PARSERS
                        + " entry is not <parser_name>=<target>: '" + trimmed + "'");
            }
            targets.put(trimmed.substring(0, eq).trim(), trimmed.substring(eq + 1).trim());
        }
        if (targets.isEmpty()) {
            throw new IllegalArgumentException(ParseCoordinatorConfig.ENV_PARSERS
                    + " names no parsers");
        }
        return targets;
    }
}
