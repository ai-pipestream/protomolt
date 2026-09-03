package ai.protomolt.proto.mcp;

import ai.protomolt.proto.actions.ActionContext;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The only {@code main} path that returns without either serving stdio or exiting the process:
 * the usage line. Unknown-argument and missing-value paths call {@code System.exit} and are not
 * unit-testable in-process.
 */
class McpMainTest {

    private static final List<String> STANDALONE_TOOLS = List.of(
            "compile", "validate-message", "diff-schemas", "check-compat", "render-json-schema",
            "render-prompt", "render-index-mappings", "eval-cel", "map-message",
            "synthesize-shape", "join-messages", "merge-schemas", "check-rules", "infer-schema",
            "mask-message", "extract-metadata", "list-types", "grpc-invoke", "reflect",
            "generate-stubs", "gather-git", "service-register", "service-list",
            "service-inspect", "service-refresh", "service-invoke", "compile-workflow", "suggest-mappings",
            "record-workflow-run", "replay-workflow", "promote-workflow",
            "export-work-record", "verify-work-record", "evaluate-work-record");

    @Test
    void helpPrintsUsageToStderrAndReturns() throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
            McpMain.main(new String[]{"--help"});
        } finally {
            System.setErr(original);
        }
        assertThat(captured.toString(StandardCharsets.UTF_8))
                .contains("usage: protomolt-mcp")
                .contains("--registry-git", "--service-workspace", "--workflow-workspace");
    }

    @Test
    void standaloneCatalogMatchesDocumentedInventory() {
        assertThat(McpMain.catalog(ActionContext.create()).names())
                .containsExactlyInAnyOrderElementsOf(STANDALONE_TOOLS);
    }
}
