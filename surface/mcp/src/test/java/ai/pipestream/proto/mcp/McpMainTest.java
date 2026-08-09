package ai.pipestream.proto.mcp;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The only {@code main} path that returns without either serving stdio or exiting the process:
 * the usage line. Unknown-argument and missing-value paths call {@code System.exit} and are not
 * unit-testable in-process.
 */
class McpMainTest {

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
                .contains("--registry-git");
    }
}
