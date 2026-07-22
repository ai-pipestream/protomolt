package ai.pipestream.proto.cli;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.grpc.service.ProtoMoltCatalog;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The command line driven directly: the dispatch takes its streams and catalog as arguments, so a
 * test runs a verb, lists them, exercises the console, and checks the exit codes a shell relies on
 * without spawning a process.
 */
class ProtoMoltCliTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static ActionCatalog catalog;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    @BeforeAll
    static void catalog() {
        catalog = ProtoMoltCatalog.full(ActionContext.create());
    }

    private int run(String[] args, String stdin) throws Exception {
        InputStream in = new ByteArrayInputStream(stdin.getBytes(StandardCharsets.UTF_8));
        return ProtoMoltCli.run(args, in,
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8), catalog);
    }

    private String out() {
        return out.toString(StandardCharsets.UTF_8);
    }

    private String err() {
        return err.toString(StandardCharsets.UTF_8);
    }

    private static String compileInput() throws Exception {
        ObjectNode input = MAPPER.createObjectNode();
        input.putObject("sources").put("p/m.proto",
                "syntax = \"proto3\"; package p; message M { string id = 1; }");
        return MAPPER.writeValueAsString(input);
    }

    @Test
    void listNamesEveryVerb() throws Exception {
        assertThat(run(new String[]{"list"}, "")).isZero();
        assertThat(out()).contains("compile").contains("eval-cel").contains("list-types");
        assertThat(out().lines().count()).isEqualTo(catalog.names().size());
    }

    @Test
    void runsAVerbFromAnInlineArgumentAndPrintsJson() throws Exception {
        int code = run(new String[]{"compile", compileInput()}, "");
        assertThat(code).isZero();
        JsonNode result = MAPPER.readTree(out());
        assertThat(result.path("ok").asBoolean()).isTrue();
        assertThat(result.path("descriptorSetBase64").asText()).isNotEmpty();
    }

    @Test
    void readsInputFromStdinWhenNoneIsGiven() throws Exception {
        int code = run(new String[]{"compile"}, compileInput());
        assertThat(code).isZero();
        assertThat(MAPPER.readTree(out()).path("ok").asBoolean()).isTrue();
    }

    @Test
    void anUnknownVerbIsAUsageError() throws Exception {
        assertThat(run(new String[]{"nope"}, "")).isEqualTo(2);
        assertThat(err()).contains("Unknown verb 'nope'");
    }

    @Test
    void aFailingVerbExitsOneWithItsErrorCode() throws Exception {
        int code = run(new String[]{"extract-metadata", "{\"schema\": {\"type\": \"no.such.Type\"}}"},
                "");
        assertThat(code).isEqualTo(1);
        assertThat(err()).contains(":");   // "<error-code>: <message>"
    }

    @Test
    void noArgumentsShowTheUsage() throws Exception {
        assertThat(run(new String[]{}, "")).isZero();
        assertThat(out()).contains("Usage:").contains("protomolt console");
    }

    /**
     * A dropped shell variable turns {@code --input "$JSON"} into a bare {@code --input}. Left
     * unchecked the flag was ignored and the run blocked on stdin instead of failing.
     */
    @Test
    void anInputFlagWithoutAValueIsAUsageError() throws Exception {
        assertThat(run(new String[]{"compile", "--input"}, compileInput())).isEqualTo(2);
        assertThat(err()).contains("Could not read input").contains("--input needs a JSON value");
    }

    @Test
    void anInputFileFlagWithoutAPathIsAUsageError() throws Exception {
        assertThat(run(new String[]{"compile", "--input-file"}, compileInput())).isEqualTo(2);
        assertThat(err()).contains("--input-file needs a path");
    }

    @Test
    void inputThatIsNotAJsonObjectIsRejected() throws Exception {
        assertThat(run(new String[]{"compile", "[1, 2]"}, "")).isEqualTo(2);
        assertThat(err()).contains("Input must be a JSON object").contains("array");
    }

    @Test
    void theConsoleRejectsInputThatIsNotAJsonObject() throws Exception {
        int code = run(new String[]{"console"}, "compile \"text\"\nexit\n");
        assertThat(code).isZero();
        assertThat(out()).contains("Input must be a JSON object").contains("string");
    }

    @Test
    void theConsoleRunsLinesThenExits() throws Exception {
        int code = run(new String[]{"console"}, "list\ncompile " + compileInput() + "\nexit\n");
        assertThat(code).isZero();
        assertThat(out()).contains("ProtoMolt console").contains("eval-cel");
        // The pretty-printed verb result appears between the prompts.
        assertThat(out()).contains("\"ok\" : true");
    }
}
