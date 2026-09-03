package ai.protomolt.proto.emit.okf;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** The verb end to end: inline sources in, a conformant bundle out, inline and zipped. */
class EmitOkfActionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void rendersInlineSourcesAsABundle() throws Exception {
        ObjectNode input = (ObjectNode) MAPPER.readTree("""
                {"schema": {"sources": {"tick/v1/tick.proto":
                   "syntax = \\"proto3\\"; package tick.v1; message Tick { string symbol = 1; double price = 2; } service Feed { rpc Watch(Tick) returns (stream Tick); }"}},
                 "title": "Tick feed"}
                """);
        ObjectNode result = ActionCatalog.defaults(ActionContext.create())
                .register(new EmitOkfAction()).execute("emit-okf", input);

        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("fileCount").asInt()).isEqualTo(5);
        assertThat(result.get("files").get("index.md").asText())
                .contains("okf_version")
                .contains("# Tick feed");
        assertThat(result.get("files").get("messages/tick.v1.Tick.md").asText())
                .contains("type: Protobuf Message")
                .contains("| `price` | `double` |");
        assertThat(result.get("files").get("services/tick.v1.Feed.md").asText())
                .contains("| server |");

        // The zip carries exactly the same files.
        byte[] zip = Base64.getDecoder().decode(result.get("zipBase64").asText());
        List<String> names = new ArrayList<>();
        try (ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(zip))) {
            for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
                names.add(entry.getName());
            }
        }
        List<String> expected = new ArrayList<>();
        result.get("files").fieldNames().forEachRemaining(expected::add);
        assertThat(names).containsExactlyElementsOf(expected);
    }
}
