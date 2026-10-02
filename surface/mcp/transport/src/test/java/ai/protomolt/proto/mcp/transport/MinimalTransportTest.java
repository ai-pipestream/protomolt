package ai.protomolt.proto.mcp.transport;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.ActionProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

class MinimalTransportTest {
    @Test
    void servesOnlyTheSuppliedCapabilitiesWithoutFullDistributionDependencies() throws Exception {
        assertThat(ServiceLoader.load(ActionProvider.class)).isEmpty();
        ActionCatalog catalog = ActionCatalog.empty(ActionContext.create()).register(new Echo());
        McpServer server = new McpServer(catalog, null, "minimal", "test");
        ObjectMapper mapper = new ObjectMapper();
        try (var session = server.openSession()) {
            var initialized = session.handle(mapper.readTree("""
                    {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                      "protocolVersion":"2025-06-18","capabilities":{},
                      "clientInfo":{"name":"boundary-test","version":"1"}}}
                    """)).orElseThrow();
            assertThat(initialized.has("error")).isFalse();
            assertThat(initialized.path("result").path("instructions").asText())
                    .doesNotContain("compile-workflow", "generate-stubs", "mesh-snapshot");
            session.handle(mapper.readTree("""
                    {"jsonrpc":"2.0","method":"notifications/initialized"}
                    """));
            var listed = session.handle(mapper.readTree("""
                    {"jsonrpc":"2.0","id":2,"method":"tools/list"}
                    """)).orElseThrow();
            assertThat(listed.path("result").path("tools")).hasSize(1);
            assertThat(listed.path("result").path("tools").get(0).path("name").asText()).isEqualTo("echo");
            var result = session.handle(mapper.readTree("""
                    {"jsonrpc":"2.0","id":3,"method":"tools/call",
                     "params":{"name":"echo","arguments":{"value":"hello"}}}
                    """)).orElseThrow();
            assertThat(result.path("result").path("isError").asBoolean()).isFalse();
            assertThat(result.toString()).contains("hello");
        }
    }

    private static final class Echo implements ProtoAction {
        @Override public String name() { return "echo"; }
        @Override public String description() { return "Echo a protobuf message"; }
        @Override public Descriptor requestType() { return Struct.getDescriptor(); }
        @Override public Descriptor responseType() { return Struct.getDescriptor(); }
        @Override public Message execute(Message request, ActionContext context) { return request; }
    }
}
