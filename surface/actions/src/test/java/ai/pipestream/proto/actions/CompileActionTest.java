package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompileActionTest {

    private final ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create());

    @Test
    void compilesSourcesIntoADecodableDescriptorSet() throws Exception {
        ObjectNode input = obj("{\"sources\": {}}");
        ObjectNode sources = (ObjectNode) input.get("sources");
        sources.put("t/doc.proto", TestFixtures.DOC_PROTO);
        ObjectNode result = catalog.execute("compile", input);
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("files")).extracting(node -> node.asText())
                .contains("t/doc.proto");
        FileDescriptorSet set = FileDescriptorSet.parseFrom(
                Base64.getDecoder().decode(result.get("descriptorSetBase64").asText()));
        assertThat(set.getFileList()).extracting(FileDescriptorProto::getName)
                .contains("t/doc.proto");
    }

    @Test
    void imports_areCompiledTogether() throws Exception {
        ObjectNode input = obj("{\"sources\": {}}");
        ObjectNode sources = (ObjectNode) input.get("sources");
        sources.put("common.proto", "syntax = \"proto3\"; package c; message Ref { string id = 1; }");
        sources.put("doc.proto", """
                syntax = "proto3";
                package c;
                import "common.proto";
                message Doc { Ref ref = 1; }
                """);
        ObjectNode result = catalog.execute("compile", input);
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("files")).extracting(node -> node.asText())
                .contains("common.proto", "doc.proto");
    }

    @Test
    void brokenSourceReturnsOkFalseWithErrors() throws Exception {
        ObjectNode input = obj("""
                {"sources": {"bad.proto": "syntax = \\"proto3\\"; message Broken { nope }"}}
                """);
        ObjectNode result = catalog.execute("compile", input);
        assertThat(result.get("ok").asBoolean()).isFalse();
        assertThat(result.get("errors")).hasSize(1);
        assertThat(result.get("errors").get(0).asText()).isNotBlank();
        assertThat(result.has("descriptorSetBase64")).isFalse();
    }

    @Test
    void missingSourcesIsInvalidInput() {
        assertThatThrownBy(() -> catalog.execute("compile", obj("{}")))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/sources");
                });
    }

    @Test
    void nonStringSourceContentIsInvalidInputWithPointer() {
        assertThatThrownBy(() -> catalog.execute("compile",
                obj("{\"sources\": {\"a.proto\": 42}}")))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/sources/a.proto");
                });
    }

    @Test
    void emptySourcesIsInvalidInput() {
        assertThatThrownBy(() -> catalog.execute("compile", obj("{\"sources\": {}}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }
}
