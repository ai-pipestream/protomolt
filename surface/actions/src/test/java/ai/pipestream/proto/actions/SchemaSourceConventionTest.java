package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The shared schema-source convention, exercised through the catalog. */
class SchemaSourceConventionTest {

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    // ---- the three forms ----

    @Test
    void typeFormResolvesThroughTheContextRegistry() throws Exception {
        ObjectNode result = catalog.execute("validate-message", obj("""
                {"schema": {"type": "actions.test.Person"},
                 "message": {"name": "Joseph", "age": 30}}
                """));
        assertThat(result).isEqualTo(obj("{\"valid\": true, \"violations\": []}"));
    }

    @Test
    void sourcesFormCompilesInlineWithoutTouchingTheRegistry() throws Exception {
        ActionContext context = TestFixtures.personContext();
        int registered = context.registry().size();
        ObjectNode input = obj("""
                {"schema": {"sources": {}}, "message": {"title": "hi"}}
                """);
        ((ObjectNode) input.get("schema").get("sources")).put("doc.proto", TestFixtures.DOC_PROTO);
        ObjectNode result = ActionCatalog.defaults(context).execute("validate-message", input);
        assertThat(result.get("valid").asBoolean()).isTrue();
        assertThat(context.registry().size()).isEqualTo(registered);
    }

    @Test
    void descriptorSetFormParsesASerializedFileDescriptorSet() throws Exception {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(TestFixtures.personFile().toProto())
                .build();
        ObjectNode input = obj("""
                {"schema": {"descriptorSetBase64": ""}, "type": "actions.test.Person",
                 "message": {"name": "Jo"}}
                """);
        ((ObjectNode) input.get("schema")).put("descriptorSetBase64",
                Base64.getEncoder().encodeToString(set.toByteArray()));
        ObjectNode result = catalog.execute("validate-message", input);
        // the validation option survives serialization: rules are still enforced
        assertThat(result.get("valid").asBoolean()).isFalse();
        assertThat(result.get("violations").get(0).get("ruleId").asText())
                .isEqualTo("string.min_len");
    }

    // ---- exactly-one enforcement ----

    @Test
    void moreThanOneSourceFormIsInvalidInput() {
        ObjectNode input = obj("""
                {"schema": {"type": "actions.test.Person", "sources": {"a.proto": "syntax = \\"proto3\\";"}},
                 "message": {}}
                """);
        assertThatThrownBy(() -> catalog.execute("validate-message", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText()).isEqualTo("/schema");
                    assertThat(e.getMessage()).contains("exactly one");
                });
    }

    @Test
    void noSourceFormIsInvalidInput() {
        assertThatThrownBy(() -> catalog.execute("validate-message",
                obj("{\"schema\": {}, \"message\": {}}")))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText()).isEqualTo("/schema");
                });
    }

    @Test
    void nonObjectSchemaIsInvalidInput() {
        assertThatThrownBy(() -> catalog.execute("validate-message",
                obj("{\"schema\": \"actions.test.Person\", \"message\": {}}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    // ---- unknown types and suggestions ----

    @Test
    void unknownRegistryTypeSuggestsSameSimpleNames() {
        ObjectNode input = obj("""
                {"schema": {"type": "wrong.package.Person"}, "message": {}}
                """);
        assertThatThrownBy(() -> catalog.execute("validate-message", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("unknown-type");
                    assertThat(e.getMessage()).contains("actions.test.Person");
                    assertThat(e.details().orElseThrow().get("suggestions"))
                            .extracting(JsonNode::asText)
                            .containsExactly("actions.test.Person");
                });
    }

    @Test
    void unknownTypeWithinInlineSourcesIsUnknownType() {
        ObjectNode input = obj("""
                {"schema": {"sources": {}}, "type": "t.Missing", "message": {}}
                """);
        ((ObjectNode) input.get("schema").get("sources")).put("doc.proto", TestFixtures.DOC_PROTO);
        assertThatThrownBy(() -> catalog.execute("validate-message", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("unknown-type");
                    assertThat(e.getMessage()).contains("t.Missing");
                });
    }

    // ---- inline compilation ----

    @Test
    void brokenInlineSourcesAreCompileFailed() {
        ObjectNode input = obj("""
                {"schema": {"sources": {"bad.proto": "syntax = \\"proto3\\"; message Broken {"}},
                 "message": {}}
                """);
        assertThatThrownBy(() -> catalog.execute("validate-message", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("compile-failed");
                    assertThat(e.details().orElseThrow().get("error").asText()).isNotBlank();
                });
    }

    @Test
    void rootMustNameAFileInTheSources() {
        ObjectNode input = obj("""
                {"schema": {"sources": {}, "root": "other.proto"}, "message": {}}
                """);
        ((ObjectNode) input.get("schema").get("sources")).put("doc.proto", TestFixtures.DOC_PROTO);
        assertThatThrownBy(() -> catalog.execute("validate-message", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/schema/root");
                });
    }

    @Test
    void rootNarrowsTheSourceSetToItsImportClosure() throws Exception {
        ObjectNode input = obj("""
                {"schema": {"sources": {}, "root": "a.proto"}}
                """);
        ObjectNode sources = (ObjectNode) input.get("schema").get("sources");
        sources.put("a.proto", "syntax = \"proto3\"; package s; message A { string x = 1; }");
        sources.put("b.proto", "syntax = \"proto3\"; package s; message B { string y = 1; }");
        ObjectNode result = catalog.execute("list-types", input);
        assertThat(result.get("types").findValuesAsText("fullName"))
                .contains("s.A")
                .doesNotContain("s.B");
    }

    // ---- default message resolution ----

    @Test
    void soleMessageSchemaNeedsNoExplicitType() throws Exception {
        ObjectNode input = obj("""
                {"schema": {"sources": {}}, "message": {"title": "hi"}, "expression": "input.title"}
                """);
        ((ObjectNode) input.get("schema").get("sources")).put("doc.proto", TestFixtures.DOC_PROTO);
        ObjectNode result = catalog.execute("eval-cel", input);
        assertThat(result.get("result").asText()).isEqualTo("hi");
    }

    @Test
    void ambiguousSchemaWithoutTypeIsInvalidInput() {
        ObjectNode input = obj("""
                {"schema": {"sources": {}}, "message": {}}
                """);
        ObjectNode sources = (ObjectNode) input.get("schema").get("sources");
        sources.put("two.proto", "syntax = \"proto3\"; package s; message A {} message B {}");
        assertThatThrownBy(() -> catalog.execute("validate-message", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText()).isEqualTo("/type");
                });
    }

    @Test
    void invalidBase64IsInvalidInput() {
        assertThatThrownBy(() -> catalog.execute("validate-message",
                obj("{\"schema\": {\"descriptorSetBase64\": \"!!!not-base64!!!\"}, \"message\": {}}")))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/schema/descriptorSetBase64");
                });
    }
}
