package ai.protomolt.proto.sources;

import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.GroupElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link OptionStrippingRewriter} drops every option statement while preserving structure.
 * Each test strips a source and re-parses the rendered output with Wire's parser, asserting on
 * the element tree rather than on rendered text.
 */
class OptionStrippingRewriterTest {

    private static ProtoFileElement stripAndReparse(String path, String content)
            throws ProtoCompilationException {
        String stripped = OptionStrippingRewriter.strip(path, content);
        return ProtoParser.Companion.parse(Location.Companion.get(path), stripped);
    }

    @Test
    void fileLevelOptionsAreStrippedPackageAndSyntaxKept() throws Exception {
        ProtoFileElement parsed = stripAndReparse("doc.proto", """
                syntax = "proto3";
                package example.doc;
                option java_package = "com.example.doc";
                option java_multiple_files = true;
                option (custom.file_option) = {key: "value"};
                message Doc { string title = 1; }
                """);
        assertThat(parsed.getOptions()).isEmpty();
        assertThat(parsed.getPackageName()).isEqualTo("example.doc");
        assertThat(parsed.getSyntax()).isEqualTo(com.squareup.wire.Syntax.PROTO_3);
        assertThat(parsed.getTypes()).hasSize(1);
    }

    @Test
    void messageAndFieldOptionsAreStrippedButDefaultsAndJsonNamesSurvive() throws Exception {
        ProtoFileElement parsed = stripAndReparse("doc.proto", """
                syntax = "proto2";
                package example;
                message Doc {
                  option deprecated = true;
                  optional int32 count = 1 [default = 42, json_name = "c", deprecated = true];
                  optional string name = 2 [default = "untitled"];
                }
                """);
        MessageElement doc = (MessageElement) parsed.getTypes().get(0);
        assertThat(doc.getOptions()).isEmpty();
        assertThat(doc.getFields()).hasSize(2);
        var count = doc.getFields().get(0);
        assertThat(count.getOptions()).isEmpty();
        assertThat(count.getDefaultValue()).isEqualTo("42");
        assertThat(count.getJsonName()).isEqualTo("c");
        assertThat(count.getTag()).isEqualTo(1);
        assertThat(doc.getFields().get(1).getDefaultValue()).isEqualTo("untitled");
    }

    @Test
    void nestedMessageOptionsAreStrippedRecursively() throws Exception {
        ProtoFileElement parsed = stripAndReparse("nested.proto", """
                syntax = "proto3";
                message Outer {
                  option (custom.message_option) = "outer";
                  message Inner {
                    option (custom.message_option) = "inner";
                    string id = 1 [(custom.field_option) = "deep"];
                  }
                }
                """);
        MessageElement outer = (MessageElement) parsed.getTypes().get(0);
        assertThat(outer.getOptions()).isEmpty();
        MessageElement inner = (MessageElement) outer.getNestedTypes().get(0);
        assertThat(inner.getOptions()).isEmpty();
        assertThat(inner.getFields().get(0).getOptions()).isEmpty();
    }

    @Test
    void allowAliasSurvivesWhileOtherEnumOptionsAreStripped() throws Exception {
        ProtoFileElement parsed = stripAndReparse("status.proto", """
                syntax = "proto3";
                enum Status {
                  option allow_alias = true;
                  option deprecated = true;
                  STATUS_UNSPECIFIED = 0;
                  STATUS_ACTIVE = 1 [deprecated = true];
                  STATUS_ON = 1;
                }
                """);
        EnumElement status = (EnumElement) parsed.getTypes().get(0);
        assertThat(status.getOptions()).hasSize(1);
        assertThat(status.getOptions().get(0).getName()).isEqualTo("allow_alias");
        assertThat(status.getConstants()).hasSize(3);
        assertThat(status.getConstants().get(1).getTag()).isEqualTo(1);
        assertThat(status.getConstants().get(2).getTag()).isEqualTo(1);
        for (var constant : status.getConstants()) {
            assertThat(constant.getOptions()).isEmpty();
        }
    }

    @Test
    void oneOfOptionsAreStrippedFieldsKept() throws Exception {
        ProtoFileElement parsed = stripAndReparse("choice.proto", """
                syntax = "proto3";
                message Pick {
                  oneof choice {
                    option (custom.oneof_option) = "x";
                    string a = 1;
                    int32 b = 2;
                  }
                }
                """);
        MessageElement pick = (MessageElement) parsed.getTypes().get(0);
        assertThat(pick.getOneOfs()).hasSize(1);
        var choice = pick.getOneOfs().get(0);
        assertThat(choice.getName()).isEqualTo("choice");
        assertThat(choice.getOptions()).isEmpty();
        assertThat(choice.getFields()).hasSize(2);
    }

    @Test
    void serviceAndRpcOptionsAreStrippedStreamingFlagsKept() throws Exception {
        ProtoFileElement parsed = stripAndReparse("svc.proto", """
                syntax = "proto3";
                message Req {}
                message Resp {}
                service Svc {
                  option deprecated = true;
                  rpc Unary(Req) returns (Resp) { option idempotency_level = IDEMPOTENT; }
                  rpc Stream(stream Req) returns (stream Resp) {}
                }
                """);
        assertThat(parsed.getServices()).hasSize(1);
        var service = parsed.getServices().get(0);
        assertThat(service.getOptions()).isEmpty();
        assertThat(service.getRpcs()).hasSize(2);
        var unary = service.getRpcs().get(0);
        assertThat(unary.getOptions()).isEmpty();
        assertThat(unary.getRequestStreaming()).isFalse();
        assertThat(unary.getResponseStreaming()).isFalse();
        var stream = service.getRpcs().get(1);
        assertThat(stream.getRequestStreaming()).isTrue();
        assertThat(stream.getResponseStreaming()).isTrue();
    }

    @Test
    void extendFieldOptionsAreStrippedExtensionKept() throws Exception {
        ProtoFileElement parsed = stripAndReparse("ext.proto", """
                syntax = "proto2";
                message Base { extensions 100 to 199; }
                extend Base {
                  optional string note = 100 [deprecated = true, (custom.opt) = "v"];
                }
                """);
        assertThat(parsed.getExtendDeclarations()).hasSize(1);
        var extend = parsed.getExtendDeclarations().get(0);
        assertThat(extend.getName()).isEqualTo("Base");
        assertThat(extend.getFields()).hasSize(1);
        assertThat(extend.getFields().get(0).getOptions()).isEmpty();
        assertThat(extend.getFields().get(0).getTag()).isEqualTo(100);
    }

    @Test
    void proto2GroupsSurviveStripping() throws Exception {
        ProtoFileElement parsed = stripAndReparse("group.proto", """
                syntax = "proto2";
                message M {
                  optional group Result = 1 {
                    optional string url = 2 [deprecated = true];
                  }
                }
                """);
        MessageElement m = (MessageElement) parsed.getTypes().get(0);
        assertThat(m.getGroups()).hasSize(1);
        GroupElement group = m.getGroups().get(0);
        assertThat(group.getName()).isEqualTo("Result");
        assertThat(group.getTag()).isEqualTo(1);
        assertThat(group.getFields()).hasSize(1);
        assertThat(group.getFields().get(0).getOptions()).isEmpty();
    }

    @Test
    void reservedRangesAndNamesSurviveStripping() throws Exception {
        ProtoFileElement parsed = stripAndReparse("reserved.proto", """
                syntax = "proto3";
                message M {
                  option deprecated = true;
                  reserved 5 to 8, 10;
                  reserved "old_field";
                  string kept = 1;
                }
                """);
        MessageElement m = (MessageElement) parsed.getTypes().get(0);
        // One ReservedElement per `reserved` statement.
        assertThat(m.getReserveds()).hasSize(2);
        assertThat(m.getFields()).hasSize(1);
    }

    @Test
    void documentationSurvivesStripping() throws Exception {
        ProtoFileElement parsed = stripAndReparse("docs.proto", """
                syntax = "proto3";
                // A documented message.
                message Doc {
                  // A documented field.
                  string title = 1 [deprecated = true];
                }
                """);
        MessageElement doc = (MessageElement) parsed.getTypes().get(0);
        assertThat(doc.getDocumentation()).contains("documented message");
        assertThat(doc.getFields().get(0).getDocumentation()).contains("documented field");
    }

    @Test
    void importsSurviveStripping() throws Exception {
        ProtoFileElement parsed = stripAndReparse("importer.proto", """
                syntax = "proto3";
                import "other/base.proto";
                import public "other/api.proto";
                option java_package = "com.example";
                message M {}
                """);
        assertThat(parsed.getImports()).containsExactly("other/base.proto");
        assertThat(parsed.getPublicImports()).containsExactly("other/api.proto");
    }

    @Test
    void unparseableSourceFailsLoudNamingThePath() {
        assertThatThrownBy(() -> OptionStrippingRewriter.strip("garbage.proto", "this is not proto"))
                .isInstanceOf(ProtoCompilationException.class)
                .hasMessageContaining("garbage.proto");
    }
}
