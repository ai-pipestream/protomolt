package ai.pipestream.proto.sources;

import com.google.protobuf.DescriptorProtos.MethodOptions;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the {@link LinkedOptionsRepair} branches the annotation-family tests do not reach:
 * enum and enum-value options, service and method options, oneof options, options on extension
 * field declarations, options on nested messages, and the scalar coercion paths of
 * {@link LinkedOptionsEncoder} (signed/unsigned integers, doubles, booleans, strings, repeated
 * scalars). Custom extensions are read back through unknown fields — exactly what a consumer
 * without the generated extension class sees.
 */
class ProtoSourceCompilerOptionsRepairTest {

    private final ProtoSourceCompiler compiler = new ProtoSourceCompiler();

    private FileDescriptor compileSingle(String path, String content) throws Exception {
        CompiledProtos compiled = compiler.compile(ProtoSourceSet.builder()
                .add(path, content, "test")
                .build());
        return compiled.descriptorFor(path).orElseThrow();
    }

    @Test
    void enumAndEnumValueOptionsSurviveRepair() throws Exception {
        FileDescriptor file = compileSingle("status.proto", """
                syntax = "proto3";
                package test.repair;
                enum Status {
                  option deprecated = true;
                  STATUS_UNSPECIFIED = 0;
                  STATUS_ACTIVE = 1 [deprecated = true];
                  STATUS_LIVE = 2;
                }
                """);

        EnumDescriptor status = file.findEnumTypeByName("Status");
        assertThat(status.getOptions().getDeprecated()).isTrue();
        assertThat(status.findValueByName("STATUS_ACTIVE").getOptions().getDeprecated()).isTrue();
        assertThat(status.findValueByName("STATUS_LIVE").getOptions().getDeprecated()).isFalse();
    }

    @Test
    void serviceAndMethodOptionsSurviveRepair() throws Exception {
        FileDescriptor file = compileSingle("svc.proto", """
                syntax = "proto3";
                package test.repair;
                message Req {}
                message Resp {}
                service Svc {
                  option deprecated = true;
                  rpc Call(Req) returns (Resp) {
                    option deprecated = true;
                    option idempotency_level = IDEMPOTENT;
                  }
                  rpc Quiet(Req) returns (Resp);
                }
                """);

        var service = file.findServiceByName("Svc");
        assertThat(service.getOptions().getDeprecated()).isTrue();
        var call = service.findMethodByName("Call");
        assertThat(call.getOptions().getDeprecated()).isTrue();
        // An enum-valued standard option, coerced from its simple name.
        assertThat(call.getOptions().getIdempotencyLevel())
                .isEqualTo(MethodOptions.IdempotencyLevel.IDEMPOTENT);
        assertThat(service.findMethodByName("Quiet").getOptions().getDeprecated()).isFalse();
    }

    @Test
    void oneofCustomOptionSurvivesRepair() throws Exception {
        FileDescriptor file = compileSingle("choice.proto", """
                syntax = "proto3";
                package test.repair;
                import "google/protobuf/descriptor.proto";
                extend google.protobuf.OneofOptions { string tag = 51240; }
                message Pick {
                  oneof choice {
                    option (tag) = "primary";
                    string a = 1;
                    int32 b = 2;
                  }
                }
                """);

        var choice = file.findMessageTypeByName("Pick").getOneofs().get(0);
        assertThat(choice.getName()).isEqualTo("choice");
        // No generated class exists for this test extension: it lands in unknown fields.
        var unknown = choice.getOptions().getUnknownFields();
        assertThat(unknown.hasField(51240)).isTrue();
        assertThat(unknown.getField(51240).getLengthDelimitedList().get(0).toStringUtf8())
                .isEqualTo("primary");
    }

    @Test
    void extensionFieldOptionsSurviveRepair() throws Exception {
        // Regression test: repairMessage used to throw "encoded descriptor has no field #N"
        // for any extend field carrying options, because Wire links extension fields onto their
        // extendee and the repair walk mistook them for regular fields. Fixed by skipping
        // extension fields in that walk (their options are repaired from the Extend blocks).
        FileDescriptor file = compileSingle("ext.proto", """
                syntax = "proto2";
                package test.repair;
                message Base { extensions 100 to 199; }
                extend Base {
                  optional string note = 100 [deprecated = true];
                  optional int32 plain = 101;
                }
                """);

        assertThat(file.findExtensionByName("note").getOptions().getDeprecated()).isTrue();
        assertThat(file.findExtensionByName("plain").getOptions().getDeprecated()).isFalse();
    }

    @Test
    void nestedExtendDeclarationsFailLoud() {
        // Wire's SchemaEncoder drops nested extend declarations from the encoded descriptor
        // entirely, so the repair pass fails the compile on the model/encoder mismatch instead
        // of emitting a descriptor set that lacks the declared extension. Regression test:
        // without options on the extension field this used to compile silently, producing an
        // incomplete descriptor; with options it failed with a less clear message.
        assertThatThrownBy(() -> compileSingle("nested_ext.proto", """
                syntax = "proto2";
                package test.repair;
                message Wrapper {
                  message Base { extensions 100 to 199; }
                  extend Base {
                    optional string tag = 100;
                  }
                }
                """))
                .isInstanceOf(ProtoCompilationException.class)
                .hasMessageContaining("extension field #100");

        assertThatThrownBy(() -> compileSingle("nested_ext.proto", """
                syntax = "proto2";
                package test.repair;
                message Wrapper {
                  message Base { extensions 100 to 199; }
                  extend Base {
                    optional string tag = 100 [deprecated = true];
                  }
                }
                """))
                .isInstanceOf(ProtoCompilationException.class)
                .hasMessageContaining("extension field #100");
    }

    @Test
    void nestedMessageOptionsSurviveRepair() throws Exception {
        FileDescriptor file = compileSingle("nested.proto", """
                syntax = "proto3";
                package test.repair;
                message Outer {
                  message Inner { option deprecated = true; }
                  message Plain {}
                }
                """);

        var outer = file.findMessageTypeByName("Outer");
        assertThat(outer.getOptions().getDeprecated()).isFalse();
        assertThat(outer.findNestedTypeByName("Inner").getOptions().getDeprecated()).isTrue();
        assertThat(outer.findNestedTypeByName("Plain").getOptions().getDeprecated()).isFalse();
    }

    @Test
    void scalarOptionCoercionsSurviveRepair() throws Exception {
        FileDescriptor file = compileSingle("scalars.proto", """
                syntax = "proto3";
                package test.repair;
                import "google/protobuf/descriptor.proto";
                extend google.protobuf.FieldOptions {
                  int32 priority = 51241;
                  uint64 mask = 51242;
                  double weight = 51243;
                  bool flag = 51244;
                  string note = 51245;
                }
                message Doc {
                  string a = 1 [(priority) = -3];
                  string b = 2 [(mask) = 4294967296];
                  string c = 3 [(weight) = 1.5];
                  string d = 4 [(flag) = true];
                  string e = 5 [(note) = "hello"];
                }
                """);

        var doc = file.findMessageTypeByName("Doc");
        var a = doc.findFieldByName("a").getOptions().getUnknownFields().getField(51241);
        assertThat(a.getVarintList().get(0)).isEqualTo(-3L);
        var b = doc.findFieldByName("b").getOptions().getUnknownFields().getField(51242);
        // 2^32: proves the unsigned path is not narrowing through a signed int.
        assertThat(b.getVarintList().get(0)).isEqualTo(4294967296L);
        var c = doc.findFieldByName("c").getOptions().getUnknownFields().getField(51243);
        assertThat(Double.longBitsToDouble(c.getFixed64List().get(0))).isEqualTo(1.5);
        var d = doc.findFieldByName("d").getOptions().getUnknownFields().getField(51244);
        assertThat(d.getVarintList().get(0)).isEqualTo(1L);
        var e = doc.findFieldByName("e").getOptions().getUnknownFields().getField(51245);
        assertThat(e.getLengthDelimitedList().get(0).toStringUtf8()).isEqualTo("hello");
    }

    @Test
    void repeatedScalarOptionEntriesUnionIntoOneList() throws Exception {
        FileDescriptor file = compileSingle("repeated.proto", """
                syntax = "proto3";
                package test.repair;
                import "google/protobuf/descriptor.proto";
                extend google.protobuf.FieldOptions { repeated string tag = 51246; }
                message Doc {
                  string a = 1 [(tag) = "x", (tag) = "y"];
                }
                """);

        var tags = file.findMessageTypeByName("Doc").findFieldByName("a")
                .getOptions().getUnknownFields().getField(51246).getLengthDelimitedList();
        assertThat(tags).hasSize(2);
        assertThat(tags.get(0).toStringUtf8()).isEqualTo("x");
        assertThat(tags.get(1).toStringUtf8()).isEqualTo("y");
    }

    @Test
    void optionBytesOnTheWireMatchTheLinkedDescriptors() throws Exception {
        // The encoded descriptor set (what gets published to a registry) must carry the same
        // option payloads the linked FileDescriptors expose.
        FileDescriptor file = compileSingle("wired.proto", """
                syntax = "proto3";
                package test.repair;
                message Doc {
                  option deprecated = true;
                  string a = 1 [deprecated = true];
                }
                """);
        assertThat(file.toProto().getMessageType(0).getOptions().getDeprecated()).isTrue();
        assertThat(file.toProto().getMessageType(0).getField(0).getOptions().getDeprecated())
                .isTrue();
    }
}
