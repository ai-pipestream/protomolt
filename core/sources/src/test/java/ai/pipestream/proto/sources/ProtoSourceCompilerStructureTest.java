package ai.pipestream.proto.sources;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Structural coverage of {@link ProtoSourceCompiler} beyond messages and options: services and
 * streaming flags, proto2 labels/defaults/groups, proto3 optional, well-known-type imports
 * encoded into the descriptor set, and path-safety variants.
 */
class ProtoSourceCompilerStructureTest {

    private final ProtoSourceCompiler compiler = new ProtoSourceCompiler();

    @Test
    void compilesServicesWithStreamingFlags() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("svc.proto", """
                        syntax = "proto3";
                        package example;
                        message Req { string q = 1; }
                        message Resp { string a = 1; }
                        service Api {
                          rpc Unary(Req) returns (Resp);
                          rpc ClientStream(stream Req) returns (Resp);
                          rpc ServerStream(Req) returns (stream Resp);
                          rpc Bidi(stream Req) returns (stream Resp);
                        }
                        """, "test")
                .build();

        FileDescriptor file = compiler.compile(set).descriptorFor("svc.proto").orElseThrow();
        var api = file.findServiceByName("Api");
        assertThat(api.getMethods()).hasSize(4);

        MethodDescriptor unary = api.findMethodByName("Unary");
        assertThat(unary.isClientStreaming()).isFalse();
        assertThat(unary.isServerStreaming()).isFalse();
        assertThat(unary.getInputType().getFullName()).isEqualTo("example.Req");
        assertThat(unary.getOutputType().getFullName()).isEqualTo("example.Resp");

        assertThat(api.findMethodByName("ClientStream").isClientStreaming()).isTrue();
        assertThat(api.findMethodByName("ClientStream").isServerStreaming()).isFalse();
        assertThat(api.findMethodByName("ServerStream").isClientStreaming()).isFalse();
        assertThat(api.findMethodByName("ServerStream").isServerStreaming()).isTrue();
        assertThat(api.findMethodByName("Bidi").isClientStreaming()).isTrue();
        assertThat(api.findMethodByName("Bidi").isServerStreaming()).isTrue();
    }

    @Test
    void proto2LabelsAndDefaultsSurvive() throws Exception {
        // Groups are deliberately not exercised here: Wire's linker rejects them
        // ("'group' is not supported"), so they never reach the encoder. The option-stripping
        // rewriter preserves them at the parse level (OptionStrippingRewriterTest).
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("legacy.proto", """
                        syntax = "proto2";
                        package legacy;
                        message Record {
                          required string id = 1;
                          optional int32 count = 2 [default = 42];
                          repeated string tags = 3;
                        }
                        """, "test")
                .build();

        FileDescriptor file = compiler.compile(set).descriptorFor("legacy.proto").orElseThrow();
        var record = file.findMessageTypeByName("Record");
        assertThat(record.findFieldByName("id").isRequired()).isTrue();
        FieldDescriptor count = record.findFieldByName("count");
        assertThat(count.isOptional()).isTrue();
        assertThat(count.getDefaultValue()).isEqualTo(42);
        assertThat(record.findFieldByName("tags").isRepeated()).isTrue();
    }

    @Test
    void proto3OptionalFieldsKeepTheirExplicitPresence() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("opt.proto", """
                        syntax = "proto3";
                        package example;
                        message Maybe {
                          optional string present = 1;
                          string plain = 2;
                        }
                        """, "test")
                .build();

        FileDescriptor file = compiler.compile(set).descriptorFor("opt.proto").orElseThrow();
        var maybe = file.findMessageTypeByName("Maybe");
        // Proto3 optional lowers to a synthetic one-field oneof: it has a containing oneof that
        // is not a "real" one, plus explicit presence. A plain field has neither.
        var present = maybe.findFieldByName("present");
        assertThat(present.getContainingOneof()).isNotNull();
        assertThat(present.getRealContainingOneof()).isNull();
        assertThat(present.hasPresence()).isTrue();
        assertThat(maybe.findFieldByName("plain").getContainingOneof()).isNull();
        assertThat(maybe.findFieldByName("plain").hasPresence()).isFalse();
    }

    @Test
    void wireSuppliedImportsAreEncodedIntoTheDescriptorSet() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("stamped.proto", """
                        syntax = "proto3";
                        package example;
                        import "google/protobuf/timestamp.proto";
                        message Stamped { google.protobuf.Timestamp at = 1; }
                        """, "test")
                .build();

        CompiledProtos compiled = compiler.compile(set);
        // The set carries more than the source file: the timestamp import came along.
        assertThat(compiled.descriptorSet().getFileList())
                .extracting(com.google.protobuf.DescriptorProtos.FileDescriptorProto::getName)
                .contains("stamped.proto", "google/protobuf/timestamp.proto");
        assertThat(compiled.descriptorFor("google/protobuf/timestamp.proto")).isPresent();
    }

    @Test
    void packageLessSourceCompilesWithRootFullName() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("bare.proto", """
                        syntax = "proto3";
                        message Bare { string id = 1; }
                        """, "test")
                .build();

        FileDescriptor file = compiler.compile(set).descriptorFor("bare.proto").orElseThrow();
        assertThat(file.getPackage()).isEmpty();
        assertThat(file.findMessageTypeByName("Bare").getFullName()).isEqualTo("Bare");
    }

    @Test
    void absoluteSourcePathIsRejectedAsUnsafe() {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("/absolute.proto", "syntax = \"proto3\";", "evil")
                .build();

        assertThatThrownBy(() -> compiler.compile(set))
                .isInstanceOf(ProtoCompilationException.class)
                .hasMessageContaining("Unsafe source path")
                .hasMessageContaining("evil");
    }

    @Test
    void linkErrorsSurfaceAsCompilationExceptions() {
        // A type that does not exist anywhere: parses fine, fails at link time.
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add("dangling.proto", """
                        syntax = "proto3";
                        message Dangling { no.such.Type ref = 1; }
                        """, "test")
                .build();

        assertThatThrownBy(() -> compiler.compile(set))
                .isInstanceOf(ProtoCompilationException.class);
    }
}
