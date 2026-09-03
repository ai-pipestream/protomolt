package ai.protomolt.proto.codegen;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Direct tests for {@link WasmProtoc}: the plugin enum's wrapper arguments and the raw
 * request/response protocol underneath {@code generate-stubs}.
 */
class WasmProtocTest {

    private static final FileDescriptorProto FOO_PROTO = FileDescriptorProto.newBuilder()
            .setName("t/v1/foo.proto")
            .setPackage("t.v1")
            .setSyntax("proto3")
            .addMessageType(DescriptorProto.newBuilder()
                    .setName("Foo")
                    .addField(FieldDescriptorProto.newBuilder()
                            .setName("id")
                            .setNumber(1)
                            .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(FieldDescriptorProto.Type.TYPE_STRING)))
            .build();

    @Test
    void pluginWrapperArgsAreTheProtocGeneratorNames() {
        assertThat(WasmProtoc.Plugin.JAVA.wrapperArg()).isEqualTo("java");
        assertThat(WasmProtoc.Plugin.KOTLIN.wrapperArg()).isEqualTo("kotlin");
        assertThat(WasmProtoc.Plugin.GRPC_JAVA.wrapperArg()).isEqualTo("grpc-java");
        assertThat(WasmProtoc.Plugin.PYTHON.wrapperArg()).isEqualTo("python");
        assertThat(WasmProtoc.Plugin.CPP.wrapperArg()).isEqualTo("cpp");
        assertThat(WasmProtoc.Plugin.CSHARP.wrapperArg()).isEqualTo("csharp");
        assertThat(WasmProtoc.Plugin.RUBY.wrapperArg()).isEqualTo("ruby");
        assertThat(WasmProtoc.Plugin.PHP.wrapperArg()).isEqualTo("php");
        assertThat(WasmProtoc.Plugin.OBJC.wrapperArg()).isEqualTo("objc");
    }

    @Test
    void runsJavaGeneratorOverRawRequest() {
        CodeGeneratorRequest request = CodeGeneratorRequest.newBuilder()
                .addProtoFile(FOO_PROTO)
                .addFileToGenerate("t/v1/foo.proto")
                .build();

        CodeGeneratorResponse response = WasmProtoc.run(WasmProtoc.Plugin.JAVA, request);

        assertThat(response.getError()).isEmpty();
        assertThat(response.getFileList())
                .extracting(CodeGeneratorResponse.File::getName)
                .anySatisfy(name -> assertThat(name).endsWith("FooOuterClass.java"));
        assertThat(response.getFileList().get(0).getContent()).contains("package t.v1;");
    }

    @Test
    void runsPythonGeneratorOverRawRequest() {
        CodeGeneratorRequest request = CodeGeneratorRequest.newBuilder()
                .addProtoFile(FOO_PROTO)
                .addFileToGenerate("t/v1/foo.proto")
                .build();

        CodeGeneratorResponse response = WasmProtoc.run(WasmProtoc.Plugin.PYTHON, request);

        assertThat(response.getError()).isEmpty();
        assertThat(response.getFileList())
                .extracting(CodeGeneratorResponse.File::getName)
                .containsExactly("t/v1/foo_pb2.py");
    }

    /**
     * Two failure channels exist. Generator-reported problems (a bogus option) arrive in the
     * response's error field. A protocol-level failure — here protoc was asked to generate a
     * file it was never given a descriptor for — exits the wasm process non-zero, and
     * {@link WasmProtoc#run} surfaces that as an {@link IllegalStateException} carrying the
     * generator's stderr. (In practice {@code generate-stubs} validates {@code files} against
     * the schema, so this path is defense in depth.)
     */
    @Test
    void fileToGenerateOutsideTheRequestFailsTheRun() {
        CodeGeneratorRequest request = CodeGeneratorRequest.newBuilder()
                .addProtoFile(FOO_PROTO)
                .addFileToGenerate("t/v1/missing.proto")
                .build();

        assertThatThrownBy(() -> WasmProtoc.run(WasmProtoc.Plugin.JAVA, request))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("protoc java generator failed")
                .hasMessageContaining("missing.proto");
    }

    @Test
    void separateRunsAreIndependent() {
        CodeGeneratorRequest request = CodeGeneratorRequest.newBuilder()
                .addProtoFile(FOO_PROTO)
                .addFileToGenerate("t/v1/foo.proto")
                .build();

        CodeGeneratorResponse first = WasmProtoc.run(WasmProtoc.Plugin.JAVA, request);
        CodeGeneratorResponse second = WasmProtoc.run(WasmProtoc.Plugin.JAVA, request);

        // Same request, same output — no state leaks between wasm instances.
        assertThat(first.getError()).isEmpty();
        assertThat(second.getError()).isEmpty();
        assertThat(second.getFileList().stream()
                        .map(CodeGeneratorResponse.File::getName)
                        .toList())
                .isEqualTo(first.getFileList().stream()
                        .map(CodeGeneratorResponse.File::getName)
                        .toList());
    }
}
