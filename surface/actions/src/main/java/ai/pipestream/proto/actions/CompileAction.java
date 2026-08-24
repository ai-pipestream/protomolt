package ai.pipestream.proto.actions;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoCompilationException;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.Base64;
import java.util.Map;

/** Compiles inline proto sources into a serialized {@code FileDescriptorSet}. */
final class CompileAction implements ProtoAction {

    @Override
    public String name() {
        return "compile";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Compiles inline .proto source files into a base64-encoded "
                + "google.protobuf.FileDescriptorSet that other actions accept as "
                + "{\"schema\": {\"descriptorSetBase64\": ...}}; returns ok:false with the "
                + "compiler diagnostics instead of failing when the sources do not compile.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("CompileRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("CompileResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        // The message bounds the source map and requires at least one entry, so an empty
        // one is refused before the verb runs.
        ProtoSourceSet.Builder builder = ProtoSourceSet.builder();
        Fields.map(input, "sources").forEach((path, text) -> builder.add(path, text, "inline"));
        CompiledProtos compiled;
        try {
            compiled = new ProtoSourceCompiler().compile(builder.build());
        } catch (ProtoCompilationException e) {
            // A compilation failure is a result, not an error: the diagnostics are the answer.
            return Reply.of(responseType())
                    .set("ok", false)
                    .add("errors", e.getMessage())
                    .build();
        }
        return Reply.of(responseType())
                .set("ok", true)
                .addAll("files", compiled.descriptorSet().getFileList().stream()
                        .map(FileDescriptorProto::getName).toList())
                .set("descriptorSetBase64", Base64.getEncoder()
                        .encodeToString(compiled.descriptorSet().toByteArray()))
                .build();
    }
}
