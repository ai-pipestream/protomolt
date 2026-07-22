package ai.pipestream.proto.actions;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoCompilationException;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;

import java.util.Base64;
import java.util.Map;

/** Compiles inline proto sources into a serialized {@code FileDescriptorSet}. */
final class CompileAction implements ProtoAction {

    @Override
    public String name() {
        return "compile";
    }

    @Override
    public String description() {
        return "Compiles inline .proto source files into a base64-encoded "
                + "google.protobuf.FileDescriptorSet that other actions accept as "
                + "{\"schema\": {\"descriptorSetBase64\": ...}}; returns ok:false with the "
                + "compiler diagnostics instead of failing when the sources do not compile.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        ObjectNode sources = properties.putObject("sources");
        sources.put("type", "object");
        sources.put("description",
                "The .proto files to compile, keyed by import path (e.g. 'example/v1/doc.proto').");
        sources.putObject("additionalProperties").put("type", "string");
        sources.put("minProperties", 1);
        ActionJson.required(schema, "sources");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ObjectNode sourcesNode = Inputs.requireObject(input, "sources");
        ProtoSourceSet.Builder builder = ProtoSourceSet.builder();
        for (Map.Entry<String, JsonNode> entry : sourcesNode.properties()) {
            if (!entry.getValue().isTextual()) {
                throw Inputs.invalidInput("Source file contents must be strings",
                        "/sources/" + entry.getKey());
            }
            builder.add(entry.getKey(), entry.getValue().asText(), "inline");
        }
        ProtoSourceSet sources = builder.build();
        if (sources.isEmpty()) {
            throw Inputs.invalidInput("'sources' must contain at least one proto file", "/sources");
        }
        ObjectNode result = context.objectMapper().createObjectNode();
        CompiledProtos compiled;
        try {
            compiled = new ProtoSourceCompiler().compile(sources);
        } catch (ProtoCompilationException e) {
            result.put("ok", false);
            ArrayNode errors = result.putArray("errors");
            errors.add(e.getMessage());
            return result;
        }
        result.put("ok", true);
        ArrayNode files = result.putArray("files");
        compiled.descriptorSet().getFileList().stream()
                .map(FileDescriptorProto::getName)
                .forEach(files::add);
        result.put("descriptorSetBase64",
                Base64.getEncoder().encodeToString(compiled.descriptorSet().toByteArray()));
        return result;
    }
}
