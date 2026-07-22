package ai.pipestream.proto.codegen;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.SchemaResolver;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.compiler.PluginProtos;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * {@code generate-stubs}: produce client and message source code for a schema, live, with no
 * protoc installation. What quarkus-grpc-zero does for a build, this does as a registry
 * operation: descriptors in, generated source files out.
 *
 * <p>Generators are protoc's own, running as WebAssembly: {@code java}, {@code kotlin},
 * {@code python}, {@code cpp}, {@code csharp}, {@code ruby}, {@code php}, and {@code objc}
 * message code, and {@code grpc-java} service stubs. Generator-reported failures return
 * {@code ok: false} with protoc's message; malformed input is an {@code invalid-input} error.</p>
 */
public final class GenerateStubsAction implements ProtoAction {

    @Override
    public String name() {
        return "generate-stubs";
    }

    @Override
    public String description() {
        return "Generates source code from a schema with protoc-as-WebAssembly, no native "
                + "toolchain: java, kotlin, python, cpp, csharp, ruby, php, and objc message code, "
                + "'grpc-java' service stubs. Returns the generated files as {name, content}. Combine 'java' and "
                + "'grpc-java' for a complete Java gRPC client.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        ObjectNode schemaSource = properties.putObject("schema");
        schemaSource.put("type", "object");
        schemaSource.put("description", "Schema source; provide exactly one of 'type', 'sources', "
                + "'descriptorSetBase64'. A registry subject's text passed as 'sources' works "
                + "for any registered schema.");
        ObjectNode generators = properties.putObject("generators");
        generators.put("type", "array");
        generators.put("description", "Generators to run, in order; default [\"java\"].");
        generators.putObject("items")
                .put("type", "string")
                .putPOJO("enum", List.of("java", "kotlin", "grpc-java", "python", "cpp", "csharp", "ruby", "php", "objc"));
        generators.put("minItems", 1);
        ObjectNode files = properties.putObject("files");
        files.put("type", "array");
        files.put("description", "Proto file paths within the schema to generate for; defaults "
                + "to every non-google file.");
        files.putObject("items").put("type", "string");
        ObjectNode parameter = properties.putObject("parameter");
        parameter.put("type", "string");
        parameter.put("description", "Optional generator parameter string, as protoc's "
                + "--<gen>_opt (applied to every requested generator).");
        ArrayNode required = schema.putArray("required");
        required.add("schema");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        List<WasmProtoc.Plugin> plugins = parseGenerators(input);
        // protoc requires every transitive dependency present, dependencies before dependents;
        // the resolved descriptor graph has them all (including well-known types), so the
        // request is built from a deps-first walk rather than the raw descriptor set.
        List<FileDescriptorProto> orderedFiles = topologicalFiles(schema.files());
        Set<String> knownFiles = new LinkedHashSet<>();
        orderedFiles.forEach(f -> knownFiles.add(f.getName()));
        List<String> filesToGenerate = parseFiles(input, knownFiles);

        PluginProtos.CodeGeneratorRequest.Builder request =
                PluginProtos.CodeGeneratorRequest.newBuilder();
        request.addAllProtoFile(orderedFiles);
        request.addAllFileToGenerate(filesToGenerate);
        JsonNode parameter = input.get("parameter");
        if (parameter != null && parameter.isTextual()) {
            request.setParameter(parameter.asText());
        }

        ObjectNode result = context.objectMapper().createObjectNode();
        ArrayNode files = context.objectMapper().createArrayNode();
        for (WasmProtoc.Plugin plugin : plugins) {
            PluginProtos.CodeGeneratorResponse response = WasmProtoc.run(plugin, request.build());
            if (!response.getError().isEmpty()) {
                result.put("ok", false);
                result.put("generator", plugin.wrapperArg());
                result.put("error", response.getError());
                return result;
            }
            for (PluginProtos.CodeGeneratorResponse.File file : response.getFileList()) {
                ObjectNode entry = files.addObject();
                entry.put("name", file.getName());
                entry.put("generator", plugin.wrapperArg());
                entry.put("content", file.getContent());
            }
        }
        result.put("ok", true);
        result.set("files", files);
        result.put("fileCount", files.size());
        return result;
    }

    private static List<FileDescriptorProto> topologicalFiles(
            List<com.google.protobuf.Descriptors.FileDescriptor> roots) {
        Set<String> seen = new LinkedHashSet<>();
        List<FileDescriptorProto> ordered = new ArrayList<>();
        for (var root : roots) {
            addDepsFirst(root, seen, ordered);
        }
        return ordered;
    }

    private static void addDepsFirst(com.google.protobuf.Descriptors.FileDescriptor file,
                                     Set<String> seen, List<FileDescriptorProto> ordered) {
        if (!seen.add(file.getName())) {
            return;
        }
        for (var dependency : file.getDependencies()) {
            addDepsFirst(dependency, seen, ordered);
        }
        ordered.add(file.toProto());
    }

    private static List<WasmProtoc.Plugin> parseGenerators(ObjectNode input) throws ActionException {
        JsonNode node = input.get("generators");
        if (node == null || node.isNull()) {
            return List.of(WasmProtoc.Plugin.JAVA);
        }
        if (!node.isArray() || node.isEmpty()) {
            throw invalidInput("'generators' must be a non-empty array of generator names",
                    "/generators");
        }
        List<WasmProtoc.Plugin> plugins = new ArrayList<>();
        for (JsonNode element : node) {
            String name = element.asText("");
            switch (name.toLowerCase(Locale.ROOT)) {
                case "java" -> plugins.add(WasmProtoc.Plugin.JAVA);
                case "kotlin" -> plugins.add(WasmProtoc.Plugin.KOTLIN);
                case "grpc-java" -> plugins.add(WasmProtoc.Plugin.GRPC_JAVA);
                case "python" -> plugins.add(WasmProtoc.Plugin.PYTHON);
                case "cpp" -> plugins.add(WasmProtoc.Plugin.CPP);
                case "csharp" -> plugins.add(WasmProtoc.Plugin.CSHARP);
                case "ruby" -> plugins.add(WasmProtoc.Plugin.RUBY);
                case "php" -> plugins.add(WasmProtoc.Plugin.PHP);
                case "objc" -> plugins.add(WasmProtoc.Plugin.OBJC);
                default -> throw invalidInput("Unknown generator '" + name
                        + "'; supported: java, kotlin, grpc-java, python, cpp, csharp, ruby, php, objc",
                        "/generators");
            }
        }
        return plugins;
    }

    private static List<String> parseFiles(ObjectNode input, Set<String> knownFiles)
            throws ActionException {
        JsonNode node = input.get("files");
        if (node == null || node.isNull()) {
            return knownFiles.stream()
                    .filter(name -> !name.startsWith("google/protobuf/"))
                    .toList();
        }
        if (!node.isArray() || node.isEmpty()) {
            throw invalidInput("'files' must be a non-empty array of proto paths", "/files");
        }
        List<String> files = new ArrayList<>();
        for (JsonNode element : node) {
            String name = element.asText("");
            if (!knownFiles.contains(name)) {
                throw invalidInput("File '" + name + "' is not in the schema; present: "
                        + String.join(", ", knownFiles), "/files");
            }
            files.add(name);
        }
        return files;
    }

    private static ActionException invalidInput(String message, String pointer) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("pointer", pointer);
        return new ActionException("invalid-input", message + " (at '" + pointer + "')", details);
    }
}
