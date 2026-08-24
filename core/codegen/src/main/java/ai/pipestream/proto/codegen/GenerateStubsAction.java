package ai.pipestream.proto.codegen;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.JsonAction;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.SchemaResolver;
import ai.pipestream.proto.actions.Scopes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.compiler.PluginProtos;

import com.google.protobuf.Descriptors.Descriptor;
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
public final class GenerateStubsAction implements JsonAction {

    /**
     * Proto enum values carry their type name, so the wire form of the Java generator is
     * CODE_GENERATOR_JAVA. The plugin enum does not, and the two are otherwise the same
     * vocabulary.
     */
    private static final String GENERATOR_PREFIX = "CODE_GENERATOR_";

    @Override
    public String name() {
        return "generate-stubs";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Generates source code from a schema with protoc-as-WebAssembly, no native "
                + "toolchain: java, kotlin, python, cpp, csharp, ruby, php, and objc message code, "
                + "'grpc-java' service stubs. Returns the generated files as {name, content}. Combine 'java' and "
                + "'grpc-java' for a complete Java gRPC client.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("GenerateStubsRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("GenerateStubsResponse");
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
            // The contract names generators with an enum, so an unknown one is refused
            // before the verb runs. Proto enum values carry their type name, and the
            // remainder is the plugin's own name with dashes written as underscores.
            String name = element.asText("");
            String value = name.startsWith(GENERATOR_PREFIX)
                    ? name.substring(GENERATOR_PREFIX.length()) : name;
            try {
                plugins.add(WasmProtoc.Plugin.valueOf(value.toUpperCase(Locale.ROOT)
                        .replace('-', '_')));
            } catch (IllegalArgumentException e) {
                throw invalidInput("'" + name + "' names a generator this build does not "
                        + "embed", "/generators");
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
