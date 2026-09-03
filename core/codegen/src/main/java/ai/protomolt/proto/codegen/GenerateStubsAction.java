package ai.protomolt.proto.codegen;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.SchemaResolver;
import ai.protomolt.proto.actions.Scopes;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Message;
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
public final class GenerateStubsAction implements ProtoAction {

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
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        List<WasmProtoc.Plugin> plugins = generators(input);
        // protoc requires every transitive dependency present, dependencies before dependents;
        // the resolved descriptor graph has them all (including well-known types), so the
        // request is built from a deps-first walk rather than the raw descriptor set.
        List<FileDescriptorProto> orderedFiles = topologicalFiles(schema.files());
        Set<String> knownFiles = new LinkedHashSet<>();
        orderedFiles.forEach(f -> knownFiles.add(f.getName()));
        List<String> filesToGenerate = filesToGenerate(input, knownFiles);

        PluginProtos.CodeGeneratorRequest.Builder request =
                PluginProtos.CodeGeneratorRequest.newBuilder();
        request.addAllProtoFile(orderedFiles);
        request.addAllFileToGenerate(filesToGenerate);
        request.setParameter(Fields.string(input, "parameter"));

        Reply result = Reply.of(responseType());
        int generated = 0;
        for (WasmProtoc.Plugin plugin : plugins) {
            PluginProtos.CodeGeneratorResponse response = WasmProtoc.run(plugin, request.build());
            if (!response.getError().isEmpty()) {
                return Reply.of(responseType())
                        .set("ok", false)
                        .set("generator", plugin.wrapperArg())
                        .set("error", response.getError())
                        .build();
            }
            for (PluginProtos.CodeGeneratorResponse.File file : response.getFileList()) {
                result.append("files")
                        .set("name", file.getName())
                        .set("generator", plugin.wrapperArg())
                        .set("content", file.getContent())
                        .build();
                generated++;
            }
        }
        return result.set("ok", true).set("fileCount", generated).build();
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

    /** The generators to run; none named selects Java. */
    private static List<WasmProtoc.Plugin> generators(Message input) throws ActionException {
        List<?> named = Fields.list(input, "generators");
        if (named.isEmpty()) {
            return List.of(WasmProtoc.Plugin.JAVA);
        }
        List<WasmProtoc.Plugin> plugins = new ArrayList<>();
        for (Object element : named) {
            // The contract names generators with an enum, so an unknown one is refused
            // before the verb runs. Proto enum values carry their type name, and the
            // remainder is the plugin's own name with dashes written as underscores.
            String name = element.toString();
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

    /** The files to generate for; none named selects every non-google file in the schema. */
    private static List<String> filesToGenerate(Message input, Set<String> knownFiles)
            throws ActionException {
        List<String> named = Fields.strings(input, "files");
        if (named.isEmpty()) {
            return knownFiles.stream()
                    .filter(name -> !name.startsWith("google/protobuf/"))
                    .toList();
        }
        for (String name : named) {
            if (!knownFiles.contains(name)) {
                throw invalidInput("File '" + name + "' is not in the schema; present: "
                        + String.join(", ", knownFiles), "/files");
            }
        }
        return named;
    }

    private static ActionException invalidInput(String message, String pointer) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("pointer", pointer);
        return new ActionException("invalid-input", message + " (at '" + pointer + "')", details);
    }
}
