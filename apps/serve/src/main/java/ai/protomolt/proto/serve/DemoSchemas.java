package ai.protomolt.proto.serve;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema;
import ai.protomolt.proto.search.index.spi.ProtoOptionsIndexingHintSource;
import ai.protomolt.proto.meta.DescriptorMetadata;
import ai.protomolt.proto.registry.GitSchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaReference;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code --demo} seed: a small order-management schema carrying validation rules,
 * indexing hints, and metadata, registered both as resolvable types (so
 * {@code {"type": "demo.shop.v1.Order"}} works on every verb immediately) and as registry
 * subjects (so the registry API and MCP resources have material).
 */
final class DemoSchemas {

    /** The demo schema's subject and import path. */
    static final String SHOP_SUBJECT = "demo/shop/v1/shop.proto";

    /** Option schemas the demo imports, registered as their own subjects. */
    static final List<String> OPTION_SUBJECTS = List.of(
            "ai/protomolt/proto/index/hints/v1/indexing_hints.proto",
            "ai/protomolt/proto/meta/v1/metadata.proto",
            "ai/protomolt/proto/validate/v1/validate.proto");

    private static final String SHOP_RESOURCE = "ai/protomolt/proto/serve/demo_shop.proto";

    private DemoSchemas() {
    }

    /**
     * Compiles the demo schema and registers its types into {@code descriptors}; when
     * {@code store} is non-null, also registers the schema (and the option schemas it
     * imports) as registry subjects.
     */
    /**
     * Stores the demo workflow: compile inline sources on this very server, then list the
     * compiled types — two of the server's own verbs composed into one typed call, so the
     * console's workflows page has something real to open and run.
     */
    static void seedWorkflow(GitSchemaRegistryStore store, int grpcPort) {
        try {
            // The service definition imports first-party contracts, so the workflow's schema
            // carries the whole source closure. Registering the entry file alone would store a
            // schema that cannot be compiled.
            String sources = new ObjectMapper()
                    .writeValueAsString(ProtoMoltServiceSchema.protoSources());
            String workflow = """
                    {"name": "compile-and-list",
                     "schema": {"sources": %s},
                     "inputType": "ai.protomolt.proto.grpc.service.v1.CompileRequest",
                     "steps": [
                       {"name": "compiled", "target": "127.0.0.1:%d",
                        "method": "ai.protomolt.proto.grpc.service.v1.ProtoMoltService/Compile",
                        "rules": ["sources = input.sources"]},
                       {"name": "types", "target": "127.0.0.1:%d",
                        "method": "ai.protomolt.proto.grpc.service.v1.ProtoMoltService/ListTypes",
                        "rules": ["schema.descriptor_set_base64 = compiled.descriptor_set_base64"]}
                     ]}
                    """.formatted(sources, grpcPort, grpcPort);
            store.putWorkflow("compile-and-list", workflow);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to seed the demo workflow", e);
        }
    }

    static void seed(DescriptorRegistry descriptors, GitSchemaRegistryStore store) {
        ProtoSourceSet.Builder sources = ProtoSourceSet.builder();
        for (String path : OPTION_SUBJECTS) {
            sources.add(path, resource(path), "demo");
        }
        String shopText = resource(SHOP_RESOURCE);
        sources.add(SHOP_SUBJECT, shopText, "demo");

        CompiledProtos compiled;
        try {
            compiled = new ProtoSourceCompiler().compile(sources.build());
        } catch (Exception e) {
            throw new IllegalStateException("The bundled demo schema failed to compile", e);
        }
        // Re-parse with the option extensions registered so the validation rules, metadata,
        // and indexing hints are readable extensions instead of unknown fields.
        for (FileDescriptor file : linkWithExtensions(compiled)) {
            descriptors.registerFile(file);
        }

        if (store == null) {
            return;
        }
        try {
            for (String path : OPTION_SUBJECTS) {
                store.register(path, resource(path), List.of());
            }
            List<SchemaReference> references = OPTION_SUBJECTS.stream()
                    .map(path -> new SchemaReference(path, path, 1))
                    .toList();
            store.register(SHOP_SUBJECT, shopText, references);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to seed the demo registry", e);
        }
    }

    private static List<FileDescriptor> linkWithExtensions(CompiledProtos compiled) {
        ExtensionRegistry extensions = ExtensionRegistry.newInstance();
        ValidationResult.registerExtensions(extensions);
        DescriptorMetadata.registerExtensions(extensions);
        ProtoOptionsIndexingHintSource.registerExtensions(extensions);
        try {
            FileDescriptorSet reparsed =
                    FileDescriptorSet.parseFrom(compiled.descriptorSet().toByteArray(), extensions);
            Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
            for (FileDescriptorProto proto : reparsed.getFileList()) {
                byName.put(proto.getName(), proto);
            }
            Map<String, FileDescriptor> built = new LinkedHashMap<>();
            for (FileDescriptorProto proto : reparsed.getFileList()) {
                build(proto, byName, built);
            }
            return List.copyOf(built.values());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to link the demo schema", e);
        }
    }

    private static FileDescriptor build(FileDescriptorProto proto,
                                        Map<String, FileDescriptorProto> byName,
                                        Map<String, FileDescriptor> built)
            throws Descriptors.DescriptorValidationException {
        FileDescriptor existing = built.get(proto.getName());
        if (existing != null) {
            return existing;
        }
        FileDescriptor[] dependencies = new FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < proto.getDependencyCount(); i++) {
            String dep = proto.getDependency(i);
            FileDescriptorProto depProto = byName.get(dep);
            if (depProto == null) {
                throw new IllegalStateException("Demo schema dependency missing from set: " + dep);
            }
            dependencies[i] = build(depProto, byName, built);
        }
        FileDescriptor file = FileDescriptor.buildFrom(proto, dependencies);
        built.put(proto.getName(), file);
        return file;
    }

    private static String resource(String path) {
        try (InputStream in = DemoSchemas.class.getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + path, e);
        }
    }
}
