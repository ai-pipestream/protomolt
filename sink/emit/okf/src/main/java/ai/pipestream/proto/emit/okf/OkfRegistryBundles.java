package ai.pipestream.proto.emit.okf;

import ai.pipestream.proto.emit.Bundle;
import ai.pipestream.proto.meta.DescriptorMetadata;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import ai.pipestream.proto.registry.StoredSchema;
import ai.pipestream.proto.registry.StoredSchemaSources;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistry;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Renders a whole schema registry as one OKF bundle: every subject becomes a
 * {@code Registry Subject} concept carrying its version table and links to the message
 * types its latest schema declares, and every declared type gets the standard concept
 * document — the registry's contents as a knowledge graph. Pointed at a {@code GitSink} on
 * the registry's own repository, the documentation lives beside the schemas it describes.
 */
public final class OkfRegistryBundles {

    private OkfRegistryBundles() {
    }

    public static Bundle render(SchemaRegistryStore store, OkfRenderer.Options options) {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(options, "options");
        ProtoSourceCompiler compiler = new ProtoSourceCompiler();
        Map<String, String> subjectDocs = new LinkedHashMap<>();
        List<FileDescriptor> allFiles = new ArrayList<>();
        for (String subject : store.subjects()) {
            StoredSchema latest = store.latest(subject).orElse(null);
            if (latest == null) {
                continue;
            }
            StoredSchemaSources.Resolved resolved = StoredSchemaSources.resolve(store, latest);
            CompiledProtos compiled;
            try {
                compiled = compiler.compile(resolved.sources());
            } catch (Exception e) {
                // Registered schemas are compile-verified; failure is a store inconsistency.
                throw new IllegalStateException("Stored subject '" + subject
                        + "' no longer compiles: " + e.getMessage(), e);
            }
            List<FileDescriptor> files = linkWithMetadata(compiled.descriptorSet());
            allFiles.addAll(files);
            FileDescriptor root = files.stream()
                    .filter(file -> file.getName().equals(resolved.rootPath()))
                    .findFirst().orElse(null);
            subjectDocs.put(docPath(subjectDocs, subject),
                    subjectConcept(subject, store, latest, root, options));
        }
        return new OkfRenderer().render(OkfRenderer.Model.of(allFiles), options, subjectDocs);
    }

    private static String subjectConcept(String subject, SchemaRegistryStore store,
                                         StoredSchema latest, FileDescriptor root,
                                         OkfRenderer.Options options) {
        List<Integer> versions = store.versions(subject);
        List<Descriptor> declared = root == null ? List.of() : root.getMessageTypes();

        StringBuilder doc = new StringBuilder();
        String resource = options.registryUrl() == null || options.registryUrl().isBlank()
                ? null
                : options.registryUrl().replaceAll("/+$", "") + "/subjects/"
                        + URLEncoder.encode(subject, StandardCharsets.UTF_8)
                        + "/versions/latest";
        OkfRenderer.frontmatter(doc, "Registry Subject", subject,
                versions.size() + " version(s); the latest declares "
                        + declared.size() + " message type(s).",
                Map.of(), "", resource);

        doc.append("Latest version ").append(latest.version())
                .append(" (global id ").append(latest.globalId()).append(").\n");

        if (!declared.isEmpty()) {
            doc.append("\n# Types\n\n");
            for (Descriptor message : declared) {
                doc.append("* [").append(message.getFullName()).append("](/")
                        .append(OkfRenderer.conceptPath(message.getFullName(), "messages"))
                        .append(") - ").append(OkfRenderer.firstSentence(
                                DescriptorMetadata.message(message)
                                        .map(meta -> meta.getDescription())
                                        .filter(s -> !s.isBlank())
                                        .orElse("Protobuf message type.")))
                        .append('\n');
            }
        }

        doc.append("\n# Versions\n\n| Version | Global id | Content hash |\n|---|---|---|\n");
        for (int i = versions.size() - 1; i >= 0; i--) {
            StoredSchema version = store.version(subject, versions.get(i)).orElse(null);
            if (version == null) {
                continue;
            }
            String hash = version.contentHash();
            doc.append("| ").append(version.version()).append(" | ")
                    .append(version.globalId()).append(" | `")
                    .append(hash.length() > 12 ? hash.substring(0, 12) : hash)
                    .append("` |\n");
        }
        return doc.toString();
    }

    /** Re-links the compiled set with the metadata extensions readable, dependencies first. */
    static List<FileDescriptor> linkWithMetadata(FileDescriptorSet set) {
        try {
            ExtensionRegistry extensions = ExtensionRegistry.newInstance();
            DescriptorMetadata.registerExtensions(extensions);
            FileDescriptorSet reparsed = FileDescriptorSet.parseFrom(
                    set.toByteArray(), extensions);
            Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
            reparsed.getFileList().forEach(proto -> byName.put(proto.getName(), proto));
            Map<String, FileDescriptor> built = new LinkedHashMap<>();
            for (FileDescriptorProto proto : reparsed.getFileList()) {
                build(proto, byName, built);
            }
            return List.copyOf(built.values());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to link a compiled schema: "
                    + e.getMessage(), e);
        }
    }

    private static FileDescriptor build(FileDescriptorProto proto,
                                        Map<String, FileDescriptorProto> byName,
                                        Map<String, FileDescriptor> built) throws Exception {
        FileDescriptor existing = built.get(proto.getName());
        if (existing != null) {
            return existing;
        }
        FileDescriptor[] dependencies = new FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < proto.getDependencyCount(); i++) {
            String name = proto.getDependency(i);
            FileDescriptorProto dependency = byName.get(name);
            if (dependency == null) {
                throw new IllegalStateException("'" + proto.getName()
                        + "' depends on '" + name + "', which the compiled set does not contain");
            }
            dependencies[i] = build(dependency, byName, built);
        }
        FileDescriptor file = FileDescriptor.buildFrom(proto, dependencies);
        built.put(proto.getName(), file);
        return file;
    }

    /**
     * The bundle path for {@code subject}, suffixed if an earlier subject already claimed it.
     * Sanitisation is many-to-one — {@code orders/value} and {@code orders:value} both reduce
     * to {@code orders_value} — and without the suffix the second subject's document would
     * replace the first one's, dropping it from the bundle.
     */
    private static String docPath(Map<String, String> taken, String subject) {
        String base = "subjects/" + sanitize(subject);
        String path = base + ".md";
        for (int i = 2; taken.containsKey(path); i++) {
            path = base + "-" + i + ".md";
        }
        return path;
    }

    private static String sanitize(String subject) {
        StringBuilder out = new StringBuilder(subject.length());
        for (char c : subject.toCharArray()) {
            out.append(Character.isLetterOrDigit(c) || c == '.' || c == '-' || c == '_'
                    ? c : '_');
        }
        return out.toString();
    }
}
