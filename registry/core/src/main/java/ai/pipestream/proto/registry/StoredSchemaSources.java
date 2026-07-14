package ai.pipestream.proto.registry;

import ai.pipestream.proto.sources.ProtoSourceSet;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolves a schema plus its transitive references out of a {@link SchemaRegistryStore} into a
 * self-contained {@link ProtoSourceSet} ready for {@code ProtoSourceCompiler} — used both for
 * register-time verification and for serving compiled descriptor sets.
 *
 * <p>Reference texts are keyed by their reference {@code name}, which by Schema Registry
 * convention is the import path the referencing schema uses. The subject's own schema has no
 * registry-assigned file name, so one is synthesized from the subject (sanitized,
 * {@code .proto}-suffixed, dodging collisions with reference names) — the same convention the
 * Confluent loader uses.</p>
 */
public final class StoredSchemaSources {

    /**
     * A resolved schema: the source set holding the schema and every transitive reference,
     * plus the synthesized import path of the schema itself.
     */
    public record Resolved(String rootPath, ProtoSourceSet sources) {
    }

    private StoredSchemaSources() {
    }

    /** Resolves an already-stored schema. */
    public static Resolved resolve(SchemaRegistryStore store, StoredSchema schema)
            throws ReferenceNotFoundException {
        return resolve(store, schema.subject(), schema.schemaText(), schema.references());
    }

    /**
     * Resolves a candidate schema against the store.
     *
     * @throws ReferenceNotFoundException when a (transitive) reference is missing
     */
    public static Resolved resolve(SchemaRegistryStore store, String subject, String schemaText,
                                   List<SchemaReference> references)
            throws ReferenceNotFoundException {
        Map<String, String> files = new LinkedHashMap<>();
        collect(store, references, files, new HashSet<>());
        String rootPath = rootFileName(subject, files);
        ProtoSourceSet.Builder sources = ProtoSourceSet.builder();
        files.forEach((path, text) -> sources.add(path, text, "registry:" + subject + " reference"));
        sources.add(rootPath, schemaText, "registry:" + subject);
        return new Resolved(rootPath, sources.build());
    }

    private static void collect(SchemaRegistryStore store, List<SchemaReference> references,
                                Map<String, String> files, Set<String> visited)
            throws ReferenceNotFoundException {
        for (SchemaReference reference : references) {
            if (files.containsKey(reference.name())
                    || !visited.add(reference.subject() + ":" + reference.version())) {
                continue; // already resolved under this import path / already visited
            }
            StoredSchema stored = store.version(reference.subject(), reference.version())
                    .orElseThrow(() -> new ReferenceNotFoundException(reference));
            collect(store, stored.references(), files, visited);
            files.put(reference.name(), stored.schemaText());
        }
    }

    private static String rootFileName(String subject, Map<String, String> files) {
        String name = subject.replaceAll("[^A-Za-z0-9._-]", "_");
        if (!name.endsWith(".proto")) {
            name += ".proto";
        }
        while (files.containsKey(name)) {
            name = "_" + name;
        }
        return name;
    }
}
