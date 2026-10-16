package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.registry.CompatibilityWriteGate;
import ai.protomolt.proto.registry.InMemorySchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaReference;
import ai.protomolt.proto.registry.StoredSchema;
import ai.protomolt.proto.repo.admission.DocumentCandidateAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemas;
import ai.protomolt.proto.repo.schema.registry.RegistryDocumentSchemas;
import ai.protomolt.proto.repo.v1.Document;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A real schema registry store with the compatibility write gate, holding the
 * repository Document and the files it imports as registered subjects. The
 * registry can be stopped, and every lookup is counted, so a test can show a
 * read made no live registry call.
 */
final class DocumentRegistry {
    final InMemorySchemaRegistryStore store = new InMemorySchemaRegistryStore(new CompatibilityWriteGate());
    final AtomicInteger lookups = new AtomicInteger();
    volatile boolean stopped;
    private final RegistryDocumentSchemas latest = new RegistryDocumentSchemas(store);

    DocumentRegistry() {
        RegistryDocumentSchemas.configureSubject(store);
        var registered = new LinkedHashSet<String>();
        for (var dependency : Document.getDescriptor().getFile().getDependencies()) registerImports(dependency, registered);
    }

    /** The registry seam a save asks; stopped, it fails as an unreachable registry does. */
    DocumentSchemas schemas() {
        return () -> {
            lookups.incrementAndGet();
            if (stopped) throw new DocumentCandidateAdmission.SchemaUnavailable("registry stopped", null);
            return latest.latest();
        };
    }

    /** An Any resolver that counts lookups and fails while the registry is stopped. */
    DocumentSchemaAdmission.Resolver resolver(DocumentSchemaAdmission.Resolver registered) {
        return selection -> {
            lookups.incrementAndGet();
            if (stopped) throw new DocumentCandidateAdmission.SchemaUnavailable("registry stopped", null);
            return registered.select(selection);
        };
    }

    StoredSchema registerDocument(String source) {
        return store.register(DocumentSchemas.SUBJECT, source, references(Document.getDescriptor().getFile()));
    }

    /** The compiled document.proto source, as the contract module ships it. */
    static String documentSource() { return source(Document.getDescriptor().getFile().getName()); }

    /** document.proto with one more field in message Document. */
    static String withField(String source, String field) {
        var marker = "message Document {";
        int at = source.indexOf(marker);
        if (at < 0) throw new IllegalStateException("document.proto has no message Document");
        return source.substring(0, at + marker.length()) + "\n  " + field + "\n" + source.substring(at + marker.length());
    }

    private void registerImports(FileDescriptor file, Set<String> registered) {
        if (file.getName().startsWith("google/protobuf/") || registered.contains(file.getName())) return;
        for (var dependency : file.getDependencies()) registerImports(dependency, registered);
        store.register(file.getName(), source(file.getName()), references(file));
        registered.add(file.getName());
    }

    private static List<SchemaReference> references(FileDescriptor file) {
        var references = new ArrayList<SchemaReference>();
        for (var dependency : file.getDependencies())
            if (!dependency.getName().startsWith("google/protobuf/"))
                references.add(new SchemaReference(dependency.getName(), dependency.getName(), 1));
        return references;
    }

    private static String source(String path) {
        try (var input = Document.class.getClassLoader().getResourceAsStream(path)) {
            if (input == null) throw new IllegalStateException("No source for " + path + " on the classpath");
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }
}
