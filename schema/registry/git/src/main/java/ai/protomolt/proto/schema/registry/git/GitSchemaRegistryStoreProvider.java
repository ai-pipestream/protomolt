package ai.protomolt.proto.schema.registry.git;

import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaRegistryStoreProvider;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/** Git storage with explicit directory and commit-author configuration. */
public final class GitSchemaRegistryStoreProvider implements SchemaRegistryStoreProvider {
    private static final Set<String> OPTIONS = Set.of("directory", "author-name", "author-email");

    @Override
    public String id() { return "git"; }

    @Override
    public SchemaRegistryStore open(Map<String, String> options, SchemaRegistryStore.WriteGate writeGate) {
        if (!OPTIONS.equals(options.keySet())) {
            throw new IllegalArgumentException("Git registry storage requires only these options: " + OPTIONS);
        }
        for (String name : OPTIONS) {
            if (options.get(name) == null || options.get(name).isBlank()) {
                throw new IllegalArgumentException("Git registry storage option must not be blank: " + name);
            }
        }
        Path directory = Path.of(options.get("directory"));
        return GitSchemaRegistryStore.builder().repositoryDir(directory)
                .author(options.get("author-name"), options.get("author-email"))
                .writeGate(writeGate).build();
    }
}
