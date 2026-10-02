package ai.protomolt.proto.registry;

import java.util.Map;

/** Explicitly selected ephemeral storage. No provider options are accepted. */
public final class InMemorySchemaRegistryStoreProvider implements SchemaRegistryStoreProvider {
    @Override
    public String id() { return "memory"; }

    @Override
    public SchemaRegistryStore open(Map<String, String> options, SchemaRegistryStore.WriteGate writeGate) {
        if (!options.isEmpty()) {
            throw new IllegalArgumentException("Memory registry storage accepts no options: " + options.keySet());
        }
        return new InMemorySchemaRegistryStore(writeGate);
    }
}
