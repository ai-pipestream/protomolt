package ai.pipestream.proto.registry;

/** Runs the store contract against {@link InMemorySchemaRegistryStore}. */
class InMemorySchemaRegistryStoreTest extends SchemaRegistryStoreContractTest {

    @Override
    protected SchemaRegistryStore create(SchemaRegistryStore.WriteGate gate) {
        return new InMemorySchemaRegistryStore(gate);
    }
}
