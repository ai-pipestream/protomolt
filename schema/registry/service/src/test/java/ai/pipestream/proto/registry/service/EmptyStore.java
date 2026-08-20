package ai.pipestream.proto.registry.service;

import ai.pipestream.proto.registry.SchemaReference;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import ai.pipestream.proto.registry.StoredSchema;

import java.util.List;
import java.util.Optional;

/** An empty but functional store. */
final class EmptyStore implements SchemaRegistryStore {
    @Override
    public List<String> subjects() {
        return List.of();
    }

    @Override
    public List<Integer> versions(String subject) {
        return List.of();
    }

    @Override
    public Optional<StoredSchema> version(String subject, int version) {
        return Optional.empty();
    }

    @Override
    public Optional<StoredSchema> latest(String subject) {
        return Optional.empty();
    }

    @Override
    public Optional<StoredSchema> byGlobalId(int globalId) {
        return Optional.empty();
    }

    @Override
    public Optional<StoredSchema> findByContent(String subject, String schemaText,
                                                List<SchemaReference> references) {
        return Optional.empty();
    }

    @Override
    public StoredSchema register(String subject, String schemaText,
                                 List<SchemaReference> references) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<String> compatibilityMode(String subject) {
        return Optional.empty();
    }

    @Override
    public void setCompatibilityMode(String subject, String mode) {
    }

    @Override
    public String globalCompatibilityMode() {
        return "BACKWARD";
    }

    @Override
    public void setGlobalCompatibilityMode(String mode) {
    }

    @Override
    public void close() {
    }
}
